"""Turn configured policies into reviews, notifications, and decision files.

The JSON decision file remains the only path into the trading kernel. The
dividend-value policy may additionally append an auditable weekly review and
send best-effort opportunity/risk email; neither side channel can bypass the
validated decision writer.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Callable
from uuid import uuid4

from fundlab.agent.llm import (
    DividendValueAdviser,
    ResponsesDividendValueAdviser,
)
from fundlab.agent.policy import DividendPolicyRuntime, PolicyDecision, build_policy
from fundlab.agent.tools import AgentMemory, AgentRunLock, EmailNotifier, ReadingLibrary
from fundlab.common.canonical import stable_digest
from fundlab.marketdata.portal import CanonicalMarketData
from fundlab.settings import AgentLLMSettings, FoundationSettings
from fundlab.strategies import AgentDecisionError, load_agent_decision, write_agent_decision
from fundlab.trading import PortfolioState, TradingRepository


class AgentServiceError(ValueError):
    """A decision or review cannot be produced outside the policy itself."""


def _adviser(settings: AgentLLMSettings) -> DividendValueAdviser:
    return ResponsesDividendValueAdviser(settings)


@dataclass(frozen=True)
class AgentDecisionService:
    settings: FoundationSettings
    adviser_factory: Callable[[AgentLLMSettings], DividendValueAdviser] = field(
        default=_adviser,
        repr=False,
        compare=False,
    )
    notifier_factory: Callable[[str | None], EmailNotifier] = field(
        default=EmailNotifier,
        repr=False,
        compare=False,
    )

    def decide(
        self,
        account_id: str,
        *,
        target_date: date | None = None,
        overwrite: bool = False,
        dry_run: bool = False,
        force_review: bool = False,
    ) -> dict[str, Any]:
        account, policy_settings = self._configured_account(account_id)
        if policy_settings.kind == "dividend-value":
            with AgentRunLock(self.settings.agent.memory.root, account_id):
                return self._decide_locked(
                    account,
                    policy_settings,
                    target_date=target_date,
                    overwrite=overwrite,
                    dry_run=dry_run,
                    force_review=force_review,
                )
        if force_review:
            raise AgentServiceError("--force-review is only valid for review-cadence policies")
        return self._decide_locked(
            account,
            policy_settings,
            target_date=target_date,
            overwrite=overwrite,
            dry_run=dry_run,
            force_review=False,
        )

    def _decide_locked(
        self,
        account,
        policy_settings,
        *,
        target_date: date | None,
        overwrite: bool,
        dry_run: bool,
        force_review: bool,
    ) -> dict[str, Any]:
        market = CanonicalMarketData.open(self.settings.paths.market_data)
        scope = market.manifest.plan.universe_scope
        if scope is None:
            raise AgentServiceError("Published snapshot carries no universe scope")
        published_end = scope.history_end

        state, head_date = self._state_and_head(account.account_id, account.initial_cash)
        effective_head = head_date if head_date is not None else published_end
        decision_date = target_date
        if decision_date is None:
            decision_date = market.next_trading_day(effective_head)
            if decision_date is None:
                raise AgentServiceError(
                    f"Snapshot calendar has no session after {effective_head.isoformat()}"
                )
        else:
            if decision_date <= effective_head:
                raise AgentServiceError(
                    f"Account already advanced through {effective_head.isoformat()}; "
                    f"cannot decide for {decision_date.isoformat()}"
                )
            if not market.trading_days(decision_date, decision_date):
                raise AgentServiceError(
                    f"{decision_date.isoformat()} is not an open session in the snapshot "
                    "calendar; the file would never be consumed"
                )

        memory = AgentMemory(self.settings.agent.memory.root, account.account_id)
        runtime = None
        if policy_settings.kind == "dividend-value":
            runtime = DividendPolicyRuntime(
                account_id=account.account_id,
                adviser=self.adviser_factory(self.settings.agent.llm),
                library=ReadingLibrary(self.settings.agent.library),
                memory=memory,
                recent_memory_entries=self.settings.agent.memory.recent_entries,
                max_memory_entry_chars=self.settings.agent.memory.max_entry_chars,
                max_memory_total_chars=self.settings.agent.memory.max_total_chars,
                state=state,
                last_decision_date=self._last_decision_date(
                    account.account_id, before=decision_date,
                ),
                force_review=force_review,
            )
        policy = build_policy(policy_settings.kind, policy_settings.params, runtime=runtime)
        agent_id = f"{policy.policy_id}-v{policy.version}"
        result: dict[str, Any] = {
            "account_id": account.account_id,
            "decision_date": decision_date.isoformat(),
            "snapshot_id": market.snapshot_id,
            "policy": {
                "kind": policy_settings.kind,
                "agent_id": agent_id,
                "config_hash": policy.config_hash,
                "scheduled": policy_settings.scheduled,
            },
            "dry_run": dry_run,
            "written": False,
        }

        existing_path = (
            Path(self.settings.daily.agent_decision_root)
            / account.account_id
            / f"{decision_date.isoformat()}.json"
        )
        existing = None
        if existing_path.exists():
            try:
                existing = load_agent_decision(
                    self.settings.daily.agent_decision_root,
                    account.account_id,
                    decision_date,
                )
            except AgentDecisionError as exc:
                raise AgentServiceError(
                    f"Existing decision file for {decision_date.isoformat()} is invalid "
                    f"({exc}); inspect and repair or remove it before retrying"
                ) from exc
            assert existing is not None
            if not overwrite:
                result.update({
                    "skipped": "already_present",
                    "existing_agent_id": existing.agent_id,
                    "existing_content_hash": existing.content_hash,
                    "file": existing.source_path,
                })
                return result

        as_of = min(decision_date, published_end)
        decision = policy.decide(market, as_of)
        result["as_of"] = as_of.isoformat()
        result["reason"] = decision.reason
        self._attach_review_result(result, decision)
        review_completed = bool(decision.audit.get("review_completed"))
        if dry_run:
            if decision.hold and existing is not None:
                if review_completed:
                    result["would_supersede_existing_decision"] = True
                else:
                    result["existing_decision_retained"] = True
            if not decision.hold:
                result["target_weights"] = {
                    instrument_id: str(weight)
                    for instrument_id, weight in decision.target_weights.items()
                }
            return result

        if decision.hold and existing is not None:
            if not review_completed:
                result.update({
                    "existing_decision_retained": True,
                    "existing_content_hash": existing.content_hash,
                    "skipped": str(decision.audit.get("skipped") or "policy_hold"),
                })
                return result
            superseded = self._supersede_existing_decision(
                account_id=account.account_id,
                decision_date=decision_date,
                expected_content_hash=existing.content_hash,
            )
            result.update({
                "superseded_file": superseded,
                "superseded_content_hash": existing.content_hash,
            })
        if review_completed:
            memory.append(self._review_memory_entry(
                account_id=account.account_id,
                decision_date=decision_date,
                as_of=as_of,
                snapshot_id=market.snapshot_id,
                agent_id=agent_id,
                config_hash=policy.config_hash,
                decision=decision,
            ))

        if decision.hold:
            result["held"] = True
        else:
            reason = (
                f"{decision.reason} | as_of {as_of.isoformat()} · "
                f"snapshot {market.snapshot_id} · config {policy.config_hash[:16]} · "
                f"context {str(decision.audit.get('context_hash', 'none'))[:16]}"
            )
            result["reason"] = reason
            result["target_weights"] = {
                instrument_id: str(weight)
                for instrument_id, weight in decision.target_weights.items()
            }
            written = write_agent_decision(
                self.settings.daily.agent_decision_root,
                account_id=account.account_id,
                decision_date=decision_date,
                target_weights=decision.target_weights,
                reason=reason,
                agent_id=agent_id,
                overwrite=overwrite,
            )
            result["written"] = True
            result["file"] = written.source_path
            if review_completed:
                memory.append({
                    "event": "decision_written",
                    "account_id": account.account_id,
                    "decision_date": decision_date.isoformat(),
                    "content_hash": written.content_hash,
                    "target_weights": result["target_weights"],
                    "agent_id": agent_id,
                })

        if review_completed and decision.highlights:
            self._notify(result, memory, account.account_id, decision_date, as_of, decision)
        return result

    def decide_all(
        self, *, overwrite: bool = False, dry_run: bool = False,
    ) -> list[dict[str, Any]]:
        """Attempt scheduled policies without cross-account failure propagation."""
        outcomes: list[dict[str, Any]] = []
        for account in self.settings.daily.accounts:
            if account.strategy != "agent-file":
                continue
            policy_settings = self.settings.agent.policies.get(account.account_id)
            if policy_settings is not None and not policy_settings.scheduled:
                outcomes.append({
                    "account_id": account.account_id,
                    "written": False,
                    "skipped": "scheduled_disabled",
                })
                continue
            try:
                outcomes.append(self.decide(
                    account.account_id,
                    overwrite=overwrite,
                    dry_run=dry_run,
                ))
            except Exception as exc:  # noqa: BLE001 - failures are isolated per account
                outcomes.append({
                    "account_id": account.account_id,
                    "written": False,
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                })
        return outcomes

    def _supersede_existing_decision(
        self,
        *,
        account_id: str,
        decision_date: date,
        expected_content_hash: str,
    ) -> str:
        """Atomically move a validated future decision out of the executable path."""
        try:
            current = load_agent_decision(
                self.settings.daily.agent_decision_root,
                account_id,
                decision_date,
            )
        except AgentDecisionError as exc:
            raise AgentServiceError(
                "Existing decision changed or became invalid before it could be superseded"
            ) from exc
        if current is None or current.content_hash != expected_content_hash:
            raise AgentServiceError(
                "Existing decision changed before it could be superseded; refusing to move it"
            )
        source = Path(current.source_path)
        archive_root = (
            Path(self.settings.daily.agent_decision_root)
            / ".superseded"
            / account_id
        )
        archive_root.mkdir(parents=True, exist_ok=True)
        archive = archive_root / (
            f"{decision_date.isoformat()}-{expected_content_hash[:16]}-{uuid4().hex}.json"
        )
        try:
            source.replace(archive)
        except OSError as exc:
            raise AgentServiceError(
                f"Could not supersede existing decision for {decision_date.isoformat()}"
            ) from exc
        try:
            replacement = load_agent_decision(
                self.settings.daily.agent_decision_root,
                account_id,
                decision_date,
            )
        except AgentDecisionError as exc:
            raise AgentServiceError(
                "A concurrent invalid decision appeared after superseding the old decision"
            ) from exc
        if replacement is not None:
            raise AgentServiceError(
                "A concurrent decision appeared after superseding the old decision"
            )
        return str(archive)

    def _configured_account(self, account_id: str):
        account = next(
            (item for item in self.settings.daily.accounts if item.account_id == account_id),
            None,
        )
        if account is None or account.strategy != "agent-file":
            raise AgentServiceError(f"Not a configured agent-file account: {account_id}")
        policy_settings = self.settings.agent.policies.get(account_id)
        if policy_settings is None:
            raise AgentServiceError(
                f"config/fundlab.yaml has no agent policy for account: {account_id}"
            )
        return account, policy_settings

    def _state_and_head(
        self, account_id: str, initial_cash,
    ) -> tuple[PortfolioState, date | None]:
        database = Path(self.settings.paths.trading_database)
        if not database.is_file():
            return PortfolioState.with_cash(initial_cash), None
        repository = TradingRepository(database)
        try:
            state, head_run_id = repository.selected_state(account_id)
        except KeyError:
            return PortfolioState.with_cash(initial_cash), None
        if head_run_id is None:
            return state, None
        return state, repository.run(head_run_id).binding.end_date

    def _last_decision_date(self, account_id: str, *, before: date) -> date | None:
        root = Path(self.settings.daily.agent_decision_root) / account_id
        if not root.is_dir():
            return None
        dated: list[date] = []
        for path in root.glob("*.json"):
            try:
                parsed = date.fromisoformat(path.stem)
            except ValueError:
                continue
            if parsed < before:
                dated.append(parsed)
        if not dated:
            return None
        latest = max(dated)
        try:
            decision = load_agent_decision(
                self.settings.daily.agent_decision_root,
                account_id,
                latest,
            )
        except AgentDecisionError as exc:
            raise AgentServiceError(
                f"Latest prior decision {latest.isoformat()} is invalid; cooldown cannot be proven"
            ) from exc
        assert decision is not None
        return latest

    @staticmethod
    def _attach_review_result(result: dict[str, Any], decision: PolicyDecision) -> None:
        if not decision.audit:
            return
        result["review"] = dict(decision.audit)
        if decision.highlights:
            result["highlights"] = [
                {
                    "kind": item.kind,
                    "instrument_id": item.instrument_id,
                    "headline": item.headline,
                    "detail": item.detail,
                    "evidence_hash": item.evidence_hash,
                }
                for item in decision.highlights
            ]
        skipped = decision.audit.get("skipped")
        if skipped:
            result["skipped"] = str(skipped)

    @staticmethod
    def _review_memory_entry(
        *,
        account_id: str,
        decision_date: date,
        as_of: date,
        snapshot_id: str,
        agent_id: str,
        config_hash: str,
        decision: PolicyDecision,
    ) -> dict[str, Any]:
        audit = dict(decision.audit)
        return {
            "event": "review",
            "account_id": account_id,
            "decision_date": decision_date.isoformat(),
            "as_of": as_of.isoformat(),
            "snapshot_id": snapshot_id,
            "agent_id": agent_id,
            "policy_config_hash": config_hash,
            "review_period": audit.get("review_period"),
            "context_hash": audit.get("context_hash"),
            "response_id": audit.get("response_id"),
            "model": audit.get("model"),
            "usage": audit.get("usage", {}),
            "adviser_action": audit.get("adviser_action"),
            "outcome": "hold" if decision.hold else "rebalance",
            "summary": audit.get("summary"),
            "selected_instruments": audit.get("selected_instruments", []),
            "selection_rationale": audit.get("selection_rationale", {}),
            "hard_rule_violations": audit.get("portfolio_hard_rule_violations", []),
            "highlight_evidence_hashes": [
                item.evidence_hash for item in decision.highlights
            ],
        }

    def _notify(
        self,
        result: dict[str, Any],
        memory: AgentMemory,
        account_id: str,
        decision_date: date,
        as_of: date,
        decision: PolicyDecision,
    ) -> None:
        sent = memory.sent_evidence_hashes()
        fresh = [item for item in decision.highlights if item.evidence_hash not in sent]
        if not fresh:
            result["email"] = {"sent": False, "detail": "unchanged evidence already sent"}
            return
        opportunities = sum(item.kind == "opportunity" for item in fresh)
        risks = sum(item.kind == "risk" for item in fresh)
        body_lines = [
            f"账户：{account_id}",
            f"评估数据截至：{as_of.isoformat()}",
            f"目标决策日：{decision_date.isoformat()}",
            f"本次结果：{'仅评估/持有' if decision.hold else '已生成组合决策'}",
            "",
        ]
        for item in fresh:
            label = "机会" if item.kind == "opportunity" else "风险"
            body_lines.extend((
                f"[{label}] {item.headline}",
                f"标的：{item.name} ({item.instrument_id})",
                item.detail,
                "",
            ))
        body_lines.extend((
            "模型结论：",
            str(decision.audit.get("summary", decision.reason)),
            "",
            "本邮件由本地 FundLab 模拟 Agent 发送，仅作研究记录，不构成投资建议。",
        ))
        evidence_hashes = [item.evidence_hash for item in fresh]
        message_id = stable_digest({
            "account_id": account_id,
            "evidence_hashes": evidence_hashes,
        })[:32]
        outcome = self.notifier_factory(self.settings.agent.notify.email_to).send(
            f"[FundLab] {account_id} 红利价值周报：机会 {opportunities} / 风险 {risks}",
            "\n".join(body_lines),
            message_id=message_id,
        )
        result["email"] = {"sent": outcome.sent, "detail": outcome.detail}
        memory.append({
            "event": "email",
            "account_id": account_id,
            "decision_date": decision_date.isoformat(),
            "sent": outcome.sent,
            "detail": outcome.detail,
            "message_id": message_id,
            "evidence_hashes": evidence_hashes,
        })
