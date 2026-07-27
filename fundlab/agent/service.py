"""Turn configured deterministic policies into validated decision files.

For account head ``H``, the default decision date is the first published
trading session after ``H``.  A configured account without a ledger head uses
the published data head as its effective head so the unattended daily loop can
bootstrap it.  Features remain pinned to ``min(decision_date, published_end)``.

An existing valid decision is an idempotent success.  Replacing its content
requires ``overwrite=True``; an existing invalid file always fails closed.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

from fundlab.agent.policy import build_policy
from fundlab.marketdata.portal import CanonicalMarketData
from fundlab.settings import FoundationSettings
from fundlab.strategies import AgentDecisionError, load_agent_decision, write_agent_decision
from fundlab.trading import TradingRepository


class AgentServiceError(ValueError):
    """A decision cannot be produced for a reason outside the policy."""


@dataclass(frozen=True)
class AgentDecisionService:
    settings: FoundationSettings

    def decide(
        self,
        account_id: str,
        *,
        target_date: date | None = None,
        overwrite: bool = False,
        dry_run: bool = False,
    ) -> dict[str, Any]:
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
        policy = build_policy(policy_settings.kind, policy_settings.params)

        market = CanonicalMarketData.open(self.settings.paths.market_data)
        scope = market.manifest.plan.universe_scope
        if scope is None:
            raise AgentServiceError("Published snapshot carries no universe scope")
        published_end = scope.history_end

        head_date = self._head_date(account_id)
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

        agent_id = f"{policy.policy_id}-v{policy.version}"
        result: dict[str, Any] = {
            "account_id": account_id,
            "decision_date": decision_date.isoformat(),
            "snapshot_id": market.snapshot_id,
            "policy": {
                "kind": policy_settings.kind,
                "agent_id": agent_id,
                "config_hash": policy.config_hash,
            },
            "dry_run": dry_run,
            "written": False,
        }

        existing_path = (
            Path(self.settings.daily.agent_decision_root)
            / account_id
            / f"{decision_date.isoformat()}.json"
        )
        if existing_path.exists():
            try:
                existing = load_agent_decision(
                    self.settings.daily.agent_decision_root,
                    account_id,
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
        if decision.hold:
            result["held"] = True
            result["reason"] = decision.reason
            return result

        reason = (
            f"{decision.reason} | as_of {as_of.isoformat()} · "
            f"snapshot {market.snapshot_id} · config {policy.config_hash[:16]}"
        )
        result["target_weights"] = {
            instrument_id: str(weight)
            for instrument_id, weight in decision.target_weights.items()
        }
        result["reason"] = reason
        if dry_run:
            return result

        written = write_agent_decision(
            self.settings.daily.agent_decision_root,
            account_id=account_id,
            decision_date=decision_date,
            target_weights=decision.target_weights,
            reason=reason,
            agent_id=agent_id,
            overwrite=overwrite,
        )
        result["written"] = True
        result["file"] = written.source_path
        return result

    def decide_all(
        self, *, overwrite: bool = False, dry_run: bool = False,
    ) -> list[dict[str, Any]]:
        """Attempt every configured agent account without cross-account failure."""

        outcomes: list[dict[str, Any]] = []
        for account in self.settings.daily.accounts:
            if account.strategy != "agent-file":
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

    def _head_date(self, account_id: str) -> date | None:
        database = Path(self.settings.paths.trading_database)
        if not database.is_file():
            return None
        repository = TradingRepository(database)
        try:
            repository.account(account_id)
        except KeyError:
            return None
        _, head_run_id = repository.selected_state(account_id)
        if head_run_id is None:
            return None
        return repository.run(head_run_id).binding.end_date
