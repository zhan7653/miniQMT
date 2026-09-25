"""Orchestration for auditable, read-only autonomous research."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
import re
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

from fundlab.marketdata.portal import CanonicalMarketData
from fundlab.agent.research_agent import ResearchAgent
from fundlab.agent.research_tools import build_research_tools
from fundlab.agent.domains import ResearchDomain, get_domain
from fundlab.agent.report import parse_research_report, research_report_schema
from fundlab.agent.tools import AgentMemory, ReadingLibrary
from fundlab.common.canonical import to_primitive
from fundlab.trading.repository import TradingRepository
from fundlab.trading.reporting import build_simulation_feedback
from fundlab.strategies import write_agent_decision
from fundlab.agent.opinion import OpinionService


class ResearchService:
    def __init__(self, settings):
        self.settings = settings

    def research(
        self,
        question,
        context=None,
        account_id=None,
        as_of=None,
        max_turns=None,
        model=None,
        dry_run=False,
        domain=None,
        profile=None,
        publish_paper=False,
        request_key=None,
    ):
        profile_config = self.settings.agent.research_profiles.get(profile, {}) if profile else {}
        if profile is not None and not profile_config:
            raise ValueError(f"Unknown research profile: {profile}")
        if publish_paper and profile_config.get('execution', 'shadow') != 'paper_auto':
            raise ValueError('Research profile is in shadow mode; paper publication requires a validated paper_auto rollout')
        domain = str(profile_config.get("domain", domain or "general"))
        model = model or profile_config.get("model") or "gpt-6-astra"
        max_turns = int(max_turns if max_turns is not None else profile_config.get("max_turns", 12))
        market = CanonicalMarketData.open(self.settings.paths.market_data)
        scope = market.manifest.plan.universe_scope
        if scope is None:
            raise ValueError("Published snapshot has no universe scope")
        if as_of is None:
            as_of = scope.history_end
        if isinstance(as_of, str):
            as_of = date.fromisoformat(as_of)
        if as_of > scope.history_end:
            raise ValueError("Research as_of exceeds the published data head")
        if as_of < scope.history_start or not market.trading_days(as_of, as_of):
            raise ValueError('Research as_of must be an available trading session')
        portfolio = {}
        if account_id:
            if not any(item.account_id == account_id for item in self.settings.daily.accounts):
                raise ValueError('Unknown configured research account')
            repository = TradingRepository(self.settings.paths.trading_database, read_only=True)
            state, head = repository.selected_state(account_id)
            if head and repository.run(head).binding.end_date > as_of:
                raise ValueError('Current portfolio is newer than research as_of; historical research needs a pinned historical state')
            portfolio = {"account_id": account_id, "head_run_id": head, "state": to_primitive(state)}
        memory_store = AgentMemory(self.settings.agent.memory.root, account_id or profile or 'research')
        memory = memory_store.recent(20)
        library = ReadingLibrary(self.settings.agent.library)
        extra_tools = {}
        def strategy_attribution(query_account_id: str):
            if query_account_id != account_id:
                raise ValueError("strategy_attribution is restricted to the selected account")
            repository = TradingRepository(self.settings.paths.trading_database, read_only=True)
            _, run_id = repository.selected_state(query_account_id)
            if run_id is None:
                return {"account_id": query_account_id, "status": "no_completed_run"}
            return {"account_id": query_account_id, "run_id": run_id, "feedback": to_primitive(build_simulation_feedback(repository, run_id))}
        if account_id:
            extra_tools["strategy_attribution"] = {
                "description": "Read the selected paper account's latest completed simulation feedback: returns, drawdown, fills, fees, slippage and quality.",
                "parameters": {"type": "object", "properties": {"query_account_id": {"type": "string"}}, "required": ["query_account_id"], "additionalProperties": False},
                "callable": strategy_attribution, "write": False,
            }
        tools = build_research_tools(
            market, as_of, portfolio=portfolio, memory=memory, library=library,
            extra_tools=extra_tools,
            opinion_service=OpinionService(self.settings, market=market),
        )
        try:
            domain_spec = get_domain(domain)
        except ValueError:
            mission = str(profile_config.get("mission", "Answer the research question with evidence and a clear no-action option.")).strip()
            preferred = profile_config.get("preferred_tools", ())
            if not isinstance(preferred, (list, tuple)):
                raise ValueError("research profile preferred_tools must be a list")
            domain_spec = ResearchDomain(domain, mission, tuple(map(str, preferred)))
        configured_tools = profile_config.get("tools")
        if configured_tools is not None:
            if not isinstance(configured_tools, (list, tuple)):
                raise ValueError("research profile tools must be a list")
            allowed = set(map(str, configured_tools))
            unknown = allowed - set(tools)
            if unknown:
                raise ValueError(f"Research profile references unknown tools: {sorted(unknown)}")
            tools = {key: value for key, value in tools.items() if key in allowed}
        domain_context = {
            "domain": domain_spec.domain_id,
            "domain_preferred_tools": domain_spec.preferred_tools,
        }
        if domain_spec.domain_id == "crisis_drawdown":
            ids = set(re.findall(r"\b\d{6}\.(?:SH|SZ)\b", question.upper()))
            if not ids:
                for policy in self.settings.agent.policies.values():
                    if policy.kind == "crisis-drawdown":
                        values = policy.params.get("risk_instruments", ())
                        if isinstance(values, (list, tuple)):
                            ids.update(map(str, values))
            if ids:
                crisis_tool = tools["crisis_features"]["callable"]
                domain_context["deterministic_crisis_features"] = [
                    crisis_tool(
                        instrument_ids=sorted(ids)[index:index + 10], drawdown_days=252,
                        event_lookback_days=60, confirmation_days=20, volatility_days=60,
                    ) for index in range(0, len(ids), 10)
                ]
                domain_context["deterministic_baseline"] = {
                    account.account_id: {
                        "risk_instruments": policy.params.get("risk_instruments", ()),
                        "minimum_drawdown": policy.params.get("minimum_drawdown"),
                        "rebound_threshold": policy.params.get("rebound_threshold"),
                        "entry_mode": policy.params.get("entry_mode"),
                        "position_caps": policy.params.get("position_caps", {}),
                    }
                    for account in self.settings.daily.accounts
                    for policy in [self.settings.agent.policies.get(account.account_id)]
                    if policy is not None and policy.kind == "crisis-drawdown"
                }
        events = []
        llm = self.settings.agent.llm
        agent = ResearchAgent(
            base_url=llm.base_url,
            model=model or llm.model,
            api_key_env=llm.api_key_env,
            reasoning_effort=llm.reasoning_effort,
            timeout_seconds=llm.timeout_seconds,
            max_retries=llm.max_retries,
            max_output_tokens=llm.max_output_tokens,
            web_tool=llm.web_tool,
            tools=tools,
            audit_fn=events.append,
        )
        result = agent.research(
            question,
            context={
                **domain_context,
                **(context or {}),
                "memory": memory,
                "library": [d.evidence() for d in library.context()],
            },
            as_of=as_of,
            account_id=account_id,
            max_turns=max_turns,
            domain=domain,
            response_schema=research_report_schema(),
            domain_instruction=domain_spec.instruction,
        )
        text = next((
            p.get("text") for i in result.get("output", [])
            if isinstance(i, dict) and i.get("type") == "message"
            for p in i.get("content", [])
            if isinstance(p, dict) and p.get("type") == "output_text"
        ), "")
        known_sources = {market.snapshot_id}
        opinion_snapshot = OpinionService(self.settings, market=market).summary(
            as_of=as_of, limit=100,
        )
        _collect_source_refs(opinion_snapshot, known_sources)
        for event in events:
            if event.get("event") == "tool_result":
                if event.get("call_id"):
                    known_sources.add(f"tool:{event['call_id']}")
                _collect_source_refs(event.get("result"), known_sources)
            elif event.get("event") == "response_evidence":
                _collect_source_refs(event.get("response"), known_sources)
        report_result = parse_research_report(text, known_sources=known_sources)
        report = {
            "response_id": result.get("id"),
            "model": result.get("model", agent.model),
            "as_of": as_of.isoformat(),
            "snapshot_id": market.snapshot_id,
            "profile": profile,
            "domain": domain,
            "account_id": account_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "question": question,
            "request_key": request_key,
            "tool_events": events,
            "context_hash": hashlib.sha256(
                json.dumps(
                    {"question": question, "as_of": as_of.isoformat(), "snapshot_id": market.snapshot_id,
                     "portfolio": portfolio, "memory": memory, "library": [d.evidence() for d in library.context()]},
                    ensure_ascii=False, default=str, sort_keys=True,
                ).encode()
            ).hexdigest(),
            "final_text": text,
            "research_report": report_result,
        }
        report["run_id"] = hashlib.sha256(
            json.dumps(report, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
        ).hexdigest()[:24]
        proposal = report_result.get("recommended_intent")
        if proposal is not None and not isinstance(proposal, dict):
            raise ValueError("recommended_intent must be an object or null")
        if publish_paper:
            if not account_id:
                raise ValueError("Publishing a paper intent requires --account-id")
            account_config = next((item for item in self.settings.daily.accounts if item.account_id == account_id), None)
            if account_config is None or account_config.strategy != "agent-file":
                raise ValueError("Paper intent requires a configured agent-file account")
            if not proposal:
                raise ValueError("Agent did not produce a recommended_intent")
            raw_weights = proposal.get("target_weights")
            if not isinstance(raw_weights, list) or not raw_weights:
                raise ValueError("recommended_intent.target_weights must be a non-empty array")
            target_weights = {}
            for item in raw_weights:
                if not isinstance(item, dict) or not item.get("instrument_id"):
                    raise ValueError("recommended_intent target weight is malformed")
                instrument_id = str(item["instrument_id"])
                if instrument_id in target_weights:
                    raise ValueError(f"recommended_intent repeats instrument: {instrument_id}")
                target_weights[instrument_id] = str(item.get("weight", ""))
            _validate_paper_weights(target_weights, market, self.settings)
            effective = proposal.get("effective_date")
            decision_date = date.fromisoformat(effective) if effective else market.next_trading_day(as_of)
            if decision_date is None:
                raise ValueError("No next trading session for paper intent")
            if decision_date <= as_of:
                raise ValueError("recommended_intent effective_date must be after the research as_of")
            if not market.trading_days(decision_date, decision_date):
                raise ValueError("recommended_intent effective_date is not an open session")
            proposal = {**proposal, "decision_date": decision_date.isoformat(), "target_weights": target_weights}
        if dry_run:
            return {"status": "ok", "report_path": None, "report": text, "evidence": report}
        root = (
            Path(self.settings.paths.report_root) / "agent-research" / as_of.isoformat()
        )
        root.mkdir(parents=True, exist_ok=True)
        name = (
            hashlib.sha256(
                (
                    json.dumps(report, ensure_ascii=False, sort_keys=True)
                    + datetime.now(timezone.utc).isoformat()
                ).encode()
            ).hexdigest()[:16]
            + ".json"
        )
        target = root / name
        fd, tmp = tempfile.mkstemp(dir=root, prefix=".tmp-", suffix=".json")
        os.close(fd)
        report_bytes = (json.dumps(report, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
        Path(tmp).write_bytes(report_bytes)
        payload_bytes = Path(tmp).read_bytes()
        if payload_bytes != report_bytes:
            Path(tmp).unlink(missing_ok=True)
            raise OSError("Research report staged bytes changed before publish")
        os.replace(tmp, target)
        if json.loads(target.read_text(encoding="utf-8")) != report:
            raise OSError("Research report failed UTF-8 round-trip verification")
        memory_store.append({
            "event": "research_run",
            "as_of": as_of.isoformat(),
            "snapshot_id": market.snapshot_id,
            "domain": domain,
            "question": question,
            "response_id": report["response_id"],
            "report_path": str(target),
            "research_report": report_result,
            "tool_events": events,
        })
        decision_file = None
        if publish_paper:
            written = write_agent_decision(
                self.settings.daily.agent_decision_root,
                account_id=account_id,
                decision_date=date.fromisoformat(proposal["decision_date"]),
                target_weights=proposal["target_weights"],
                reason=str(proposal.get("reason") or report_result["thesis"]),
                agent_id=f"research-{domain}",
            )
            decision_file = written.source_path
        return {"status": "ok", "report_path": str(target), "report": text, "evidence": report, "decision_file": decision_file}

    def insight(self, *, as_of=None, max_turns=None, model=None, profile="daily-insight"):
        """Synthesize current strategy signals into a read-only daily Insight."""
        if as_of is None:
            market = CanonicalMarketData.open(self.settings.paths.market_data)
            as_of = market.manifest.plan.universe_scope.history_end
        if isinstance(as_of, str):
            as_of = date.fromisoformat(as_of)
        rows = []
        root = Path(self.settings.daily.agent_decision_root)
        for account in self.settings.daily.accounts:
            if account.strategy != "agent-file":
                continue
            policy = self.settings.agent.policies.get(account.account_id)
            if policy is None:
                continue
            latest = None
            for path in sorted((root / account.account_id).glob("*.json"), reverse=True):
                if path.name.startswith("."):
                    continue
                try:
                    candidate = date.fromisoformat(path.stem)
                except ValueError:
                    continue
                if candidate <= as_of:
                    try:
                        latest = json.loads(path.read_text(encoding="utf-8"))
                    except (OSError, json.JSONDecodeError):
                        latest = {"error": "invalid decision file"}
                    break
            rows.append({"account_id": account.account_id, "policy": policy.kind, "latest_decision": latest})
        question = (
            f"Produce the daily strategy insight for {as_of.isoformat()}. Compare the configured "
            "strategies, identify consensus and disagreement, changed signals, concentration and "
            "risk, and state what should be checked next. If a current opinion snapshot exists, "
            "first read its compact strategy-relevant summaries and expand details only when "
            "they could change the conclusion. If no current snapshot exists, say so explicitly. "
            "Do not invent missing data."
        )
        from fundlab.common.canonical import stable_digest
        from fundlab.agent.opinion.repository import OpinionRepository
        from fundlab.marketdata.warehouse import MarketDataWarehouse
        opinion = OpinionRepository(self.settings.agent.opinion.root).load(as_of=as_of.isoformat())
        request_key = stable_digest({
            'version': 'daily-insight-r2', 'as_of': as_of, 'profile': profile,
            'profile_config': self.settings.agent.research_profiles.get(profile, {}),
            'model': model, 'max_turns': max_turns, 'signals': rows,
            'market': MarketDataWarehouse(self.settings.paths.market_data).current_snapshot_id(),
            'opinion': (opinion or {}).get('snapshot_id'),
        })
        existing = self._existing_insight(as_of, profile, request_key)
        if existing is not None:
            return existing
        return self.research(
            question, as_of=as_of, max_turns=max_turns, model=model,
            domain="general", profile=profile,
            context={"strategy_signals": rows},
            request_key=request_key,
        )

    def _existing_insight(self, as_of: date, profile: str | None, request_key: str) -> dict[str, object] | None:
        root = Path(self.settings.paths.report_root) / "agent-research" / as_of.isoformat()
        if not root.is_dir():
            return None
        candidates = []
        for path in sorted(root.glob("*.json"), reverse=True):
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, UnicodeDecodeError, json.JSONDecodeError):
                continue
            if not isinstance(payload, dict) or payload.get("profile") != profile or payload.get('request_key') != request_key:
                continue
            if not isinstance(payload.get('research_report'), dict):
                continue
            candidates.append((path, payload))
        if not candidates:
            return None
        path, payload = candidates[0]
        return {
            "status": "ok",
            "reused": True,
            "report_path": str(path),
            "report": payload.get("final_text", ""),
            "evidence": payload,
            "decision_file": None,
        }


def _collect_source_refs(value, found: set[str]) -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            if key in {"snapshot_id", "source_observation_id", "source_ref", "url"} and isinstance(item, str):
                found.add(item)
            _collect_source_refs(item, found)
    elif isinstance(value, (list, tuple)):
        for item in value:
            _collect_source_refs(item, found)


def _validate_paper_weights(weights, market, settings) -> None:
    total = Decimal("0")
    for instrument_id, raw in weights.items():
        try:
            weight = Decimal(str(raw))
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise ValueError(f"Invalid paper weight for {instrument_id}") from exc
        if not weight.is_finite() or weight < 0:
            raise ValueError(f"Paper weight must be finite and non-negative: {instrument_id}")
        instrument = market.instrument(instrument_id)
        if instrument.asset_type.value not in settings.risk_policy.allowed_asset_types:
            raise ValueError(f"Paper intent asset type is not allowed: {instrument_id}")
        if weight > settings.risk_policy.max_position_weight:
            raise ValueError(f"Paper intent exceeds max position weight: {instrument_id}")
        total += weight
    if total > Decimal("1"):
        raise ValueError("Paper intent weights must sum to <= 1")
