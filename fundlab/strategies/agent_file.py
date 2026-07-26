from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Mapping

from fundlab.common.canonical import stable_digest
from fundlab.marketdata.portal import PointInTimeMarketView
from fundlab.trading.intent import PortfolioIntent, decimal_value
from fundlab.trading.state import PortfolioState


class AgentDecisionError(ValueError):
    """A decision file exists but cannot be trusted for execution."""


@dataclass(frozen=True)
class AgentDecision:
    account_id: str
    decision_date: date
    target_weights: Mapping[str, Decimal]
    reason: str
    agent_id: str
    source_path: str

    @property
    def content_hash(self) -> str:
        return stable_digest({
            "account_id": self.account_id,
            "decision_date": self.decision_date,
            "target_weights": self.target_weights,
            "reason": self.reason,
            "agent_id": self.agent_id,
        })


def load_agent_decision(
    decision_root: str | Path, account_id: str, decision_date: date,
) -> AgentDecision | None:
    """Read one dropped decision file, or None when the agent stayed silent.

    The file contract is ``<decision_root>/<account_id>/<YYYY-MM-DD>.json``:

    .. code-block:: json

        {
          "account_id": "paper-1",
          "decision_date": "2026-07-27",
          "target_weights": {"510300.SH": "0.6", "511010.SH": "0.4"},
          "reason": "why the agent wants this allocation",
          "agent_id": "my-agent"
        }

    ``agent_id`` is optional. A malformed or mismatched file raises instead of
    being silently skipped, because silence and corruption must stay
    distinguishable in the run evidence.
    """
    path = Path(decision_root) / account_id / f"{decision_date.isoformat()}.json"
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AgentDecisionError(f"Unreadable agent decision {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise AgentDecisionError(f"Agent decision must be a JSON object: {path}")
    declared_account = str(payload.get("account_id", ""))
    if declared_account != account_id:
        raise AgentDecisionError(
            f"Agent decision account mismatch in {path}: {declared_account!r} != {account_id!r}"
        )
    declared_date = str(payload.get("decision_date", ""))
    if declared_date != decision_date.isoformat():
        raise AgentDecisionError(
            f"Agent decision date mismatch in {path}: {declared_date!r} != {decision_date.isoformat()!r}"
        )
    raw_weights = payload.get("target_weights")
    if not isinstance(raw_weights, dict) or not raw_weights:
        raise AgentDecisionError(f"Agent decision needs a non-empty target_weights object: {path}")
    weights: dict[str, Decimal] = {}
    for symbol, value in raw_weights.items():
        if not str(symbol).strip():
            raise AgentDecisionError(f"Agent decision has an empty symbol: {path}")
        try:
            weight = decimal_value(value if isinstance(value, (str, int, float, Decimal)) else str(value))
        except (InvalidOperation, ValueError, TypeError) as exc:
            raise AgentDecisionError(
                f"Agent decision weight for {symbol} is not a decimal in {path}: {value!r}"
            ) from exc
        if weight < 0:
            raise AgentDecisionError(f"Agent decision weight for {symbol} is negative: {path}")
        weights[str(symbol)] = weight
    reason = str(payload.get("reason", "")).strip()
    if not reason:
        raise AgentDecisionError(f"Agent decision must state a reason: {path}")
    return AgentDecision(
        account_id=account_id,
        decision_date=decision_date,
        target_weights=dict(sorted(weights.items())),
        reason=reason,
        agent_id=str(payload.get("agent_id", "agent-file")).strip() or "agent-file",
        source_path=str(path),
    )


@dataclass(frozen=True)
class FileIntentSource:
    """Out-of-process agent socket: execute one dropped JSON decision per day.

    Absence of the file is a first-class outcome (hold current positions); a
    present-but-invalid file fails the run instead of degrading to a hold.
    The decision content is bound into the strategy config hash so a replayed
    run cannot silently execute a different decision.
    """

    decision_root: Path
    account_id: str
    target_date: date
    strategy_id: str = "agent_file"
    strategy_version: str = "1"
    _decision: AgentDecision | None = field(init=False, default=None)

    def __post_init__(self) -> None:
        object.__setattr__(self, "decision_root", Path(self.decision_root))
        object.__setattr__(self, "_decision", load_agent_decision(
            self.decision_root, self.account_id, self.target_date,
        ))

    @property
    def decision(self) -> AgentDecision | None:
        return self._decision

    @property
    def config_hash(self) -> str:
        return stable_digest({
            "strategy_id": self.strategy_id,
            "strategy_version": self.strategy_version,
            "decision": None if self._decision is None else self._decision.content_hash,
            "decision_date": self.target_date,
        })

    def decide(
        self,
        *,
        account_id: str,
        market: PointInTimeMarketView,
        state: PortfolioState,
    ) -> PortfolioIntent | None:
        if account_id != self.account_id:
            raise ValueError(
                f"FileIntentSource is pinned to {self.account_id!r}, got {account_id!r}"
            )
        decision = self._decision
        if decision is None or market.as_of != self.target_date:
            return None
        observation_hash = stable_digest({
            "snapshot_id": market.snapshot_id,
            "as_of": market.as_of,
            "state_hash": state.state_hash,
            "decision": decision.content_hash,
        })
        return PortfolioIntent.create(
            account_id=account_id,
            decision_date=market.as_of,
            snapshot_id=market.snapshot_id,
            strategy_id=self.strategy_id,
            strategy_version=self.strategy_version,
            strategy_config_hash=self.config_hash,
            observation_hash=observation_hash,
            target_weights=decision.target_weights,
            reason=decision.reason,
            metadata={"agent_id": decision.agent_id, "decision_path": decision.source_path},
        )
