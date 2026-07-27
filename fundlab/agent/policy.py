"""Deterministic decision policies for local ``agent-file`` accounts.

The MVP deliberately ships one policy: a momentum filter that selects a
declared risk-on or risk-off allocation.  A policy receives the immutable
point-in-time market portal plus an explicit ``as_of`` date and returns only a
target-weight decision; the service remains the sole decision-file writer.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Mapping, Protocol

from fundlab.agent.features import InstrumentSnapshot, build_instrument_snapshots
from fundlab.common.canonical import stable_digest
from fundlab.marketdata.portal import CanonicalMarketData


class AgentPolicyError(ValueError):
    """The policy cannot produce a trustworthy deterministic decision."""


@dataclass(frozen=True)
class PolicyDecision:
    target_weights: Mapping[str, Decimal]
    reason: str
    hold: bool = False


class DecisionPolicy(Protocol):
    policy_id: str
    version: str

    @property
    def config_hash(self) -> str: ...

    def decide(self, market: CanonicalMarketData, as_of: date) -> PolicyDecision: ...


def _decimal(value: object, label: str) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ArithmeticError, ValueError, TypeError) as exc:
        raise AgentPolicyError(f"{label} is not a decimal number: {value!r}") from exc
    if not parsed.is_finite():
        raise AgentPolicyError(f"{label} must be a finite number: {value!r}")
    return parsed


def _weights(raw: object, label: str) -> Mapping[str, Decimal]:
    if not isinstance(raw, Mapping) or not raw:
        raise AgentPolicyError(f"{label} must be a non-empty mapping of instrument -> weight")
    parsed: dict[str, Decimal] = {}
    for key, value in raw.items():
        instrument_id = str(key).strip()
        if not instrument_id:
            raise AgentPolicyError(f"{label} contains an empty instrument id")
        if instrument_id in parsed:
            raise AgentPolicyError(f"{label} contains duplicate instrument {instrument_id!r}")
        parsed[instrument_id] = _decimal(value, f"{label}[{key}]")
    total = sum(parsed.values())
    if any(value < 0 for value in parsed.values()) or total > Decimal("1"):
        raise AgentPolicyError(f"{label} weights must be non-negative and sum to <= 1: {raw}")
    return dict(sorted(parsed.items()))


@dataclass(frozen=True)
class MomentumRotationPolicy:
    """Choose declared risk-on/off weights from adjusted-close momentum."""

    risk_instrument: str
    defensive_instrument: str
    momentum_days: int
    threshold: Decimal
    risk_on: Mapping[str, Decimal]
    risk_off: Mapping[str, Decimal]
    policy_id: str = "momentum-rotation"
    version: str = "1"

    def __post_init__(self) -> None:
        risk_instrument = self.risk_instrument.strip()
        defensive_instrument = self.defensive_instrument.strip()
        if not risk_instrument or not defensive_instrument:
            raise AgentPolicyError("risk_instrument and defensive_instrument cannot be empty")
        object.__setattr__(self, "risk_instrument", risk_instrument)
        object.__setattr__(self, "defensive_instrument", defensive_instrument)
        if self.momentum_days < 1:
            raise AgentPolicyError("momentum_days must be at least 1")
        object.__setattr__(self, "threshold", _decimal(self.threshold, "threshold"))
        object.__setattr__(self, "risk_on", _weights(self.risk_on, "risk_on"))
        object.__setattr__(self, "risk_off", _weights(self.risk_off, "risk_off"))

    @property
    def config_hash(self) -> str:
        return stable_digest({
            "policy_id": self.policy_id,
            "version": self.version,
            "risk_instrument": self.risk_instrument,
            "defensive_instrument": self.defensive_instrument,
            "momentum_days": self.momentum_days,
            "threshold": self.threshold,
            "risk_on": self.risk_on,
            "risk_off": self.risk_off,
        })

    @property
    def instrument_ids(self) -> tuple[str, ...]:
        instrument_ids = {self.risk_instrument, self.defensive_instrument}
        instrument_ids.update(self.risk_on)
        instrument_ids.update(self.risk_off)
        return tuple(sorted(instrument_ids))

    def decide(self, market: CanonicalMarketData, as_of: date) -> PolicyDecision:
        snapshots = build_instrument_snapshots(
            market,
            self.instrument_ids,
            as_of=as_of,
            windows=(self.momentum_days,),
        )
        return self.decide_from_snapshots(snapshots)

    def decide_from_snapshots(
        self, snapshots: Mapping[str, InstrumentSnapshot],
    ) -> PolicyDecision:
        """Pure feature-to-decision seam used by focused policy tests."""

        risk = snapshots.get(self.risk_instrument)
        if risk is None:
            raise AgentPolicyError(f"No features for risk instrument {self.risk_instrument}")
        momentum = risk.momentum_for(self.momentum_days)
        if momentum is None:
            raise AgentPolicyError(
                f"{self.risk_instrument} lacks {self.momentum_days} traded sessions of history "
                f"(have {risk.sessions}); refusing to guess"
            )
        momentum_value = Decimal(str(momentum))
        risk_on = momentum_value > self.threshold
        weights = self.risk_on if risk_on else self.risk_off
        stance = "risk-on" if risk_on else "risk-off"
        comparator = ">" if risk_on else "<="
        allocation = " ".join(f"{key}={value}" for key, value in weights.items())
        reason = (
            f"momentum-rotation v{self.version}: {self.risk_instrument} "
            f"{self.momentum_days}-session adjusted momentum {momentum_value * 100:.4f}% "
            f"{comparator} threshold {self.threshold * 100}% -> {stance}; "
            f"target {allocation}"
        )
        return PolicyDecision(target_weights=weights, reason=reason)


_MOMENTUM_PARAMS = frozenset({
    "risk_instrument",
    "defensive_instrument",
    "momentum_days",
    "threshold",
    "risk_on",
    "risk_off",
})


def build_policy(kind: str, params: Mapping[str, object]) -> DecisionPolicy:
    """Build the configured MVP policy and fail closed on misspelled keys."""

    if kind != "momentum-rotation":
        raise AgentPolicyError(f"Unknown agent policy kind: {kind!r}")
    unknown = set(map(str, params)) - _MOMENTUM_PARAMS
    if unknown:
        raise AgentPolicyError(
            f"Unknown {kind} params {sorted(unknown)}; allowed: {sorted(_MOMENTUM_PARAMS)}"
        )
    try:
        return MomentumRotationPolicy(
            risk_instrument=str(params["risk_instrument"]),
            defensive_instrument=str(params["defensive_instrument"]),
            momentum_days=int(str(params.get("momentum_days", 60))),
            threshold=_decimal(params.get("threshold", "0"), "threshold"),
            risk_on=params["risk_on"],
            risk_off=params["risk_off"],
        )
    except KeyError as exc:
        raise AgentPolicyError(f"{kind} config is missing {exc}") from exc
    except (InvalidOperation, ArithmeticError) as exc:
        raise AgentPolicyError(f"{kind} config has a non-numeric value: {exc}") from exc
    except (TypeError, ValueError) as exc:
        if isinstance(exc, AgentPolicyError):
            raise
        raise AgentPolicyError(f"{kind} config is invalid: {exc}") from exc
