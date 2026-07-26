from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
import math
from types import MappingProxyType
from typing import Any, Mapping

from fundlab.common.canonical import deep_freeze, stable_digest


def decimal_value(value: Decimal | str | int | float) -> Decimal:
    if isinstance(value, Decimal):
        result = value
    elif isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("Decimal input must be finite")
        result = Decimal(str(value))
    else:
        result = Decimal(value)
    if not result.is_finite():
        raise ValueError("Decimal input must be finite")
    return result


@dataclass(frozen=True)
class PortfolioIntent:
    intent_id: str
    account_id: str
    decision_date: date
    snapshot_id: str
    strategy_id: str
    strategy_version: str
    strategy_config_hash: str
    observation_hash: str
    target_weights: Mapping[str, Decimal]
    reason: str
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        identity = (
            self.intent_id,
            self.account_id,
            self.snapshot_id,
            self.strategy_id,
            self.strategy_version,
            self.strategy_config_hash,
            self.observation_hash,
        )
        if not all(str(item).strip() for item in identity) or not self.reason.strip():
            raise ValueError("PortfolioIntent identity, hashes, and reason cannot be empty")
        normalized = {str(symbol): decimal_value(weight) for symbol, weight in self.target_weights.items()}
        if any(not symbol.strip() for symbol in normalized):
            raise ValueError("PortfolioIntent symbols cannot be empty")
        if any(weight < 0 for weight in normalized.values()):
            raise ValueError("PortfolioIntent target weights cannot be negative")
        object.__setattr__(self, "target_weights", MappingProxyType(dict(sorted(normalized.items()))))
        object.__setattr__(self, "metadata", deep_freeze(self.metadata))

    @classmethod
    def create(
        cls,
        *,
        account_id: str,
        decision_date: date,
        snapshot_id: str,
        strategy_id: str,
        strategy_version: str,
        strategy_config_hash: str,
        observation_hash: str,
        target_weights: Mapping[str, Decimal | str | int | float],
        reason: str,
        metadata: Mapping[str, Any] | None = None,
    ) -> "PortfolioIntent":
        normalized = {symbol: decimal_value(weight) for symbol, weight in target_weights.items()}
        body = {
            "account_id": account_id,
            "decision_date": decision_date,
            "snapshot_id": snapshot_id,
            "strategy_id": strategy_id,
            "strategy_version": strategy_version,
            "strategy_config_hash": strategy_config_hash,
            "observation_hash": observation_hash,
            "target_weights": normalized,
            "reason": reason,
            "metadata": metadata or {},
        }
        return cls(f"intent-{stable_digest(body)[:24]}", target_weights=normalized, metadata=metadata or {}, **{
            key: value for key, value in body.items() if key not in {"target_weights", "metadata"}
        })


@dataclass(frozen=True)
class RiskPolicy:
    policy_id: str
    version: str
    max_position_weight: Decimal = Decimal("1")
    minimum_cash_weight: Decimal = Decimal("0")
    allowed_asset_types: frozenset[str] = frozenset({"stock", "etf"})

    def __post_init__(self) -> None:
        maximum = decimal_value(self.max_position_weight)
        minimum_cash = decimal_value(self.minimum_cash_weight)
        if not self.policy_id or not self.version:
            raise ValueError("Risk policy identity cannot be empty")
        if not Decimal("0") < maximum <= Decimal("1"):
            raise ValueError("max_position_weight must be in (0, 1]")
        if not Decimal("0") <= minimum_cash < Decimal("1"):
            raise ValueError("minimum_cash_weight must be in [0, 1)")
        object.__setattr__(self, "max_position_weight", maximum)
        object.__setattr__(self, "minimum_cash_weight", minimum_cash)
        object.__setattr__(self, "allowed_asset_types", frozenset(self.allowed_asset_types))

    @property
    def config_hash(self) -> str:
        return stable_digest(self)


@dataclass(frozen=True)
class RiskAssessment:
    intent_id: str
    accepted: bool
    requested_weights: Mapping[str, Decimal]
    approved_weights: Mapping[str, Decimal]
    codes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "requested_weights", MappingProxyType(dict(self.requested_weights)))
        object.__setattr__(self, "approved_weights", MappingProxyType(dict(self.approved_weights)))


def assess_intent(intent: PortfolioIntent, instruments: Mapping[str, Any], policy: RiskPolicy) -> RiskAssessment:
    codes: list[str] = []
    approved: dict[str, Decimal] = {}
    for symbol, requested in intent.target_weights.items():
        instrument = instruments.get(symbol)
        if instrument is None:
            codes.append(f"unknown_instrument:{symbol}")
            continue
        asset_type = getattr(getattr(instrument, "asset_type", None), "value", getattr(instrument, "asset_type", None))
        if asset_type not in policy.allowed_asset_types:
            codes.append(f"asset_type_not_allowed:{symbol}")
            continue
        if requested > policy.max_position_weight:
            approved[symbol] = policy.max_position_weight
            codes.append(f"position_reduced:{symbol}")
        else:
            approved[symbol] = requested
    if any(code.startswith(("unknown_instrument", "asset_type_not_allowed")) for code in codes):
        return RiskAssessment(intent.intent_id, False, intent.target_weights, {}, tuple(sorted(codes)))
    total = sum(approved.values(), Decimal("0"))
    investable = Decimal("1") - policy.minimum_cash_weight
    if total > investable:
        codes.append("leverage_or_minimum_cash_violation")
        return RiskAssessment(intent.intent_id, False, intent.target_weights, {}, tuple(sorted(codes)))
    return RiskAssessment(intent.intent_id, True, intent.target_weights, approved, tuple(sorted(codes)))
