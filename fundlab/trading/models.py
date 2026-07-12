from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from enum import StrEnum
from types import MappingProxyType
from typing import Any, Mapping


class AccountStatus(StrEnum):
    ACTIVE = "active"
    PAUSED = "paused"
    CLOSED = "closed"


class DecisionSourceType(StrEnum):
    RULE_STRATEGY = "rule_strategy"
    LLM_AGENT = "llm_agent"


class ValidationStatus(StrEnum):
    VALID = "valid"
    REJECTED = "rejected"


class RiskOutcome(StrEnum):
    ACCEPTED = "accepted"
    REDUCED = "reduced"
    REJECTED = "rejected"


class OrderStatus(StrEnum):
    PENDING = "pending"
    FILLED = "filled"
    PARTIAL_FILLED = "partial-filled"
    REJECTED = "rejected"
    CANCELLED = "cancelled"


def _frozen_mapping(value: Mapping[str, Any]) -> Mapping[str, Any]:
    return MappingProxyType(dict(value))


@dataclass(frozen=True)
class ValidationResult:
    status: ValidationStatus
    codes: tuple[str, ...] = ()
    detail: str | None = None

    @property
    def accepted(self) -> bool:
        return self.status is ValidationStatus.VALID


@dataclass(frozen=True)
class RiskResult:
    outcome: RiskOutcome
    requested_quantity: int
    approved_quantity: int
    codes: tuple[str, ...] = ()
    detail: str | None = None

    def __post_init__(self) -> None:
        if self.requested_quantity < 0 or self.approved_quantity < 0:
            raise ValueError("Risk quantities cannot be negative")
        if self.approved_quantity > self.requested_quantity:
            raise ValueError("Approved quantity cannot exceed requested quantity")


@dataclass(frozen=True)
class DecisionEnvelope:
    decision_id: str
    account_id: str
    source_type: DecisionSourceType
    source_id: str
    config_version: str
    decision_date: date
    target_weights: Mapping[str, float]
    reason: str
    data_version: str
    observation_hash: str
    validation: ValidationResult
    original_json: str
    order_ids: tuple[str, ...] = ()
    source_metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not all((self.decision_id, self.account_id, self.source_id, self.config_version, self.data_version)):
            raise ValueError("Decision identity and version fields cannot be empty")
        if not self.observation_hash:
            raise ValueError("observation_hash cannot be empty")
        normalized = {str(symbol): float(weight) for symbol, weight in self.target_weights.items()}
        if not normalized:
            raise ValueError("target_weights cannot be empty")
        if any(not symbol for symbol in normalized):
            raise ValueError("Target symbols must be non-empty")
        object.__setattr__(self, "target_weights", _frozen_mapping(normalized))
        object.__setattr__(self, "source_metadata", _frozen_mapping(self.source_metadata))

    @property
    def executable(self) -> bool:
        return self.source_type is DecisionSourceType.RULE_STRATEGY and self.validation.accepted


@dataclass(frozen=True)
class AccountBindings:
    strategy_id: str
    strategy_config_version: str
    universe_version: str
    benchmark_symbol: str
    execution_profile_version: str
    risk_profile_version: str

    def __post_init__(self) -> None:
        if not all(vars(self).values()):
            raise ValueError("Account bindings cannot be empty")
