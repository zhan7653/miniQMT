from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from enum import StrEnum
from types import MappingProxyType
from typing import Any, Mapping

from fundlab.common.canonical import deep_freeze, stable_digest
from fundlab.trading.fees import FeeBreakdown, money
from fundlab.trading.intent import RiskAssessment, decimal_value


class Side(StrEnum):
    BUY = "buy"
    SELL = "sell"


class ExecutionStatus(StrEnum):
    PENDING = "pending"
    FILLED = "filled"
    PARTIAL = "partial"
    REJECTED = "rejected"
    EXPIRED = "expired"


@dataclass(frozen=True)
class ExecutionPolicy:
    policy_id: str
    version: str
    maximum_participation: Decimal = Decimal("0.05")
    base_slippage_bps: Decimal = Decimal("2")
    impact_bps_at_max_participation: Decimal = Decimal("20")
    block_at_price_limit: bool = True
    allow_partial_fills: bool = True
    subscribe_rights: bool = False

    def __post_init__(self) -> None:
        participation = decimal_value(self.maximum_participation)
        base = decimal_value(self.base_slippage_bps)
        impact = decimal_value(self.impact_bps_at_max_participation)
        if not self.policy_id or not self.version:
            raise ValueError("Execution policy identity cannot be empty")
        if not Decimal("0") < participation <= Decimal("1"):
            raise ValueError("maximum_participation must be in (0, 1]")
        if base < 0 or impact < 0:
            raise ValueError("Slippage parameters cannot be negative")
        if self.subscribe_rights:
            raise ValueError("Rights subscription is not implemented; use the explicit do-not-subscribe policy")
        object.__setattr__(self, "maximum_participation", participation)
        object.__setattr__(self, "base_slippage_bps", base)
        object.__setattr__(self, "impact_bps_at_max_participation", impact)

    @property
    def config_hash(self) -> str:
        return stable_digest(self)


@dataclass(frozen=True)
class PositionLot:
    lot_id: str
    instrument_id: str
    quantity: int
    acquired_on: date
    sellable_on: date
    cost_amount: Decimal

    def __post_init__(self) -> None:
        if not self.lot_id or not self.instrument_id or self.quantity <= 0:
            raise ValueError("Position lot identity and quantity must be positive")
        if self.sellable_on < self.acquired_on:
            raise ValueError("Position lot cannot settle before acquisition")
        cost = money(self.cost_amount)
        if cost < 0:
            raise ValueError("Position lot cost cannot be negative")
        object.__setattr__(self, "cost_amount", cost)


@dataclass(frozen=True)
class Order:
    order_id: str
    intent_id: str
    instrument_id: str
    side: Side
    created_on: date
    execution_date: date
    expiry_date: date
    requested_quantity: int
    remaining_quantity: int
    status: ExecutionStatus = ExecutionStatus.PENDING

    def __post_init__(self) -> None:
        if not self.order_id or not self.intent_id or not self.instrument_id:
            raise ValueError("Order identity cannot be empty")
        if self.requested_quantity <= 0 or not 0 <= self.remaining_quantity <= self.requested_quantity:
            raise ValueError("Order quantities are invalid")
        if self.created_on >= self.execution_date or self.execution_date > self.expiry_date:
            raise ValueError("Order dates are invalid")


@dataclass(frozen=True)
class Entitlement:
    action_id: str
    instrument_id: str
    quantity: int
    captured_on: date
    distributed_quantity: int = 0
    allocated_cost: Decimal = Decimal("0")
    adjusted_on: date | None = None

    def __post_init__(self) -> None:
        if not self.action_id or not self.instrument_id or self.quantity < 0:
            raise ValueError("Corporate-action entitlement is invalid")
        if self.distributed_quantity < 0:
            raise ValueError("Distributed entitlement quantity cannot be negative")
        allocated = money(self.allocated_cost)
        if allocated < 0:
            raise ValueError("Allocated entitlement cost cannot be negative")
        if self.adjusted_on is not None and self.adjusted_on < self.captured_on:
            raise ValueError("Entitlement cost cannot be adjusted before capture")
        object.__setattr__(self, "allocated_cost", allocated)


@dataclass(frozen=True)
class Fill:
    fill_id: str
    order_id: str
    instrument_id: str
    side: Side
    session_date: date
    quantity: int
    reference_price: Decimal
    price: Decimal
    amount: Decimal
    slippage_amount: Decimal
    fees: FeeBreakdown
    realized_pnl: Decimal


@dataclass(frozen=True)
class Valuation:
    session_date: date
    cash: Decimal
    market_value: Decimal
    total_equity: Decimal
    nav: Decimal
    stale_instruments: tuple[str, ...]


@dataclass(frozen=True)
class LedgerEvent:
    session_date: date
    event_type: str
    entity_type: str
    entity_id: str
    payload: Mapping[str, Any]

    def __post_init__(self) -> None:
        if not self.event_type or not self.entity_type or not self.entity_id:
            raise ValueError("Ledger event identity cannot be empty")
        object.__setattr__(self, "payload", deep_freeze(self.payload))


@dataclass(frozen=True)
class PortfolioState:
    initial_cash: Decimal
    cash: Decimal
    lots: tuple[PositionLot, ...] = ()
    pending_orders: tuple[Order, ...] = ()
    entitlements: tuple[Entitlement, ...] = ()
    last_prices: Mapping[str, Decimal] = field(default_factory=dict)
    realized_pnl: Decimal = Decimal("0")
    fees_paid: Decimal = Decimal("0")
    dividend_income: Decimal = Decimal("0")
    incomplete_reasons: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        initial_cash, cash = money(self.initial_cash), money(self.cash)
        if initial_cash <= 0 or cash < 0:
            raise ValueError("Portfolio initial cash must be positive and cash cannot be negative")
        object.__setattr__(self, "initial_cash", initial_cash)
        object.__setattr__(self, "cash", cash)
        object.__setattr__(self, "lots", tuple(self.lots))
        object.__setattr__(self, "pending_orders", tuple(self.pending_orders))
        object.__setattr__(self, "entitlements", tuple(self.entitlements))
        object.__setattr__(self, "last_prices", MappingProxyType({
            str(symbol): decimal_value(price) for symbol, price in self.last_prices.items()
        }))
        object.__setattr__(self, "realized_pnl", money(self.realized_pnl))
        object.__setattr__(self, "fees_paid", money(self.fees_paid))
        object.__setattr__(self, "dividend_income", money(self.dividend_income))
        object.__setattr__(self, "incomplete_reasons", tuple(sorted(set(self.incomplete_reasons))))

    @classmethod
    def with_cash(cls, initial_cash: Decimal | str | int | float) -> "PortfolioState":
        cash = money(initial_cash)
        return cls(cash, cash)

    @property
    def state_hash(self) -> str:
        return stable_digest(self)

    def quantity(self, instrument_id: str) -> int:
        return sum(lot.quantity for lot in self.lots if lot.instrument_id == instrument_id)

    def sellable_quantity(self, instrument_id: str, on_date: date) -> int:
        return sum(
            lot.quantity for lot in self.lots
            if lot.instrument_id == instrument_id and lot.sellable_on <= on_date
        )


@dataclass(frozen=True)
class SessionResult:
    state: PortfolioState
    events: tuple[LedgerEvent, ...]
    fills: tuple[Fill, ...]
    valuation: Valuation


@dataclass(frozen=True)
class IntentResult:
    state: PortfolioState
    assessment: RiskAssessment
    orders: tuple[Order, ...]
    events: tuple[LedgerEvent, ...]
