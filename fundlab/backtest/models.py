from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal


@dataclass
class OrderIntent:
    intent_id: str
    account_id: str
    strategy_id: str
    signal_date: str
    execution_date: str
    symbol: str
    target_weight: float
    reason: str = ""
    validation_codes: tuple[str, ...] = ()


@dataclass
class Order:
    order_id: str
    account_id: str
    symbol: str
    side: Literal["buy", "sell"]
    order_type: Literal["target_weight"]
    target_weight: float
    quantity: int
    signal_date: str
    execution_date: str
    status: Literal["pending", "filled", "partial-filled", "partial_filled", "rejected", "cancelled"] = "pending"
    reason: str = ""
    reject_reason: str | None = None
    requested_quantity: int | None = None
    risk_result: Any | None = None


@dataclass
class Trade:
    trade_id: str
    order_id: str
    account_id: str
    symbol: str
    side: Literal["buy", "sell"]
    date: str
    datetime: str
    price: float
    quantity: int
    amount: float
    fee: float
    slippage: float


@dataclass
class Position:
    symbol: str
    quantity: int = 0
    avg_cost: float = 0.0
    market_price: float = 0.0
    market_value: float = 0.0
    unrealized_pnl: float = 0.0


@dataclass
class Account:
    account_id: str
    initial_cash: float
    cash: float
    positions: dict[str, Position] = field(default_factory=dict)
    total_asset: float = 0.0
    nav: float = 1.0
    dividend_receivables: list[dict] = field(default_factory=list)

    @classmethod
    def create(cls, account_id: str, initial_cash: float) -> "Account":
        return cls(account_id=account_id, initial_cash=initial_cash, cash=initial_cash, total_asset=initial_cash)

    def market_value(self) -> float:
        return sum(position.market_value for position in self.positions.values())
