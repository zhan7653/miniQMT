from __future__ import annotations

from dataclasses import dataclass
from math import floor
from typing import Literal, Mapping

from fundlab.trading.models import OrderStatus, RiskOutcome, RiskResult
from fundlab.trading.profiles import ExecutionProfile


@dataclass(frozen=True)
class TargetOrder:
    symbol: str
    side: Literal["buy", "sell"]
    target_weight: float
    requested_quantity: int


@dataclass(frozen=True)
class ExecutionResult:
    symbol: str
    side: Literal["buy", "sell"]
    requested_quantity: int
    actual_quantity: int
    status: OrderStatus
    risk: RiskResult
    raw_price: float | None
    fill_price: float | None
    amount: float
    commission: float
    slippage: float


def size_target_orders(*, target_weights: Mapping[str, float], positions: Mapping[str, int],
                       total_asset: float, raw_open_prices: Mapping[str, float | None],
                       lot_size: int = 100) -> tuple[TargetOrder, ...]:
    symbols = set(positions) | {symbol for symbol in target_weights if symbol != "cash"}
    orders: list[TargetOrder] = []
    for symbol in symbols:
        price = raw_open_prices.get(symbol)
        if price is None or price <= 0:
            continue
        target_quantity = floor(total_asset * float(target_weights.get(symbol, 0.0)) / price / lot_size) * lot_size
        delta = target_quantity - int(positions.get(symbol, 0))
        if delta:
            orders.append(TargetOrder(symbol, "buy" if delta > 0 else "sell",
                                      float(target_weights.get(symbol, 0.0)), abs(delta)))
    return tuple(sorted(orders, key=lambda item: (item.side == "buy", item.symbol)))


def execute_quantity(*, symbol: str, side: Literal["buy", "sell"], requested_quantity: int,
                     raw_open_price: float | None, available_cash: float,
                     available_position: int, frozen_average_amount: float | None,
                     profile: ExecutionProfile) -> ExecutionResult:
    codes: list[str] = []
    approved = max(0, requested_quantity // profile.lot_size * profile.lot_size)
    if approved != requested_quantity:
        codes.append("lot_rounding")
    if raw_open_price is None or raw_open_price <= 0:
        return _rejected(symbol, side, requested_quantity, "missing_raw_open")
    if profile.capacity_fraction is not None:
        if frozen_average_amount is None or frozen_average_amount < 0:
            return _rejected(symbol, side, requested_quantity, "missing_frozen_liquidity")
        capacity = floor(frozen_average_amount * profile.capacity_fraction / raw_open_price
                         / profile.lot_size) * profile.lot_size
        if approved > capacity:
            approved = capacity
            codes.append("liquidity_capacity")
    if side == "sell" and approved > available_position:
        approved = available_position // profile.lot_size * profile.lot_size
        codes.append("position_capacity")
    fill_price = raw_open_price * (1 + profile.slippage_bps / 10_000 if side == "buy"
                                   else 1 - profile.slippage_bps / 10_000)
    if side == "buy" and approved:
        while approved > 0:
            amount = fill_price * approved
            fee = max(amount * profile.commission_bps / 10_000, profile.minimum_commission)
            if amount + fee <= available_cash + 1e-9:
                break
            approved -= profile.lot_size
        if approved < requested_quantity:
            codes.append("cash_capacity")
    if approved <= 0:
        return _rejected(symbol, side, requested_quantity, codes[-1] if codes else "zero_quantity")
    amount = fill_price * approved
    commission = max(amount * profile.commission_bps / 10_000, profile.minimum_commission)
    outcome = RiskOutcome.ACCEPTED if approved == requested_quantity and not codes else RiskOutcome.REDUCED
    status = OrderStatus.FILLED if outcome is RiskOutcome.ACCEPTED else OrderStatus.PARTIAL_FILLED
    risk = RiskResult(outcome, requested_quantity, approved, tuple(dict.fromkeys(codes)), ";".join(codes) or None)
    return ExecutionResult(symbol, side, requested_quantity, approved, status, risk, raw_open_price,
                           fill_price, amount, commission,
                           abs(fill_price - raw_open_price) * approved)


def _rejected(symbol: str, side: Literal["buy", "sell"], requested: int, code: str) -> ExecutionResult:
    risk = RiskResult(RiskOutcome.REJECTED, requested, 0, (code,), code)
    return ExecutionResult(symbol, side, requested, 0, OrderStatus.REJECTED, risk, None, None, 0, 0, 0)
