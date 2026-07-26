from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Mapping

from fundlab.common.canonical import stable_digest
from fundlab.trading.fees import money
from fundlab.trading.repository import RunStatus, TradingRepository
from fundlab.trading.state import PortfolioState


RATIO = Decimal("0.00000001")


@dataclass(frozen=True)
class SimulationFeedback:
    run_id: str
    binding_hash: str
    result_hash: str
    snapshot_id: str
    mode: str
    start_date: str
    end_date: str
    event_chain_head: str
    final_state_hash: str
    quality: str
    incomplete_reasons: tuple[str, ...]
    sessions: int
    starting_equity: Decimal
    final_equity: Decimal
    run_return: Decimal
    overall_return: Decimal
    max_drawdown: Decimal
    orders: int
    requested_quantity: int
    fills: int
    filled_quantity: int
    fill_ratio: Decimal
    rejected_orders: int
    expired_orders: int
    partial_fills: int
    turnover_amount: Decimal
    fees: Decimal
    slippage_amount: Decimal
    realized_pnl: Decimal
    dividend_income: Decimal

    @property
    def feedback_hash(self) -> str:
        return stable_digest(self)


def build_simulation_feedback(repository: TradingRepository, run_id: str) -> SimulationFeedback:
    run = repository.run(run_id)
    if run.status is not RunStatus.COMPLETE or run.result_hash is None or run.final_state_hash is None:
        raise ValueError("Feedback is available only for a completed simulation run")
    state = repository.final_state(run_id)
    initial_state = repository.initial_state(run_id)
    checkpoints = repository.checkpoints(run_id)
    events = repository.events(run_id)
    orders = [item for item in events if item["event_type"] == "order_created"]
    fills = [item for item in events if item["event_type"] == "fill_created"]
    rejected = [item for item in events if item["event_type"] == "order_rejected"]
    expired = [item for item in events if item["event_type"] == "order_expired"]
    requested_quantity = sum(int(item["payload"]["requested_quantity"]) for item in orders)
    filled_quantity = sum(int(item["payload"]["quantity"]) for item in fills)
    partial = sum(str(item["payload"].get("status")) == "partial" for item in fills)
    turnover = money(sum((_decimal(item["payload"].get("amount")) for item in fills), Decimal("0")))
    fees = money(sum((_fee_total(item["payload"].get("fees", {})) for item in fills), Decimal("0")))
    slippage = money(sum(
        (_decimal(item["payload"].get("slippage_amount")) for item in fills), Decimal("0")
    ))
    realized = money(sum(
        (_decimal(item["payload"].get("realized_pnl")) for item in fills), Decimal("0")
    ))
    dividends = money(sum(
        (_decimal(item["payload"].get("gross_cash")) for item in events
         if item["event_type"] == "cash_dividend_paid"),
        Decimal("0"),
    ))
    starting_equity = _starting_equity(repository, run.binding.parent_run_id, initial_state)
    if starting_equity <= 0:
        raise RuntimeError("Simulation feedback requires positive starting equity")
    starting_nav = (starting_equity / state.initial_cash).quantize(RATIO, rounding=ROUND_HALF_UP)
    navs = [starting_nav, *(_decimal(item["nav"]) for item in checkpoints)]
    final_equity = _decimal(checkpoints[-1]["total_equity"]) if checkpoints else state.cash
    run_return = (final_equity / starting_equity - Decimal("1")).quantize(
        RATIO, rounding=ROUND_HALF_UP,
    )
    overall_return = (final_equity / state.initial_cash - Decimal("1")).quantize(
        RATIO, rounding=ROUND_HALF_UP,
    )
    fill_ratio = (
        Decimal("0") if requested_quantity == 0
        else (Decimal(filled_quantity) / Decimal(requested_quantity)).quantize(RATIO, rounding=ROUND_HALF_UP)
    )
    return SimulationFeedback(
        run_id,
        run.binding.binding_hash,
        run.result_hash,
        run.binding.snapshot_id,
        run.binding.mode.value,
        run.binding.start_date.isoformat(),
        run.binding.end_date.isoformat(),
        repository.verify_run(run_id),
        run.final_state_hash,
        "complete" if not state.incomplete_reasons else "incomplete",
        state.incomplete_reasons,
        len(checkpoints),
        starting_equity,
        final_equity,
        run_return,
        overall_return,
        _max_drawdown(navs),
        len(orders),
        requested_quantity,
        len(fills),
        filled_quantity,
        fill_ratio,
        len(rejected),
        len(expired),
        partial,
        turnover,
        fees,
        slippage,
        realized,
        dividends,
    )


def _max_drawdown(navs: list[Decimal]) -> Decimal:
    peak: Decimal | None = None
    maximum = Decimal("0")
    for nav in navs:
        peak = nav if peak is None else max(peak, nav)
        if peak > 0:
            maximum = max(maximum, (peak - nav) / peak)
    return maximum.quantize(RATIO, rounding=ROUND_HALF_UP)


def _starting_equity(
    repository: TradingRepository, parent_run_id: str | None, initial_state: PortfolioState,
) -> Decimal:
    if parent_run_id is not None:
        parent_checkpoints = repository.checkpoints(parent_run_id)
        if not parent_checkpoints:
            raise RuntimeError("Completed parent run has no valuation checkpoint")
        return _decimal(parent_checkpoints[-1]["total_equity"])
    equity = initial_state.cash
    for symbol in {lot.instrument_id for lot in initial_state.lots}:
        price = initial_state.last_prices.get(symbol)
        if price is None:
            equity += sum(
                (lot.cost_amount for lot in initial_state.lots if lot.instrument_id == symbol),
                Decimal("0"),
            )
        else:
            equity += Decimal(initial_state.quantity(symbol)) * price
    return money(equity)


def _fee_total(payload: Mapping[str, Any]) -> Decimal:
    return sum((_decimal(value) for value in payload.values()), Decimal("0"))


def _decimal(value: Any) -> Decimal:
    return Decimal("0") if value is None else Decimal(str(value))
