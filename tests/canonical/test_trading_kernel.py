from __future__ import annotations

from dataclasses import replace
from datetime import date
from decimal import Decimal
import sqlite3

import pytest

from fundlab.marketdata import CorporateAction, CorporateActionType
from fundlab.marketdata.contracts import (
    DATA_GAP_QUARANTINE_RULE_ID,
    EXECUTION_EVIDENCE_GAP_RULE_ID,
    PriceLimitState,
)
from fundlab.trading import (
    Entitlement,
    ExecutionPolicy,
    FeeRule,
    FeeSchedule,
    PortfolioIntent,
    PortfolioState,
    PositionLot,
    Order,
    RiskPolicy,
    Side,
    SimulationService,
    StaticAllocationSource,
    TradingKernel,
    TradingRepository,
    build_simulation_feedback,
)
from tests.canonical.fixtures import DAYS, ready_market


def fees(*, verified=True):
    return FeeSchedule(
        "cn-test",
        "2026-07",
        (FeeRule(
            date(2023, 8, 28),
            None,
            frozenset({"stock"}),
            frozenset({"SH"}),
            Decimal("0.0003"),
            Decimal("5"),
            Decimal("0.0005"),
            Decimal("0.00001"),
            evidence="fixture",
        ),),
        verified,
        "fixture account tariff" if verified else "not verified",
    )


def policies():
    return (
        ExecutionPolicy(
            "daily-conservative", "1", Decimal("0.05"), Decimal("0"), Decimal("0"),
        ),
        RiskPolicy("long-cash", "1"),
    )


def test_historical_and_daily_clocks_share_exact_economic_kernel(tmp_path):
    market = ready_market(tmp_path / "market")
    repository = TradingRepository(tmp_path / "trading.sqlite3")
    initial = PortfolioState.with_cash(100_000)
    repository.create_account("shared", "shared", initial)
    execution, risk = policies()
    source = StaticAllocationSource({"600000.SH": Decimal("0.80")})
    service = SimulationService(
        market_data=market,
        repository=repository,
        execution_policy=execution,
        risk_policy=risk,
        fee_schedule=fees(),
        seed=7,
    )

    historical = service.run_historical("shared", DAYS[0], DAYS[-1], source)
    daily = None
    for day in DAYS:
        daily = service.run_daily("shared", day, source)
    assert daily is not None
    assert historical.final_state.state_hash == daily.final_state.state_hash
    assert historical.final_state.cash == daily.final_state.cash
    assert historical.final_state.lots == daily.final_state.lots
    assert repository.verify_run(historical.run.run_id) == historical.event_chain_head
    assert repository.verify_run(daily.run.run_id) == daily.event_chain_head
    feedback = build_simulation_feedback(repository, historical.run.run_id)
    assert feedback.quality == "complete" and feedback.sessions == len(DAYS)
    assert feedback.orders > 0 and feedback.fills > 0
    assert feedback.turnover_amount > 0 and feedback.fees > 0
    assert feedback.feedback_hash

    repeated = service.run_daily("shared", DAYS[-1], source)
    assert repeated.reused and repeated.run.run_id == daily.run.run_id
    with pytest.raises(ValueError, match="precedes account head"):
        service.run_daily("shared", DAYS[-2], source)
    with pytest.raises(ValueError, match="different run binding"):
        service.run_daily(
            "shared", DAYS[-1], StaticAllocationSource({"600000.SH": Decimal("0.70")}),
        )

    reused = service.run_historical("shared", DAYS[0], DAYS[-1], source)
    assert reused.reused and reused.run.run_id == historical.run.run_id


def test_next_open_fill_uses_close_sized_order_real_fees_and_partial_expiry(tmp_path):
    market = ready_market(tmp_path / "market")
    execution, risk = policies()
    kernel = TradingKernel(
        instruments={item.instrument_id: item for item in market.instruments()},
        sessions=market.all_trading_days(),
        execution_policy=execution,
        risk_policy=risk,
        fee_schedule=fees(),
    )
    state = PortfolioState.with_cash(100_000)
    first = kernel.process_session(state, market.session(DAYS[0]))
    intent = PortfolioIntent.create(
        account_id="a",
        decision_date=DAYS[0],
        snapshot_id=market.snapshot_id,
        strategy_id="fixture",
        strategy_version="1",
        strategy_config_hash="config",
        observation_hash="observation",
        target_weights={"600000.SH": Decimal("1")},
        reason="buy",
    )
    submitted = kernel.submit_intent(
        first.state, intent, market.session(DAYS[0]), next_session=DAYS[1],
    )
    assert submitted.orders[0].requested_quantity == 10_000  # sized at T close=10, never T+1 open
    second = kernel.process_session(submitted.state, market.session(DAYS[1]))
    assert len(second.fills) == 1
    fill = second.fills[0]
    assert fill.quantity < 10_000  # T+1 open gap and fees constrain cash
    assert fill.fees.broker_commission >= Decimal("5.00")
    assert fill.fees.transfer_fee > 0
    assert fill.fees.stamp_duty == Decimal("0.00")
    types = [event.event_type for event in second.events]
    assert "fill_created" in types and "order_expired" in types


def test_data_gap_quarantine_uses_stale_value_and_defers_due_order(tmp_path):
    market = ready_market(tmp_path / "market")
    execution, risk = policies()
    instrument = market.instrument("600000.SH")
    kernel = TradingKernel(
        instruments={instrument.instrument_id: instrument},
        sessions=market.all_trading_days(),
        execution_policy=execution,
        risk_policy=risk,
        fee_schedule=fees(),
    )
    lot = PositionLot(
        "held", instrument.instrument_id, 100, DAYS[0], DAYS[0], Decimal("1000"),
    )
    order = Order(
        "defer-me", "intent", instrument.instrument_id, Side.SELL,
        DAYS[0], DAYS[1], DAYS[1], 100, 100,
    )
    state = PortfolioState(
        Decimal("2000"),
        Decimal("1000"),
        (lot,),
        (order,),
        last_prices={instrument.instrument_id: Decimal("10")},
    )
    ordinary = market.session(DAYS[1])
    quarantine = replace(
        ordinary.bars[instrument.instrument_id],
        open=None,
        high=None,
        low=None,
        close=None,
        volume=0,
        amount=None,
        suspended=False,
        is_st=None,
        trade_rule_id=DATA_GAP_QUARANTINE_RULE_ID,
        price_limit_state=PriceLimitState.UNKNOWN,
        previous_close=None,
        price_limit_ratio=None,
        limit_up=None,
        limit_down=None,
    )
    result = kernel.process_session(
        state,
        replace(ordinary, bars={instrument.instrument_id: quarantine}),
    )

    assert result.fills == ()
    assert result.valuation.stale_instruments == (instrument.instrument_id,)
    assert result.valuation.market_value == Decimal("1000.00")
    assert len(result.state.pending_orders) == 1
    assert result.state.pending_orders[0].execution_date == DAYS[2]
    deferred = [event for event in result.events if event.event_type == "order_deferred"]
    assert deferred and deferred[0].payload["reason"] == "instrument_data_gap_quarantine"


def test_execution_evidence_guard_preserves_close_value_and_defers_due_order(tmp_path):
    market = ready_market(tmp_path / "market")
    execution, risk = policies()
    instrument = market.instrument("600000.SH")
    kernel = TradingKernel(
        instruments={instrument.instrument_id: instrument},
        sessions=market.all_trading_days(),
        execution_policy=execution,
        risk_policy=risk,
        fee_schedule=fees(),
    )
    lot = PositionLot(
        "held", instrument.instrument_id, 100, DAYS[0], DAYS[0], Decimal("1000"),
    )
    order = Order(
        "defer-me", "intent", instrument.instrument_id, Side.SELL,
        DAYS[0], DAYS[1], DAYS[1], 100, 100,
    )
    state = PortfolioState(
        Decimal("2000"),
        Decimal("1000"),
        (lot,),
        (order,),
        last_prices={instrument.instrument_id: Decimal("10")},
    )
    ordinary = market.session(DAYS[1])
    guarded = replace(
        ordinary.bars[instrument.instrument_id],
        trade_rule_id=EXECUTION_EVIDENCE_GAP_RULE_ID,
    )

    result = kernel.process_session(
        state,
        replace(ordinary, bars={instrument.instrument_id: guarded}),
    )

    assert result.fills == ()
    assert result.valuation.stale_instruments == ()
    assert result.valuation.market_value == Decimal(str(guarded.close * 100)).quantize(
        Decimal("0.01")
    )
    assert result.state.pending_orders[0].execution_date == DAYS[2]
    deferred = [event for event in result.events if event.event_type == "order_deferred"]
    assert deferred and deferred[0].payload["reason"] == "execution_evidence_gap"


def test_execution_evidence_guard_never_fills_on_last_known_session(tmp_path):
    market = ready_market(tmp_path / "market")
    execution, risk = policies()
    instrument = market.instrument("600000.SH")
    kernel = TradingKernel(
        instruments={instrument.instrument_id: instrument},
        sessions=(DAYS[1],),
        execution_policy=execution,
        risk_policy=risk,
        fee_schedule=fees(),
    )
    order = Order(
        "never-fill", "intent", instrument.instrument_id, Side.BUY,
        DAYS[0], DAYS[1], DAYS[1], 100, 100,
    )
    state = PortfolioState(
        Decimal("2000"), Decimal("2000"), (), (order,),
    )
    ordinary = market.session(DAYS[1])
    guarded = replace(
        ordinary.bars[instrument.instrument_id],
        trade_rule_id=EXECUTION_EVIDENCE_GAP_RULE_ID,
    )

    result = kernel.process_session(
        state,
        replace(ordinary, bars={instrument.instrument_id: guarded}),
    )

    assert result.fills == ()
    assert result.state.pending_orders == ()
    blocked = [
        event for event in result.events
        if event.event_type == "order_not_executed"
    ]
    assert blocked and blocked[0].payload["reason"] == "execution_evidence_gap"


def test_star_orders_use_200_minimum_one_share_step_and_full_odd_residual(tmp_path):
    market = ready_market(tmp_path / "market")
    base_instrument = market.instrument("600000.SH")
    instrument = replace(
        base_instrument,
        board="star",
        buy_lot=200,
        quantity_step=1,
        odd_lot_sell_all=True,
    )
    execution, risk = policies()
    kernel = TradingKernel(
        instruments={instrument.instrument_id: instrument},
        sessions=market.all_trading_days(),
        execution_policy=execution,
        risk_policy=risk,
        fee_schedule=fees(),
    )
    first_market = market.session(DAYS[0])
    first_bar = replace(
        first_market.bars[instrument.instrument_id],
        buy_lot=200,
        quantity_step=1,
        odd_lot_sell_all=True,
    )
    first_market = replace(first_market, bars={instrument.instrument_id: first_bar})

    intent = PortfolioIntent.create(
        account_id="star",
        decision_date=DAYS[0],
        snapshot_id=market.snapshot_id,
        strategy_id="fixture",
        strategy_version="1",
        strategy_config_hash="config",
        observation_hash="observation",
        target_weights={instrument.instrument_id: Decimal("1")},
        reason="minimum-and-step",
    )
    submitted = kernel.submit_intent(
        PortfolioState.with_cash(2010),
        intent,
        first_market,
        next_session=DAYS[1],
    )
    assert submitted.orders[0].requested_quantity == 201
    below_minimum = kernel.submit_intent(
        PortfolioState.with_cash(1990),
        intent,
        first_market,
        next_session=DAYS[1],
    )
    assert below_minimum.orders == ()

    odd_lot = PositionLot(
        "odd-star",
        instrument.instrument_id,
        173,
        DAYS[0],
        DAYS[0],
        Decimal("1730"),
    )
    liquidation = PortfolioIntent.create(
        account_id="star",
        decision_date=DAYS[0],
        snapshot_id=market.snapshot_id,
        strategy_id="fixture",
        strategy_version="1",
        strategy_config_hash="config",
        observation_hash="odd-residual",
        target_weights={},
        reason="liquidate-odd-residual",
    )
    sell = kernel.submit_intent(
        PortfolioState(Decimal("1730"), Decimal("0"), (odd_lot,)),
        liquidation,
        first_market,
        next_session=DAYS[1],
    )
    assert sell.orders[0].requested_quantity == 173
    second_market = market.session(DAYS[1])
    second_market = replace(second_market, bars={
        instrument.instrument_id: replace(
            second_market.bars[instrument.instrument_id],
            buy_lot=200,
            quantity_step=1,
            odd_lot_sell_all=True,
        ),
    })
    sold = kernel.process_session(sell.state, second_market)
    assert sold.fills[0].quantity == 173


def test_t_plus_one_and_price_limit_are_conservative(tmp_path):
    market = ready_market(tmp_path / "market")
    execution, risk = policies()
    instrument = market.instrument("600000.SH")
    kernel = TradingKernel(
        instruments={instrument.instrument_id: instrument}, sessions=market.all_trading_days(),
        execution_policy=execution, risk_policy=risk, fee_schedule=fees(),
    )
    lot = PositionLot("lot", instrument.instrument_id, 1000, DAYS[1], DAYS[2], Decimal("10000"))
    base = PortfolioState(Decimal("10000"), Decimal("0"), (lot,))
    buy_intent = PortfolioIntent.create(
        account_id="a", decision_date=DAYS[0], snapshot_id=market.snapshot_id,
        strategy_id="s", strategy_version="1", strategy_config_hash="c", observation_hash="o",
        target_weights={}, reason="liquidate",
    )
    submitted = kernel.submit_intent(base, buy_intent, market.session(DAYS[0]), next_session=DAYS[1])
    assert submitted.orders[0].side is Side.SELL
    result = kernel.process_session(submitted.state, market.session(DAYS[1]))
    assert not result.fills
    assert any(event.payload.get("reason") == "t_plus_sellable_capacity" for event in result.events)

    bar = market.session(DAYS[1]).bars["600000.SH"]
    locked = replace(bar, open=bar.limit_down, high=bar.limit_down, low=bar.limit_down, close=bar.limit_down)
    locked_session = replace(market.session(DAYS[1]), bars={"600000.SH": locked})
    sellable = replace(base, lots=(replace(lot, sellable_on=DAYS[1]),))
    submitted = kernel.submit_intent(sellable, buy_intent, market.session(DAYS[0]), next_session=DAYS[1])
    result = kernel.process_session(submitted.state, locked_session)
    assert not result.fills
    assert any(event.payload.get("reason") == "limit_down_locked" for event in result.events)


def test_cash_dividend_is_explicit_and_unknown_tax_marks_run_incomplete(tmp_path):
    market = ready_market(tmp_path / "market")
    instrument = market.instrument("600000.SH")
    execution, risk = policies()
    kernel = TradingKernel(
        instruments={instrument.instrument_id: instrument}, sessions=market.all_trading_days(),
        execution_policy=execution, risk_policy=risk, fee_schedule=fees(),
    )
    action = CorporateAction(
        "dividend-1", instrument.instrument_id, CorporateActionType.CASH_DIVIDEND,
        DAYS[0], DAYS[0], DAYS[1], DAYS[2], None, 0.5, None, None,
        "fixture", "obs-fixture",
    )
    lot = PositionLot("lot", instrument.instrument_id, 1000, DAYS[0], DAYS[0], Decimal("10000"))
    state = PortfolioState(Decimal("20000"), Decimal("10000"), (lot,))
    record_session = replace(market.session(DAYS[0]), record_actions=(action,))
    recorded = kernel.process_session(state, record_session)
    assert recorded.state.entitlements[0].quantity == 1000
    pay_session = replace(market.session(DAYS[2]), pay_actions=(action,))
    paid = kernel.process_session(recorded.state, pay_session)
    assert paid.state.cash == Decimal("10500.00")
    assert "dividend_tax_unmodeled:dividend-1" in paid.state.incomplete_reasons
    assert any(event.event_type == "cash_dividend_paid" for event in paid.events)


def test_unheld_corporate_action_does_not_pollute_account_state_or_events(tmp_path):
    market = ready_market(tmp_path / "market")
    instrument = market.instrument("600000.SH")
    execution, risk = policies()
    kernel = TradingKernel(
        instruments={instrument.instrument_id: instrument},
        sessions=market.all_trading_days(),
        execution_policy=execution,
        risk_policy=risk,
        fee_schedule=fees(),
    )
    action = CorporateAction(
        "unheld-dividend", instrument.instrument_id, CorporateActionType.CASH_DIVIDEND,
        DAYS[0], DAYS[0], DAYS[1], DAYS[2], None, 0.5, None, None,
        "fixture", "obs-unheld",
    )
    empty = PortfolioState.with_cash(20_000)

    recorded = kernel.process_session(
        empty, replace(market.session(DAYS[0]), record_actions=(action,)),
    )
    ex_date = kernel.process_session(
        recorded.state, replace(market.session(DAYS[1]), ex_actions=(action,)),
    )
    paid = kernel.process_session(
        ex_date.state, replace(market.session(DAYS[2]), pay_actions=(action,)),
    )

    assert paid.state.entitlements == ()
    assert paid.state.cash == Decimal("20000.00")
    assert paid.state.incomplete_reasons == ()
    action_events = {
        event.event_type
        for result in (recorded, ex_date, paid)
        for event in result.events
        if event.entity_type == "corporate_action"
    }
    assert action_events == set()


def test_buying_after_record_date_does_not_create_missing_entitlement_noise(tmp_path):
    market = ready_market(tmp_path / "market")
    instrument = market.instrument("600000.SH")
    execution, risk = policies()
    kernel = TradingKernel(
        instruments={instrument.instrument_id: instrument},
        sessions=market.all_trading_days(),
        execution_policy=execution,
        risk_policy=risk,
        fee_schedule=fees(),
    )
    cash = CorporateAction(
        "later-buyer-cash", instrument.instrument_id, CorporateActionType.CASH_DIVIDEND,
        DAYS[0], DAYS[0], DAYS[1], DAYS[2], None, 0.5, None, None,
        "fixture", "obs-later-buyer-cash",
    )
    shares = CorporateAction(
        "later-buyer-shares", instrument.instrument_id, CorporateActionType.STOCK_DIVIDEND,
        DAYS[0], DAYS[0], DAYS[1], None, DAYS[2], None, 0.1, None,
        "fixture", "obs-later-buyer-shares",
    )
    split = CorporateAction(
        "later-buyer-split", instrument.instrument_id, CorporateActionType.SPLIT,
        DAYS[0], DAYS[0], DAYS[1], None, DAYS[1], None, None, None,
        "fixture", "obs-later-buyer-split", quantity_multiplier=2.0,
    )
    record = kernel.process_session(
        PortfolioState.with_cash(30_000),
        replace(market.session(DAYS[0]), record_actions=(cash, shares, split)),
    )
    lot = PositionLot(
        "later-purchase", instrument.instrument_id, 1000,
        DAYS[1], DAYS[1], Decimal("10000"),
    )
    later_buyer = replace(
        record.state, cash=Decimal("20000"), lots=(lot,),
    )

    ex_date = kernel.process_session(
        later_buyer,
        replace(market.session(DAYS[1]), ex_actions=(cash, shares, split)),
    )
    distribution = kernel.process_session(
        ex_date.state,
        replace(market.session(DAYS[2]), pay_actions=(cash,), listing_actions=(shares,)),
    )

    assert distribution.state.quantity(instrument.instrument_id) == 1000
    assert distribution.state.cash == Decimal("20000.00")
    assert distribution.state.dividend_income == Decimal("0.00")
    assert distribution.state.incomplete_reasons == ()
    assert not any(
        event.entity_type == "corporate_action"
        for result in (record, ex_date, distribution)
        for event in result.events
    )


def test_legacy_zero_entitlements_are_pruned_without_economic_effect(tmp_path):
    market = ready_market(tmp_path / "market")
    instrument = market.instrument("600000.SH")
    execution, risk = policies()
    kernel = TradingKernel(
        instruments={instrument.instrument_id: instrument},
        sessions=market.all_trading_days(),
        execution_policy=execution,
        risk_policy=risk,
        fee_schedule=fees(),
    )
    action = CorporateAction(
        "legacy-zero", instrument.instrument_id, CorporateActionType.CASH_DIVIDEND,
        DAYS[0], DAYS[0], DAYS[1], DAYS[1], None, 0.5, None, None,
        "fixture", "obs-legacy-zero",
    )
    lot = PositionLot(
        "current-position", instrument.instrument_id, 1000,
        DAYS[0], DAYS[0], Decimal("10000"),
    )
    legacy = PortfolioState(
        Decimal("30000"), Decimal("20000"), (lot,),
        entitlements=(Entitlement("legacy-zero", instrument.instrument_id, 0, DAYS[0]),),
    )

    result = kernel.process_session(
        legacy,
        replace(market.session(DAYS[1]), ex_actions=(action,), pay_actions=(action,)),
    )

    assert result.state.entitlements == ()
    assert result.state.cash == Decimal("20000.00")
    assert result.state.quantity(instrument.instrument_id) == 1000
    assert result.state.incomplete_reasons == ()
    assert [
        event.event_type for event in result.events
        if event.event_type == "zero_entitlements_pruned"
    ] == ["zero_entitlements_pruned"]
    assert not any(event.entity_type == "corporate_action" for event in result.events)


def test_split_preserves_and_allocates_cost_basis_before_partial_sale(tmp_path):
    market = ready_market(tmp_path / "market")
    instrument = market.instrument("600000.SH")
    execution, risk = policies()
    kernel = TradingKernel(
        instruments={instrument.instrument_id: instrument}, sessions=market.all_trading_days(),
        execution_policy=execution, risk_policy=risk, fee_schedule=fees(),
    )
    split = CorporateAction(
        "split-1", instrument.instrument_id, CorporateActionType.SPLIT,
        DAYS[0], DAYS[0], DAYS[1], None, DAYS[1], None, None, None,
        "fixture", "obs-fixture", quantity_multiplier=2.0,
    )
    lot = PositionLot("original", instrument.instrument_id, 1000, DAYS[0], DAYS[0], Decimal("10000"))
    state = PortfolioState(Decimal("20000"), Decimal("10000"), (lot,))
    recorded = kernel.process_session(
        state, replace(market.session(DAYS[0]), record_actions=(split,)),
    )
    distributed = kernel.process_session(
        recorded.state,
        replace(market.session(DAYS[1]), ex_actions=(split,), listing_actions=(split,)),
    )
    assert distributed.state.quantity(instrument.instrument_id) == 2000
    assert sum(item.cost_amount for item in distributed.state.lots) == Decimal("10000.00")
    assert [item.cost_amount for item in distributed.state.lots] == [Decimal("10000.00")]
    order = Order(
        "partial-sale", "intent", instrument.instrument_id, Side.SELL,
        DAYS[1], DAYS[2], DAYS[2], 1000, 1000,
    )
    sold = kernel.process_session(
        replace(distributed.state, pending_orders=(order,)), market.session(DAYS[2]),
    )
    assert sold.fills[0].quantity == 1000
    assert sold.fills[0].realized_pnl == Decimal("6988.88")
    assert sum(item.cost_amount for item in sold.state.lots) == Decimal("5000.00")


def test_reverse_split_reduces_quantity_without_destroying_cost_basis(tmp_path):
    market = ready_market(tmp_path / "market")
    instrument = market.instrument("600000.SH")
    execution, risk = policies()
    kernel = TradingKernel(
        instruments={instrument.instrument_id: instrument}, sessions=market.all_trading_days(),
        execution_policy=execution, risk_policy=risk, fee_schedule=fees(),
    )
    split = CorporateAction(
        "reverse-split", instrument.instrument_id, CorporateActionType.SPLIT,
        DAYS[0], DAYS[0], DAYS[1], None, DAYS[1], None, None, None,
        "fixture", "obs-fixture", quantity_multiplier=0.25,
    )
    lot = PositionLot(
        "original", instrument.instrument_id, 1000, DAYS[0], DAYS[0], Decimal("10000"),
    )
    recorded = kernel.process_session(
        PortfolioState(Decimal("20000"), Decimal("10000"), (lot,)),
        replace(market.session(DAYS[0]), record_actions=(split,)),
    )
    result = kernel.process_session(
        recorded.state, replace(market.session(DAYS[1]), ex_actions=(split,)),
    )

    assert result.state.quantity(instrument.instrument_id) == 250
    assert sum(item.cost_amount for item in result.state.lots) == Decimal("10000.00")
    assert any(event.event_type == "split_applied" for event in result.events)


def test_same_session_factor_confirmed_split_applies_atomically(tmp_path):
    market = ready_market(tmp_path / "market")
    instrument = market.instrument("600000.SH")
    execution, risk = policies()
    kernel = TradingKernel(
        instruments={instrument.instrument_id: instrument},
        sessions=market.all_trading_days(),
        execution_policy=execution,
        risk_policy=risk,
        fee_schedule=fees(),
    )
    split = CorporateAction(
        "factor-only-reverse-split",
        instrument.instrument_id,
        CorporateActionType.SPLIT,
        DAYS[1],
        DAYS[1],
        DAYS[1],
        None,
        DAYS[1],
        None,
        None,
        None,
        "canonical-dual-factor-action-r2",
        "obs-factor-audit",
        quantity_multiplier=0.625,
    )
    lot = PositionLot(
        "original", instrument.instrument_id, 1000, DAYS[0], DAYS[0], Decimal("10000"),
    )

    result = kernel.process_session(
        PortfolioState(Decimal("20000"), Decimal("10000"), (lot,)),
        replace(
            market.session(DAYS[1]),
            record_actions=(split,),
            ex_actions=(split,),
            listing_actions=(split,),
        ),
    )

    assert result.state.quantity(instrument.instrument_id) == 625
    assert sum(item.cost_amount for item in result.state.lots) == Decimal("10000.00")
    assert result.state.entitlements == ()
    assert not any(
        item.event_type == "corporate_action_entitlement" for item in result.events
    )
    applied = next(item for item in result.events if item.event_type == "split_applied")
    assert applied.payload["entitlement_mode"] == "same_session_atomic"


def test_completed_runs_and_ledger_rows_cannot_be_mutated(tmp_path):
    market = ready_market(tmp_path / "market")
    repository = TradingRepository(tmp_path / "trading.sqlite3")
    repository.create_account("a", "a", PortfolioState.with_cash(100_000))
    execution, risk = policies()
    outcome = SimulationService(
        market_data=market, repository=repository, execution_policy=execution,
        risk_policy=risk, fee_schedule=fees(),
    ).run_historical("a", DAYS[0], DAYS[1], StaticAllocationSource({"600000.SH": Decimal("0.5")}))
    connection = sqlite3.connect(repository.path)
    with pytest.raises(sqlite3.IntegrityError, match="terminal runs are immutable"):
        connection.execute("UPDATE simulation_runs SET status='failed' WHERE run_id=?", (outcome.run.run_id,))
    with pytest.raises(sqlite3.IntegrityError, match="ledger events are immutable"):
        connection.execute(
            "UPDATE trading_ledger_events SET event_type='tampered' WHERE run_id=?",
            (outcome.run.run_id,),
        )


def test_single_daily_run_drawdown_includes_promoted_parent_starting_nav(tmp_path):
    market = ready_market(tmp_path / "market", close_values=(10.0, 9.5, 9.0, 8.5))
    repository = TradingRepository(tmp_path / "trading.sqlite3")
    repository.create_account("daily-down", "daily-down", PortfolioState.with_cash(100_000))
    execution, risk = policies()
    source = StaticAllocationSource({"600000.SH": Decimal("0.8")})
    service = SimulationService(
        market_data=market, repository=repository, execution_policy=execution,
        risk_policy=risk, fee_schedule=fees(),
    )
    service.run_daily("daily-down", DAYS[0], source)
    parent = service.run_daily("daily-down", DAYS[1], source)
    falling = service.run_daily("daily-down", DAYS[2], source)
    feedback = build_simulation_feedback(repository, falling.run.run_id)
    parent_equity = Decimal(repository.checkpoints(parent.run.run_id)[-1]["total_equity"])
    assert feedback.sessions == 1 and feedback.starting_equity == parent_equity
    assert feedback.final_equity < feedback.starting_equity
    assert feedback.run_return < 0 and feedback.max_drawdown > 0
