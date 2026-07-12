from datetime import date

from fundlab.backtest.cost import CostModel
from fundlab.backtest.slippage import SlippageModel
from fundlab.trading.decision import (
    DecisionValidationContext, build_decision_envelope, validate_target_weights,
)
from fundlab.trading.execution import execute_quantity, size_target_orders
from fundlab.trading.models import DecisionSourceType, OrderStatus, RiskOutcome, ValidationStatus
from fundlab.trading.profiles import ExecutionProfile, ResearchRiskProfile


def test_cost_model_etf_commission_only():
    assert CostModel(commission_rate=0.00003, min_commission=0).calculate(100_000) == 3


def test_slippage_model_adjusts_buy_and_sell():
    model = SlippageModel(base_bps=2)

    buy_price, buy_slippage = model.adjust_price(10, "buy")
    sell_price, sell_slippage = model.adjust_price(10, "sell")

    assert buy_price == 10.002
    assert sell_price == 9.998
    assert buy_slippage == sell_slippage == 0.002


def test_decision_validation_rejects_illegal_universe_and_untrusted_cross_border_data():
    result = validate_target_weights(
        {"513100.SH": 0.6, "OUTSIDE.SH": 0.5, "cash": -0.1},
        DecisionValidationContext(
            universe=frozenset({"513100.SH"}),
            cross_border_symbols=frozenset({"513100.SH"}),
        ),
        ResearchRiskProfile("research", "v1"),
    )
    assert not result.accepted
    assert set(result.codes) == {
        "untrusted_premium_discount", "symbol_not_in_universe", "invalid_weight",
    }


def test_rejected_decision_persists_missing_data_reasons_and_original_targets():
    original = {"513100.SH": 0.6, "OUTSIDE.SH": 0.4, "cash": 0.0}
    decision = build_decision_envelope(
        decision_id="decision-1", account_id="account-1",
        source_type=DecisionSourceType.RULE_STRATEGY, source_id="strategy-1",
        config_version="config-v1", decision_date=date(2026, 1, 2),
        target_weights=original, reason="rebalance", data_version="data-v1",
        observation_hash="observation-hash",
        context=DecisionValidationContext(
            universe=frozenset({"513100.SH"}),
            missing_symbols=frozenset({"513100.SH"}),
            cross_border_symbols=frozenset({"513100.SH"}),
        ),
        profile=ResearchRiskProfile("research", "v1"),
    )
    assert decision.validation.status is ValidationStatus.REJECTED
    assert decision.validation.codes == (
        "symbol_not_in_universe", "missing_required_data", "untrusted_premium_discount",
    )
    assert dict(decision.target_weights) == original
    assert not decision.executable


def test_shared_sizing_sells_first_rounds_lots_and_keeps_cash_nonnegative():
    orders = size_target_orders(
        target_weights={"BUY.SH": 0.9, "SELL.SH": 0.0},
        positions={"SELL.SH": 1000}, total_asset=10_000,
        raw_open_prices={"BUY.SH": 10.0, "SELL.SH": 10.0}, lot_size=100,
    )
    assert [order.side for order in orders] == ["sell", "buy"]
    result = execute_quantity(
        symbol="BUY.SH", side="buy", requested_quantity=900, raw_open_price=10.0,
        available_cash=5_005.0, available_position=0, frozen_average_amount=1_000_000,
        profile=ExecutionProfile("execution", "v1", commission_bps=3, slippage_bps=2,
                                 capacity_fraction=0.05),
    )
    assert result.status is OrderStatus.PARTIAL_FILLED
    assert result.risk.outcome is RiskOutcome.REDUCED
    assert result.actual_quantity == 500
    assert result.amount + result.commission <= 5_005.0
