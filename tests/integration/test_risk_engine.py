from fundlab.backtest import BacktestEngine
from fundlab.backtest.execution import ExecutionPlanner
from fundlab.backtest.models import Account
from fundlab.risk import LiquidityCheck, PositionLimit, RiskEngine, UniverseCheck
from fundlab.strategies import EqualWeightStrategy
from scripts.create_fake_data import create_fake_v2_portal


def test_position_limit_and_cash_check_adjust_targets(tmp_path):
    portal = create_fake_v2_portal(tmp_path / "v2")
    recorder = BacktestEngine(
        data_portal=portal,
        strategy=EqualWeightStrategy(["510300.SH", "510500.SH", "518880.SH"], cash_weight=0.0),
        start_date="2026-01-02",
        end_date="2026-01-08",
        risk_engine=RiskEngine([UniverseCheck(), PositionLimit(max_weight_per_symbol=0.25)]),
    ).run()

    assert all(intent.target_weight <= 0.25 for intent in recorder.intents)


def test_liquidity_check_scales_large_order(tmp_path):
    portal = create_fake_v2_portal(tmp_path / "v2")
    original_quantity = None
    account = Account.create("test", 10_000_000)
    intents = ExecutionPlanner().create_intents("test", "s", "2026-01-02", "2026-01-05", {"510300.SH": 0.9})
    orders = ExecutionPlanner().create_orders(intents, account, portal)
    original_quantity = orders[0].quantity
    checked = RiskEngine([LiquidityCheck(max_single_order_participation=0.001)]).check_orders(
        orders, account, "2026-01-05", portal
    )

    assert checked[0].quantity < original_quantity
    assert checked[0].reason == "participation_limit_scaled"
