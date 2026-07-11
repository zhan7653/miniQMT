from fundlab.backtest import BacktestEngine
from fundlab.strategies import EqualWeightStrategy
from scripts.create_fake_data import create_fake_v2_portal


def test_equal_weight_backtest_runs_next_open_execution(tmp_path):
    portal = create_fake_v2_portal(tmp_path / "v2")
    strategy = EqualWeightStrategy(["510300.SH", "510500.SH", "518880.SH"], cash_weight=0.02)
    recorder = BacktestEngine(
        data_portal=portal,
        strategy=strategy,
        start_date="2026-01-02",
        end_date="2026-02-12",
        initial_cash=1_000_000,
        rebalance_frequency="monthly",
    ).run()

    account_daily = recorder.account_daily_frame()
    orders = recorder.orders_frame()
    trades = recorder.trades_frame()

    assert not account_daily.empty
    assert account_daily.iloc[-1]["total_asset"] > 0
    assert len(orders) >= 3
    assert len(trades) >= 3
    assert set(trades["date"]) >= {"2026-01-05"}
    assert orders.iloc[0]["signal_date"] == "2026-01-02"
    assert orders.iloc[0]["execution_date"] == "2026-01-05"


def test_strategy_cannot_access_execution_open_by_design(tmp_path):
    portal = create_fake_v2_portal(tmp_path / "v2")
    strategy = EqualWeightStrategy(["510300.SH"], cash_weight=0.02)

    targets = strategy.on_rebalance("2026-01-02", portal, {})

    assert targets["510300.SH"] == 0.98
