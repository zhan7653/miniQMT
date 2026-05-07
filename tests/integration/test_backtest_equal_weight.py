from fundlab.backtest import BacktestEngine
from fundlab.common.config import get_path, load_config
from fundlab.data.portal import DataPortal
from fundlab.data.storage import ParquetStore, SQLiteStore
from fundlab.strategies import EqualWeightStrategy
from scripts.create_fake_data import create_fake_data


def build_portal() -> DataPortal:
    create_fake_data()
    config = load_config()
    return DataPortal(
        sqlite_store=SQLiteStore(get_path(config, "sqlite_db")),
        parquet_store=ParquetStore(get_path(config, "parquet_root")),
        config=config,
    )


def test_equal_weight_backtest_runs_next_open_execution():
    portal = build_portal()
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


def test_strategy_cannot_access_execution_open_by_design():
    portal = build_portal()
    strategy = EqualWeightStrategy(["510300.SH"], cash_weight=0.02)

    targets = strategy.on_rebalance("2026-01-02", portal, {})

    assert targets["510300.SH"] == 0.98

