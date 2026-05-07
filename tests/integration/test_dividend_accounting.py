from fundlab.backtest import BacktestEngine
from fundlab.common.config import get_path, load_config
from fundlab.data.portal import DataPortal
from fundlab.data.storage import ParquetStore, SQLiteStore
from fundlab.strategies import EqualWeightStrategy
from scripts.create_fake_data import create_fake_data


def build_portal() -> DataPortal:
    create_fake_data()
    config = load_config()
    return DataPortal(SQLiteStore(get_path(config, "sqlite_db")), ParquetStore(get_path(config, "parquet_root")), config)


def test_dividend_cash_is_accrued_and_paid():
    portal = build_portal()
    recorder = BacktestEngine(
        data_portal=portal,
        strategy=EqualWeightStrategy(["510300.SH"], cash_weight=0.02),
        start_date="2026-01-02",
        end_date="2026-01-20",
        initial_cash=1_000_000,
        rebalance_frequency="monthly",
    ).run()

    event_types = [event["event_type"] for event in recorder.account_events]
    paid_events = [event for event in recorder.account_events if event["event_type"] == "dividend_paid"]

    assert "dividend_receivable" in event_types
    assert paid_events
    assert paid_events[0]["amount"] > 0

