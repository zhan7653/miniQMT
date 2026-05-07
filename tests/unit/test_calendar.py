from fundlab.data.portal import DataPortal
from fundlab.data.storage import ParquetStore, SQLiteStore
from scripts.create_fake_data import create_fake_data


def build_portal() -> DataPortal:
    create_fake_data()
    return DataPortal(
        sqlite_store=SQLiteStore("data/warehouse/sqlite/fundlab.db"),
        parquet_store=ParquetStore("data/warehouse/parquet"),
    )


def test_calendar_queries():
    portal = build_portal()

    assert portal.is_trading_day("2026-01-02") is True
    assert portal.is_trading_day("2026-01-03") is False
    assert portal.next_trading_day("2026-01-02") == "2026-01-05"
    assert portal.previous_trading_day("2026-01-05") == "2026-01-02"

