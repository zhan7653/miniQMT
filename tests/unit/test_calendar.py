from fundlab.common.config import get_path, load_config
from fundlab.data.portal import DataPortal
from fundlab.data.storage import ParquetStore, SQLiteStore
from scripts.create_fake_data import create_fake_data


def build_portal() -> DataPortal:
    create_fake_data()
    config = load_config()
    return DataPortal(
        sqlite_store=SQLiteStore(get_path(config, "sqlite_db")),
        parquet_store=ParquetStore(get_path(config, "parquet_root")),
    )


def test_calendar_queries():
    portal = build_portal()

    assert portal.is_trading_day("2026-01-02") is True
    assert portal.is_trading_day("2026-01-03") is False
    assert portal.next_trading_day("2026-01-02") == "2026-01-05"
    assert portal.previous_trading_day("2026-01-05") == "2026-01-02"
