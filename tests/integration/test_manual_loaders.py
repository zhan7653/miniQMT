from datetime import date

from fundlab.common.config import get_path, load_config
from fundlab.data.loaders import CalendarLoader, FundUniverseLoader
from fundlab.data.sources import ManualSource
from fundlab.data.storage import SQLiteStore
from scripts.init_db import init_db


def test_manual_universe_loader_and_calendar_loader():
    init_db()
    config = load_config()
    sqlite_store = SQLiteStore(get_path(config, "sqlite_db"))
    sqlite_store.execute_many("DELETE FROM fund_master", [])
    sqlite_store.execute_many("DELETE FROM trading_calendar", [])

    universe_count = FundUniverseLoader(ManualSource("data/raw/manual"), sqlite_store).load()
    calendar_count = CalendarLoader(sqlite_store).load_business_days(date(2026, 1, 1), date(2026, 1, 9))

    assert universe_count == 3
    assert calendar_count == 7
    assert sqlite_store.get_fund_master()["symbol"].tolist() == ["510300.SH", "510500.SH", "518880.SH"]
    assert sqlite_store.get_trading_days("2026-01-01", "2026-01-09")["date"].tolist() == [
        "2026-01-01",
        "2026-01-02",
        "2026-01-05",
        "2026-01-06",
        "2026-01-07",
        "2026-01-08",
        "2026-01-09",
    ]
