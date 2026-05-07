from __future__ import annotations

from datetime import date

from fundlab.common.config import get_path, load_config
from fundlab.data.loaders import CalendarLoader
from fundlab.data.storage import SQLiteStore
from scripts.init_db import init_db


def update_calendar(start_date: date = date(2026, 1, 1), end_date: date = date(2026, 12, 31)) -> int:
    init_db()
    config = load_config()
    loader = CalendarLoader(SQLiteStore(get_path(config, "sqlite_db")))
    return loader.load_business_days(start_date, end_date)


def main() -> None:
    row_count = update_calendar()
    print(f"Updated trading_calendar with business-day placeholder: {row_count} rows")


if __name__ == "__main__":
    main()

