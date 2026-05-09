from __future__ import annotations

from datetime import date, datetime

from fundlab.common.config import get_path, load_config
from fundlab.data.loaders import CalendarLoader
from fundlab.data.sources import XtQuantSource
from fundlab.data.storage import SQLiteStore, record_data_update
from scripts.init_db import init_db


def update_calendar(
    start_date: date | str | None = None,
    end_date: date | str | None = None,
    config_path: str | None = None,
) -> int:
    init_db(config_path)
    config = load_config(config_path)
    start_date = start_date or config.get("data", {}).get("default_start_date", "2023-01-01")
    end_date = end_date or config.get("data", {}).get("default_end_date", date.today().isoformat())
    start_text = start_date.isoformat() if isinstance(start_date, date) else str(start_date)
    end_text = end_date.isoformat() if isinstance(end_date, date) else str(end_date)
    started_at = datetime.now().isoformat(timespec="seconds")
    loader = CalendarLoader(SQLiteStore(get_path(config, "sqlite_db")))
    sqlite_store = SQLiteStore(get_path(config, "sqlite_db"))
    try:
        source = XtQuantSource(config.get("xtquant", {}))
        source.connect()
        frame = source.get_trading_calendar(start_text, end_text)
        row_count = loader.load_frame(frame)
        record_data_update(
            sqlite_store,
            job_name="update_calendar",
            source=source.name,
            table_name="trading_calendar",
            start_date=start_text,
            end_date=end_text,
            row_count=row_count,
            status="success",
            started_at=started_at,
        )
        return row_count
    except Exception as exc:
        record_data_update(
            sqlite_store,
            job_name="update_calendar",
            source="xtquant",
            table_name="trading_calendar",
            start_date=start_text,
            end_date=end_text,
            row_count=0,
            status="failed",
            error_message=repr(exc),
            started_at=started_at,
        )
        raise


def main() -> None:
    row_count = update_calendar()
    print(f"Updated trading_calendar from xtquant: {row_count} rows")


if __name__ == "__main__":
    main()
