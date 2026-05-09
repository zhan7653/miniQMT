from __future__ import annotations

from datetime import date, datetime

from fundlab.common.config import get_path, load_config
from fundlab.data.loaders import IndexValuationLoader
from fundlab.data.sources import XtQuantSource
from fundlab.data.storage import SQLiteStore, record_data_update
from scripts.init_db import init_db


def update_index_valuation(
    start_date: str | None = None,
    end_date: str | None = None,
    config_path: str | None = None,
) -> int:
    init_db(config_path)
    config = load_config(config_path)
    start_text = start_date or config.get("data", {}).get("default_start_date", "2023-01-01")
    end_text = end_date or config.get("data", {}).get("default_end_date", date.today().isoformat())
    sqlite_store = SQLiteStore(get_path(config, "sqlite_db"))
    started_at = datetime.now().isoformat(timespec="seconds")
    source = XtQuantSource(config.get("xtquant", {}))
    try:
        source.connect()
        frame = source.get_index_valuation(start_text, end_text)
        row_count = IndexValuationLoader(sqlite_store).load_frame(frame) if not frame.empty else 0
        record_data_update(
            sqlite_store,
            job_name="update_index_valuation",
            source=source.name,
            table_name="index_valuation",
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
            job_name="update_index_valuation",
            source=source.name,
            table_name="index_valuation",
            start_date=start_text,
            end_date=end_text,
            row_count=0,
            status="failed",
            error_message=repr(exc),
            started_at=started_at,
        )
        raise


def main() -> None:
    row_count = update_index_valuation()
    print(f"Updated index_valuation from xtquant: {row_count} rows")


if __name__ == "__main__":
    main()
