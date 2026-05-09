from __future__ import annotations

from datetime import datetime

from fundlab.common.config import get_path, load_config
from fundlab.data.loaders import FundUniverseLoader
from fundlab.data.sources import XtQuantSource
from fundlab.data.storage import SQLiteStore, record_data_update
from scripts.init_db import init_db


def update_universe(config_path: str | None = None) -> int:
    init_db(config_path)
    config = load_config(config_path)
    sqlite_store = SQLiteStore(get_path(config, "sqlite_db"))
    started_at = datetime.now().isoformat(timespec="seconds")
    source = XtQuantSource(config.get("xtquant", {}))
    try:
        source.connect()
        loader = FundUniverseLoader(source=source, sqlite_store=sqlite_store)
        row_count = loader.load()
        record_data_update(
            sqlite_store,
            job_name="update_universe",
            source=source.name,
            table_name="fund_master",
            start_date=None,
            end_date=None,
            row_count=row_count,
            status="success",
            started_at=started_at,
        )
        return row_count
    except Exception as exc:
        record_data_update(
            sqlite_store,
            job_name="update_universe",
            source=source.name,
            table_name="fund_master",
            start_date=None,
            end_date=None,
            row_count=0,
            status="failed",
            error_message=repr(exc),
            started_at=started_at,
        )
        raise


def main() -> None:
    row_count = update_universe()
    print(f"Updated fund_master from xtquant: {row_count} rows")


if __name__ == "__main__":
    main()
