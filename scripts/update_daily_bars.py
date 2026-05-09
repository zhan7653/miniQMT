from __future__ import annotations

from datetime import date, datetime

from fundlab.common.config import get_path, load_config
from fundlab.data.loaders import DailyBarLoader
from fundlab.data.sources import XtQuantSource
from fundlab.data.storage import SQLiteStore, record_data_update
from scripts.init_db import init_db


def update_daily_bars(
    start_date: str | None = None,
    end_date: str | None = None,
    symbols: list[str] | None = None,
    config_path: str | None = None,
) -> int:
    init_db(config_path)
    config = load_config(config_path)
    start_text = start_date or config.get("data", {}).get("default_start_date", "2023-01-01")
    end_text = end_date or config.get("data", {}).get("default_end_date", date.today().isoformat())
    sqlite_store = SQLiteStore(get_path(config, "sqlite_db"))
    started_at = datetime.now().isoformat(timespec="seconds")

    if symbols is None:
        fund_master = sqlite_store.get_fund_master()
        symbols = fund_master.loc[fund_master["include_in_universe"] == 1, "symbol"].tolist()
    if not symbols:
        raise ValueError("No symbols to update. Run scripts.update_universe first or pass symbols.")

    source = XtQuantSource(config.get("xtquant", {}))
    try:
        source.connect()
        source.download_daily_bar(symbols, start_text, end_text)
        frame = source.get_daily_bar(symbols, start_text, end_text)
        paths = DailyBarLoader(get_path(config, "parquet_root")).load_frame_by_year(frame)
        row_count = int(len(frame))
        record_data_update(
            sqlite_store,
            job_name="update_daily_bars",
            source=source.name,
            table_name="fund_daily_bar",
            start_date=start_text,
            end_date=end_text,
            row_count=row_count,
            status="success",
            started_at=started_at,
            extra={"files": len(paths), "symbols": len(symbols)},
        )
        return row_count
    except Exception as exc:
        record_data_update(
            sqlite_store,
            job_name="update_daily_bars",
            source=source.name,
            table_name="fund_daily_bar",
            start_date=start_text,
            end_date=end_text,
            row_count=0,
            status="failed",
            error_message=repr(exc),
            started_at=started_at,
            extra={"symbols": len(symbols)},
        )
        raise


def main() -> None:
    row_count = update_daily_bars()
    print(f"Updated fund_daily_bar from xtquant: {row_count} rows")


if __name__ == "__main__":
    main()
