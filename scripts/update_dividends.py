from __future__ import annotations

from fundlab.common.config import get_path, load_config
from fundlab.data.loaders import DividendLoader
from fundlab.data.storage import SQLiteStore
from scripts.create_fake_data import fake_dividend_frame
from scripts.init_db import init_db


def update_dividends() -> int:
    init_db()
    config = load_config()
    return DividendLoader(SQLiteStore(get_path(config, "sqlite_db"))).load_frame(fake_dividend_frame())


def main() -> None:
    row_count = update_dividends()
    print(f"Updated fund_dividend with fake data: {row_count} rows")


if __name__ == "__main__":
    main()

