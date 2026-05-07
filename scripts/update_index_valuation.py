from __future__ import annotations

from fundlab.common.config import get_path, load_config
from fundlab.data.loaders import IndexValuationLoader
from fundlab.data.storage import SQLiteStore
from scripts.create_fake_data import fake_index_valuation_frame
from scripts.init_db import init_db


def update_index_valuation() -> int:
    init_db()
    config = load_config()
    return IndexValuationLoader(SQLiteStore(get_path(config, "sqlite_db"))).load_frame(fake_index_valuation_frame())


def main() -> None:
    row_count = update_index_valuation()
    print(f"Updated index_valuation with fake data: {row_count} rows")


if __name__ == "__main__":
    main()

