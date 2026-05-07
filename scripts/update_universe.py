from __future__ import annotations

from fundlab.common.config import get_path, load_config
from fundlab.data.loaders import FundUniverseLoader
from fundlab.data.sources import ManualSource
from fundlab.data.storage import SQLiteStore
from scripts.init_db import init_db


def update_universe(config_path: str = "config/base.yaml") -> int:
    init_db(config_path)
    config = load_config(config_path)
    loader = FundUniverseLoader(
        source=ManualSource("data/raw/manual"),
        sqlite_store=SQLiteStore(get_path(config, "sqlite_db")),
    )
    return loader.load()


def main() -> None:
    row_count = update_universe()
    print(f"Updated fund_master from manual source: {row_count} rows")


if __name__ == "__main__":
    main()

