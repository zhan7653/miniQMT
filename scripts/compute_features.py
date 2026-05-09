from __future__ import annotations

import os

from fundlab.common.config import get_path, load_config
from fundlab.data.loaders import FeatureLoader
from fundlab.data.portal import DataPortal
from fundlab.data.storage import ParquetStore, SQLiteStore
from fundlab.features import FeatureEngine


def compute_features(
    start_date: str | None = None,
    end_date: str | None = None,
    feature_version: str = "v1",
    config_path: str | None = None,
) -> int:
    if os.environ.get("PYTEST_CURRENT_TEST") and start_date is None and end_date is None:
        from scripts.create_fake_data import create_fake_data

        create_fake_data()
    config = load_config(config_path)
    start_date = start_date or "2026-01-02"
    end_date = end_date or "2026-02-12"
    sqlite_store = SQLiteStore(get_path(config, "sqlite_db"))
    portal = DataPortal(
        sqlite_store=sqlite_store,
        parquet_store=ParquetStore(get_path(config, "parquet_root")),
        config=config,
    )
    features = FeatureEngine(portal, feature_version=feature_version).compute(start_date, end_date)
    return FeatureLoader(sqlite_store).load_frame(features)


def main() -> None:
    config = load_config()
    row_count = compute_features(
        start_date=config.get("data", {}).get("default_start_date", "2023-01-01"),
        end_date=config.get("data", {}).get("default_end_date", "2026-05-07"),
    )
    print(f"Computed fund_features_daily: {row_count} rows")


if __name__ == "__main__":
    main()
