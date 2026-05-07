from __future__ import annotations

from fundlab.common.config import get_path, load_config
from fundlab.data.loaders import FeatureLoader
from fundlab.data.portal import DataPortal
from fundlab.data.storage import ParquetStore, SQLiteStore
from fundlab.features import FeatureEngine
from scripts.create_fake_data import create_fake_data


def compute_features(start_date: str = "2026-01-02", end_date: str = "2026-02-12", feature_version: str = "v1") -> int:
    create_fake_data()
    config = load_config()
    sqlite_store = SQLiteStore(get_path(config, "sqlite_db"))
    portal = DataPortal(
        sqlite_store=sqlite_store,
        parquet_store=ParquetStore(get_path(config, "parquet_root")),
        config=config,
    )
    features = FeatureEngine(portal, feature_version=feature_version).compute(start_date, end_date)
    return FeatureLoader(sqlite_store).load_frame(features)


def main() -> None:
    row_count = compute_features()
    print(f"Computed fund_features_daily: {row_count} rows")


if __name__ == "__main__":
    main()

