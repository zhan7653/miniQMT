import pandas as pd

from fundlab.common.config import get_path, load_config
from fundlab.data.loaders import FeatureLoader
from fundlab.data.portal import DataPortal
from fundlab.data.storage import ParquetStore, SQLiteStore
from fundlab.features import FeatureEngine
from scripts.create_fake_data import create_fake_data
from scripts.compute_features import compute_features


def build_portal() -> DataPortal:
    create_fake_data()
    config = load_config()
    return DataPortal(
        sqlite_store=SQLiteStore(get_path(config, "sqlite_db")),
        parquet_store=ParquetStore(get_path(config, "parquet_root")),
        config=config,
    )


def test_feature_engine_computes_warmup_nulls_and_scores():
    portal = build_portal()
    features = FeatureEngine(portal).compute("2026-01-02", "2026-02-12")

    assert len(features) == 90
    early = features[(features["date"] == "2026-01-08") & (features["symbol"] == "510300.SH")].iloc[0]
    later = features[(features["date"] == "2026-02-02") & (features["symbol"] == "510300.SH")].iloc[0]

    assert pd.isna(early["ret_20d"])
    assert pd.notna(later["ret_20d"])
    assert pd.notna(later["amount_avg_20d"])


def test_get_features_respects_available_date():
    portal = build_portal()
    config = load_config()
    sqlite_store = SQLiteStore(get_path(config, "sqlite_db"))
    rows = pd.DataFrame(
        [
            {
                "date": "2026-01-09",
                "symbol": "510300.SH",
                "feature_version": "v_future_test",
                "ret_20d": 9.99,
                "available_date": "2026-01-12",
            }
        ]
    )
    FeatureLoader(sqlite_store).load_frame(rows)

    hidden = portal.get_features(["510300.SH"], "2026-01-09", feature_version="v_future_test", asof="2026-01-09")
    visible = portal.get_features(["510300.SH"], "2026-01-09", feature_version="v_future_test", asof="2026-01-12")

    assert hidden.empty
    assert visible.loc["510300.SH", "ret_20d"] == 9.99


def test_compute_features_script_loads_features():
    row_count = compute_features()
    config = load_config()
    sqlite_store = SQLiteStore(get_path(config, "sqlite_db"))

    assert row_count == 90
    assert sqlite_store.read_frame("SELECT COUNT(*) AS count FROM fund_features_daily").iloc[0]["count"] >= 90

