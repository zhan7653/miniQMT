from fundlab.common.config import get_path, load_config
from fundlab.data.portal import DataPortal
from fundlab.data.storage import ParquetStore, SQLiteStore
from scripts.compute_features import compute_features
from scripts.create_fake_data import create_fake_data


def test_future_feature_available_late_does_not_affect_signal_date():
    create_fake_data()
    compute_features()
    config = load_config()
    sqlite_store = SQLiteStore(get_path(config, "sqlite_db"))
    portal = DataPortal(sqlite_store, ParquetStore(get_path(config, "parquet_root")), config)

    sqlite_store.execute_many(
        """
        INSERT INTO fund_features_daily (date, symbol, feature_version, momentum_score, valuation_score, liquidity_score, available_date)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(date, symbol, feature_version) DO UPDATE SET
            momentum_score=excluded.momentum_score,
            valuation_score=excluded.valuation_score,
            liquidity_score=excluded.liquidity_score,
            available_date=excluded.available_date
        """,
        [["2026-01-09", "510300.SH", "poison", 1.0, 1.0, 1.0, "2026-01-12"]],
    )

    hidden = portal.get_features(["510300.SH"], "2026-01-09", feature_version="poison", asof="2026-01-09")
    visible = portal.get_features(["510300.SH"], "2026-01-09", feature_version="poison", asof="2026-01-12")

    assert hidden.empty
    assert not visible.empty

