from fundlab.backtest import BacktestEngine
from fundlab.common.config import get_path, load_config
from fundlab.data.portal import DataPortal
from fundlab.data.storage import ParquetStore, SQLiteStore
from fundlab.strategies import (
    AssetAllocationStrategy,
    DividendValueStrategy,
    EqualWeightStrategy,
    MomentumRotationStrategy,
    ValueMomentumStrategy,
)
from scripts.compute_features import compute_features
from scripts.create_fake_data import create_fake_data


def build_portal() -> DataPortal:
    create_fake_data()
    compute_features()
    config = load_config()
    return DataPortal(SQLiteStore(get_path(config, "sqlite_db")), ParquetStore(get_path(config, "parquet_root")), config)


def test_all_rule_strategies_run_without_direct_data_access():
    portal = build_portal()
    strategies = [
        EqualWeightStrategy(["510300.SH", "510500.SH", "518880.SH"]),
        DividendValueStrategy(),
        MomentumRotationStrategy(),
        AssetAllocationStrategy(),
        ValueMomentumStrategy(),
    ]

    for strategy in strategies:
        recorder = BacktestEngine(portal, strategy, "2026-01-02", "2026-02-12").run()
        assert not recorder.account_daily_frame().empty

