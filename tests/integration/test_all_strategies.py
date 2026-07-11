from fundlab.backtest import BacktestEngine
from fundlab.strategies import (
    AssetAllocationStrategy,
    DividendValueStrategy,
    EqualWeightStrategy,
    MomentumRotationStrategy,
    ValueMomentumStrategy,
)
from scripts.create_fake_data import create_fake_v2_portal


def test_all_rule_strategies_run_without_direct_data_access(tmp_path):
    portal = create_fake_v2_portal(tmp_path / "v2")
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
