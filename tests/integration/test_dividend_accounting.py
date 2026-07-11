from fundlab.backtest import BacktestEngine
from fundlab.strategies import EqualWeightStrategy
from scripts.create_fake_data import create_fake_v2_portal


def test_untrusted_legacy_dividends_are_not_accrued(tmp_path):
    portal = create_fake_v2_portal(tmp_path / "v2")
    recorder = BacktestEngine(
        data_portal=portal,
        strategy=EqualWeightStrategy(["510300.SH"], cash_weight=0.02),
        start_date="2026-01-02",
        end_date="2026-01-20",
        initial_cash=1_000_000,
        rebalance_frequency="monthly",
    ).run()

    event_types = [event["event_type"] for event in recorder.account_events]
    paid_events = [event for event in recorder.account_events if event["event_type"] == "dividend_paid"]

    assert "dividend_receivable" not in event_types
    assert not paid_events
