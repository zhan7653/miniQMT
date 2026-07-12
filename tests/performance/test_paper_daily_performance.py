from __future__ import annotations

from time import perf_counter

from fundlab.paper.lifecycle import CreateAccountRequest, PaperAccountLifecycle
from fundlab.paper.repository import PaperLedgerRepository
from fundlab.paper.runner import DailyPaperRunner
from fundlab.paper.strategy_registry import StrategyRegistry
from fundlab.trading import AccountBindings, ExecutionProfile, ResearchRiskProfile
from scripts.create_fake_data import create_fake_v2_portal


def test_ten_accounts_advance_one_published_day_under_thirty_seconds(tmp_path):
    """Measured isolated published-data evidence for AC-15."""
    portal = create_fake_v2_portal(tmp_path / "published")
    repository = PaperLedgerRepository(tmp_path / "paper.sqlite3")
    lifecycle = PaperAccountLifecycle(repository)
    lifecycle.register_profiles(
        ExecutionProfile("performance", "v1", capacity_fraction=0.05),
        ResearchRiskProfile("performance-risk", "v1", reject_untrusted_premium_discount=False),
    )
    registry = StrategyRegistry()
    registry.register_universe(
        "fake-universe-v1", ["510300.SH", "510500.SH", "518880.SH"]
    )
    registry.register_config("equal_weight", "performance-v1", {
        "symbols": ["510300.SH", "510500.SH", "518880.SH"], "cash_weight": 0.0,
    })
    for index in range(10):
        lifecycle.create(CreateAccountRequest(
            account_id=f"account-{index:02d}", name=f"Account {index:02d}",
            bindings=AccountBindings("equal_weight", "performance-v1", "fake-universe-v1",
                                     "510300.SH", "v1", "v1"),
            execution_profile_id="performance", risk_profile_id="performance-risk",
        ))

    runner = DailyPaperRunner(repository, lambda *_: portal, registry)
    started = perf_counter()
    result = runner.run_date("2026-01-02")
    elapsed = perf_counter() - started

    assert len(result.accounts) == 10
    assert all(account.status == "complete" for account in result.accounts)
    assert len(repository.table_rows("paper_account_snapshots")) == 10
    assert elapsed < 30.0, f"10-account daily advance took {elapsed:.3f}s"
