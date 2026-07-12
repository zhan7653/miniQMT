from __future__ import annotations

import json

import pytest

from fundlab.backtest import BacktestEngine
from fundlab.paper.lifecycle import CreateAccountRequest, PaperAccountLifecycle
from fundlab.paper.repository import PaperLedgerRepository
from fundlab.paper.runner import DailyPaperRunner
from fundlab.paper.strategy_registry import StrategyRegistry
from fundlab.strategies import EqualWeightStrategy
from fundlab.trading import AccountBindings, ExecutionProfile, ResearchRiskProfile
from scripts.create_fake_data import create_fake_v2_portal


ABS_TOLERANCE = 1e-8


class _BoundedPortal:
    def __init__(self, portal, end_date: str):
        self._portal = portal
        self.end_date = end_date
        self.data_version = portal.data_version

    def __getattr__(self, name):
        return getattr(self._portal, name)

    def next_trading_day(self, value):
        next_day = self._portal.next_trading_day(value)
        return next_day if next_day is not None and next_day <= self.end_date else None


def test_frozen_daily_paper_matches_shared_backtest_ledger(tmp_path):
    """Exact frozen-input parity evidence for AC-8 and AC-12."""
    portal = create_fake_v2_portal(tmp_path / "published")
    symbols = ["510300.SH", "510500.SH", "518880.SH"]
    days = portal.get_trading_days("2026-01-02", "2026-01-15")
    bounded_portal = _BoundedPortal(portal, days[-1])
    strategy = EqualWeightStrategy(symbols, cash_weight=0.0)
    backtest = BacktestEngine(
        bounded_portal, strategy, days[0], days[-1], initial_cash=1_000_000.0, rebalance_frequency="daily",
    ).run()

    repository = PaperLedgerRepository(tmp_path / "paper.sqlite3")
    lifecycle = PaperAccountLifecycle(repository)
    lifecycle.register_profiles(
        ExecutionProfile("parity", "v1", commission_bps=0.3, slippage_bps=0.0, capacity_fraction=None),
        ResearchRiskProfile("parity-risk", "v1", reject_untrusted_premium_discount=False),
    )
    lifecycle.create(CreateAccountRequest(
        account_id="paper", name="paper", initial_cash=1_000_000.0,
        bindings=AccountBindings("equal_weight", "frozen-v1", "fake-universe-v1", "510300.SH", "v1", "v1"),
        execution_profile_id="parity", risk_profile_id="parity-risk",
    ))
    registry = StrategyRegistry()
    registry.register_universe("fake-universe-v1", symbols)
    registry.register_config("equal_weight", "frozen-v1", {"symbols": symbols, "cash_weight": 0.0})
    DailyPaperRunner(repository, lambda *_: bounded_portal, registry).backfill(days[0], days[-1])

    paper_snapshots = repository.account_snapshots("paper")
    paper_positions = repository.table_rows("paper_daily_positions", where="account_id=?", parameters=("paper",))
    paper_orders = repository.table_rows("paper_orders", where="account_id=?", parameters=("paper",))
    paper_fills = repository.table_rows("paper_fills", where="account_id=?", parameters=("paper",))
    paper_decisions = repository.table_rows("paper_decisions", where="account_id=?", parameters=("paper",))
    backtest_daily = backtest.account_daily_frame().to_dict("records")
    backtest_positions = backtest.position_daily

    assert len(paper_snapshots) == len(backtest_daily)
    for paper, reference in zip(paper_snapshots, backtest_daily, strict=True):
        assert paper["trade_date"] == reference["date"]
        assert paper["cash"] == pytest.approx(reference["cash"], abs=ABS_TOLERANCE)
        assert paper["market_value"] == pytest.approx(reference["market_value"], abs=ABS_TOLERANCE)
        assert paper["total_asset"] == pytest.approx(reference["total_asset"], abs=ABS_TOLERANCE)
        assert paper["nav"] == pytest.approx(reference["nav"], abs=ABS_TOLERANCE)

    paper_position_key = {(row["trade_date"], row["symbol"]): row["quantity"] for row in paper_positions}
    backtest_position_key = {(row["date"], row["symbol"]): row["quantity"] for row in backtest_positions}
    assert paper_position_key == backtest_position_key
    assert [json.loads(row["target_weights_json"]) for row in paper_decisions] == [
        dict(decision.target_weights) for decision in backtest.decisions
    ]
    assert [(row["execution_date"], row["symbol"], row["requested_quantity"], row["actual_quantity"])
            for row in paper_orders if row["requested_quantity"]] == [
                (order.execution_date, order.symbol, order.requested_quantity, order.quantity)
                for order in backtest.orders
            ]
    assert len(paper_fills) == len(backtest.trades)
    for paper, reference in zip(paper_fills, backtest.trades, strict=True):
        assert paper["fill_date"] == reference.date
        assert paper["quantity"] == reference.quantity
        assert paper["price"] == pytest.approx(reference.price, abs=ABS_TOLERANCE)
