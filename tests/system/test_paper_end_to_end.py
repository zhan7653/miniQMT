from __future__ import annotations

import json
from collections import Counter

import pytest

from fundlab.paper.lifecycle import CreateAccountRequest, PaperAccountLifecycle
from fundlab.paper.reporting import CORPORATE_ACTION_DISCLOSURE, build_paper_report
from fundlab.paper.repository import PaperLedgerRepository
from fundlab.paper.runner import DailyPaperRunner, ReplayRunError
from fundlab.paper.strategy_registry import StrategyRegistry
from fundlab.trading import AccountBindings, ExecutionProfile, ResearchRiskProfile
from scripts.create_fake_data import create_fake_v2_portal


SYMBOLS = ("510300.SH", "510500.SH", "518880.SH")


class _TrackingPortal:
    def __init__(self, portal):
        self._portal = portal
        self.data_version = portal.data_version
        self.liquidity_feature_dates: list[str] = []

    def __getattr__(self, name):
        return getattr(self._portal, name)

    def get_features(self, symbols, value, fields=None):
        if fields == ["amount_avg_20d"]:
            self.liquidity_feature_dates.append(str(value))
        return self._portal.get_features(symbols, value, fields=fields)


def _integrated_subject(tmp_path):
    portal = _TrackingPortal(create_fake_v2_portal(tmp_path / "published"))
    repository = PaperLedgerRepository(tmp_path / "paper.sqlite3")
    lifecycle = PaperAccountLifecycle(repository)
    lifecycle.register_profiles(
        ExecutionProfile("etf_default", "v1", capacity_fraction=0.05),
        ResearchRiskProfile("research_default", "v1", reject_untrusted_premium_discount=False),
    )
    registry = StrategyRegistry()
    registry.register_universe("reviewed-v1", list(SYMBOLS))
    registry.register_config("equal_weight", "reviewed_v1", {"symbols": list(SYMBOLS), "cash_weight": 0.0})
    registry.register_config("momentum_rotation", "reviewed_v1", {"max_positions": 3, "cash_weight": 0.0})
    for account_id, strategy_id in (
        ("equal_weight_reviewed_v1", "equal_weight"),
        ("momentum_reviewed_v1", "momentum_rotation"),
    ):
        lifecycle.create(CreateAccountRequest(
            account_id=account_id,
            name=account_id,
            bindings=AccountBindings(strategy_id, "reviewed_v1", "reviewed-v1", "510300.SH", "v1", "v1"),
            execution_profile_id="etf_default",
            risk_profile_id="research_default",
            initial_cash=1_000_000.0,
        ))
    return portal, repository, lifecycle, DailyPaperRunner(repository, lambda *_: portal, registry)


def test_default_accounts_are_isolated_auditable_idempotent_and_report_healthy(tmp_path):
    """Integrated evidence for AC-1/2/5/8/9/10/11/13/14/17."""
    portal, repository, _, runner = _integrated_subject(tmp_path)
    days = portal.get_trading_days("2026-01-02", "2026-02-12")
    results = runner.backfill(days[0], days[-1])
    assert len(results) == len(days) and all(result.status == "complete" for result in results)

    equal, momentum = (repository.get_account(account_id) for account_id in
                       ("equal_weight_reviewed_v1", "momentum_reviewed_v1"))
    assert equal.bindings.strategy_id == "equal_weight"
    assert momentum.bindings.strategy_id == "momentum_rotation"
    assert equal.account_id != momentum.account_id
    for account in (equal, momentum):
        snapshots = repository.account_snapshots(account.account_id)
        decisions = repository.table_rows("paper_decisions", where="account_id=?", parameters=(account.account_id,))
        orders = repository.table_rows("paper_orders", where="account_id=?", parameters=(account.account_id,))
        fills = repository.table_rows("paper_fills", where="account_id=?", parameters=(account.account_id,))
        assert len(snapshots) == len(days)
        assert decisions and orders and fills
        assert {row["account_id"] for row in decisions + orders + fills + snapshots} == {account.account_id}
        assert {row["data_version"] for row in decisions + fills + snapshots} == {portal.data_version}
        assert all(row["execution_date"] > decisions[0]["decision_date"] for row in orders)
        assert all(row["price"] > 0 for row in fills)
        assert repository.list_events(account.account_id)

        benchmark = {day: portal.get_close_price_for_valuation("510300.SH", day) for day in days}
        bundle = build_paper_report(repository, account.account_id, benchmark_closes=benchmark)
        payload = json.loads(bundle.to_json())
        required = {"total_return", "rolling_returns", "annualized_volatility", "downside_volatility",
                    "max_drawdown", "sharpe", "sortino", "calmar"}
        assert required <= set(payload["returns_and_risk"])
        assert {"benchmark", "excess", "costs", "turnover", "exposure", "data_health"} <= set(payload)
        assert payload["data_health"]["corporate_actions_complete"] is False
        assert payload["data_health"]["total_return_complete"] is False
        assert CORPORATE_ACTION_DISCLOSURE in bundle.to_markdown()
        unavailable = payload["returns_and_risk"]["rolling_returns"]["60d"]
        assert unavailable["value"] is None and unavailable["reason"]

    before = {table: len(repository.table_rows(table)) for table in
              ("paper_decisions", "paper_orders", "paper_fills", "paper_account_snapshots", "paper_event_ledger")}
    repeated = runner.backfill(days[0], days[-1])
    assert all(result.reused for result in repeated)
    assert before == {table: len(repository.table_rows(table)) for table in before}

    decision_dates = {row["decision_id"]: row["decision_date"]
                      for row in repository.table_rows("paper_decisions")}
    executed_orders = [row for row in repository.table_rows("paper_orders")
                       if row["actual_quantity"]]
    assert Counter(portal.liquidity_feature_dates) == Counter(
        decision_dates[row["decision_id"]] for row in executed_orders
    )


def test_lifecycle_and_explicit_replay_preserve_prior_history(tmp_path):
    """Integrated evidence for AC-3/4/16 and replay immutability.

    AC-6/7 are injected by the companion failure-boundary test in exact VAL-4.
    """
    portal, repository, lifecycle, runner = _integrated_subject(tmp_path)
    days = portal.get_trading_days("2026-01-02", "2026-01-12")
    runner.run_date(days[0])
    lifecycle.pause("equal_weight_reviewed_v1")
    runner.run_date(days[1])
    assert repository.pending_orders("equal_weight_reviewed_v1") == ()
    assert len(repository.account_snapshots("equal_weight_reviewed_v1")) == 2
    assert len(repository.table_rows("paper_decisions", where="account_id=?",
                                     parameters=("equal_weight_reviewed_v1",))) == 1

    lifecycle.resume("equal_weight_reviewed_v1")
    runner.run_date(days[2])
    runner.run_date(days[3])
    assert repository.current_positions("equal_weight_reviewed_v1")
    lifecycle.close("equal_weight_reviewed_v1")
    closed_snapshot_count = len(repository.account_snapshots("equal_weight_reviewed_v1"))
    runner.run_date(days[4])
    assert len(repository.account_snapshots("equal_weight_reviewed_v1")) == closed_snapshot_count
    assert repository.current_positions("equal_weight_reviewed_v1")

    replay_account = "momentum_reviewed_v1"
    old_snapshots = repository.account_snapshots(replay_account, 1)
    old_events = repository.list_events(replay_account, 1)
    replay = runner.replay(
        replay_account, reason="corrected frozen input",
        start_date=days[1], target_date=days[3],
    )
    assert replay.status == "complete" and replay.activated and not replay.reused
    assert replay.ledger_version == 2 and replay.parent_version == 1
    assert replay.rebuilt_start_date == days[0]
    assert replay.required_dates == tuple(days[:4])
    validation = repository.validate_replay_version(
        replay_account, replay.ledger_version, replay.required_dates,
    )
    assert validation.complete
    v2_runs = repository.table_rows(
        "paper_daily_runs", where="account_id=? AND ledger_version=?",
        parameters=(replay_account, replay.ledger_version),
    )
    assert {row["trade_date"] for row in v2_runs if row["status"] == "complete"} == set(replay.required_dates)
    assert repository.account_snapshots(replay_account, 1) == old_snapshots
    assert repository.list_events(replay_account, 1) == old_events
    selected = repository.get_account(replay_account)
    v2_snapshots = repository.account_snapshots(replay_account, replay.ledger_version)
    assert selected.selected_ledger_version == replay.ledger_version
    assert selected.cash == v2_snapshots[-1]["cash"]

    counts = {table: len(repository.table_rows(table)) for table in (
        "paper_ledger_versions", "paper_daily_runs", "paper_account_snapshots",
        "paper_daily_positions", "paper_decisions", "paper_orders", "paper_fills", "paper_event_ledger",
    )}
    repeated = runner.replay(
        replay_account, reason="corrected frozen input",
        start_date=days[1], target_date=days[3],
    )
    assert repeated.ledger_version == replay.ledger_version and repeated.reused and repeated.activated
    assert counts == {table: len(repository.table_rows(table)) for table in counts}

    parent = repository.get_account(replay_account)
    parent_snapshots = repository.account_snapshots(replay_account, replay.ledger_version)

    def fail_replay_finalize(*_):
        raise RuntimeError("injected replay finalize failure")

    runner.system_finalize = fail_replay_finalize
    with pytest.raises(ReplayRunError, match="injected replay finalize failure") as raised:
        runner.replay(
            replay_account, reason="failed corrected input",
            start_date=days[2], target_date=days[3],
        )
    failed = raised.value.result
    after_failure = repository.get_account(replay_account)
    assert not failed.activated and failed.status == "failed"
    assert after_failure.selected_ledger_version == parent.selected_ledger_version
    assert after_failure.cash == parent.cash
    assert repository.account_snapshots(replay_account, replay.ledger_version) == parent_snapshots
    assert any(event["event_type"] == "replay_failed"
               for event in repository.list_events(replay_account, failed.ledger_version))
