from __future__ import annotations

import json
import hashlib
from datetime import date

import pandas as pd
import pytest

from fundlab.paper.lifecycle import CreateAccountRequest, PaperAccountLifecycle
from fundlab.paper.repository import PaperLedgerRepository
from fundlab.paper.runner import DailyPaperRunner
from fundlab.paper.strategy_registry import StrategyRegistry
from fundlab.trading import AccountBindings, ExecutionProfile, ResearchRiskProfile


class FakePortal:
    data_version = "complete-v1"
    days = ("2024-01-02", "2024-01-03", "2024-01-04")

    def get_trading_days(self, start_date, end_date):
        start, end = str(start_date), str(end_date)
        return [value for value in self.days if start <= value <= end]

    def is_trading_day(self, value):
        return str(value) in self.days

    def next_trading_day(self, value):
        value = str(value)
        return next((item for item in self.days if item > value), None)

    def get_universe(self, value):
        raise AssertionError("published portal universe must not be queried")

    def get_open_price_for_execution(self, symbol, value):
        return {"2024-01-03": 10.0, "2024-01-04": 11.0}.get(str(value))

    def get_close_price_for_valuation(self, symbol, value):
        return {"2024-01-02": 9.5, "2024-01-03": 10.5, "2024-01-04": 11.5}[str(value)]

    def get_price(self, symbol, value, **kwargs):
        return self.get_close_price_for_valuation(symbol, value)

    def get_features(self, symbols, value, fields=None):
        return pd.DataFrame({"amount_avg_20d": [10_000_000.0]}, index=list(symbols))


class StaticTargetStrategy:
    def __init__(self, targets):
        self.targets = targets

    def on_rebalance(self, *args, **kwargs):
        return dict(self.targets)


def _subject(tmp_path, *, accounts=("alpha",)):
    repository = PaperLedgerRepository(tmp_path / "paper.db")
    lifecycle = PaperAccountLifecycle(repository)
    lifecycle.register_profiles(
        ExecutionProfile("execution", "v1", capacity_fraction=None),
        ResearchRiskProfile("risk", "v1", reject_untrusted_premium_discount=False),
    )
    registry = StrategyRegistry()
    registry.register_config("equal_weight", "v1", {
        "symbols": ["510300.SH", "999999.SH"], "cash_weight": 0.0,
    })
    registry.register_universe("universe-v1", ["510300.SH"])
    for account_id in accounts:
        lifecycle.create(CreateAccountRequest(
            account_id=account_id, name=account_id,
            bindings=AccountBindings("equal_weight", "v1", "universe-v1", "510300.SH", "v1", "v1"),
            execution_profile_id="execution", risk_profile_id="risk", initial_cash=100_000.0,
        ))
    return repository, lifecycle, DailyPaperRunner(repository, lambda *_: FakePortal(), registry)


def test_t_decision_executes_at_t_plus_one_open_and_values_at_close(tmp_path):
    repository, _, runner = _subject(tmp_path)

    first = runner.run_date(date(2024, 1, 2))
    assert first.status == "complete"
    assert repository.table_rows("paper_decisions")[0]["data_version"] == "complete-v1"
    pending = repository.pending_orders("alpha")
    assert pending[0]["execution_date"] == "2024-01-03"

    second = runner.run_date("2024-01-03")
    fill = repository.table_rows("paper_fills")[0]
    snapshot = repository.account_snapshots("alpha")[-1]
    assert second.accounts[0].status == "complete"
    assert fill["price"] == pytest.approx(10.002)
    assert snapshot["market_value"] == fill["quantity"] * 10.5
    assert snapshot["data_version"] == "complete-v1"


def test_rerun_is_idempotent_and_backfill_is_ascending(tmp_path):
    repository, _, runner = _subject(tmp_path)
    results = runner.backfill("2024-01-02", "2024-01-04")
    assert [item.trade_date for item in results] == list(FakePortal.days)
    counts = {table: len(repository.table_rows(table)) for table in
              ("paper_decisions", "paper_orders", "paper_fills", "paper_account_snapshots")}

    repeated = runner.backfill("2024-01-02", "2024-01-04")
    assert all(item.reused for item in repeated)
    assert counts == {table: len(repository.table_rows(table)) for table in counts}


def test_backfill_does_not_depend_on_nullable_bulk_calendar_filter(tmp_path):
    repository, _, runner = _subject(tmp_path)

    class NullableBulkCalendarPortal(FakePortal):
        def get_trading_days(self, start_date, end_date):
            raise ValueError("Cannot mask with non-boolean array containing NA / NaN values")

        def next_trading_day(self, value):
            raise ValueError("Cannot mask with non-boolean array containing NA / NaN values")

    runner.portal_factory = lambda *_: NullableBulkCalendarPortal()
    results = runner.backfill("2024-01-01", "2024-01-04")

    assert [item.trade_date for item in results] == list(FakePortal.days)
    assert len(repository.account_snapshots("alpha")) == 3


def test_bound_universe_drives_equal_weight_without_published_universe(tmp_path):
    repository, _, runner = _subject(tmp_path)
    runner.run_date("2024-01-02")

    decision = repository.table_rows("paper_decisions")[0]
    assert set(json.loads(decision["original_json"])) == {"510300.SH", "cash"}


def test_momentum_strategy_sees_exact_bound_universe(tmp_path):
    repository, lifecycle, runner = _subject(tmp_path)
    symbols = ("510300.SH", "510500.SH", "518880.SH")
    runner.strategies.register_universe("momentum-universe", symbols)
    runner.strategies.register_config("momentum_rotation", "momentum-v1", {"max_positions": 2})
    lifecycle.create(CreateAccountRequest(
        account_id="momentum", name="momentum",
        bindings=AccountBindings("momentum_rotation", "momentum-v1", "momentum-universe",
                                 "510300.SH", "v1", "v1"),
        execution_profile_id="execution", risk_profile_id="risk",
    ))

    class MomentumPortal(FakePortal):
        def get_features(self, requested, value, fields=None):
            assert tuple(requested) == symbols
            return pd.DataFrame({
                "ret_20d": [0.1, 0.3, -0.1], "ret_60d": [0.2, 0.4, 0.5],
                "amount_avg_20d": [1_000_000.0] * 3,
            }, index=list(requested))

    runner.portal_factory = lambda *_: MomentumPortal()
    result = runner.run_date("2024-01-02")
    assert result.status == "complete"
    decision = repository.table_rows("paper_decisions", where="account_id=?", parameters=("momentum",))[0]
    targets = json.loads(decision["original_json"])
    assert set(targets) <= {*symbols, "cash"}
    assert "510500.SH" in targets


def test_unknown_bound_universe_is_account_failure(tmp_path):
    repository, lifecycle, runner = _subject(tmp_path)
    lifecycle.create(CreateAccountRequest(
        account_id="unknown-universe", name="unknown-universe",
        bindings=AccountBindings("equal_weight", "v1", "missing-universe",
                                 "510300.SH", "v1", "v1"),
        execution_profile_id="execution", risk_profile_id="risk",
    ))

    result = runner.run_date("2024-01-02")
    failed = next(item for item in result.accounts if item.account_id == "unknown-universe")
    assert failed.status == "failed"
    assert "unknown bound universe version" in failed.error
    assert repository.account_snapshots("unknown-universe") == ()


def test_universe_versions_are_immutable():
    registry = StrategyRegistry()
    registry.register_universe("default-v1", ["510300.SH", "510500.SH"],
                               cross_border_symbols=["510500.SH"])
    registry.register_universe("default-v1", ["510300.SH", "510500.SH"],
                               cross_border_symbols=["510500.SH"])
    assert registry.get_universe("default-v1") == ("510300.SH", "510500.SH")
    assert registry.get_universe_metadata("default-v1").cross_border_symbols == {"510500.SH"}
    with pytest.raises(ValueError, match="immutable"):
        registry.register_universe("default-v1", ["518880.SH"])
    with pytest.raises(ValueError, match="immutable"):
        registry.register_universe("default-v1", ["510300.SH", "510500.SH"])


def test_strategy_config_query_is_defensive_and_requires_registered_pair():
    registry = StrategyRegistry()
    source = {"symbols": ["510300.SH"], "cash_weight": 0.02}
    registry.register_config("equal_weight", "reviewed-v1", source)

    config = registry.get_config("equal_weight", "reviewed-v1")
    source["cash_weight"] = 0.5
    source["symbols"].append("510500.SH")
    assert config["cash_weight"] == 0.02
    assert config["symbols"] == ["510300.SH"]
    config["symbols"].append("518880.SH")
    assert registry.get_config("equal_weight", "reviewed-v1")["symbols"] == ["510300.SH"]
    with pytest.raises(TypeError):
        config["cash_weight"] = 0.1
    with pytest.raises(ValueError, match="unknown rule strategy"):
        registry.get_config("not-a-strategy", "v1")
    with pytest.raises(ValueError, match="unknown config version"):
        registry.get_config("equal_weight", "missing")
    with pytest.raises(ValueError, match="unknown config version"):
        StrategyRegistry().create("equal_weight", "missing")


def test_replay_rebuilds_from_earliest_parent_date_and_activates_atomically(tmp_path):
    repository, _, runner = _subject(tmp_path)
    runner.backfill("2024-01-02", "2024-01-04")
    old_snapshots = repository.account_snapshots("alpha", 1)
    old_events = repository.list_events("alpha", 1)
    daily_batches = repository.table_rows("paper_date_batches", where="operation='daily'")
    daily_hash = hashlib.sha256(json.dumps(daily_batches, sort_keys=True).encode()).hexdigest()

    replay = runner.replay(
        "alpha", start_date="2024-01-03", target_date="2024-01-04", reason="correct inputs",
    )

    assert replay.status == "complete" and replay.activated
    assert replay.rebuilt_start_date == "2024-01-02"
    assert replay.requested_start_date == "2024-01-03"
    assert repository.get_account("alpha").selected_ledger_version == replay.ledger_version == 2
    assert len(repository.account_snapshots("alpha", 2)) == 3
    assert repository.account_snapshots("alpha", 1) == old_snapshots
    assert repository.list_events("alpha", 1) == old_events
    assert hashlib.sha256(json.dumps(
        repository.table_rows("paper_date_batches", where="operation='daily'"), sort_keys=True,
    ).encode()).hexdigest() == daily_hash
    replay_batches = repository.table_rows("paper_date_batches", where="operation='replay'")
    assert len(replay_batches) == 3
    assert all(row["account_id"] == "alpha" and row["ledger_version"] == 2
               for row in replay_batches)
    assert all(json.loads(row["provenance_json"])["caller"]["entrypoint"] == "replay"
               for row in replay_batches)
    assert not ({row["batch_id"] for row in daily_batches} & {row["batch_id"] for row in replay_batches})

    repeated = runner.replay(
        "alpha", start_date="2024-01-03", target_date="2024-01-04", reason="correct inputs",
    )
    assert repeated.ledger_version == 2 and repeated.reused
    assert len(repository.list_ledger_versions("alpha")) == 2


def test_mixed_selected_versions_repeat_normal_backfill_without_scope_collision(tmp_path):
    repository, _, runner = _subject(tmp_path, accounts=("alpha", "beta"))
    initial = runner.backfill("2024-01-02", "2024-01-03")
    assert all(result.status == "complete" for result in initial)
    replay = runner.replay(
        "alpha", start_date="2024-01-03", target_date="2024-01-03", reason="mixed scope",
    )
    assert replay.activated and repository.get_account("alpha").selected_ledger_version == 2
    assert repository.get_account("beta").selected_ledger_version == 1

    protected_batches = repository.table_rows("paper_date_batches")
    protected_ids = {row["batch_id"] for row in protected_batches}
    protected_hash = hashlib.sha256(json.dumps(
        sorted(protected_batches, key=lambda row: row["scope_key"]), sort_keys=True,
    ).encode()).hexdigest()

    first = runner.backfill("2024-01-04", "2024-01-04")
    assert len(first) == 1 and first[0].status == "complete"
    assert all(not account.reused for account in first[0].accounts)
    mixed_batch_id = first[0].batch_id
    run_count = len(repository.table_rows("paper_daily_runs"))
    batch_count = len(repository.table_rows("paper_date_batches"))

    second = runner.backfill("2024-01-04", "2024-01-04")
    third = runner.backfill("2024-01-04", "2024-01-04")
    for repeated in (second, third):
        assert len(repeated) == 1 and repeated[0].status == "complete"
        assert repeated[0].batch_id == mixed_batch_id
        assert repeated[0].reused
        assert all(account.status == "complete" and account.reused
                   for account in repeated[0].accounts)

    assert len(repository.table_rows("paper_daily_runs")) == run_count
    assert len(repository.table_rows("paper_date_batches")) == batch_count
    unchanged = tuple(row for row in repository.table_rows("paper_date_batches")
                      if row["batch_id"] in protected_ids)
    assert hashlib.sha256(json.dumps(
        sorted(unchanged, key=lambda row: row["scope_key"]), sort_keys=True,
    ).encode()).hexdigest() == protected_hash


def test_reused_is_scoped_to_selected_ledger_version(tmp_path):
    repository, _, runner = _subject(tmp_path)
    runner.backfill("2024-01-02", "2024-01-04")
    assert repository.table_rows(
        "paper_daily_runs",
        where="account_id=? AND ledger_version=? AND trade_date=? AND status='complete'",
        parameters=("alpha", 1, "2024-01-04"),
    )
    replay = runner.replay(
        "alpha", start_date="2024-01-03", target_date="2024-01-03", reason="version reuse scope",
    )
    assert replay.activated and replay.ledger_version == 2
    assert repository.table_rows(
        "paper_daily_runs",
        where="account_id=? AND ledger_version=? AND trade_date=?",
        parameters=("alpha", 2, "2024-01-04"),
    ) == ()

    first = runner.run_date("2024-01-04")
    assert first.status == "complete" and not first.reused
    assert len(first.accounts) == 1 and not first.accounts[0].reused
    assert repository.table_rows(
        "paper_daily_runs",
        where="account_id=? AND ledger_version=? AND trade_date=? AND status='complete'",
        parameters=("alpha", 2, "2024-01-04"),
    )
    run_count = len(repository.table_rows("paper_daily_runs"))
    batch_count = len(repository.table_rows("paper_date_batches"))

    second = runner.run_date("2024-01-04")
    third = runner.run_date("2024-01-04")
    for repeated in (second, third):
        assert repeated.status == "complete" and repeated.reused
        assert repeated.batch_id == first.batch_id
        assert len(repeated.accounts) == 1 and repeated.accounts[0].reused
    assert len(repository.table_rows("paper_daily_runs")) == run_count
    assert len(repository.table_rows("paper_date_batches")) == batch_count


def test_missing_t_close_persists_rejected_decision_without_orders(tmp_path):
    repository, lifecycle, runner = _subject(tmp_path)
    runner.strategies.register("static-target", StaticTargetStrategy)
    runner.strategies.register_config("static-target", "v1", {"targets": {"510500.SH": 1.0}})
    runner.strategies.register_universe("missing-data-v1", ["510500.SH"])
    lifecycle.create(CreateAccountRequest(
        account_id="missing-data", name="missing-data",
        bindings=AccountBindings("static-target", "v1", "missing-data-v1", "510300.SH", "v1", "v1"),
        execution_profile_id="execution", risk_profile_id="risk",
    ))

    class MissingClosePortal(FakePortal):
        def get_close_price_for_valuation(self, symbol, value):
            return None if symbol == "510500.SH" else super().get_close_price_for_valuation(symbol, value)

    runner.portal_factory = lambda *_: MissingClosePortal()

    result = runner.run_date("2024-01-02")
    assert result.status == "complete"
    decision = repository.table_rows("paper_decisions", where="account_id=?", parameters=("missing-data",))[0]
    validation = json.loads(decision["validation_json"])
    assert "missing_required_data" in validation["codes"]
    assert repository.table_rows("paper_orders", where="account_id=?", parameters=("missing-data",)) == ()


@pytest.mark.parametrize("trusted", [False, True])
def test_cross_border_decision_requires_pinned_t_premium_observation(tmp_path, trusted):
    repository, lifecycle, runner = _subject(tmp_path)
    lifecycle.register_profiles(
        ExecutionProfile("execution", "v1", capacity_fraction=None),
        ResearchRiskProfile("cross-risk", "v1", reject_untrusted_premium_discount=True),
    )
    runner.strategies.register("cross-target", StaticTargetStrategy)
    runner.strategies.register_config("cross-target", "v1", {"targets": {"513100.SH": 0.9, "cash": 0.1}})
    runner.strategies.register_universe(
        "cross-v1", ["513100.SH"], cross_border_symbols=["513100.SH"],
    )
    lifecycle.create(CreateAccountRequest(
        account_id="cross", name="cross",
        bindings=AccountBindings("cross-target", "v1", "cross-v1", "510300.SH", "v1", "v1"),
        execution_profile_id="execution", risk_profile_id="cross-risk",
    ))

    class CrossPortal(FakePortal):
        def get_close_price_for_valuation(self, symbol, value):
            return 10.0 if symbol == "513100.SH" else super().get_close_price_for_valuation(symbol, value)

        def get_features(self, symbols, value, fields=None):
            data = {"amount_avg_20d": [1_000_000.0] * len(symbols)}
            if trusted:
                data["premium_discount"] = [0.01] * len(symbols)
            return pd.DataFrame(data, index=list(symbols))

    runner.portal_factory = lambda *_: CrossPortal()
    runner.run_date("2024-01-02")
    decision = repository.table_rows("paper_decisions", where="account_id=?", parameters=("cross",))[0]
    codes = json.loads(decision["validation_json"])["codes"]
    assert ("untrusted_premium_discount" not in codes) is trusted
    orders = repository.table_rows("paper_orders", where="account_id=?", parameters=("cross",))
    assert bool(orders) is trusted
    assert repository.table_rows("paper_fills", where="account_id=?", parameters=("cross",)) == ()


def test_paused_values_and_cancels_without_new_decision_while_closed_is_skipped(tmp_path):
    repository, lifecycle, runner = _subject(tmp_path, accounts=("paused", "closed"))
    runner.run_date("2024-01-02")
    lifecycle.pause("paused")
    lifecycle.close("closed")

    result = runner.run_date("2024-01-03")
    assert [item.account_id for item in result.accounts] == ["paused"]
    assert repository.pending_orders("paused") == ()
    assert len(repository.account_snapshots("paused")) == 2
    assert len(repository.table_rows("paper_decisions", where="account_id=?", parameters=("paused",))) == 1
    assert len(repository.account_snapshots("closed")) == 1
