from __future__ import annotations

import hashlib
import json
import pytest

from fundlab.paper.lifecycle import CreateAccountRequest, PaperAccountLifecycle
from fundlab.paper.repository import PaperLedgerRepository
from fundlab.paper.runner import DailyPaperRunner, PreflightError, ReplayRunError
from fundlab.paper.strategy_registry import StrategyRegistry
from fundlab.trading import AccountBindings, ExecutionProfile, ResearchRiskProfile
from tests.integration.test_paper_daily_runner import FakePortal


class BrokenStrategy:
    should_fail = True

    def on_rebalance(self, *args, **kwargs):
        if self.should_fail:
            raise RuntimeError("account strategy failure")
        return {"510300.SH": 0.95, "cash": 0.05}


def _repository(tmp_path):
    repository = PaperLedgerRepository(tmp_path / "paper.db")
    lifecycle = PaperAccountLifecycle(repository)
    lifecycle.register_profiles(
        ExecutionProfile("execution", "v1", capacity_fraction=None),
        ResearchRiskProfile("risk", "v1", reject_untrusted_premium_discount=False),
    )
    registry = StrategyRegistry()
    registry.register("broken", BrokenStrategy)
    registry.register_config("equal_weight", "v1", {"symbols": ["510300.SH"]})
    registry.register_config("broken", "v1", {})
    registry.register_universe("u1", ["510300.SH"])
    for account_id, strategy_id in (("bad", "broken"), ("good", "equal_weight")):
        lifecycle.create(CreateAccountRequest(
            account_id=account_id, name=account_id,
            bindings=AccountBindings(strategy_id, "v1", "u1", "510300.SH", "v1", "v1"),
            execution_profile_id="execution", risk_profile_id="risk",
        ))
    return repository, registry


def test_account_failure_rolls_back_only_its_savepoint(tmp_path):
    repository, registry = _repository(tmp_path)
    result = DailyPaperRunner(repository, lambda *_: FakePortal(), registry).run_date("2024-01-02")

    states = {item.account_id: item.status for item in result.accounts}
    assert states == {"bad": "failed", "good": "complete"}
    assert result.status == "failed"
    assert repository.account_snapshots("bad") == ()
    assert len(repository.account_snapshots("good")) == 1
    assert repository.table_rows("paper_daily_runs", where="account_id=?", parameters=("bad",))[0]["status"] == "failed"


def test_all_account_failures_cannot_appear_successful_and_can_retry(tmp_path):
    repository, registry = _repository(tmp_path)
    repository.connection.execute("UPDATE paper_accounts SET status='closed' WHERE account_id='good'")
    runner = DailyPaperRunner(repository, lambda *_: FakePortal(), registry)

    BrokenStrategy.should_fail = True
    failed = runner.run_date("2024-01-02")
    assert failed.status == "failed"
    assert failed.accounts[0].status == "failed"

    BrokenStrategy.should_fail = False
    retried = runner.run_date("2024-01-02")
    assert retried.status == "complete"
    assert retried.accounts[0].status == "complete"
    BrokenStrategy.should_fail = True


def test_system_failure_rolls_back_whole_date_batch(tmp_path):
    repository, registry = _repository(tmp_path)

    def fail_batch(*_):
        raise RuntimeError("system finalize failure")

    runner = DailyPaperRunner(repository, lambda *_: FakePortal(), registry,
                              system_finalize=fail_batch)
    with pytest.raises(RuntimeError, match="system finalize failure"):
        runner.run_date("2024-01-02")

    assert repository.table_rows("paper_account_snapshots") == ()
    assert repository.table_rows("paper_daily_runs") == ()
    batch = repository.table_rows("paper_date_batches")[0]
    assert batch["status"] == "failed"
    assert "system finalize failure" in batch["error_json"]


def test_preflight_failure_records_system_batch_without_account_writes(tmp_path):
    repository, registry = _repository(tmp_path)

    class IncompletePortal(FakePortal):
        data_version = ""

    with pytest.raises(PreflightError, match="not pinned"):
        DailyPaperRunner(repository, lambda *_: IncompletePortal(), registry).run_date("2024-01-02")

    assert repository.table_rows("paper_daily_runs") == ()
    assert repository.table_rows("paper_account_snapshots") == ()
    batch = repository.table_rows("paper_date_batches")[0]
    assert batch["status"] == "failed"
    assert batch["data_version"] == "unavailable"


def test_replay_account_failure_retains_parent_selection_cash_and_failure_metadata(tmp_path):
    repository, registry = _repository(tmp_path)
    BrokenStrategy.should_fail = False
    runner = DailyPaperRunner(repository, lambda *_: FakePortal(), registry)
    runner.backfill("2024-01-02", "2024-01-03")
    parent = repository.get_account("bad")
    parent_rows = repository.account_snapshots("bad", 1)

    BrokenStrategy.should_fail = True
    with pytest.raises(ReplayRunError) as raised:
        runner.replay("bad", start_date="2024-01-03", target_date="2024-01-03", reason="bad replay")

    result = raised.value.result
    assert result.status == "failed" and not result.activated
    current = repository.get_account("bad")
    assert current.selected_ledger_version == 1 and current.cash == parent.cash
    assert repository.account_snapshots("bad", 1) == parent_rows
    assert any(event["event_type"] == "replay_failed"
               for event in repository.list_events("bad", result.ledger_version))
    BrokenStrategy.should_fail = True


def test_replay_system_failure_does_not_activate_partial_version(tmp_path):
    repository, registry = _repository(tmp_path)
    BrokenStrategy.should_fail = False
    runner = DailyPaperRunner(repository, lambda *_: FakePortal(), registry)
    runner.backfill("2024-01-02", "2024-01-03")
    parent = repository.get_account("good")
    daily_batches = repository.table_rows("paper_date_batches", where="operation='daily'")
    daily_hash = hashlib.sha256(json.dumps(daily_batches, sort_keys=True).encode()).hexdigest()

    def fail_batch(*_):
        raise RuntimeError("replay system failure")

    runner.system_finalize = fail_batch
    with pytest.raises(ReplayRunError, match="replay system failure") as raised:
        runner.replay("good", start_date="2024-01-03", target_date="2024-01-03", reason="system replay")

    result = raised.value.result
    current = repository.get_account("good")
    assert current.selected_ledger_version == 1 and current.cash == parent.cash
    assert repository.validate_replay_version("good", result.ledger_version,
                                              result.required_dates).complete is False
    assert hashlib.sha256(json.dumps(
        repository.table_rows("paper_date_batches", where="operation='daily'"), sort_keys=True,
    ).encode()).hexdigest() == daily_hash
    replay_batches = repository.table_rows("paper_date_batches", where="operation='replay'")
    assert replay_batches and all(row["status"] == "failed" for row in replay_batches)
    provenance = json.loads(replay_batches[0]["provenance_json"])
    assert provenance["caller"]["entrypoint"] == "replay"
    assert provenance["caller"]["reason"] == "system replay"
    BrokenStrategy.should_fail = True
