from datetime import date
import hashlib
import json
import sqlite3

import pytest

from fundlab.paper.repository import ImmutableHistoryError, PaperLedgerRepository, ReplayValidationError
from fundlab.trading import (
    AccountBindings, DecisionEnvelope, DecisionSourceType, ExecutionProfile, ResearchRiskProfile,
    ValidationResult, ValidationStatus,
)


def repository(tmp_path):
    repo = PaperLedgerRepository(tmp_path / "paper.sqlite3")
    repo.register_execution_profile(ExecutionProfile("execution", "v1"))
    repo.register_risk_profile(ResearchRiskProfile("risk", "v1"))
    return repo


def create_account(repo, account_id="a1"):
    return repo.create_account(
        account_id=account_id, name=account_id, initial_cash=1_000_000,
        bindings=AccountBindings("equal_weight", "v1", "u1", "510300.SH", "v1", "v1"),
        execution_profile_id="execution", risk_profile_id="risk", schedule="daily",
    )


def test_profiles_and_bindings_are_immutable(tmp_path):
    repo = repository(tmp_path)
    create_account(repo)
    assert repo.register_execution_profile(ExecutionProfile("execution", "v1")) is False
    with pytest.raises(ImmutableHistoryError):
        repo.register_execution_profile(ExecutionProfile("execution", "v1", commission_bps=9))
    with pytest.raises(sqlite3.IntegrityError, match="immutable"):
        repo.connection.execute("UPDATE paper_accounts SET strategy_id='other' WHERE account_id='a1'")


def test_account_savepoint_removes_partial_writes_and_keeps_other_account(tmp_path):
    repo = repository(tmp_path)
    create_account(repo, "good")
    create_account(repo, "bad")

    def process(db, account, _batch):
        db.append_event(account.account_id, 1, date(2026, 1, 5), "started", "account", account.account_id, {})
        if account.account_id == "bad":
            raise RuntimeError("injected")
        db.record_daily_state(account_id="good", ledger_version=1, trade_date=date(2026, 1, 5), cash=1_000_000,
                              positions={}, data_version="data-v1")

    result = repo.run_date_batch(trade_date=date(2026, 1, 5), data_version="data-v1",
                                 config_fingerprint="cfg", account_ids=["bad", "good"], process_account=process)
    assert result.completed_accounts == ("good",)
    assert "bad" in result.failed_accounts
    assert [event["event_type"] for event in repo.list_events("bad")] == ["account_created"]
    assert len(repo.account_snapshots("good")) == 1
    assert repo.table_rows("paper_daily_runs", where="status=?", parameters=("failed",))[0]["account_id"] == "bad"


def test_system_failure_rolls_back_account_writes_and_records_separate_failure(tmp_path):
    repo = repository(tmp_path)
    create_account(repo)

    def process(db, account, _batch):
        db.record_daily_state(account_id=account.account_id, ledger_version=1, trade_date="2026-01-05",
                              cash=900_000, positions={}, data_version="data-v1")

    with pytest.raises(RuntimeError, match="system"):
        repo.run_date_batch(trade_date="2026-01-05", data_version="data-v1", config_fingerprint="cfg",
                            account_ids=["a1"], process_account=process,
                            system_finalize=lambda *_: (_ for _ in ()).throw(RuntimeError("system")))
    assert repo.account_snapshots("a1") == ()
    assert repo.get_account("a1").cash == 1_000_000
    assert repo.table_rows("paper_date_batches")[0]["status"] == "failed"
    assert repo.table_rows("paper_daily_runs") == ()


def test_idempotent_batch_and_explicit_replay_preserve_old_history(tmp_path):
    repo = repository(tmp_path)
    create_account(repo)
    calls = 0

    def process(db, account, _batch):
        nonlocal calls
        calls += 1
        db.record_daily_state(account_id=account.account_id, ledger_version=account.selected_ledger_version,
                              trade_date="2026-01-05", cash=1_000_000, positions={}, data_version="data-v1")

    first = repo.run_date_batch(trade_date="2026-01-05", data_version="data-v1", config_fingerprint="cfg",
                                account_ids=["a1"], process_account=process)
    second = repo.run_date_batch(trade_date="2026-01-05", data_version="data-v1", config_fingerprint="cfg",
                                 account_ids=["a1"], process_account=process)
    assert first.batch_id == second.batch_id and calls == 1
    replay = repo.create_replay_version("a1", reason="correct source", start_date="2026-01-05", end_date="2026-01-05")
    assert replay.parent_version == 1 and replay.ledger_version == 2
    repo.record_daily_state(account_id="a1", ledger_version=2, trade_date="2026-01-05", cash=999_000,
                            positions={}, data_version="data-v2")
    assert repo.account_snapshots("a1", 1)[0]["cash"] == 1_000_000
    assert repo.account_snapshots("a1", 2)[0]["cash"] == 999_000
    with pytest.raises(sqlite3.IntegrityError, match="immutable"):
        repo.connection.execute("UPDATE paper_account_snapshots SET cash=0 WHERE ledger_version=1")


def test_failed_account_can_retry_without_replaying_completed_account(tmp_path):
    repo = repository(tmp_path)
    create_account(repo, "good")
    create_account(repo, "retry")
    attempts = {"good": 0, "retry": 0}

    def process(db, account, _batch):
        attempts[account.account_id] += 1
        if account.account_id == "retry" and attempts["retry"] == 1:
            raise RuntimeError("transient")
        db.record_daily_state(account_id=account.account_id, ledger_version=1, trade_date="2026-01-05",
                              cash=1_000_000, positions={}, data_version="v1")

    repo.run_date_batch(trade_date="2026-01-05", data_version="v1", config_fingerprint="cfg",
                        account_ids=["good", "retry"], process_account=process)
    result = repo.run_date_batch(trade_date="2026-01-05", data_version="v1", config_fingerprint="cfg",
                                 account_ids=["good", "retry"], process_account=process)
    assert attempts == {"good": 1, "retry": 2}
    assert result.completed_accounts == ("retry",) and result.skipped_accounts == ("good",)
    assert repo.table_rows("paper_daily_runs", where="account_id=?", parameters=("retry",))[0]["status"] == "complete"


def test_replay_batch_uses_target_version_and_atomic_validated_activation(tmp_path):
    repo = repository(tmp_path)
    create_account(repo)

    def process(db, account, batch_id):
        db.record_daily_state(
            account_id=account.account_id, ledger_version=account.selected_ledger_version,
            trade_date="2026-01-05", cash=1_000_000 - account.selected_ledger_version * 1000,
            positions={}, data_version=f"v{account.selected_ledger_version}",
        )

    daily = repo.run_date_batch(trade_date="2026-01-05", data_version="v1", config_fingerprint="cfg",
                                account_ids=["a1"], process_account=process,
                                provenance={"published_manifest": "manifest-v1"})
    original_batch = repo.table_rows("paper_date_batches", where="batch_id=?", parameters=(daily.batch_id,))[0]
    original_serialized = json.dumps(original_batch, sort_keys=True, separators=(",", ":"))
    original_hash = hashlib.sha256(original_serialized.encode()).hexdigest()
    replay = repo.create_replay_version("a1", reason="correct source", start_date="2026-01-05", end_date="2026-01-05")
    result = repo.run_date_batch(
        trade_date="2026-01-05", data_version="v2", config_fingerprint="cfg2",
        account_ids=["a1"], process_account=process, ledger_version_overrides={"a1": replay.ledger_version},
        provenance={"published_manifest": "manifest-v2"},
    )
    assert result.completed_accounts == ("a1",)
    batches = repo.table_rows("paper_date_batches")
    assert len(batches) == 2
    replay_batch = next(row for row in batches if row["batch_id"] == result.batch_id)
    assert replay_batch["operation"] == "replay"
    assert (replay_batch["account_id"], replay_batch["ledger_version"], replay_batch["parent_ledger_version"]) == (
        "a1", 2, 1,
    )
    preserved = repo.table_rows("paper_date_batches", where="batch_id=?", parameters=(daily.batch_id,))[0]
    assert hashlib.sha256(json.dumps(preserved, sort_keys=True, separators=(",", ":")).encode()).hexdigest() == original_hash
    assert (preserved["status"], preserved["data_version"], preserved["config_fingerprint"]) == (
        "complete", "v1", "cfg",
    )
    repeated = repo.run_date_batch(
        trade_date="2026-01-05", data_version="v2", config_fingerprint="cfg2",
        account_ids=["a1"], process_account=process, ledger_version_overrides={"a1": replay.ledger_version},
        provenance={"published_manifest": "manifest-v2"},
    )
    assert repeated.skipped_accounts == ("a1",)
    assert repo.get_account("a1").selected_ledger_version == 1
    assert repo.get_account("a1").cash == 999_000
    assert repo.account_snapshots("a1", 1)[0]["cash"] == 999_000
    assert repo.account_snapshots("a1", 2)[0]["cash"] == 998_000
    validation = repo.validate_replay_version("a1", 2, ["2026-01-05"])
    assert validation.complete
    promoted = repo.activate_replay_version(
        "a1", 2, required_dates=["2026-01-05"], validation=lambda _db, result: result.complete,
    )
    assert promoted.selected_ledger_version == 2 and promoted.cash == 998_000
    assert repo.account_snapshots("a1", 1)[0]["cash"] == 999_000


def test_replay_system_failure_does_not_mutate_completed_daily_batch(tmp_path):
    repo = repository(tmp_path)
    create_account(repo)

    def process(db, account, _batch):
        db.record_daily_state(account_id=account.account_id, ledger_version=account.selected_ledger_version,
                              trade_date="2026-01-05", cash=900_000, positions={}, data_version="v1")

    daily = repo.run_date_batch(trade_date="2026-01-05", data_version="v1", config_fingerprint="daily-cfg",
                                account_ids=["a1"], process_account=process)
    original = repo.table_rows("paper_date_batches", where="batch_id=?", parameters=(daily.batch_id,))[0]
    original_bytes = json.dumps(original, sort_keys=True, separators=(",", ":")).encode()
    replay = repo.create_replay_version("a1", reason="bad source correction",
                                        start_date="2026-01-05", end_date="2026-01-05")
    with pytest.raises(RuntimeError, match="replay system failure"):
        repo.run_date_batch(
            trade_date="2026-01-05", data_version="v2", config_fingerprint="replay-cfg",
            account_ids=["a1"], process_account=process,
            ledger_version_overrides={"a1": replay.ledger_version},
            system_finalize=lambda *_: (_ for _ in ()).throw(RuntimeError("replay system failure")),
        )
    preserved = repo.table_rows("paper_date_batches", where="batch_id=?", parameters=(daily.batch_id,))[0]
    assert hashlib.sha256(json.dumps(preserved, sort_keys=True, separators=(",", ":")).encode()).digest() == hashlib.sha256(original_bytes).digest()
    failed_replay = repo.table_rows("paper_date_batches", where="operation=?", parameters=("replay",))[0]
    assert failed_replay["status"] == "failed" and failed_replay["batch_id"] != daily.batch_id
    assert (preserved["status"], preserved["data_version"], preserved["config_fingerprint"]) == (
        "complete", "v1", "daily-cfg",
    )
def test_failed_replay_validation_keeps_selection_and_records_failure(tmp_path):
    repo = repository(tmp_path)
    create_account(repo)
    replay = repo.create_replay_version("a1", reason="repair", start_date="2026-01-05", end_date="2026-01-06")
    with pytest.raises(ReplayValidationError, match="failed validation"):
        repo.activate_replay_version("a1", replay.ledger_version, required_dates=["2026-01-05", "2026-01-06"])
    assert repo.get_account("a1").selected_ledger_version == 1
    failure = repo.list_events("a1", replay.ledger_version)[0]
    assert failure["event_type"] == "replay_validation_failed"
    assert failure["payload"]["missing_daily_runs"] == ["2026-01-05", "2026-01-06"]
    with pytest.raises(ReplayValidationError, match="activate_replay_version"):
        repo.select_ledger_version("a1", replay.ledger_version)


def test_completed_mixed_ledger_scope_reconstructs_from_canonical_account_days(tmp_path):
    repo = repository(tmp_path)
    create_account(repo, "a1")
    create_account(repo, "a2")
    calls = 0

    def process(db, account, _batch):
        nonlocal calls
        calls += 1
        db.record_daily_state(
            account_id=account.account_id, ledger_version=account.selected_ledger_version,
            trade_date="2026-01-05", cash=1_000_000 - account.selected_ledger_version,
            positions={}, data_version="data-v1",
        )

    daily = repo.run_date_batch(
        trade_date="2026-01-05", data_version="data-v1", config_fingerprint="v1-scope",
        account_ids=["a1", "a2"], process_account=process,
    )
    replay = repo.create_replay_version("a1", reason="correct", start_date="2026-01-05", end_date="2026-01-05")
    replay_batch = repo.run_date_batch(
        trade_date="2026-01-05", data_version="data-v1", config_fingerprint="replay-scope",
        account_ids=["a1"], process_account=process, ledger_version_overrides={"a1": replay.ledger_version},
    )
    repo.activate_replay_version("a1", replay.ledger_version, required_dates=["2026-01-05"])
    before_batches = {
        row["batch_id"]: hashlib.sha256(json.dumps(row, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        for row in repo.table_rows("paper_date_batches")
    }
    before_run_count = len(repo.table_rows("paper_daily_runs"))

    mixed = repo.run_date_batch(
        trade_date="2026-01-05", data_version="data-v1", config_fingerprint="mixed-scope",
        account_ids=["a1", "a2"], process_account=lambda *_: pytest.fail("canonical account-day should be reused"),
    )
    assert mixed.status == "complete" and mixed.skipped_accounts == ("a1", "a2")
    assert mixed.batch_id not in {daily.batch_id, replay_batch.batch_id}
    assert len(repo.table_rows("paper_daily_runs")) == before_run_count
    mixed_row = repo.table_rows("paper_date_batches", where="batch_id=?", parameters=(mixed.batch_id,))[0]
    assert mixed_row["status"] == "complete" and mixed_row["operation"] == "daily"

    for _ in range(2):
        reused = repo.run_date_batch(
            trade_date="2026-01-05", data_version="data-v1", config_fingerprint="mixed-scope",
            account_ids=["a1", "a2"], process_account=lambda *_: pytest.fail("terminal scope should reconstruct"),
        )
        assert reused.batch_id == mixed.batch_id
        assert reused.completed_accounts == () and reused.skipped_accounts == ("a1", "a2")
    assert len(repo.table_rows("paper_daily_runs")) == before_run_count
    after_batches = {row["batch_id"]: row for row in repo.table_rows("paper_date_batches")}
    for batch_id, digest in before_batches.items():
        assert hashlib.sha256(json.dumps(after_batches[batch_id], sort_keys=True,
                                        separators=(",", ":")).encode()).hexdigest() == digest


def test_completed_scope_with_missing_canonical_result_is_rejected(tmp_path):
    repo = repository(tmp_path)
    create_account(repo)
    scope = "daily:missing-canonical"
    provenance = {
        "operation": "daily", "trade_date": "2026-01-05", "data_version": "data-v1",
        "config_fingerprint": "cfg", "accounts": [["a1", 1]], "caller": {},
    }
    repo.connection.execute(
        "INSERT INTO paper_date_batches VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        ("missing-batch", scope, "2026-01-05", "daily", None, None, None, "complete", "data-v1",
         "cfg", json.dumps(provenance, sort_keys=True, separators=(",", ":")),
         "2026-01-05T00:00:00+00:00", "2026-01-05T00:01:00+00:00", None),
    )
    with pytest.raises(ImmutableHistoryError, match="lacks the requested canonical"):
        repo.run_date_batch(
            trade_date="2026-01-05", data_version="data-v1", config_fingerprint="cfg",
            account_ids=["a1"], process_account=lambda *_: None, scope_key=scope,
        )


def test_current_and_daily_positions_are_version_isolated(tmp_path):
    repo = repository(tmp_path)
    create_account(repo)
    repo.record_daily_state(account_id="a1", ledger_version=1, trade_date="2026-01-05", cash=500_000,
                            positions={"510300.SH": (1000, 4.5, 5.0)}, data_version="v1")
    assert repo.current_positions("a1")[0]["quantity"] == 1000
    assert repo.account_snapshots("a1")[0]["total_asset"] == 505_000


def test_decision_target_weights_are_stored_as_a_json_object(tmp_path):
    repo = repository(tmp_path)
    create_account(repo)
    decision = DecisionEnvelope(
        decision_id="d1", account_id="a1", source_type=DecisionSourceType.RULE_STRATEGY,
        source_id="equal_weight", config_version="v1", decision_date=date(2026, 1, 5),
        target_weights={"510300.SH": 0.6, "518880.SH": 0.4}, reason="scheduled",
        data_version="data-v1", observation_hash="hash",
        validation=ValidationResult(ValidationStatus.VALID), original_json="{}",
    )
    repo.record_decision(decision, 1)
    stored = repo.table_rows("paper_decisions")[0]["target_weights_json"]
    assert json.loads(stored) == {"510300.SH": 0.6, "518880.SH": 0.4}
