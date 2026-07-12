from datetime import date

import pytest

from fundlab.paper.lifecycle import CreateAccountRequest, PaperAccountLifecycle
from fundlab.paper.repository import PaperLedgerRepository
from fundlab.trading import (
    AccountBindings, AccountStatus, DecisionEnvelope, DecisionSourceType, ExecutionProfile,
    ResearchRiskProfile, ValidationResult, ValidationStatus,
)


def setup(tmp_path):
    repository = PaperLedgerRepository(tmp_path / "paper.sqlite3")
    lifecycle = PaperAccountLifecycle(repository)
    lifecycle.register_profiles(ExecutionProfile("execution", "v1"), ResearchRiskProfile("risk", "v1"))
    return repository, lifecycle


def request(account_id):
    return CreateAccountRequest(
        account_id=account_id, name=account_id,
        bindings=AccountBindings("equal_weight", "v1", "u1", "510300.SH", "v1", "v1"),
        execution_profile_id="execution", risk_profile_id="risk",
    )


def test_multiple_accounts_are_isolated_and_closed_history_remains_queryable(tmp_path):
    repo, lifecycle = setup(tmp_path)
    lifecycle.create(request("one"))
    lifecycle.create(request("two"))
    repo.record_daily_state(account_id="one", ledger_version=1, trade_date="2026-01-05", cash=900_000,
                            positions={}, data_version="v1")
    closed = lifecycle.close("one")
    assert closed.status is AccountStatus.CLOSED
    assert repo.get_account("two").cash == 1_000_000
    assert repo.account_snapshots("one")[0]["cash"] == 900_000
    with pytest.raises(ValueError, match="closed -> active"):
        lifecycle.resume("one")


def test_pause_cancels_pending_orders_and_resume_does_not_restore_them(tmp_path):
    repo, lifecycle = setup(tmp_path)
    lifecycle.create(request("one"))
    decision = DecisionEnvelope(
        decision_id="d1", account_id="one", source_type=DecisionSourceType.RULE_STRATEGY,
        source_id="equal_weight", config_version="v1", decision_date=date(2026, 1, 5),
        target_weights={"510300.SH": 1.0}, reason="scheduled", data_version="v1",
        observation_hash="hash", validation=ValidationResult(ValidationStatus.VALID), original_json="{}",
    )
    repo.record_decision(decision, 1)
    repo.record_order(order_id="o1", decision_id="d1", account_id="one", ledger_version=1,
                      symbol="510300.SH", original_target_weight=1.0, execution_date="2026-01-06")
    assert lifecycle.pause("one").status is AccountStatus.PAUSED
    assert repo.pending_orders("one") == ()
    row = repo.table_rows("paper_orders")[0]
    assert row["status"] == "cancelled" and row["outcome_reason"] == "account_paused"
    assert lifecycle.resume("one").status is AccountStatus.ACTIVE
    assert repo.pending_orders("one") == ()


def test_lifecycle_events_are_append_only(tmp_path):
    repo, lifecycle = setup(tmp_path)
    lifecycle.create(request("one"))
    lifecycle.pause("one")
    lifecycle.resume("one")
    assert [event["event_type"] for event in repo.list_events("one")] == [
        "account_created", "account_paused", "account_active"
    ]
