from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from fundlab.agent.evaluations import (
    AgentEvaluationError,
    list_agent_evaluations,
    load_agent_evaluation,
    write_agent_evaluation,
)


AS_OF = date(2026, 7, 31)
TARGET = date(2026, 8, 3)
GENERATED = datetime(2026, 8, 2, 9, 0, tzinfo=ZoneInfo("Asia/Hong_Kong"))


def ready_evaluation(root: Path, **overrides):
    values = {
        "status": "ready",
        "account_id": "paper-crash-global",
        "policy_kind": "crisis-drawdown",
        "agent_id": "crisis-drawdown-v1",
        "config_hash": "config-a",
        "parameters": {
            "risk_instruments": ["510300.SH"],
            "minimum_drawdown": "0.20",
        },
        "as_of": AS_OF,
        "decision_date": TARGET,
        "snapshot_id": "snap-a",
        "state_hash": "state-a",
        "hold": False,
        "action": "enter",
        "reason": "confirmed reversal",
        "target_weights": {"510300.SH": "0.2", "511010.SH": "0.8"},
        "audit": {
            "premium_data_used": False,
            "signals": {
                "510300.SH": {
                    "current_drawdown": "-0.21",
                    "rebound_from_low": "0.06",
                },
            },
        },
        "generated_at": GENERATED,
    }
    values.update(overrides)
    return write_agent_evaluation(root, **values)


def test_evaluation_round_trip_is_utf8_hashed_and_idempotent(tmp_path):
    first = ready_evaluation(tmp_path)
    repeated = ready_evaluation(
        tmp_path,
        generated_at=datetime(2026, 8, 2, 9, 30, tzinfo=GENERATED.tzinfo),
    )

    assert repeated.revision_id == first.revision_id
    assert repeated.generated_at == first.generated_at
    assert str(repeated.target_weights["510300.SH"]) == "0.2"
    assert repeated.audit["premium_data_used"] is False
    raw = Path(first.source_path).read_bytes()
    assert b"\xef\xbf\xbd" not in raw
    assert "confirmed reversal" in raw.decode("utf-8")
    loaded = load_agent_evaluation(
        tmp_path, first.account_id, AS_OF, first.revision_id,
    )
    assert loaded.content_hash == first.content_hash


def test_same_identity_cannot_silently_change_evidence(tmp_path):
    ready_evaluation(tmp_path)

    with pytest.raises(AgentEvaluationError, match="different evidence"):
        ready_evaluation(tmp_path, reason="nondeterministic alternative")


def test_snapshot_or_config_change_creates_immutable_revision(tmp_path):
    first = ready_evaluation(tmp_path)
    corrected = ready_evaluation(
        tmp_path,
        snapshot_id="snap-corrected",
        generated_at=datetime(2026, 8, 2, 10, 0, tzinfo=GENERATED.tzinfo),
    )
    reconfigured = ready_evaluation(
        tmp_path,
        config_hash="config-b",
        generated_at=datetime(2026, 8, 2, 11, 0, tzinfo=GENERATED.tzinfo),
    )

    assert len({first.revision_id, corrected.revision_id, reconfigured.revision_id}) == 3
    listed = list_agent_evaluations(tmp_path, first.account_id, limit_dates=90)
    assert [item.revision_id for item in listed] == [
        reconfigured.revision_id,
        corrected.revision_id,
        first.revision_id,
    ]


def test_failure_evidence_is_explicit_and_does_not_carry_a_decision(tmp_path):
    failure = write_agent_evaluation(
        tmp_path,
        status="error",
        account_id="paper-crash-global",
        policy_kind="crisis-drawdown",
        agent_id="crisis-drawdown-v1",
        config_hash="config-a",
        parameters={"minimum_drawdown": "0.20"},
        decision_date=TARGET,
        error_type="AgentPolicyError",
        error="history unavailable",
        generated_at=GENERATED,
    )

    assert failure.status == "error"
    assert failure.as_of is None
    assert failure.hold is None
    assert failure.target_weights == {}
    assert failure.evidence_date == GENERATED.date()


def test_corrupt_or_misplaced_evidence_fails_closed(tmp_path):
    evaluation = ready_evaluation(tmp_path)
    path = Path(evaluation.source_path)
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["reason"] = "tampered"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(AgentEvaluationError, match="content hash mismatch"):
        load_agent_evaluation(tmp_path, evaluation.account_id, AS_OF, evaluation.revision_id)

    with pytest.raises(AgentEvaluationError, match="Invalid evaluation account id"):
        list_agent_evaluations(tmp_path, "../escape")


@pytest.mark.parametrize(
    "overrides, message",
    [
        ({"hold": None}, "hold flag"),
        ({"snapshot_id": None}, "needs as_of"),
        ({"target_weights": {"510300.SH": "1.2"}}, "exceed one"),
        ({"error_type": "Wrong"}, "cannot carry an error"),
    ],
)
def test_ready_evaluation_contract_rejects_invalid_payloads(tmp_path, overrides, message):
    with pytest.raises(AgentEvaluationError, match=message):
        ready_evaluation(tmp_path, **overrides)
