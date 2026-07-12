from datetime import date
import sqlite3

import pytest

from fundlab.common.config import load_paper_trading_config
from fundlab.paper.schema import SCHEMA_VERSION, TABLE_SPECS, initialize_schema
from fundlab.trading import (
    DecisionEnvelope, DecisionSourceType, ExecutionProfile, OrderStatus, RebalanceFrequency,
    ResearchRiskProfile, ValidationResult, ValidationStatus, scheduled_trading_days,
)


def test_decision_envelope_is_immutable_normalized_and_only_rules_execute():
    decision = DecisionEnvelope(
        decision_id="d1", account_id="a1", source_type=DecisionSourceType.RULE_STRATEGY,
        source_id="equal_weight", config_version="v1", decision_date=date(2026, 1, 5),
        target_weights={"510300.SH": 0.5, "518880.SH": 0.5}, reason="scheduled",
        data_version="published-v1", observation_hash="abc", original_json="{}",
        validation=ValidationResult(ValidationStatus.VALID), source_metadata={"lookback": 20},
    )
    assert decision.executable
    with pytest.raises(TypeError):
        decision.target_weights["510300.SH"] = 0.2
    assert not DecisionEnvelope(**{**decision.__dict__, "source_type": DecisionSourceType.LLM_AGENT}).executable
    rejected = DecisionEnvelope(**{
        **decision.__dict__, "target_weights": {"510300.SH": 1.1},
        "validation": ValidationResult(ValidationStatus.REJECTED, ("leverage",)),
    })
    assert rejected.target_weights["510300.SH"] == 1.1 and not rejected.executable


def test_default_profiles_match_v1_contract():
    execution = ExecutionProfile("etf_default", "v1")
    risk = ResearchRiskProfile("research_default", "v1")
    assert (execution.commission_bps, execution.minimum_commission, execution.slippage_bps) == (3, 0, 2)
    assert execution.capacity_fraction == 0.05 and execution.capacity_lookback_days == 20
    assert risk.max_position_weight is None and risk.minimum_cash_weight is None
    assert not risk.allow_short and not risk.allow_leverage and risk.require_nonnegative_cash
    assert OrderStatus.PARTIAL_FILLED.value == "partial-filled"


def test_schedule_uses_last_trading_day_of_pinned_period():
    days = [date(2026, 1, 29), date(2026, 1, 30), date(2026, 2, 2), date(2026, 2, 3)]
    assert scheduled_trading_days(days, RebalanceFrequency.DAILY) == tuple(days)
    assert scheduled_trading_days(days, "weekly") == (date(2026, 1, 30), date(2026, 2, 3))
    assert scheduled_trading_days(days, "monthly") == (date(2026, 1, 30), date(2026, 2, 3))


def test_schema_declares_independent_ledger_tables_and_constraints():
    connection = sqlite3.connect(":memory:")
    initialize_schema(connection)
    tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert SCHEMA_VERSION == 1
    assert {spec.name.value for spec in TABLE_SPECS} <= tables
    append_only = {spec.name.value for spec in TABLE_SPECS if spec.append_only}
    assert {"paper_decisions", "paper_fills", "paper_event_ledger"} <= append_only
    columns = {row[1] for row in connection.execute("PRAGMA table_info(paper_event_ledger)")}
    assert {"ledger_version", "event_sequence"} <= columns


def test_date_batches_are_replay_aware_and_terminal_completion_is_immutable():
    connection = sqlite3.connect(":memory:")
    initialize_schema(connection)
    connection.execute(
        "INSERT INTO paper_date_batches VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        ("daily-1", "daily:2026-01-05", "2026-01-05", "daily", None, None, None,
         "running", "data-v1", "config-v1", "{}", "2026-01-05T00:00:00", None, None),
    )
    connection.execute("UPDATE paper_date_batches SET status='failed' WHERE batch_id='daily-1'")
    connection.execute("UPDATE paper_date_batches SET status='running' WHERE batch_id='daily-1'")
    connection.execute(
        "UPDATE paper_date_batches SET status='complete', completed_at='2026-01-05T00:01:00' WHERE batch_id='daily-1'"
    )
    with pytest.raises(sqlite3.IntegrityError, match="immutable"):
        connection.execute("UPDATE paper_date_batches SET status='failed' WHERE batch_id='daily-1'")
    with pytest.raises(sqlite3.IntegrityError, match="immutable"):
        connection.execute("DELETE FROM paper_date_batches WHERE batch_id='daily-1'")

    # The same trade date can have separately idempotent replay scopes.
    connection.execute("INSERT INTO paper_execution_profiles VALUES ('e','v1','{}','now')")
    connection.execute("INSERT INTO paper_risk_profiles VALUES ('r','v1','{}','now')")
    connection.execute(
        "INSERT INTO paper_accounts VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        ("a1", "account", "active", 1000, 1000, "strategy", "v1", "universe-v1", "510300.SH",
         "e", "v1", "r", "v1", "daily", 1, "now", "now"),
    )
    connection.execute(
        "INSERT INTO paper_date_batches VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        ("replay-1", "replay:a1:2:2026-01-05", "2026-01-05", "replay", "a1", 2, 1,
         "running", "data-v2", "config-v1", '{"reason":"correction"}', "now", None, None),
    )
    assert connection.execute(
        "SELECT COUNT(*) FROM paper_date_batches WHERE trade_date='2026-01-05'"
    ).fetchone()[0] == 2
    date_batch_spec = next(spec for spec in TABLE_SPECS if spec.name.value == "paper_date_batches")
    assert date_batch_spec.unique_constraints == (("scope_key",),)
    assert date_batch_spec.immutable_when_complete


def test_paper_config_resolves_ignored_v2_runtime_paths():
    config = load_paper_trading_config("config/paper_trading.yaml")
    assert config.paths.database.as_posix().endswith("/data/warehouse/v2/paper_trading.sqlite3")
    assert config.paths.report_root.as_posix().endswith("/data/reports/data_v2/paper_trading")
    assert config.execution_profiles["etf_default_v1"].capacity_fraction == 0.05
    assert config.risk_profiles["research_default_v1"].max_position_weight is None
    with pytest.raises(TypeError):
        config.execution_profiles["replacement"] = ExecutionProfile("x", "v1")
