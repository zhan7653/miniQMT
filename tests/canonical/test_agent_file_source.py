from __future__ import annotations

from datetime import date
from decimal import Decimal
import json

import pytest

from fundlab.strategies import AgentDecisionError, FileIntentSource, load_agent_decision
from fundlab.trading import (
    ExecutionPolicy,
    IntentSource,
    PortfolioState,
    RiskPolicy,
    SimulationService,
    TradingRepository,
)
from tests.canonical.fixtures import DAYS, ready_market
from tests.canonical.test_trading_kernel import fees, policies


def write_decision(root, account_id: str, day: date, payload: dict) -> None:
    path = root / account_id / f"{day.isoformat()}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def decision_payload(account_id: str, day: date) -> dict:
    return {
        "account_id": account_id,
        "decision_date": day.isoformat(),
        "target_weights": {"600000.SH": "0.5"},
        "reason": "agent wants half the book in the fixture bank",
        "agent_id": "fixture-agent",
    }


def test_absent_decision_is_a_hold_not_an_error(tmp_path):
    source = FileIntentSource(tmp_path / "decisions", "paper-1", DAYS[0])
    assert isinstance(source, IntentSource)
    assert source.decision is None
    market = ready_market(tmp_path / "market")
    view_day = DAYS[0]
    from fundlab.marketdata.portal import PointInTimeMarketView

    intent = source.decide(
        account_id="paper-1",
        market=PointInTimeMarketView(market, view_day),
        state=PortfolioState.with_cash(100_000),
    )
    assert intent is None


def test_dropped_decision_executes_and_binds_content_hash(tmp_path):
    market = ready_market(tmp_path / "market")
    repository = TradingRepository(tmp_path / "trading.sqlite3")
    repository.create_account("paper-1", "paper", PortfolioState.with_cash(100_000))
    execution, risk = policies()
    service = SimulationService(
        market_data=market,
        repository=repository,
        execution_policy=execution,
        risk_policy=risk,
        fee_schedule=fees(),
    )
    decisions = tmp_path / "decisions"
    write_decision(decisions, "paper-1", DAYS[0], decision_payload("paper-1", DAYS[0]))
    outcome = None
    for day in DAYS:
        outcome = service.run_daily(
            "paper-1", day, FileIntentSource(decisions, "paper-1", day),
        )
    assert outcome is not None
    assert outcome.run.binding.strategy_id == "agent_file"
    final = repository.final_state(outcome.run.run_id)
    assert final.lots, "the day-one decision should have produced a position"

    # Idempotent daily replay with the same (absent) decision reuses the run.
    repeated = service.run_daily(
        "paper-1", DAYS[-1], FileIntentSource(decisions, "paper-1", DAYS[-1]),
    )
    assert repeated.reused and repeated.run.run_id == outcome.run.run_id


def test_decision_content_changes_config_hash(tmp_path):
    decisions = tmp_path / "decisions"
    write_decision(decisions, "paper-1", DAYS[0], decision_payload("paper-1", DAYS[0]))
    first = FileIntentSource(decisions, "paper-1", DAYS[0]).config_hash
    absent = FileIntentSource(decisions, "paper-1", DAYS[1]).config_hash
    assert first != absent
    changed = decision_payload("paper-1", DAYS[0])
    changed["target_weights"] = {"600000.SH": "0.9"}
    write_decision(decisions, "paper-1", DAYS[0], changed)
    assert FileIntentSource(decisions, "paper-1", DAYS[0]).config_hash != first


def test_malformed_or_mismatched_decisions_fail_closed(tmp_path):
    decisions = tmp_path / "decisions"
    target = DAYS[0]

    bad_account = decision_payload("other", target)
    write_decision(decisions, "paper-1", target, bad_account)
    with pytest.raises(AgentDecisionError, match="account mismatch"):
        load_agent_decision(decisions, "paper-1", target)

    bad_date = decision_payload("paper-1", target)
    bad_date["decision_date"] = DAYS[1].isoformat()
    write_decision(decisions, "paper-1", target, bad_date)
    with pytest.raises(AgentDecisionError, match="date mismatch"):
        load_agent_decision(decisions, "paper-1", target)

    negative = decision_payload("paper-1", target)
    negative["target_weights"] = {"600000.SH": "-0.2"}
    write_decision(decisions, "paper-1", target, negative)
    with pytest.raises(AgentDecisionError, match="negative"):
        load_agent_decision(decisions, "paper-1", target)

    silent = decision_payload("paper-1", target)
    silent["reason"] = "  "
    write_decision(decisions, "paper-1", target, silent)
    with pytest.raises(AgentDecisionError, match="reason"):
        load_agent_decision(decisions, "paper-1", target)

    path = decisions / "paper-1" / f"{target.isoformat()}.json"
    path.write_text("{not json", encoding="utf-8")
    with pytest.raises(AgentDecisionError, match="Unreadable"):
        load_agent_decision(decisions, "paper-1", target)


def test_pinned_account_rejects_foreign_account(tmp_path):
    market = ready_market(tmp_path / "market")
    from fundlab.marketdata.portal import PointInTimeMarketView

    source = FileIntentSource(tmp_path / "decisions", "paper-1", DAYS[0])
    with pytest.raises(ValueError, match="pinned"):
        source.decide(
            account_id="paper-2",
            market=PointInTimeMarketView(market, DAYS[0]),
            state=PortfolioState.with_cash(1),
        )


def test_decimal_weights_normalize(tmp_path):
    decisions = tmp_path / "decisions"
    payload = decision_payload("paper-1", DAYS[0])
    payload["target_weights"] = {"600000.SH": 0.25}
    write_decision(decisions, "paper-1", DAYS[0], payload)
    decision = load_agent_decision(decisions, "paper-1", DAYS[0])
    assert decision is not None
    assert decision.target_weights["600000.SH"] == Decimal("0.25")
