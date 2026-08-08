from __future__ import annotations

import json
import urllib.error
from dataclasses import replace
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

import fundlab.agent.policy as policy_module
from fundlab.agent import (
    AgentDecisionService,
    AgentPolicyError,
    BenchmarkEvaluation,
    Charter,
    DividendCandidate,
    DividendPolicyRuntime,
    DividendValuePolicy,
    PortfolioPerformancePoint,
    evaluate_benchmark,
)
from fundlab.agent.llm import (
    AdviserResult,
    DividendReview,
    HoldingAssessment,
    ResponsesAPIError,
    ResponsesDividendValueAdviser,
    ReviewActionReason,
    ReviewOpportunity,
)
from fundlab.agent.tools import AgentMemory, AgentMemoryError, ReadingLibrary
from fundlab.settings import (
    AgentLibrarySettings,
    AgentLLMSettings,
    AgentMemorySettings,
    AgentPolicySettings,
    AgentSettings,
    DailyAccountSettings,
    load_foundation_settings,
)
from fundlab.strategies import load_agent_decision, write_agent_decision
from fundlab.trading import PortfolioState, PositionLot
from tests.canonical.fixtures import DAYS, FUTURE_DAYS, ready_market
from tests.canonical.test_daily_pipeline import build_settings


def charter() -> Charter:
    return Charter(
        "dividend-value",
        "1",
        "2026-07-27",
        "Only persistent, liquid dividend payers; weekly research and low turnover.",
        {
            "asset_types": ["stock"],
            "exclude_st": True,
            "min_dividend_years": 3,
            "min_yield_floor": "0.02",
            "max_single_weight": "0.15",
            "min_cash_weight": "0.05",
            "min_positions": 5,
            "max_positions": 20,
        },
    )


def dividend_payload(
    report_time: str,
    *,
    dividend_type: str = "年度分红",
    description: str = "10派1元(含税)",
) -> str:
    return json.dumps({
        "raw": {
            "报告时间": report_time,
            "分红类型": dividend_type,
            "实施方案分红说明": description,
        },
    }, ensure_ascii=False)


def candidate_market(
    *,
    as_of: date,
    symbol: str,
    actions: list[dict[str, object]],
    close: float = 10.0,
):
    sessions = tuple(as_of - timedelta(days=19 - index) for index in range(20))
    bar_frame = pd.DataFrame([
        {
            "instrument_id": symbol,
            "session_date": session.isoformat(),
            "close": close,
            "suspended": False,
            "is_st": False,
            "amount": 100_000_000.0,
        }
        for session in sessions
    ])
    action_frame = pd.DataFrame(actions)

    class StubMarket:
        @staticmethod
        def instruments(**kwargs):
            return (SimpleNamespace(instrument_id=symbol, name=symbol),)

        @staticmethod
        def trading_days(start, end):
            return sessions

        @staticmethod
        def bars(*args, **kwargs):
            return bar_frame

        @staticmethod
        def corporate_actions(*args, **kwargs):
            return action_frame

    return StubMarket()


def candidates(count: int = 12) -> tuple[DividendCandidate, ...]:
    return tuple(
        DividendCandidate(
            instrument_id=f"600{index:03d}.SH",
            name=f"Dividend {index}",
            as_of=DAYS[-1],
            last_close=10.0 + index,
            ttm_dividend=(0.08 - index * 0.001) * (10.0 + index),
            ttm_yield=0.08 - index * 0.001,
            ttm_special_dividend=0.0,
            latest_fiscal_year=2025,
            latest_fiscal_dividend=(0.08 - index * 0.001) * (10.0 + index),
            latest_fiscal_yield=0.08 - index * 0.001,
            normalized_dividend=(0.08 - index * 0.001) * (10.0 + index),
            normalized_yield=0.08 - index * 0.001,
            sustainable_dividend=(0.08 - index * 0.001) * (10.0 + index),
            sustainable_yield=0.08 - index * 0.001,
            normalization_years=3,
            dividend_years=8,
            avg_amount=100_000_000.0,
            annual_dividends=tuple(
                (year, (0.08 - index * 0.001) * (10.0 + index))
                for year in range(2021, 2026)
            ),
            payout_variability=0.0,
        )
        for index in range(count)
    )


class FakeAdviser:
    config_hash = "fake-adviser-config"

    def __init__(
        self,
        *,
        action: str = "rebalance",
        opportunity: bool = True,
        opportunity_instrument: str | None = None,
        action_reason_categories: tuple[str, ...] | None = None,
    ):
        self.action = action
        self.opportunity = opportunity
        self.opportunity_instrument = opportunity_instrument
        self.action_reason_categories = action_reason_categories
        self.context = None
        self.calls = 0

    def review(self, context, *, top_n: int) -> AdviserResult:
        self.calls += 1
        self.context = context
        selected = tuple(item["instrument_id"] for item in context["eligible_candidates"][:top_n])
        opportunities = ()
        if self.opportunity:
            opportunities = (ReviewOpportunity(
                self.opportunity_instrument or selected[0],
                "高股息候选",
                "股息持续且稳定",
            ),)
        holdings = tuple(context["portfolio"]["holdings"])
        action_reasons = ()
        if self.action == "rebalance":
            categories = self.action_reason_categories or (
                ("portfolio_construction",) if holdings else ("initial_portfolio",)
            )
            action_reasons = tuple(
                ReviewActionReason(category, f"{category} evidence")
                for category in categories
            )
        return AdviserResult(
            response_id="resp-test",
            model="gpt-5.6-sol",
            review=DividendReview(
                action=self.action,
                summary="Evidence supports the selected persistent dividend payers.",
                selected_instruments=selected,
                selection_rationale={item: "persistent payout" for item in selected},
                opportunities=opportunities,
                benchmark_assessment="Benchmark evidence is diagnostic, not an automatic signal.",
                portfolio_assessment="Portfolio evidence supports the stated action.",
                action_reasons=action_reasons,
                holding_assessments=tuple(
                    HoldingAssessment(item["instrument_id"], "retain", "thesis remains valid")
                    for item in holdings
                ),
                watch_items=("Watch dividend evidence and relative performance.",),
            ),
            usage={"input_tokens": 1000, "output_tokens": 500, "total_tokens": 1500},
        )


def runtime(
    tmp_path, adviser, *, state=None, last_decision_date=None, force_review=True,
):
    return DividendPolicyRuntime(
        account_id="paper-dividend",
        adviser=adviser,
        library=ReadingLibrary(AgentLibrarySettings(root=tmp_path / "library")),
        memory=AgentMemory(tmp_path / "memory", "paper-dividend"),
        recent_memory_entries=20,
        max_memory_entry_chars=20_000,
        max_memory_total_chars=120_000,
        state=state or PortfolioState.with_cash("1000000"),
        last_decision_date=last_decision_date,
        force_review=force_review,
    )


def policy(
    tmp_path, adviser, *, state=None, last_decision_date=None, force_review=True,
):
    return DividendValuePolicy(
        charter=charter(),
        top_n=10,
        min_yield=Decimal("0.04"),
        min_dividend_years=5,
        min_avg_amount=Decimal("20000000"),
        alert_min_yield=Decimal("0.06"),
        cash_reserve=Decimal("0.05"),
        candidate_pool_size=12,
        rebalance_cooldown_days=28,
        benchmark_instrument="159207.SZ",
        benchmark_min_common_sessions=60,
        runtime=runtime(
            tmp_path,
            adviser,
            state=state,
            last_decision_date=last_decision_date,
            force_review=force_review,
        ),
    )


def test_dividend_policy_keeps_llm_inside_candidate_and_charter_gates(tmp_path, monkeypatch):
    measured = candidates()
    monkeypatch.setattr(policy_module, "build_dividend_candidates", lambda *args, **kwargs: measured)
    adviser = FakeAdviser()

    decision = policy(tmp_path, adviser).decide(object(), DAYS[-1])

    assert decision.hold is False
    assert decision.target_weights == {
        item.instrument_id: Decimal("0.0950") for item in measured[:10]
    }
    assert any(item.kind == "opportunity" for item in decision.highlights)
    assert decision.audit["response_id"] == "resp-test"
    assert adviser.context["charter"]["content_hash"] == charter().content_hash
    assert adviser.context["schema_version"] == "dividend-value-context-v3"
    assert adviser.context["tactics"]["primary_yield_metric"] == (
        "conservative_sustainable_yield"
    )
    assert adviser.context["tactics"][
        "opportunity_email_minimum_conservative_sustainable_yield"
    ] == "0.06"
    assert adviser.context["tactics"]["rebalance_cooldown_days"] == 28
    assert adviser.context["tactics"]["benchmark_policy"] == {
        "instrument_id": "159207.SZ",
        "role": "evaluation_only",
        "minimum_common_sessions_before_actionable": 60,
        "short_horizon_behavior": "diagnostic_only",
        "persistent_lag_must_not_be_sole_rebalance_reason": True,
        "benchmark_never_triggers_automatic_rebalance": True,
    }
    assert adviser.context["benchmark"]["status"] == "unavailable"


def test_dividend_policy_gates_on_normalized_not_ttm_yield(tmp_path, monkeypatch):
    measured = list(candidates())
    measured[0] = replace(
        measured[0],
        ttm_dividend=5.0,
        ttm_yield=0.50,
        normalized_dividend=0.30,
        normalized_yield=0.03,
        sustainable_dividend=0.30,
        sustainable_yield=0.03,
    )
    monkeypatch.setattr(
        policy_module, "build_dividend_candidates", lambda *args, **kwargs: tuple(measured),
    )

    decision = policy(tmp_path, FakeAdviser()).decide(object(), DAYS[-1])

    assert measured[0].instrument_id not in decision.target_weights
    assert set(decision.target_weights) == {
        item.instrument_id for item in measured[1:11]
    }


def test_policy_candidate_order_never_uses_ttm_as_a_tiebreaker(tmp_path, monkeypatch):
    measured = tuple(
        replace(
            item,
            ttm_yield=index / 100,
            latest_fiscal_yield=0.08,
            normalized_yield=0.08,
            sustainable_yield=0.08,
            payout_variability=0.0,
            dividend_years=5,
        )
        for index, item in enumerate(candidates())
    )
    monkeypatch.setattr(
        policy_module, "build_dividend_candidates", lambda *args, **kwargs: measured,
    )
    adviser = FakeAdviser()

    policy(tmp_path, adviser).decide(object(), DAYS[-1])

    assert [
        item["instrument_id"] for item in adviser.context["eligible_candidates"]
    ] == sorted(item.instrument_id for item in measured)


def test_email_floor_uses_conservative_yield_after_recent_cut(tmp_path, monkeypatch):
    measured = list(candidates())
    measured[0] = replace(
        measured[0],
        latest_fiscal_dividend=0.55,
        latest_fiscal_yield=0.055,
        normalized_dividend=0.80,
        normalized_yield=0.08,
        sustainable_dividend=0.55,
        sustainable_yield=0.055,
    )
    monkeypatch.setattr(
        policy_module, "build_dividend_candidates", lambda *args, **kwargs: tuple(measured),
    )
    adviser = FakeAdviser(opportunity_instrument=measured[0].instrument_id)

    with pytest.raises(AgentPolicyError, match="below the email yield floor"):
        policy(tmp_path, adviser).decide(object(), DAYS[-1])

    assert adviser.context["tactics"][
        "opportunity_email_minimum_conservative_sustainable_yield"
    ] == "0.06"


def test_recent_decision_suppresses_optional_rebalance_but_keeps_review(tmp_path, monkeypatch):
    measured = candidates()
    monkeypatch.setattr(policy_module, "build_dividend_candidates", lambda *args, **kwargs: measured)
    lots = tuple(
        PositionLot(
            f"lot-{index}",
            item.instrument_id,
            950,
            date(2026, 7, 1),
            date(2026, 7, 2),
            Decimal("95000"),
        )
        for index, item in enumerate(measured[:10])
    )
    state = PortfolioState(
        Decimal("1000000"),
        Decimal("50000"),
        lots=lots,
        last_prices={item.instrument_id: Decimal("100") for item in measured[:10]},
    )

    decision = policy(
        tmp_path,
        FakeAdviser(action="rebalance"),
        state=state,
        last_decision_date=date(2026, 7, 10),
    ).decide(object(), DAYS[-1])

    assert decision.hold is True
    assert decision.target_weights == {}
    assert decision.audit["can_rebalance"] is False
    assert "suppressed" in decision.reason


def test_hard_rule_violation_forces_rebalance_even_when_adviser_holds(tmp_path, monkeypatch):
    measured = candidates()
    monkeypatch.setattr(policy_module, "build_dividend_candidates", lambda *args, **kwargs: measured)
    state = PortfolioState(
        Decimal("1000000"),
        Decimal("50000"),
        lots=(PositionLot(
            "lot-etf", "510300.SH", 10000, date(2026, 7, 1), date(2026, 7, 2),
            Decimal("950000"),
        ),),
        last_prices={"510300.SH": Decimal("95")},
    )

    decision = policy(
        tmp_path,
        FakeAdviser(action="hold"),
        state=state,
        last_decision_date=date(2026, 7, 10),
    ).decide(object(), DAYS[-1])

    assert decision.hold is False
    assert decision.audit["rebalance_required"] is True
    assert any(item.kind == "risk" for item in decision.highlights)


def test_benchmark_uses_first_invested_open_and_adjusted_total_return():
    class BenchmarkMarket:
        adjusted_calls = []

        @staticmethod
        def instrument(instrument_id):
            assert instrument_id == "159207.SZ"
            return SimpleNamespace(
                instrument_id=instrument_id,
                name="高股息ETF广发",
                asset_type=SimpleNamespace(value="etf"),
            )

        @classmethod
        def adjusted_history(cls, instrument_ids, start, end, *, as_of):
            cls.adjusted_calls.append((instrument_ids, start, end, as_of))
            return pd.DataFrame([
                {"session_date": DAYS[1], "open": 10.0, "close": 10.1},
                {"session_date": DAYS[2], "open": 10.2, "close": 10.4},
                {"session_date": DAYS[3], "open": 10.4, "close": 10.5},
            ])

    points = (
        PortfolioPerformancePoint(DAYS[0], Decimal("1"), Decimal("0")),
        PortfolioPerformancePoint(DAYS[1], Decimal("0.99"), Decimal("950000")),
        PortfolioPerformancePoint(DAYS[2], Decimal("1.01"), Decimal("960000")),
        PortfolioPerformancePoint(DAYS[3], Decimal("1.00"), Decimal("955000")),
    )

    evidence = evaluate_benchmark(
        BenchmarkMarket(),
        instrument_id="159207.SZ",
        as_of=DAYS[-1],
        portfolio_points=points,
        minimum_actionable_sessions=4,
        previous_review_as_of=DAYS[2],
    )

    assert BenchmarkMarket.adjusted_calls == [
        (("159207.SZ",), DAYS[1], DAYS[3], DAYS[3]),
    ]
    assert evidence.status == "ready"
    assert evidence.actionability == "diagnostic_only"
    assert evidence.common_sessions == 3
    assert evidence.portfolio_return == Decimal("0E-8")
    assert evidence.benchmark_total_return == Decimal("0.05000000")
    assert evidence.excess_return == Decimal("-0.05000000")
    assert evidence.portfolio_max_drawdown == Decimal("0.01000000")
    assert evidence.benchmark_max_drawdown == Decimal("0E-8")
    assert evidence.benchmark_nav == Decimal("1.05000000")
    assert evidence.since_previous_review == {
        "start": DAYS[2].isoformat(),
        "end": DAYS[3].isoformat(),
        "common_trading_sessions": 1,
        "portfolio_return": "-0.00990099",
        "benchmark_total_return": "0.00961538",
        "excess_return": "-0.01951637",
        "role": "diagnostic review-to-review comparison",
    }

    actionable = evaluate_benchmark(
        BenchmarkMarket(),
        instrument_id="159207.SZ",
        as_of=DAYS[-1],
        portfolio_points=points,
        minimum_actionable_sessions=3,
    )
    assert actionable.actionability == "supporting_evidence"
    assert actionable.can_support_rebalance is True


@pytest.mark.parametrize(
    ("actionability", "categories", "expected_hold", "ignored"),
    (
        ("diagnostic_only", ("persistent_benchmark_lag",), True, True),
        (
            "diagnostic_only",
            ("persistent_benchmark_lag", "valuation_or_ranking_change"),
            False,
            True,
        ),
        ("supporting_evidence", ("persistent_benchmark_lag",), True, False),
        (
            "supporting_evidence",
            ("persistent_benchmark_lag", "dividend_evidence_change"),
            False,
            False,
        ),
    ),
)
def test_benchmark_lag_never_acts_alone_and_short_history_is_ignored(
    tmp_path, monkeypatch, actionability, categories, expected_hold, ignored,
):
    measured = candidates()
    monkeypatch.setattr(
        policy_module, "build_dividend_candidates", lambda *args, **kwargs: measured,
    )
    benchmark = BenchmarkEvaluation(
        instrument_id="159207.SZ",
        name="高股息ETF广发",
        as_of=DAYS[-1],
        minimum_actionable_sessions=60,
        status="ready",
        reason=None,
        actionability=actionability,
        comparison_start=DAYS[0],
        comparison_end=DAYS[-1],
        common_sessions=10 if actionability == "diagnostic_only" else 60,
    )
    monkeypatch.setattr(
        policy_module.DividendValuePolicy,
        "_benchmark_evaluation",
        lambda self, market, as_of: benchmark,
    )
    adviser = FakeAdviser(
        action="rebalance",
        opportunity=False,
        action_reason_categories=categories,
    )

    decision = policy(tmp_path, adviser).decide(object(), DAYS[-1])

    assert decision.hold is expected_hold
    assert (
        "persistent_benchmark_lag" in decision.audit["ignored_action_reasons"]
    ) is ignored
    if categories == ("persistent_benchmark_lag",):
        assert decision.audit["adviser_rebalance_allowed"] is False
        assert "cannot be the sole" in decision.audit["action_guard_reason"]


def test_liquidity_gate_requires_all_20_session_amounts_and_counts_suspensions():
    from fundlab.agent.dividend import build_dividend_candidates

    as_of = date(2026, 7, 24)
    sessions = tuple(as_of - timedelta(days=19 - index) for index in range(20))
    symbols = ("600000.SH", "600001.SH", "600002.SH")
    bars = []
    for symbol in symbols:
        for index, session in enumerate(sessions):
            if symbol == "600001.SH" and index == 0:
                continue
            suspended = symbol == "600002.SH" and index == 0
            bars.append({
                "instrument_id": symbol,
                "session_date": session.isoformat(),
                "close": 10.0,
                "suspended": suspended,
                "is_st": False,
                "amount": 0.0 if suspended else 21_000_000.0,
            })
    actions = [{
        "instrument_id": symbol,
        "action_type": "cash_dividend",
        "cash_per_share": 1.0,
        "ex_date": date(year, 6, 1).isoformat(),
        "known_date": date(year, 5, 1).isoformat(),
        "source_payload": dividend_payload(f"{year}年报"),
    } for symbol in symbols for year in range(2021, 2027)]

    class StubMarket:
        @staticmethod
        def instruments(**kwargs):
            return tuple(SimpleNamespace(instrument_id=item, name=item) for item in symbols)

        @staticmethod
        def trading_days(start, end):
            return sessions

        @staticmethod
        def bars(*args, **kwargs):
            return pd.DataFrame(bars)

        @staticmethod
        def corporate_actions(*args, **kwargs):
            return pd.DataFrame(actions)

    found = build_dividend_candidates(
        StubMarket(),
        as_of=as_of,
        min_dividend_years=5,
        min_avg_amount=20_000_000.0,
    )

    assert [item.instrument_id for item in found] == ["600000.SH"]
    assert found[0].amount_observed_sessions == 20


def test_ttm_dividend_excludes_cash_paid_exactly_365_days_ago():
    from fundlab.agent.dividend import build_dividend_candidates

    as_of = date(2026, 7, 31)
    sessions = tuple(as_of - timedelta(days=19 - index) for index in range(20))
    symbol = "601001.SH"
    bars = pd.DataFrame([
        {
            "instrument_id": symbol,
            "session_date": session.isoformat(),
            "close": 16.45,
            "suspended": False,
            "is_st": False,
            "amount": 21_000_000.0,
        }
        for session in sessions
    ])
    actions = pd.DataFrame([
        {
            "instrument_id": symbol,
            "action_type": "cash_dividend",
            "cash_per_share": 0.755,
            "ex_date": "2025-07-31",
            "known_date": "2025-07-24",
            "source_payload": dividend_payload("2024年报"),
        },
        {
            "instrument_id": symbol,
            "action_type": "cash_dividend",
            "cash_per_share": 0.547,
            "ex_date": "2026-07-22",
            "known_date": "2026-07-16",
            "source_payload": dividend_payload("2025年报"),
        },
    ])

    class StubMarket:
        @staticmethod
        def instruments(**kwargs):
            return (SimpleNamespace(instrument_id=symbol, name="晋控煤业"),)

        @staticmethod
        def trading_days(start, end):
            return sessions

        @staticmethod
        def bars(*args, **kwargs):
            return bars

        @staticmethod
        def corporate_actions(*args, **kwargs):
            return actions

    found = build_dividend_candidates(
        StubMarket(),
        as_of=as_of,
        min_dividend_years=1,
        min_avg_amount=20_000_000.0,
    )

    assert len(found) == 1
    assert found[0].ttm_dividend == pytest.approx(0.547)
    assert found[0].ttm_yield == pytest.approx(0.547 / 16.45)


def test_fiscal_attribution_separates_ttm_latest_and_normalized_yields():
    from fundlab.agent.dividend import build_dividend_candidates

    as_of = date(2026, 7, 31)
    sessions = tuple(as_of - timedelta(days=19 - index) for index in range(20))
    symbol = "002027.SZ"
    bars = pd.DataFrame([
        {
            "instrument_id": symbol,
            "session_date": session.isoformat(),
            "close": 5.62,
            "suspended": False,
            "is_st": False,
            "amount": 100_000_000.0,
        }
        for session in sessions
    ])
    actions = pd.DataFrame([
        {
            "instrument_id": symbol,
            "action_type": "cash_dividend",
            "cash_per_share": amount,
            "ex_date": ex_date,
            "known_date": ex_date,
            "source_payload": dividend_payload(
                report_time,
                dividend_type=dividend_type,
            ),
        }
        for amount, ex_date, report_time, dividend_type in (
            (0.33, "2024-06-13", "2023年报", "年度分红"),
            (0.10, "2024-09-26", "2024半年报", "中期分红"),
            (0.23, "2025-08-22", "2024年报", "年度分红"),
            (0.10, "2025-10-17", "2025半年报", "中期分红"),
            (0.05, "2025-11-25", "2025三季报", "季度分红"),
            (0.19, "2026-07-09", "2025年报", "年度分红"),
        )
    ])

    class StubMarket:
        @staticmethod
        def instruments(**kwargs):
            return (SimpleNamespace(instrument_id=symbol, name="分众传媒"),)

        @staticmethod
        def trading_days(start, end):
            return sessions

        @staticmethod
        def bars(*args, **kwargs):
            return bars

        @staticmethod
        def corporate_actions(*args, **kwargs):
            return actions

    found = build_dividend_candidates(
        StubMarket(),
        as_of=as_of,
        min_dividend_years=3,
        min_avg_amount=20_000_000.0,
    )

    assert len(found) == 1
    candidate = found[0]
    assert candidate.ttm_dividend == pytest.approx(0.57)
    assert candidate.ttm_yield == pytest.approx(0.57 / 5.62)
    assert candidate.latest_fiscal_year == 2025
    assert candidate.latest_fiscal_dividend == pytest.approx(0.34)
    assert candidate.latest_fiscal_yield == pytest.approx(0.34 / 5.62)
    assert candidate.normalized_dividend == pytest.approx(0.33)
    assert candidate.normalized_yield == pytest.approx(0.33 / 5.62)
    assert candidate.sustainable_dividend == pytest.approx(0.33)
    assert candidate.sustainable_yield == pytest.approx(0.33 / 5.62)
    assert candidate.annual_dividends == (
        (2023, pytest.approx(0.33)),
        (2024, pytest.approx(0.33)),
        (2025, pytest.approx(0.34)),
    )
    evidence = candidate.evidence()
    assert evidence["ttm_cash_yield"] == pytest.approx(0.10142349)
    assert evidence["latest_completed_fiscal_yield"] == pytest.approx(0.06049822)
    assert evidence["normalized_3y_fiscal_yield"] == pytest.approx(0.05871886)
    assert evidence["conservative_sustainable_yield"] == pytest.approx(0.05871886)


def test_latest_completed_fiscal_year_uses_point_in_time_evidence_not_month_cutoff():
    from fundlab.agent.dividend import build_dividend_candidates

    as_of = date(2026, 5, 31)
    symbol = "600000.SH"
    actions = [
        {
            "instrument_id": symbol,
            "action_type": "cash_dividend",
            "cash_per_share": amount,
            "ex_date": ex_date,
            "known_date": ex_date,
            "source_payload": dividend_payload(f"{fiscal_year}年报"),
        }
        for fiscal_year, amount, ex_date in (
            (2022, 1.0, "2023-05-20"),
            (2023, 1.0, "2024-05-20"),
            (2024, 1.0, "2025-05-20"),
            (2025, 0.2, "2026-05-20"),
        )
    ]

    found = build_dividend_candidates(
        candidate_market(as_of=as_of, symbol=symbol, actions=actions),
        as_of=as_of,
        min_dividend_years=3,
        min_avg_amount=20_000_000.0,
    )

    assert len(found) == 1
    assert found[0].latest_fiscal_year == 2025
    assert found[0].latest_fiscal_dividend == pytest.approx(0.2)
    assert found[0].normalized_dividend == pytest.approx(1.0)
    assert found[0].sustainable_dividend == pytest.approx(0.2)


def test_raw_candidate_order_never_uses_ttm_as_a_tiebreaker():
    from fundlab.agent.dividend import build_dividend_candidates

    as_of = date(2026, 7, 31)
    symbols = ("600001.SH", "600002.SH")
    sessions = tuple(as_of - timedelta(days=19 - index) for index in range(20))
    bars = pd.DataFrame([
        {
            "instrument_id": symbol,
            "session_date": session.isoformat(),
            "close": 10.0,
            "suspended": False,
            "is_st": False,
            "amount": 100_000_000.0,
        }
        for symbol in symbols
        for session in sessions
    ])
    actions = pd.DataFrame([
        {
            "instrument_id": symbol,
            "action_type": "cash_dividend",
            "cash_per_share": 1.0,
            "ex_date": (
                "2025-07-01"
                if fiscal_year == 2025 and symbol == "600001.SH"
                else ex_date
            ),
            "known_date": (
                "2025-07-01"
                if fiscal_year == 2025 and symbol == "600001.SH"
                else ex_date
            ),
            "source_payload": dividend_payload(f"{fiscal_year}年报"),
        }
        for symbol in symbols
        for fiscal_year, ex_date in (
            (2023, "2024-05-20"),
            (2024, "2025-05-20"),
            (2025, "2026-05-20"),
        )
    ])

    class StubMarket:
        @staticmethod
        def instruments(**kwargs):
            return tuple(
                SimpleNamespace(instrument_id=symbol, name=symbol)
                for symbol in symbols
            )

        @staticmethod
        def trading_days(start, end):
            return sessions

        @staticmethod
        def bars(*args, **kwargs):
            return bars

        @staticmethod
        def corporate_actions(*args, **kwargs):
            return actions

    found = build_dividend_candidates(
        StubMarket(),
        as_of=as_of,
        min_dividend_years=3,
        min_avg_amount=20_000_000.0,
    )

    assert [item.instrument_id for item in found] == list(symbols)
    assert found[0].ttm_yield == 0.0
    assert found[1].ttm_yield == pytest.approx(0.1)


def test_zero_ttm_cash_does_not_remove_fiscally_sustainable_candidate():
    from fundlab.agent.dividend import build_dividend_candidates

    as_of = date(2026, 6, 30)
    symbol = "600000.SH"
    actions = [
        {
            "instrument_id": symbol,
            "action_type": "cash_dividend",
            "cash_per_share": 1.0,
            "ex_date": ex_date,
            "known_date": ex_date,
            "source_payload": dividend_payload(f"{fiscal_year}年报"),
        }
        for fiscal_year, ex_date in (
            (2022, "2023-05-20"),
            (2023, "2024-05-20"),
            (2024, "2025-05-20"),
        )
    ]

    found = build_dividend_candidates(
        candidate_market(as_of=as_of, symbol=symbol, actions=actions),
        as_of=as_of,
        min_dividend_years=3,
        min_avg_amount=20_000_000.0,
    )

    assert len(found) == 1
    assert found[0].ttm_dividend == 0.0
    assert found[0].ttm_yield == 0.0
    assert found[0].sustainable_yield == pytest.approx(0.1)


def test_unattributed_cash_dividend_fails_closed_for_the_instrument():
    from fundlab.agent.dividend import build_dividend_candidates

    as_of = date(2026, 7, 31)
    symbol = "600000.SH"
    actions = [
        {
            "instrument_id": symbol,
            "action_type": "cash_dividend",
            "cash_per_share": 1.0,
            "ex_date": ex_date,
            "known_date": ex_date,
            "source_payload": dividend_payload(f"{fiscal_year}年报"),
        }
        for fiscal_year, ex_date in (
            (2023, "2024-05-20"),
            (2024, "2025-05-20"),
            (2025, "2026-05-20"),
        )
    ]
    actions.append({
        "instrument_id": symbol,
        "action_type": "cash_dividend",
        "cash_per_share": 0.2,
        "ex_date": "2026-01-15",
        "known_date": "2026-01-10",
        "source_payload": json.dumps({"raw": {"分红类型": "中期分红"}}, ensure_ascii=False),
    })

    found = build_dividend_candidates(
        candidate_market(as_of=as_of, symbol=symbol, actions=actions),
        as_of=as_of,
        min_dividend_years=3,
        min_avg_amount=20_000_000.0,
    )

    assert found == ()


def test_recorded_week_and_config_skip_the_model_and_duplicate_email(tmp_path):
    adviser = FakeAdviser()
    dividend_policy = policy(tmp_path, adviser, force_review=False)
    assert dividend_policy.runtime is not None
    dividend_policy.runtime.memory.append({
        "event": "review",
        "review_period": "2026-W29",
        "policy_config_hash": dividend_policy.config_hash,
    })

    class FridayMarket:
        @staticmethod
        def next_trading_day(day):
            assert day == FUTURE_DAYS[0]
            return FUTURE_DAYS[1]

    decision = dividend_policy.decide(FridayMarket(), FUTURE_DAYS[0])

    assert decision.hold is True
    assert decision.audit["skipped"] == "already_reviewed"
    assert adviser.calls == 0


def test_library_is_allowlisted_bounded_and_hashes_original(tmp_path):
    root = tmp_path / "library"
    root.mkdir()
    (root / "allowed.md").write_text("abcdefgh", encoding="utf-8")
    (root / "ignored.md").write_text("secret", encoding="utf-8")
    library = ReadingLibrary(AgentLibrarySettings(
        root=root,
        documents=("allowed.md",),
        max_document_chars=5,
        max_total_chars=5,
    ))

    docs = library.context()
    assert len(docs) == 1 and docs[0].content == "abcde" and docs[0].truncated
    assert docs[0].content_hash
    assert "ignored.md" not in [item.name for item in docs]


def test_memory_fails_closed_on_corruption(tmp_path):
    memory = AgentMemory(tmp_path, "paper-dividend")
    memory.append({"event": "review", "review_period": "2026-W29"})
    with memory.path.open("a", encoding="utf-8") as handle:
        handle.write("{broken\n")
    with pytest.raises(AgentMemoryError, match="Corrupt agent memory"):
        memory.recent(20)


def test_memory_context_is_bounded_even_when_an_entry_is_large(tmp_path):
    memory = AgentMemory(tmp_path, "paper-dividend")
    memory.append({"event": "review", "summary": "x" * 500})
    recent = memory.recent(20, max_entry_chars=100, max_total_chars=200)
    assert recent == [{
        "event": "review",
        "content_hash": recent[0]["content_hash"],
        "truncated": True,
    }]


class FakeHTTPResponse:
    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None

    def read(self):
        return json.dumps(self.payload).encode("utf-8")


def response_payload(top_n=2):
    selected = [f"60000{index}.SH" for index in range(top_n)]
    structured = {
        "action": "hold",
        "summary": "No portfolio change this week.",
        "selected_instruments": selected,
        "selection_rationale": [
            {"instrument_id": item, "rationale": "stable dividends"} for item in selected
        ],
        "opportunities": [],
        "benchmark_assessment": "Benchmark history is not actionable yet.",
        "portfolio_assessment": "No material portfolio change is justified.",
        "action_reasons": [],
        "holding_assessments": [],
        "watch_items": ["Watch the dividend evidence."],
    }
    return {
        "id": "resp-1",
        "model": "gpt-5.6-sol",
        "status": "completed",
        "error": None,
        "output": [{
            "type": "message",
            "content": [{"type": "output_text", "text": json.dumps(structured)}],
        }],
        "usage": {"input_tokens": 10, "output_tokens": 5, "total_tokens": 15},
    }


def test_responses_adapter_uses_plural_endpoint_strict_schema_and_large_cap(monkeypatch):
    monkeypatch.setenv("FUNDLAB_LLM_API_KEY", "test-key")
    captured = {}

    def open_fn(request, *, timeout):
        captured["url"] = request.full_url
        captured["timeout"] = timeout
        captured["body"] = json.loads(request.data.decode("utf-8"))
        return FakeHTTPResponse(response_payload())

    adviser = ResponsesDividendValueAdviser(AgentLLMSettings(
        base_url="https://relay.example/v1",
        max_output_tokens=32768,
    ), open_fn=open_fn)
    result = adviser.review({"facts": []}, top_n=2)

    assert captured["url"] == "https://relay.example/v1/responses"
    assert captured["timeout"] == 300
    assert captured["body"]["store"] is False
    assert captured["body"]["reasoning"] == {"effort": "medium"}
    assert captured["body"]["max_output_tokens"] == 32768
    assert captured["body"]["text"]["format"]["strict"] is True
    assert "uniqueItems" not in (
        captured["body"]["text"]["format"]["schema"]["properties"]
        ["selected_instruments"]
    )
    assert "metadata" not in captured["body"]
    assert result.response_id == "resp-1"


def test_responses_adapter_retries_only_transient_http_failures(monkeypatch):
    monkeypatch.setenv("FUNDLAB_LLM_API_KEY", "test-key")
    calls = []
    sleeps = []

    def open_fn(request, *, timeout):
        calls.append(request.full_url)
        if len(calls) < 3:
            raise urllib.error.HTTPError(request.full_url, 429, "busy", {}, None)
        return FakeHTTPResponse(response_payload())

    adviser = ResponsesDividendValueAdviser(
        AgentLLMSettings(base_url="https://relay.example/v1", max_retries=2),
        open_fn=open_fn,
        sleep_fn=sleeps.append,
    )
    adviser.review({"facts": []}, top_n=2)

    assert len(calls) == 3
    assert sleeps == [1.0, 2.0]


def test_responses_adapter_rechecks_schema_limits_locally(monkeypatch):
    monkeypatch.setenv("FUNDLAB_LLM_API_KEY", "test-key")
    payload = response_payload()
    structured = json.loads(payload["output"][0]["content"][0]["text"])
    structured["summary"] = "x" * 4_001
    payload["output"][0]["content"][0]["text"] = json.dumps(structured)
    adviser = ResponsesDividendValueAdviser(
        AgentLLMSettings(base_url="https://relay.example/v1"),
        open_fn=lambda request, *, timeout: FakeHTTPResponse(payload),
    )

    with pytest.raises(ResponsesAPIError, match="invalid action or summary"):
        adviser.review({"facts": []}, top_n=2)


def test_responses_adapter_rejects_duplicate_selections_locally(monkeypatch):
    monkeypatch.setenv("FUNDLAB_LLM_API_KEY", "test-key")
    payload = response_payload()
    structured = json.loads(payload["output"][0]["content"][0]["text"])
    structured["selected_instruments"][1] = structured["selected_instruments"][0]
    structured["selection_rationale"][1]["instrument_id"] = (
        structured["selection_rationale"][0]["instrument_id"]
    )
    payload["output"][0]["content"][0]["text"] = json.dumps(structured)
    adviser = ResponsesDividendValueAdviser(
        AgentLLMSettings(base_url="https://relay.example/v1"),
        open_fn=lambda request, *, timeout: FakeHTTPResponse(payload),
    )

    with pytest.raises(ResponsesAPIError, match="empty or duplicated"):
        adviser.review({"facts": []}, top_n=2)


def test_responses_adapter_rejects_non_string_fields_after_relay_schema_claim(monkeypatch):
    monkeypatch.setenv("FUNDLAB_LLM_API_KEY", "test-key")
    payload = response_payload()
    structured = json.loads(payload["output"][0]["content"][0]["text"])
    structured["selection_rationale"][0]["rationale"] = 42
    payload["output"][0]["content"][0]["text"] = json.dumps(structured)
    adviser = ResponsesDividendValueAdviser(
        AgentLLMSettings(base_url="https://relay.example/v1"),
        open_fn=lambda request, *, timeout: FakeHTTPResponse(payload),
    )

    with pytest.raises(ResponsesAPIError, match="fields must be strings"):
        adviser.review({"facts": []}, top_n=2)


def write_charter(path: Path) -> None:
    path.write_text(
        """charter:
  id: dividend-value
  version: "1"
  philosophy: persistent dividends
  hard_rules:
    asset_types: [stock]
    exclude_st: true
    min_dividend_years: 3
    min_yield_floor: "0.02"
    max_single_weight: "0.15"
    min_cash_weight: "0.05"
    min_positions: 5
    max_positions: 20
""",
        encoding="utf-8",
    )


def dividend_service_settings(tmp_path, *, action="hold"):
    settings = build_settings(tmp_path, (
        DailyAccountSettings(
            "paper-dividend", "Paper Dividend", Decimal("1000000"), "agent-file",
        ),
    ))
    charter_path = tmp_path / "charter.yaml"
    write_charter(charter_path)
    agent = AgentSettings(
        policies={
            "paper-dividend": AgentPolicySettings(
                "paper-dividend",
                "dividend-value",
                {
                    "charter": str(charter_path),
                    "top_n": 10,
                    "min_yield": "0.04",
                    "min_dividend_years": 5,
                    "min_avg_amount": "20000000",
                    "alert_min_yield": "0.06",
                    "cash_reserve": "0.05",
                    "candidate_pool_size": 12,
                    "rebalance_cooldown_days": 28,
                },
                scheduled=False,
            ),
        },
        llm=AgentLLMSettings(base_url="https://relay.example/v1"),
        library=AgentLibrarySettings(root=tmp_path / "library"),
        memory=AgentMemorySettings(root=tmp_path / "memory", recent_entries=20),
    )
    return replace(settings, agent=agent), FakeAdviser(action=action)


def test_disabled_dividend_policy_is_skipped_by_scheduled_batch(tmp_path):
    settings, adviser = dividend_service_settings(tmp_path)
    outcomes = AgentDecisionService(
        settings,
        adviser_factory=lambda _: adviser,
    ).decide_all()
    assert outcomes == [{
        "account_id": "paper-dividend",
        "written": False,
        "skipped": "scheduled_disabled",
    }]


def test_forced_weekly_hold_records_memory_and_can_email_without_decision(
    tmp_path, monkeypatch,
):
    ready_market(tmp_path / "market")
    measured = candidates()
    monkeypatch.setattr(policy_module, "build_dividend_candidates", lambda *args, **kwargs: measured)
    settings, adviser = dividend_service_settings(tmp_path, action="hold")

    result = AgentDecisionService(
        settings,
        adviser_factory=lambda _: adviser,
    ).decide("paper-dividend", force_review=True)

    assert result["held"] is True and result["written"] is False
    assert result["email"]["sent"] is False
    assert load_agent_decision(
        settings.daily.agent_decision_root, "paper-dividend", FUTURE_DAYS[0],
    ) is None
    entries = AgentMemory(tmp_path / "memory", "paper-dividend").entries()
    assert [item["event"] for item in entries] == ["review", "email"]


def test_overwrite_hold_supersedes_old_future_decision_and_dry_run_does_not(
    tmp_path, monkeypatch,
):
    ready_market(tmp_path / "market")
    measured = candidates()
    monkeypatch.setattr(policy_module, "build_dividend_candidates", lambda *args, **kwargs: measured)
    settings, adviser = dividend_service_settings(tmp_path, action="hold")
    prior = write_agent_decision(
        settings.daily.agent_decision_root,
        account_id="paper-dividend",
        decision_date=FUTURE_DAYS[0],
        target_weights={"600000.SH": "0.5"},
        reason="prior future rebalance",
        agent_id="prior-agent",
    )
    service = AgentDecisionService(settings, adviser_factory=lambda _: adviser)

    cadence_skip = service.decide("paper-dividend", overwrite=True)
    assert cadence_skip["existing_decision_retained"] is True
    assert cadence_skip["skipped"] == "policy_hold"
    assert adviser.calls == 0
    assert load_agent_decision(
        settings.daily.agent_decision_root, "paper-dividend", FUTURE_DAYS[0],
    ).content_hash == prior.content_hash

    preview = service.decide(
        "paper-dividend", overwrite=True, dry_run=True, force_review=True,
    )
    assert preview["would_supersede_existing_decision"] is True
    assert load_agent_decision(
        settings.daily.agent_decision_root, "paper-dividend", FUTURE_DAYS[0],
    ).content_hash == prior.content_hash

    result = service.decide(
        "paper-dividend", overwrite=True, force_review=True,
    )
    assert result["held"] is True
    assert result["superseded_content_hash"] == prior.content_hash
    assert Path(result["superseded_file"]).is_file()
    assert load_agent_decision(
        settings.daily.agent_decision_root, "paper-dividend", FUTURE_DAYS[0],
    ) is None


def test_committed_dividend_agent_config_is_scheduled_and_uses_confirmed_budget():
    settings = load_foundation_settings(
        Path(__file__).resolve().parents[2] / "config" / "fundlab.yaml"
    )
    policy_settings = settings.agent.policies["paper-dividend"]
    assert policy_settings.scheduled is True
    assert settings.agent.llm.model == "gpt-5.6-sol"
    assert settings.agent.llm.max_output_tokens == 32768
    assert settings.agent.llm.store is False
    assert policy_settings.params["benchmark_instrument"] == "159207.SZ"
    assert policy_settings.params["benchmark_min_common_sessions"] == 60
    assert any(account.account_id == "paper-dividend" for account in settings.daily.accounts)
