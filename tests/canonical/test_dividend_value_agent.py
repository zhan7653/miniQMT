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
    Charter,
    DividendCandidate,
    DividendPolicyRuntime,
    DividendValuePolicy,
)
from fundlab.agent.llm import (
    AdviserResult,
    DividendReview,
    ResponsesAPIError,
    ResponsesDividendValueAdviser,
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


def candidates(count: int = 12) -> tuple[DividendCandidate, ...]:
    return tuple(
        DividendCandidate(
            instrument_id=f"600{index:03d}.SH",
            name=f"Dividend {index}",
            as_of=DAYS[-1],
            last_close=10.0 + index,
            ttm_dividend=1.0,
            ttm_yield=0.08 - index * 0.001,
            dividend_years=8,
            avg_amount=100_000_000.0,
            annual_dividends=tuple((year, 1.0) for year in range(2021, 2026)),
            payout_variability=0.0,
        )
        for index in range(count)
    )


class FakeAdviser:
    config_hash = "fake-adviser-config"

    def __init__(self, *, action: str = "rebalance", opportunity: bool = True):
        self.action = action
        self.opportunity = opportunity
        self.context = None
        self.calls = 0

    def review(self, context, *, top_n: int) -> AdviserResult:
        self.calls += 1
        self.context = context
        selected = tuple(item["instrument_id"] for item in context["eligible_candidates"][:top_n])
        opportunities = ()
        if self.opportunity:
            opportunities = (ReviewOpportunity(selected[0], "高股息候选", "股息持续且稳定"),)
        return AdviserResult(
            response_id="resp-test",
            model="gpt-5.6-sol",
            review=DividendReview(
                action=self.action,
                summary="Evidence supports the selected persistent dividend payers.",
                selected_instruments=selected,
                selection_rationale={item: "persistent payout" for item in selected},
                opportunities=opportunities,
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
    assert decision.highlights[0].kind == "opportunity"
    assert decision.audit["response_id"] == "resp-test"
    assert adviser.context["charter"]["content_hash"] == charter().content_hash
    assert adviser.context["tactics"]["rebalance_cooldown_days"] == 28


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
    } for symbol in symbols for year in range(2022, 2027)]

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
    assert any(account.account_id == "paper-dividend" for account in settings.daily.accounts)
