from __future__ import annotations

from dataclasses import replace
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
from fastapi.testclient import TestClient

from fundlab.agent.evaluations import evaluation_root, write_agent_evaluation
from fundlab.agent.tools import AgentMemory
from fundlab.pipeline import DailyPipeline
from fundlab.settings import (
    AgentMemorySettings,
    AgentPolicySettings,
    AgentSettings,
    DailyAccountSettings,
)
from fundlab.web.app import create_app
from fundlab.web.runner import DailyRunLauncher
from fundlab.web.schedule import ScheduledTaskState, TaskSchedulerError
from fundlab.web.service import DashboardError, DashboardService, _account_event_visible
from fundlab.strategies import write_agent_decision
from fundlab.trading import TradingRepository
from tests.canonical.fixtures import DAYS, FUTURE_DAYS, ready_market
from tests.canonical.test_daily_pipeline import (
    build_settings,
    evening_of,
    registry_with_calendars,
)


class _ReadySnapshotResolver:
    def __init__(self, settings) -> None:
        service = DashboardService(settings)
        self._summary = service.market_summary()
        self._names = service._instrument_names()

    def lookup(self, instrument_ids) -> dict[str, str]:
        return {
            instrument_id: self._names[instrument_id]
            for instrument_id in instrument_ids
            if instrument_id in self._names
        }

    def market_summary(self) -> dict:
        return dict(self._summary)


class _SequencedNameResolver:
    def __init__(self) -> None:
        self.calls = 0

    def lookup_state(self, instrument_ids) -> tuple[dict[str, str], bool]:
        self.calls += 1
        if self.calls == 1:
            return {}, True
        return {
            instrument_id: "Fixture Bank"
            for instrument_id in instrument_ids
            if instrument_id == "600000.SH"
        }, False

    def lookup(self, instrument_ids) -> dict[str, str]:
        raise AssertionError("account detail must use the atomic name lookup")


def _ready_snapshot_resolver(settings):
    return _ReadySnapshotResolver(settings)


class FakeScheduler:
    def __init__(self):
        self.state = ScheduledTaskState(exists=False)
        self.fail = False

    def _guard(self):
        if self.fail:
            raise TaskSchedulerError("scheduler unavailable")

    def query(self) -> ScheduledTaskState:
        self._guard()
        return self.state

    def register(self, time_str: str) -> ScheduledTaskState:
        self._guard()
        self.state = ScheduledTaskState(
            exists=True, enabled=True, state="Ready", time=time_str,
            days=("Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"),
        )
        return self.state

    def set_enabled(self, enabled: bool) -> ScheduledTaskState:
        self._guard()
        if not self.state.exists:
            raise TaskSchedulerError("task does not exist")
        self.state = ScheduledTaskState(
            exists=True, enabled=enabled, state="Ready" if enabled else "Disabled",
            time=self.state.time, days=self.state.days,
        )
        return self.state

    def delete(self) -> None:
        self._guard()
        self.state = ScheduledTaskState(exists=False)


def write_subprocess_config(tmp_path) -> str:
    """A real on-disk config so a spawned `fundlab daily run` works offline."""
    config_dir = tmp_path / "config"
    config_dir.mkdir(exist_ok=True)
    path = config_dir / "fundlab.yaml"
    path.write_text(
        "paths:\n"
        "  market_data: ../market\n"
        "  trading_database: ../trading.sqlite3\n"
        "  report_root: ../reports\n"
        "daily:\n"
        "  report_dir: ../daily-reports\n"
        "execution:\n"
        "  policy_id: subprocess-test\n"
        "  version: \"1\"\n"
        "  maximum_participation: \"0.05\"\n"
        "  base_slippage_bps: \"0\"\n"
        "  impact_bps_at_max_participation: \"0\"\n"
        "risk:\n"
        "  policy_id: subprocess-test\n"
        "  version: \"1\"\n"
        "fees:\n"
        "  schedule_id: subprocess-test\n"
        "  version: \"1\"\n"
        "  trusted_for_simulation: true\n"
        "  verification_note: deterministic subprocess fixture\n"
        "  rules:\n"
        "    - effective_from: \"2015-08-01\"\n"
        "      asset_types: [stock, etf]\n"
        "      exchanges: [SH, SZ]\n"
        "      broker_commission_rate: \"0.0003\"\n"
        "      minimum_commission: \"5\"\n"
        "      evidence: subprocess-test\n",
        encoding="utf-8",
    )
    return str(path)


@pytest.fixture()
def dashboard(tmp_path):
    ready_market(tmp_path / "market")
    settings = build_settings(tmp_path, (
        DailyAccountSettings(
            "paper-1", "Paper 1", Decimal("100000"), "static",
            {"600000.SH": Decimal("0.5")},
        ),
        DailyAccountSettings("paper-agent", "Agent", Decimal("100000"), "agent-file"),
    ))
    settings = replace(settings, agent=AgentSettings(
        policies={
            "paper-agent": AgentPolicySettings("paper-agent", "momentum-rotation", {
                "risk_instrument": "600000.SH",
                "defensive_instrument": "600000.SH",
                "momentum_days": 2,
                "threshold": "0",
                "risk_on": {"600000.SH": "0.6"},
                "risk_off": {"600000.SH": "0.1"},
            }),
        },
        memory=AgentMemorySettings(root=tmp_path / "agent-memory"),
    ))
    pipeline = DailyPipeline(
        settings,
        registry=registry_with_calendars(),
        now_fn=lambda: evening_of(DAYS[-1]),
    )
    result = pipeline.run()
    assert result.status == "ok"
    scheduler = FakeScheduler()
    launcher = DailyRunLauncher(
        tmp_path, tmp_path / "logs", config_path=write_subprocess_config(tmp_path),
    )
    app = create_app(
        settings, repo_root=tmp_path, scheduler=scheduler, launcher=launcher,
        snapshot_resolver=_ready_snapshot_resolver(settings),
    )
    return TestClient(app), scheduler, settings


def test_overview_reports_market_accounts_and_schedule(dashboard):
    client, scheduler, _ = dashboard

    payload = client.get("/api/overview").json()

    assert payload["market"]["snapshot_id"].startswith("snap-")
    assert payload["market"]["published_end"] == DAYS[-1].isoformat()
    accounts = {item["account_id"]: item for item in payload["accounts"]}
    assert accounts["paper-1"]["exists"] and accounts["paper-1"]["head_date"] == DAYS[-1].isoformat()
    assert accounts["paper-1"]["strategy_name"] == "静态股债配置"
    assert accounts["paper-1"]["strategy_uses_llm"] is False
    assert accounts["paper-1"]["strategy_instruments"] == [{
        "instrument_id": "600000.SH",
        "instrument_name": "Fixture Bank",
        "role": "目标配置",
    }]
    assert accounts["paper-agent"]["strategy_kind"] == "momentum-rotation"
    assert accounts["paper-agent"]["strategy_name"] == "动量轮动"
    assert "中期动量" in accounts["paper-agent"]["strategy_description"]
    assert payload["schedule"]["exists"] is False
    assert payload["last_report"]["status"] == "ok"

    scheduler.fail = True
    degraded = client.get("/api/overview").json()
    assert "error" in degraded["schedule"], "overview must degrade, not fail, when the scheduler errors"


def test_sector_momentum_profile_exposes_confirmed_sector_roles(dashboard):
    _, _, settings = dashboard
    account = DailyAccountSettings(
        "paper-sector-momentum",
        "Paper China Sector Momentum",
        Decimal("100000"),
        "agent-file",
    )
    sector_policy = AgentPolicySettings(
        "paper-sector-momentum",
        "sector-momentum",
        {
            "whitelist_version": "cn-core-sector-etf-2026-08-08-v1",
            "sector_mapping": {
                "银行": "512800.SH",
                "证券公司": "512880.SH",
            },
            "defensive_instrument": "511010.SH",
        },
    )
    service = DashboardService(replace(
        settings,
        agent=replace(
            settings.agent,
            policies={**settings.agent.policies, account.account_id: sector_policy},
        ),
    ))

    profile = service._strategy_profile(account, {
        "512800.SH": "银行ETF华宝",
        "512880.SH": "证券ETF国泰",
        "511010.SH": "国债ETF",
    })

    assert profile["strategy_kind"] == "sector-momentum"
    assert profile["strategy_name"] == "行业动量轮动"
    assert profile["strategy_universe"] == "人工确认的 A 股行业 ETF 池"
    assert profile["strategy_instruments"] == [
        {
            "instrument_id": "512800.SH",
            "instrument_name": "银行ETF华宝",
            "role": "行业池：银行",
        },
        {
            "instrument_id": "512880.SH",
            "instrument_name": "证券ETF国泰",
            "role": "行业池：证券公司",
        },
        {
            "instrument_id": "511010.SH",
            "instrument_name": "国债ETF",
            "role": "防守资产",
        },
    ]


def test_moving_average_grid_profile_exposes_single_grid_instrument(dashboard):
    _, _, settings = dashboard
    account = DailyAccountSettings(
        "paper-ma-grid-511380",
        "Paper MA Grid 511380",
        Decimal("100000"),
        "moving-average-grid",
    )
    policy = AgentPolicySettings(
        account.account_id,
        "moving-average-grid",
        {
            "instrument": "511380.SH",
            "activation_date": "2026-08-07",
            "max_weight": "0.85",
            "minimum_grid_step": "0.01",
        },
    )
    service = DashboardService(replace(
        settings,
        agent=replace(
            settings.agent,
            policies={**settings.agent.policies, account.account_id: policy},
        ),
    ))

    profile = service._strategy_profile(account, {"511380.SH": "可转债ETF博时"})

    assert profile["strategy_kind"] == "moving-average-grid"
    assert profile["strategy_name"] == "自适应均线网格"
    assert profile["strategy_universe"] == "单只 ETF＋现金"
    assert profile["strategy_instruments"] == [{
        "instrument_id": "511380.SH",
        "instrument_name": "可转债ETF博时",
        "role": "网格标的",
    }]


def test_dashboard_index_bypasses_stale_asset_cache(dashboard):
    client, _, _ = dashboard

    response = client.get("/")

    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"
    assert "/static/app.js?v=20260807-4" in response.text
    assert client.get("/static/app.js").headers["cache-control"] == "no-store"


def test_dashboard_name_cache_has_no_hardcoded_or_sticky_fallback(dashboard):
    client, _, _ = dashboard

    script = client.get("/static/app.js").text

    assert "const ETF_LABELS" not in script
    assert "else delete instrumentNames[instrumentId]" in script
    assert "Array.isArray(collection) ? collection : []" in script
    assert 'value = "正在读取"' in script
    assert "scheduleIsRefreshing(error)" in script
    assert "scheduleIsRefreshing(error) &&" not in script
    assert 'return api("/api/schedule");' not in script
    assert 'method: "DELETE"' in script
    assert "data.instrument_names_pending" in script
    assert "nameRetryAttempt: nameRetryAttempt + 1" in script


def test_account_event_visibility_filters_only_non_economic_corporate_action_noise():
    assert not _account_event_visible("corporate_action_ex_date", {
        "instrument_id": "600000.SH", "action_type": "cash_dividend",
    })
    assert not _account_event_visible("corporate_action_entitlement", {
        "instrument_id": "600000.SH", "quantity": 0,
    })
    assert not _account_event_visible("cash_dividend_paid", {
        "instrument_id": "600000.SH", "entitled_quantity": "0", "gross_cash": "0",
    })
    assert _account_event_visible("corporate_action_entitlement", {
        "instrument_id": "600000.SH", "quantity": 100,
    })
    assert _account_event_visible("cash_dividend_paid", {
        "instrument_id": "600000.SH", "entitled_quantity": 100, "gross_cash": "50",
    })
    assert _account_event_visible("split_applied", {
        "instrument_id": "600000.SH", "pre_event_quantity": 100,
    })
    assert not _account_event_visible("split_applied", {
        "instrument_id": "600000.SH", "pre_event_quantity": 0,
    })
    assert _account_event_visible("portfolio_valued", {"total_equity": "100000"})


def test_account_detail_has_curve_positions_events(dashboard):
    client, _, _ = dashboard

    detail = client.get("/api/accounts/paper-1").json()

    assert detail["head_run_id"]
    assert len(detail["equity_curve"]) >= 2
    assert detail["equity_curve"][0]["kind"] == "initial"
    assert detail["feedback"]["quality"] == "complete"
    assert detail["strategy_name"] == "静态股债配置"
    assert detail["strategy_instruments"][0]["instrument_name"] == "Fixture Bank"
    assert detail["instrument_names_pending"] is False
    assert any(
        event["event_type"] == "portfolio_intent_received"
        for event in detail["recent_events"]
    )
    assert client.get("/api/accounts/nonexistent").status_code == 404


def test_account_detail_exposes_pending_names_until_atomic_refresh_completes(dashboard):
    _, _, settings = dashboard
    resolver = _SequencedNameResolver()
    service = DashboardService(settings, name_resolver=resolver)

    pending = service.account_detail("paper-1")
    ready = service.account_detail("paper-1")

    assert pending["instrument_names_pending"] is True
    assert pending["strategy_instruments"][0]["instrument_name"] is None
    assert ready["instrument_names_pending"] is False
    assert ready["strategy_instruments"][0]["instrument_name"] == "Fixture Bank"


def test_dividend_account_detail_exposes_benchmark_review_history(dashboard):
    _, _, settings = dashboard
    dividend_settings = replace(
        settings,
        agent=replace(settings.agent, policies={
            "paper-agent": AgentPolicySettings("paper-agent", "dividend-value", {}),
        }),
    )
    AgentMemory(
        dividend_settings.agent.memory.root, "paper-agent",
    ).append({
        "event": "review",
        "as_of": "2026-07-31",
        "benchmark_assessment": "组合仍处观察期，维持独立判断。",
        "benchmark": {
            "schema_version": "dividend-benchmark-v1",
            "instrument_id": "159207.SZ",
            "status": "ready",
            "actionability": "diagnostic_only",
            "comparison_end": "2026-07-31",
            "portfolio_nav": "1.012",
            "benchmark_nav_on_portfolio_scale": "1.008",
            "portfolio_return": "0.012",
            "benchmark_total_return": "0.008",
            "excess_return": "0.004",
            "common_trading_sessions": 12,
        },
    })

    detail = DashboardService(dividend_settings).account_detail("paper-agent")

    assert detail["benchmark"]["instrument_id"] == "159207.SZ"
    assert detail["benchmark"]["review_as_of"] == "2026-07-31"
    assert detail["benchmark"]["benchmark_assessment"] == "组合仍处观察期，维持独立判断。"
    assert detail["benchmark_history"] == [{
        "as_of": "2026-07-31",
        "comparison_end": "2026-07-31",
        "portfolio_nav": "1.012",
        "benchmark_nav": "1.008",
        "portfolio_return": "0.012",
        "benchmark_total_return": "0.008",
        "excess_return": "0.004",
        "common_trading_sessions": 12,
        "actionability": "diagnostic_only",
    }]


def test_runs_listing_and_detail(dashboard):
    client, _, _ = dashboard

    runs = client.get("/api/runs").json()
    assert runs and runs[0]["status"] == "ok"

    detail = client.get(f"/api/runs/{runs[0]['file']}").json()
    assert detail["stages"]
    assert client.get("/api/runs/daily-nope.json").status_code == 404
    assert client.get("/api/runs/..%2Fescape.json").status_code in (404, 422)
    # Windows path separators must never escape the reports directory.
    assert client.get("/api/runs/daily-..%5C..%5Cescape.json").status_code == 404


def test_runs_listing_ignores_hidden_pending_and_invalid_json(dashboard):
    client, _, settings = dashboard
    report_root = settings.daily.report_root
    (report_root / ".daily-pending.json").write_text(
        '{"status": "pending"}', encoding="utf-8",
    )
    (report_root / "daily-corrupt.json").write_text("{", encoding="utf-8")

    files = {item["file"] for item in client.get("/api/runs").json()}

    assert ".daily-pending.json" not in files
    assert "daily-corrupt.json" not in files


def test_run_detail_turns_read_errors_into_dashboard_errors(dashboard, monkeypatch):
    _, _, settings = dashboard
    service = DashboardService(settings)
    report_root = settings.daily.report_root
    malformed = report_root / "daily-malformed.json"
    malformed.write_text("{", encoding="utf-8")
    non_object = report_root / "daily-array.json"
    non_object.write_text("[]", encoding="utf-8")
    unreadable = report_root / "daily-unreadable.json"
    unreadable.write_text("{}", encoding="utf-8")

    with pytest.raises(DashboardError):
        service.daily_report("daily-missing.json")
    with pytest.raises(DashboardError):
        service.daily_report(malformed.name)
    with pytest.raises(DashboardError):
        service.daily_report(non_object.name)

    original_read_text = Path.read_text

    def raise_io_error(path, *args, **kwargs):
        if path == unreadable:
            raise OSError("fixture read failure")
        return original_read_text(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", raise_io_error)
    with pytest.raises(DashboardError):
        service.daily_report(unreadable.name)


def test_schedule_lifecycle(dashboard):
    client, scheduler, _ = dashboard

    assert client.get("/api/schedule").json()["exists"] is False
    created = client.put("/api/schedule", json={"time": "19:30"}).json()
    assert created["exists"] and created["time"] == "19:30" and created["enabled"]

    disabled = client.put("/api/schedule", json={"enabled": False}).json()
    assert disabled["enabled"] is False

    assert client.put("/api/schedule", json={"time": "9:99"}).status_code == 422
    deleted = client.delete("/api/schedule").json()
    assert deleted["deleted"] and deleted["exists"] is False
    assert deleted["enabled"] is None and deleted["days"] == []
    assert client.get("/api/schedule").json()["exists"] is False

    missing = client.put("/api/schedule", json={"enabled": True})
    assert missing.status_code == 404

    scheduler.fail = True
    assert client.get("/api/schedule").status_code == 502
    assert client.put("/api/schedule", json={"time": "20:00"}).status_code == 502


def test_agent_decide_endpoint_runs_the_configured_policy(dashboard):
    client, _, settings = dashboard

    wrong_policy = client.post(
        "/api/agent/decide/paper-agent", json={"force_review": True},
    )
    assert wrong_policy.status_code == 422
    assert "force-review" in wrong_policy.json()["detail"]

    preview = client.post("/api/agent/decide/paper-agent", json={"dry_run": True})
    assert preview.status_code == 200, preview.text
    payload = preview.json()
    assert payload["written"] is False
    assert payload["decision_date"] == FUTURE_DAYS[0].isoformat()
    assert payload["as_of"] == DAYS[-1].isoformat()
    decision_path = (
        settings.daily.agent_decision_root / "paper-agent"
        / f"{FUTURE_DAYS[0].isoformat()}.json"
    )
    assert not decision_path.exists()

    written = client.post("/api/agent/decide/paper-agent", json={})
    assert written.status_code == 200, written.text
    assert written.json()["written"] is True
    assert decision_path.is_file()
    listed = client.get("/api/agent/decisions/paper-agent").json()
    assert listed[0]["decision_date"] == FUTURE_DAYS[0].isoformat() and listed[0]["valid"]

    duplicate = client.post("/api/agent/decide/paper-agent", json={})
    assert duplicate.status_code == 200
    assert duplicate.json()["written"] is False
    assert duplicate.json()["skipped"] == "already_present"

    not_agent = client.post("/api/agent/decide/paper-1", json={})
    assert not_agent.status_code == 422


def test_crisis_monitor_separates_signal_decision_execution_and_history(
    dashboard, tmp_path,
):
    _, scheduler, settings = dashboard
    params = {
        "risk_instruments": ["600000.SH"],
        "defensive_instrument": "511010.SH",
        "entry_mode": "reversal",
        "minimum_drawdown": "0.20",
        "rebound_threshold": "0.05",
    }
    crisis_settings = replace(
        settings,
        agent=replace(settings.agent, policies={
            "paper-agent": AgentPolicySettings(
                "paper-agent", "crisis-drawdown", params,
            ),
        }),
    )
    market = DashboardService(crisis_settings).market_summary()
    repository = TradingRepository(crisis_settings.paths.trading_database)
    state, _ = repository.selected_state("paper-agent")
    evidence = write_agent_evaluation(
        evaluation_root(crisis_settings.daily.agent_decision_root),
        status="ready",
        account_id="paper-agent",
        policy_kind="crisis-drawdown",
        agent_id="crisis-drawdown-v1",
        config_hash="crisis-config-a",
        parameters=params,
        as_of=DAYS[-1],
        decision_date=FUTURE_DAYS[0],
        snapshot_id=market["snapshot_id"],
        state_hash=state.state_hash,
        hold=False,
        action="enter",
        reason="confirmed reversal",
        target_weights={"600000.SH": "0.4", "511010.SH": "0.6"},
        audit={
            "premium_data_used": False,
            "action": "enter",
            "signals": {
                "600000.SH": {
                    "current_drawdown": "-0.22",
                    "event_drawdown": "-0.30",
                    "rebound_from_low": "0.08",
                    "recovery_ratio": "0.73",
                    "above_confirmation_average": True,
                    "annualized_volatility": "0.20",
                },
            },
        },
        generated_at=datetime(2026, 8, 2, 8, 0, tzinfo=ZoneInfo("Asia/Hong_Kong")),
    )
    write_agent_decision(
        crisis_settings.daily.agent_decision_root,
        account_id="paper-agent",
        decision_date=FUTURE_DAYS[0],
        target_weights={"600000.SH": "0.4", "511010.SH": "0.6"},
        reason="official crisis decision",
        agent_id="crisis-drawdown-v1",
    )
    client = TestClient(create_app(
        crisis_settings,
        repo_root=tmp_path,
        scheduler=scheduler,
        launcher=DailyRunLauncher(tmp_path, tmp_path / "monitor-logs"),
        snapshot_resolver=_ready_snapshot_resolver(crisis_settings),
    ))

    monitor = client.get("/api/agent/monitor")
    assert monitor.status_code == 200, monitor.text
    payload = monitor.json()
    assert payload["summary"] == {
        "configured": 1, "current": 1, "errors": 0, "triggered": 1, "manual": 0,
    }
    account = payload["accounts"][0]
    assert account["evaluation_status"] == "current"
    assert account["evaluation"]["revision_id"] == evidence.revision_id
    assert account["evaluation"]["action"] == "enter"
    assert account["nearest_signal"]["instrument_id"] == "600000.SH"
    assert account["nearest_signal"]["instrument_name"] == "Fixture Bank"
    assert account["decision"]["status"] == "queued"
    assert account["execution"]["positions"] == []
    assert account["manual_intervention"] is None

    history = client.get("/api/agent/evaluations/paper-agent?limit=90").json()
    assert history["records"][0]["is_current"] is True
    assert history["records"][0]["audit"]["premium_data_used"] is False
    detail = client.get("/api/accounts/paper-agent").json()
    assert detail["strategy_config_changes"][0]["initial"] is True

    write_agent_decision(
        crisis_settings.daily.agent_decision_root,
        account_id="paper-agent",
        decision_date=FUTURE_DAYS[0],
        target_weights={"511010.SH": "1"},
        reason="manual override",
        agent_id="dashboard",
        overwrite=True,
    )
    contaminated = client.get("/api/agent/monitor").json()["accounts"][0]
    assert contaminated["manual_intervention"]["since"] == FUTURE_DAYS[0].isoformat()
    assert contaminated["decision"]["agent_id"] == "dashboard"


def test_crisis_monitor_never_presents_old_success_as_current_after_failure(
    dashboard, tmp_path,
):
    _, scheduler, settings = dashboard
    params = {
        "risk_instruments": ["600000.SH"],
        "defensive_instrument": "511010.SH",
        "minimum_drawdown": "0.20",
    }
    crisis_settings = replace(
        settings,
        agent=replace(settings.agent, policies={
            "paper-agent": AgentPolicySettings(
                "paper-agent", "crisis-drawdown", params,
            ),
        }),
    )
    market = DashboardService(crisis_settings).market_summary()
    repository = TradingRepository(crisis_settings.paths.trading_database)
    state, _ = repository.selected_state("paper-agent")
    root = evaluation_root(crisis_settings.daily.agent_decision_root)
    ready = write_agent_evaluation(
        root,
        status="ready",
        account_id="paper-agent",
        policy_kind="crisis-drawdown",
        agent_id="crisis-drawdown-v1",
        config_hash="config-a",
        parameters=params,
        as_of=DAYS[-1],
        decision_date=FUTURE_DAYS[0],
        snapshot_id=market["snapshot_id"],
        state_hash=state.state_hash,
        hold=True,
        action="wait",
        reason="no trigger",
        audit={"signals": {}},
        generated_at=datetime(2026, 8, 2, 8, 0, tzinfo=ZoneInfo("Asia/Hong_Kong")),
    )
    write_agent_evaluation(
        root,
        status="error",
        account_id="paper-agent",
        policy_kind="crisis-drawdown",
        agent_id="crisis-drawdown-v1",
        config_hash="config-a",
        parameters=params,
        error_type="AgentPolicyError",
        error="published history unavailable",
        generated_at=datetime(2026, 8, 2, 9, 0, tzinfo=ZoneInfo("Asia/Hong_Kong")),
    )
    client = TestClient(create_app(
        crisis_settings,
        repo_root=tmp_path,
        scheduler=scheduler,
        launcher=DailyRunLauncher(tmp_path, tmp_path / "monitor-error-logs"),
        snapshot_resolver=_ready_snapshot_resolver(crisis_settings),
    ))

    account = client.get("/api/agent/monitor").json()["accounts"][0]

    assert account["evaluation_status"] == "error"
    assert account["evaluation"]["status"] == "error"
    assert account["last_success"]["revision_id"] == ready.revision_id
    assert "history unavailable" in account["stale_reason"]


def test_agent_decision_submission_validates_and_lists(dashboard):
    client, _, settings = dashboard

    accounts = client.get("/api/agent/accounts").json()
    assert [item["account_id"] for item in accounts] == ["paper-agent"]
    assert accounts[0]["head_date"] == DAYS[-1].isoformat()

    future = "2026-07-17"
    ok = client.post("/api/agent/decisions/paper-agent", json={
        "decision_date": future,
        "target_weights": {"600000.SH": "0.4"},
        "reason": "dashboard test decision",
    })
    assert ok.status_code == 200, ok.text
    decision_path = settings.daily.agent_decision_root / "paper-agent" / f"{future}.json"
    assert decision_path.is_file()

    listed = client.get("/api/agent/decisions/paper-agent").json()
    assert listed[0]["decision_date"] == future and listed[0]["valid"]

    duplicate = client.post("/api/agent/decisions/paper-agent", json={
        "decision_date": future,
        "target_weights": {"600000.SH": "0.5"},
        "reason": "second try",
    })
    assert duplicate.status_code == 422 and "覆盖" in duplicate.json()["detail"]

    overwritten = client.post("/api/agent/decisions/paper-agent", json={
        "decision_date": future,
        "target_weights": {"600000.SH": "0.5"},
        "reason": "second try",
        "overwrite": True,
    })
    assert overwritten.status_code == 200

    stale = client.post("/api/agent/decisions/paper-agent", json={
        "decision_date": DAYS[0].isoformat(),
        "target_weights": {"600000.SH": "0.5"},
        "reason": "too old",
    })
    assert stale.status_code == 422 and "已推进" in stale.json()["detail"]

    for bad in (
        {"decision_date": future, "target_weights": {}, "reason": "x"},
        {"decision_date": future, "target_weights": {"600000.SH": "abc"}, "reason": "x"},
        {"decision_date": future, "target_weights": {"600000.SH": "-1"}, "reason": "x"},
        {"decision_date": future, "target_weights": {"600000.SH": "NaN"}, "reason": "x"},
        {"decision_date": future, "target_weights": {"600000.SH": "Infinity"}, "reason": "x"},
        {"decision_date": future, "target_weights": {"600000.SH": "0.6", "510300.SH": "0.6"}, "reason": "x"},
        {"decision_date": future, "target_weights": {"600000.SH": "0.5"}, "reason": "  "},
        {"decision_date": "not-a-date", "target_weights": {"600000.SH": "0.5"}, "reason": "x"},
    ):
        response = client.post("/api/agent/decisions/paper-agent", json={**bad, "overwrite": True})
        assert response.status_code == 422, bad

    assert client.post("/api/agent/decisions/paper-1", json={
        "decision_date": future,
        "target_weights": {"600000.SH": "0.5"},
        "reason": "wrong account type",
    }).status_code == 422

    assert client.get("/api/agent/decisions/paper-1").status_code == 404

    decision_path.write_text("{}", encoding="utf-8")
    corrupt_overwrite = client.post("/api/agent/decisions/paper-agent", json={
        "decision_date": future,
        "target_weights": {"600000.SH": "0.5"},
        "reason": "must not erase corrupt evidence",
        "overwrite": True,
    })
    assert corrupt_overwrite.status_code == 422
    assert "损坏" in corrupt_overwrite.json()["detail"]
    assert decision_path.read_text(encoding="utf-8") == "{}"


def test_manual_run_subprocess_succeeds_offline(dashboard, tmp_path):
    client, _, _ = dashboard

    status = client.get("/api/daily/run/status").json()
    assert status["running"] is False and status["last"] is None

    launched = client.post("/api/daily/run", json={"skip_data": True, "skip_accounts": True})
    assert launched.status_code == 200
    assert launched.json()["started"] is True

    import time as time_module
    deadline = time_module.time() + 120
    while time_module.time() < deadline:
        state = client.get("/api/daily/run/status").json()
        if not state["running"]:
            break
        time_module.sleep(1)
    assert state["running"] is False
    assert state["last"] is not None
    assert state["last"]["exit_code"] == 0, state.get("log_tail")
    assert state["log_tail"], "log tail should show subprocess output"


def test_manual_run_single_flight_refuses_concurrent_start(tmp_path):
    import sys

    launcher = DailyRunLauncher(
        tmp_path, tmp_path / "logs",
        command_override=[sys.executable, "-c", "import time; time.sleep(15)"],
    )
    first = launcher.start()
    assert first["started"] is True
    second = launcher.start()
    assert second["started"] is False and second["reason"] == "already_running"
    status = launcher.status()
    assert status["running"] is True
    # Clean up the sleeper so the test suite does not linger.
    launcher._process.kill()
    launcher._process.wait(timeout=30)
    final = launcher.status()
    assert final["running"] is False and final["last"]["exit_code"] is not None


def test_configured_but_uncreated_account_detail_is_graceful(dashboard, tmp_path):
    _, scheduler, settings = dashboard
    extended = build_settings(tmp_path, (
        *settings.daily.accounts,
        DailyAccountSettings("paper-new", "New", Decimal("50000"), "static",
                             {"600000.SH": Decimal("1")}),
    ))
    client = TestClient(create_app(
        extended, repo_root=tmp_path, scheduler=scheduler,
        launcher=DailyRunLauncher(tmp_path, tmp_path / "logs"),
        snapshot_resolver=_ready_snapshot_resolver(extended),
    ))

    detail = client.get("/api/accounts/paper-new").json()

    assert detail["exists"] is False
    assert detail["equity_curve"] == [] and detail["positions"] == []
    assert detail["cash"] == "50000"


def test_invalid_decision_filename_does_not_break_listing(dashboard):
    client, _, settings = dashboard
    root = settings.daily.agent_decision_root / "paper-agent"
    root.mkdir(parents=True, exist_ok=True)
    (root / "2026-02-30.json").write_text("{}", encoding="utf-8")

    listed = client.get("/api/agent/decisions/paper-agent")

    assert listed.status_code == 200
    bad = [item for item in listed.json() if item["file"] == "2026-02-30.json"]
    assert bad and bad[0]["valid"] is False


def test_index_serves_dashboard(dashboard):
    client, _, _ = dashboard

    page = client.get("/")
    assert page.status_code == 200
    assert "FundLab" in page.text
    assert client.get("/static/app.js").status_code == 200
    assert client.get("/static/vendor/echarts.min.js").status_code == 200
