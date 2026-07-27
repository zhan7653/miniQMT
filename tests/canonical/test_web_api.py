from __future__ import annotations

from dataclasses import replace
from datetime import datetime, time
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient

from fundlab.pipeline import DailyPipeline
from fundlab.settings import AgentPolicySettings, AgentSettings, DailyAccountSettings
from fundlab.web.app import create_app
from fundlab.web.runner import DailyRunLauncher
from fundlab.web.schedule import ScheduledTaskState, TaskSchedulerError
from tests.canonical.fixtures import DAYS, FUTURE_DAYS, ready_market
from tests.canonical.test_daily_pipeline import (
    build_settings,
    evening_of,
    registry_with_calendars,
)


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
    settings = replace(settings, agent=AgentSettings(policies={
        "paper-agent": AgentPolicySettings("paper-agent", "momentum-rotation", {
            "risk_instrument": "600000.SH",
            "defensive_instrument": "600000.SH",
            "momentum_days": 2,
            "threshold": "0",
            "risk_on": {"600000.SH": "0.6"},
            "risk_off": {"600000.SH": "0.1"},
        }),
    }))
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
    )
    return TestClient(app), scheduler, settings


def test_overview_reports_market_accounts_and_schedule(dashboard):
    client, scheduler, _ = dashboard

    payload = client.get("/api/overview").json()

    assert payload["market"]["snapshot_id"].startswith("snap-")
    assert payload["market"]["published_end"] == DAYS[-1].isoformat()
    accounts = {item["account_id"]: item for item in payload["accounts"]}
    assert accounts["paper-1"]["exists"] and accounts["paper-1"]["head_date"] == DAYS[-1].isoformat()
    assert payload["schedule"]["exists"] is False
    assert payload["last_report"]["status"] == "ok"

    scheduler.fail = True
    degraded = client.get("/api/overview").json()
    assert "error" in degraded["schedule"], "overview must degrade, not fail, when the scheduler errors"


def test_account_detail_has_curve_positions_events(dashboard):
    client, _, _ = dashboard

    detail = client.get("/api/accounts/paper-1").json()

    assert detail["head_run_id"]
    assert len(detail["equity_curve"]) >= 2
    assert detail["equity_curve"][0]["kind"] == "initial"
    assert detail["feedback"]["quality"] == "complete"
    assert any(
        event["event_type"] == "portfolio_intent_received"
        for event in detail["recent_events"]
    )
    assert client.get("/api/accounts/nonexistent").status_code == 404


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


def test_schedule_lifecycle(dashboard):
    client, scheduler, _ = dashboard

    assert client.get("/api/schedule").json()["exists"] is False
    created = client.put("/api/schedule", json={"time": "19:30"}).json()
    assert created["exists"] and created["time"] == "19:30" and created["enabled"]

    disabled = client.put("/api/schedule", json={"enabled": False}).json()
    assert disabled["enabled"] is False

    assert client.put("/api/schedule", json={"time": "9:99"}).status_code == 422
    assert client.delete("/api/schedule").json()["deleted"]
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
