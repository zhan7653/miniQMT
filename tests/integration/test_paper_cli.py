from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess
import sys

import pytest
import yaml

from scripts.create_fake_data import create_fake_v2_portal
from fundlab.paper.runner import AccountDayResult, DailyRunResult
from scripts import run_paper_daily


ROOT = Path(__file__).resolve().parents[2]
MANAGE = ROOT / "scripts" / "manage_paper_accounts.py"
DAILY = ROOT / "scripts" / "run_paper_daily.py"


def _config(tmp_path: Path) -> Path:
    platform = tmp_path / "base.yaml"
    platform.write_text(yaml.safe_dump({
        "paths": {
            "v2_catalog": str(tmp_path / "catalog.sqlite3"),
            "v2_published_root": str(tmp_path / "published"),
        },
    }), encoding="utf-8")
    config = tmp_path / "paper.yaml"
    config.write_text(yaml.safe_dump({
        "paths": {"paper_db": str(tmp_path / "paper.sqlite3"),
                  "paper_report_root": str(tmp_path / "reports")},
        "paper_trading": {"default_initial_cash": 1_000_000,
                          "default_benchmark": "510300.SH", "risk_free_rate": 0.0},
        "data_platform_config": platform.name,
        "universes": {"reviewed-v1": ["510300.SH"]},
        "execution_profiles": {"default": {"profile_id": "execution", "version": "v1"}},
        "risk_profiles": {"default": {"profile_id": "risk", "version": "v1",
                                         "reject_untrusted_premium_discount": False}},
        "strategy_configs": [{"strategy_id": "equal_weight", "version": "reviewed_v1",
                              "parameters": {"symbols": ["510300.SH"], "cash_weight": 0.0}}],
        "default_accounts": [{
            "account_id": "equal", "name": "Equal", "strategy_id": "equal_weight",
            "strategy_config_version": "reviewed_v1", "universe_version": "reviewed-v1",
            "benchmark_symbol": "510300.SH", "execution_profile_id": "execution",
            "execution_profile_version": "v1", "risk_profile_id": "risk",
            "risk_profile_version": "v1", "schedule": "daily",
        }],
    }, sort_keys=False), encoding="utf-8")
    return config


def _run(script: Path, cwd: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run([sys.executable, str(script), *args], cwd=cwd, text=True,
                          encoding="utf-8", capture_output=True, check=False)


def test_management_cli_is_any_cwd_structured_and_bootstrap_is_idempotent(tmp_path):
    config = _config(tmp_path)
    first = _run(MANAGE, tmp_path, "--config", str(config), "bootstrap")
    assert first.returncode == 0, first.stderr
    assert json.loads(first.stdout)["created"] == ["equal"]
    repeated = _run(MANAGE, tmp_path, "--config", str(config), "bootstrap")
    assert repeated.returncode == 0
    assert json.loads(repeated.stdout)["reused"] == ["equal"]

    listed = _run(MANAGE, tmp_path, "--config", str(config), "list", "--status", "active")
    payload = json.loads(listed.stdout)
    assert listed.returncode == 0 and payload["ok"] is True
    assert payload["accounts"][0]["bindings"] == {
        "benchmark_symbol": "510300.SH", "execution_profile_version": "v1",
        "risk_profile_version": "v1", "strategy_config_version": "reviewed_v1",
        "strategy_id": "equal_weight", "universe_version": "reviewed-v1",
    }


def test_lifecycle_and_query_commands_return_machine_readable_results(tmp_path):
    config = _config(tmp_path)
    assert _run(MANAGE, tmp_path, "--config", str(config), "bootstrap").returncode == 0
    paused = _run(MANAGE, tmp_path, "--config", str(config), "pause", "equal")
    assert json.loads(paused.stdout)["account"]["status"] == "paused"
    resumed = _run(MANAGE, tmp_path, "--config", str(config), "resume", "equal")
    assert json.loads(resumed.stdout)["account"]["status"] == "active"
    shown = json.loads(_run(MANAGE, tmp_path, "--config", str(config), "show", "equal").stdout)
    assert shown["account"]["selected_ledger_version"] == 1


def test_replay_cli_rebuilds_activates_and_reuses_exact_request(tmp_path):
    create_fake_v2_portal(tmp_path)
    config = _config(tmp_path)
    assert _run(MANAGE, tmp_path, "--config", str(config), "bootstrap").returncode == 0
    assert _run(DAILY, tmp_path, "--config", str(config), "--date", "2026-01-02",
                "--no-reports").returncode == 0
    replay_args = (
        "--config", str(config), "replay", "equal", "--start-date", "2026-01-02",
        "--target-date", "2026-01-02", "--reason", "corrected published input",
    )
    first = _run(MANAGE, tmp_path, *replay_args)
    first_payload = json.loads(first.stdout)
    assert first.returncode == 0 and first_payload["ok"] is True
    assert first_payload["replay"]["status"] == "complete"
    assert first_payload["replay"]["activated"] is True
    assert first_payload["replay"]["reused"] is False
    assert first_payload["replay"]["ledger_version"] == 2
    assert first_payload["replay"]["parent_version"] == 1

    repeated = _run(MANAGE, tmp_path, *replay_args)
    repeated_payload = json.loads(repeated.stdout)
    assert repeated.returncode == 0 and repeated_payload["replay"]["reused"] is True
    assert repeated_payload["replay"]["ledger_version"] == 2
    shown = json.loads(_run(MANAGE, tmp_path, "--config", str(config), "show", "equal").stdout)
    assert shown["account"]["selected_ledger_version"] == 2
    assert len(shown["ledger_versions"]) == 2


def test_replay_cli_failure_is_structured_and_preserves_parent_selection(tmp_path):
    create_fake_v2_portal(tmp_path)
    config = _config(tmp_path)
    assert _run(MANAGE, tmp_path, "--config", str(config), "bootstrap").returncode == 0
    assert _run(DAILY, tmp_path, "--config", str(config), "--date", "2026-01-02",
                "--no-reports").returncode == 0
    raw = yaml.safe_load(config.read_text(encoding="utf-8"))
    raw["universes"] = {"other-v1": ["510300.SH"]}
    config.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")

    failed = _run(
        MANAGE, tmp_path, "--config", str(config), "replay", "equal",
        "--start-date", "2026-01-02", "--target-date", "2026-01-02",
        "--reason", "invalid replay binding",
    )
    payload = json.loads(failed.stdout)
    assert failed.returncode == 3 and payload["ok"] is False
    assert payload["error"]["type"] == "ReplayRunError"
    assert payload["replay"]["status"] == "failed"
    assert payload["replay"]["activated"] is False
    assert "unknown bound universe version: reviewed-v1" in payload["replay"]["error"]
    shown = json.loads(_run(MANAGE, tmp_path, "--config", str(config), "show", "equal").stdout)
    assert shown["account"]["selected_ledger_version"] == 1


def test_daily_cli_reports_preflight_failure_as_json_and_safe_exit_code(tmp_path):
    config = _config(tmp_path)
    result = _run(DAILY, tmp_path, "--config", str(config), "--date", "2026-01-02")
    payload = json.loads(result.stdout)
    assert result.returncode == 1
    assert payload["ok"] is False
    assert payload["error"]["type"] in {"PreflightError", "OperationalError"}


def test_daily_cli_advances_and_writes_three_canonical_report_formats(tmp_path):
    create_fake_v2_portal(tmp_path)
    config = _config(tmp_path)
    assert _run(MANAGE, tmp_path, "--config", str(config), "bootstrap").returncode == 0
    result = _run(DAILY, tmp_path, "--config", str(config), "--date", "2026-01-02")
    payload = json.loads(result.stdout)
    assert result.returncode == 0 and payload["ok"] is True
    report = payload["reports"][0]
    assert {"account_id", "json", "csv", "markdown"} == set(report)
    for kind in ("json", "csv", "markdown"):
        assert Path(report[kind]).is_file()
    report_payload = json.loads(Path(report["json"]).read_text(encoding="utf-8"))
    assert report_payload["account"]["account_id"] == "equal"
    assert report_payload["data_health"]["corporate_actions_complete"] is False


def test_registry_wires_explicit_versioned_universe_from_config(tmp_path):
    config = _config(tmp_path)
    raw = yaml.safe_load(config.read_text(encoding="utf-8"))
    registry = run_paper_daily._registry(raw)
    assert registry.get_universe("reviewed-v1") == ("510300.SH",)


def test_daily_runner_uses_bound_config_universe_not_portal_universe(tmp_path, monkeypatch, capsys):
    portal = create_fake_v2_portal(tmp_path)
    config = _config(tmp_path)
    assert _run(MANAGE, tmp_path, "--config", str(config), "bootstrap").returncode == 0

    def forbidden_portal_universe(*_args, **_kwargs):
        raise AssertionError("published portal universe table must not be consulted")

    monkeypatch.setattr(portal, "get_universe", forbidden_portal_universe)
    monkeypatch.setattr(run_paper_daily, "_portal", lambda _path: portal)
    code = run_paper_daily.main([
        "--config", str(config), "--date", "2026-01-02", "--no-reports",
    ])
    payload = json.loads(capsys.readouterr().out)
    assert code == 0 and payload["ok"] is True
    assert payload["results"][0]["status"] == "complete"


def test_bootstrap_rejects_unknown_default_universe_as_structured_error(tmp_path):
    config = _config(tmp_path)
    raw = yaml.safe_load(config.read_text(encoding="utf-8"))
    raw["default_accounts"][0]["universe_version"] = "missing-v1"
    config.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")
    result = _run(MANAGE, tmp_path, "--config", str(config), "bootstrap")
    payload = json.loads(result.stdout)
    assert result.returncode == 1 and payload["ok"] is False
    assert "unknown bound universe version: missing-v1" in payload["error"]["message"]


def test_daily_unknown_bound_universe_is_structured_account_failure(tmp_path):
    create_fake_v2_portal(tmp_path)
    config = _config(tmp_path)
    assert _run(MANAGE, tmp_path, "--config", str(config), "bootstrap").returncode == 0
    raw = yaml.safe_load(config.read_text(encoding="utf-8"))
    raw["universes"] = {"other-v1": ["510300.SH"]}
    config.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")

    result = _run(DAILY, tmp_path, "--config", str(config), "--date", "2026-01-02", "--no-reports")
    payload = json.loads(result.stdout)
    assert result.returncode == 3 and payload["ok"] is False
    account = payload["results"][0]["accounts"][0]
    assert account["status"] == "failed"
    assert "unknown bound universe version: reviewed-v1" in account["error"]


def test_invalid_backfill_invocation_is_structured(tmp_path):
    config = _config(tmp_path)
    result = _run(DAILY, tmp_path, "--config", str(config), "--target-date", "2026-01-02")
    assert result.returncode == 1
    assert "--start-date is required" in json.loads(result.stdout)["error"]["message"]


def test_all_account_failure_is_not_misclassified_as_cli_success(tmp_path, monkeypatch, capsys):
    config = _config(tmp_path)
    failed = DailyRunResult(
        trade_date="2026-01-02", batch_id="batch-failed", status="failed",
        data_version="complete-v1",
        accounts=(AccountDayResult(
            account_id="equal", trade_date="2026-01-02", status="failed",
            error="injected account failure",
        ),),
    )

    class FailedRunner:
        def __init__(self, *_args, **_kwargs):
            pass

        def run_date(self, _trade_date):
            return failed

    monkeypatch.setattr(run_paper_daily, "_portal", lambda _path: object())
    monkeypatch.setattr(run_paper_daily, "DailyPaperRunner", FailedRunner)
    args = argparse.Namespace(
        config=config, database=None, date="2026-01-02", target_date=None,
        start_date=None, no_reports=True,
    )
    payload, code = run_paper_daily.execute(args)
    assert code == 3
    assert payload["ok"] is False
    assert payload["results"][0]["status"] == "failed"
    assert payload["results"][0]["accounts"][0]["error"] == "injected account failure"

    main_code = run_paper_daily.main([
        "--config", str(config), "--date", "2026-01-02", "--no-reports",
    ])
    rendered = json.loads(capsys.readouterr().out)
    assert main_code == 3
    assert rendered["ok"] is False
    assert rendered["results"][0]["status"] == "failed"
    assert rendered["results"][0]["accounts"][0]["error"] == "injected account failure"


@pytest.mark.parametrize("case, message", [
    ("strategy", "unknown rule strategy"),
    ("config", "unknown config version"),
    ("universe", "unknown bound universe version"),
    ("symbols", "strategy symbols are outside bound universe"),
    ("benchmark", "benchmark must belong to the bound universe"),
    ("execution", "unknown execution profile id/version binding"),
    ("risk", "unknown risk profile id/version binding"),
    ("cash", "initial_cash must be positive"),
])
def test_bootstrap_binding_validation_has_no_persistence_side_effects(tmp_path, case, message):
    config = _config(tmp_path)
    raw = yaml.safe_load(config.read_text(encoding="utf-8"))
    account = raw["default_accounts"][0]
    if case == "strategy":
        account["strategy_id"] = "missing_strategy"
    elif case == "config":
        account["strategy_config_version"] = "missing-v1"
    elif case == "universe":
        account["universe_version"] = "missing-v1"
    elif case == "symbols":
        raw["strategy_configs"][0]["parameters"]["symbols"] = ["510500.SH"]
    elif case == "benchmark":
        account["benchmark_symbol"] = "510500.SH"
    elif case == "execution":
        account["execution_profile_version"] = "missing-v1"
    elif case == "risk":
        account["risk_profile_version"] = "missing-v1"
    elif case == "cash":
        account["initial_cash"] = 0
    config.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")

    result = _run(MANAGE, tmp_path, "--config", str(config), "bootstrap")
    payload = json.loads(result.stdout)
    assert result.returncode == 1 and payload["ok"] is False
    assert message in payload["error"]["message"]
    assert not (tmp_path / "paper.sqlite3").exists()


@pytest.mark.parametrize("case, extra, message", [
    ("strategy", ["--strategy-id", "missing"], "unknown rule strategy"),
    ("config", ["--strategy-config-version", "missing-v1"], "unknown config version"),
    ("universe", ["--universe-version", "missing-v1"], "unknown bound universe version"),
    ("benchmark", ["--benchmark", "510500.SH"], "benchmark must belong to the bound universe"),
    ("execution", ["--execution-profile", "missing"], "missing"),
    ("risk", ["--risk-profile", "missing"], "missing"),
    ("cash", ["--initial-cash", "0"], "initial_cash must be positive"),
    ("negative_cash", ["--initial-cash", "-1"], "initial_cash must be positive"),
])
def test_manual_create_binding_validation_is_structured_and_side_effect_free(
    tmp_path, case, extra, message,
):
    config = _config(tmp_path)
    arguments = [
        "--config", str(config), "create", "--account-id", "manual",
        "--strategy-id", "equal_weight", "--strategy-config-version", "reviewed_v1",
        "--universe-version", "reviewed-v1", "--execution-profile", "default",
        "--risk-profile", "default",
    ]
    option = extra[0]
    if option in arguments:
        arguments[arguments.index(option) + 1] = extra[1]
    else:
        arguments.extend(extra)
    result = _run(MANAGE, tmp_path, *arguments)
    payload = json.loads(result.stdout)
    assert result.returncode == 1 and payload["ok"] is False
    assert message in payload["error"]["message"]
    assert not (tmp_path / "paper.sqlite3").exists(), case


def test_manual_create_rejects_strategy_symbols_outside_universe_without_side_effects(tmp_path):
    config = _config(tmp_path)
    raw = yaml.safe_load(config.read_text(encoding="utf-8"))
    raw["strategy_configs"][0]["parameters"]["symbols"] = ["510500.SH"]
    config.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")
    result = _run(
        MANAGE, tmp_path, "--config", str(config), "create", "--account-id", "manual",
        "--strategy-id", "equal_weight", "--strategy-config-version", "reviewed_v1",
        "--universe-version", "reviewed-v1", "--execution-profile", "default",
        "--risk-profile", "default",
    )
    assert result.returncode == 1
    assert "outside bound universe" in json.loads(result.stdout)["error"]["message"]
    assert not (tmp_path / "paper.sqlite3").exists()


@pytest.mark.parametrize("script", [MANAGE, DAILY])
def test_cli_parser_errors_are_canonical_json_while_help_is_preserved(tmp_path, script):
    invalid = _run(script, tmp_path)
    payload = json.loads(invalid.stdout)
    assert invalid.returncode == 2 and payload["ok"] is False
    assert payload["command"] == "parse" and payload["error"]["type"] == "ArgumentError"
    help_result = _run(script, tmp_path, "--help")
    assert help_result.returncode == 0
    assert "usage:" in help_result.stdout.lower()
