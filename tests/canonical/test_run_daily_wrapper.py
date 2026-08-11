from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest


_SCRIPT = Path(__file__).parents[2] / "scripts" / "run-daily.ps1"


def _powershell() -> str:
    pwsh = shutil.which("pwsh")
    if pwsh is None:
        pytest.skip("PowerShell is unavailable")
    return pwsh


def _quote(value: Path) -> str:
    return str(value).replace("'", "''")


def _run_wrapper(tmp_path: Path, report: dict[str, object] | None = None):
    script = tmp_path / "scripts" / "run-daily.ps1"
    script.parent.mkdir()
    shutil.copyfile(_SCRIPT, script)
    (tmp_path / "data" / "reports" / "daily").mkdir(parents=True)
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    if report is not None:
        (fake_bin / "daily-report.json").write_text(
            json.dumps(report),
            encoding="utf-8",
        )
    (fake_bin / "uv.ps1").write_text(
        """
$source = Join-Path $PSScriptRoot 'daily-report.json'
if (
    $args -join ' ' -eq 'run fundlab daily run' -and
    (Test-Path -LiteralPath $source)
) {
    Copy-Item -LiteralPath $source -Destination (
        Join-Path (Get-Location) 'data/reports/daily/daily-current.json'
    )
}
exit 0
""".strip() + "\n",
        encoding="utf-8",
    )
    environment = os.environ.copy()
    environment["PATH"] = f"{fake_bin}{os.pathsep}{environment['PATH']}"
    return subprocess.run(
        [_powershell(), "-NoProfile", "-File", str(script)],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        env=environment,
    )


def test_wrapper_does_not_reuse_old_report_in_the_clock_window(tmp_path):
    report_dir = tmp_path / "reports"
    report_dir.mkdir()
    (report_dir / "daily-old.json").write_text(json.dumps({
        "stages": [{
            "name": "evidence",
            "status": "blocked",
            "detail": {"retryable": True},
        }],
    }), encoding="utf-8")

    command = (
        f". '{_quote(_SCRIPT)}' -FunctionsOnly; "
        f"$snapshot = Get-DailyReportSnapshot -DailyReportDir '{_quote(report_dir)}'; "
        "if ($null -eq (Get-DailyAttemptReport "
        f"-DailyReportDir '{_quote(report_dir)}' -ExistingReportPaths $snapshot)) "
        "{ 'missing' } else { 'found' }"
    )
    completed = subprocess.run(
        [_powershell(), "-NoProfile", "-Command", command],
        check=True,
        capture_output=True,
        text=True,
    )

    assert completed.stdout.strip() == "missing"


def test_wrapper_excludes_promoted_pending_report_but_accepts_new_report(tmp_path):
    report_dir = tmp_path / "reports"
    pending_dir = report_dir / ".pending"
    pending_dir.mkdir(parents=True)
    recovered = pending_dir / "daily-recovered.json"
    recovered.write_text('{"status": "blocked"}', encoding="utf-8")

    command = (
        f". '{_quote(_SCRIPT)}' -FunctionsOnly; "
        f"$root = '{_quote(report_dir)}'; "
        "$reportSnapshot = Get-DailyReportSnapshot -DailyReportDir $root; "
        "$pendingNames = Get-DailyPendingReportNameSnapshot -DailyReportDir $root; "
        "Move-Item -LiteralPath (Join-Path $root '.pending/daily-recovered.json') "
        "-Destination (Join-Path $root 'daily-recovered.json'); "
        "$promoted = Get-DailyAttemptReport -DailyReportDir $root "
        "-ExistingReportPaths $reportSnapshot -PendingReportNames $pendingNames; "
        "if ($null -eq $promoted) { 'promoted:missing' } else { 'promoted:found' }; "
        "Set-Content -LiteralPath (Join-Path $root 'daily-current.json') "
        "-Value '{\"status\": \"ok\"}'; "
        "$current = Get-DailyAttemptReport -DailyReportDir $root "
        "-ExistingReportPaths $reportSnapshot -PendingReportNames $pendingNames; "
        "if ($null -eq $current) { 'current:missing' } "
        "else { 'current:' + (Split-Path -Leaf $current.Path) }"
    )
    completed = subprocess.run(
        [_powershell(), "-NoProfile", "-Command", command],
        check=True,
        capture_output=True,
        text=True,
    )

    assert completed.stdout.splitlines() == [
        "promoted:missing",
        "current:daily-current.json",
    ]


def test_wrapper_treats_zero_exit_without_new_report_as_audit_failure(tmp_path):
    completed = _run_wrapper(tmp_path)

    assert completed.returncode == 2
    log = next((tmp_path / "logs" / "daily").glob("run-*.log"))
    assert "daily run audit failed: no new formal report" in log.read_text(encoding="utf-8")
    assert (tmp_path / "logs" / "daily" / "LAST-RUN-BLOCKED").is_file()
    assert not (tmp_path / "logs" / "daily" / "LAST-RUN-DEGRADED").exists()


def test_wrapper_records_non_quarantine_degraded_details(tmp_path):
    completed = _run_wrapper(tmp_path, {
        "status": "degraded",
        "target_date": "2026-08-07",
        "snapshot_id": "snapshot-current",
        "stages": [
            {
                "name": "universe",
                "status": "degraded",
                "detail": {
                    "pending_onboarding": {
                        "688001.SH": {
                            "missing_fields": ["exchange_product_class"],
                        },
                    },
                },
            },
            {
                "name": "accounts",
                "status": "degraded",
                "detail": {
                    "degraded": ["paper-partial"],
                    "blocked": ["paper-blocked"],
                },
            },
        ],
        "accounts": [
            {
                "account_id": "paper-partial",
                "status": "degraded",
                "sessions_advanced": 1,
                "error": "later session failed",
            },
            {
                "account_id": "paper-blocked",
                "status": "blocked",
                "error": "account unavailable",
            },
        ],
    })

    assert completed.returncode == 0, completed.stderr
    marker_path = tmp_path / "logs" / "daily" / "LAST-RUN-DEGRADED"
    marker = json.loads(marker_path.read_text(encoding="utf-8"))
    assert marker["summary"] == (
        "degraded stages: universe, accounts; affected accounts: "
        "paper-partial (degraded), paper-blocked (blocked)"
    )
    assert [stage["name"] for stage in marker["degraded_stages"]] == [
        "universe",
        "accounts",
    ]
    assert marker["degraded_stages"][0]["detail"]["pending_onboarding"] == {
        "688001.SH": {"missing_fields": ["exchange_product_class"]},
    }
    assert marker["affected_accounts"] == [
        {
            "account_id": "paper-partial",
            "status": "degraded",
            "sessions_advanced": 1,
            "error": "later session failed",
        },
        {
            "account_id": "paper-blocked",
            "status": "blocked",
            "error": "account unavailable",
        },
    ]
    assert "quarantine" not in marker
    log = next((tmp_path / "logs" / "daily").glob("run-*.log"))
    log_text = log.read_text(encoding="utf-8")
    assert f"[DEGRADED] daily run completed with {marker['summary']}" in log_text
    assert "quarantined instruments" not in log_text


def test_wrapper_preserves_quarantine_and_combines_degraded_details(tmp_path):
    quarantine = {
        "instrument_count": 3,
        "instrument_ids": ["510050.SH", "600000.SH", "688001.SH"],
        "policy": "daily-instrument-data-gap-quarantine-never-block-v2",
    }
    affected_account = {
        "account_id": "paper-partial",
        "status": "degraded",
        "sessions_advanced": 1,
        "error": "later session failed",
    }
    completed = _run_wrapper(tmp_path, {
        "status": "degraded",
        "target_date": "2026-08-07",
        "snapshot_id": "snapshot-current",
        "stages": [
            {
                "name": "validate",
                "status": "degraded",
                "detail": {
                    "validator_added_execution_guard": ["510050.SH"],
                },
            },
            {
                "name": "quarantine",
                "status": "degraded",
                "detail": quarantine,
            },
            {
                "name": "accounts",
                "status": "degraded",
                "detail": {"degraded": ["paper-partial"], "blocked": []},
            },
        ],
        "accounts": [affected_account],
    })

    assert completed.returncode == 0, completed.stderr
    marker_path = tmp_path / "logs" / "daily" / "LAST-RUN-DEGRADED"
    marker = json.loads(marker_path.read_text(encoding="utf-8"))
    assert marker["summary"] == (
        "3 quarantined instruments; degraded stages: validate, quarantine, "
        "accounts; affected accounts: paper-partial (degraded)"
    )
    assert [stage["name"] for stage in marker["degraded_stages"]] == [
        "validate",
        "quarantine",
        "accounts",
    ]
    assert marker["affected_accounts"] == [affected_account]
    assert marker["quarantine"] == quarantine
    log = next((tmp_path / "logs" / "daily").glob("run-*.log"))
    assert f"[DEGRADED] daily run completed with {marker['summary']}" in (
        log.read_text(encoding="utf-8")
    )
