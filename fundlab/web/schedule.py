"""Windows Task Scheduler wrapper for the FundLab Daily task.

The dashboard manages the OS-level task so the automation keeps firing when
the web service is closed. All mutations go through explicit PowerShell
commands; errors surface to the API instead of being swallowed.
"""

from __future__ import annotations

import json
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

TASK_NAME = "FundLab Daily"

# MSFT_TaskWeeklyTrigger.DaysOfWeek bitmask, Sunday first.
_DAY_BITS = (
    (1, "Sunday"),
    (2, "Monday"),
    (4, "Tuesday"),
    (8, "Wednesday"),
    (16, "Thursday"),
    (32, "Friday"),
    (64, "Saturday"),
)


class TaskSchedulerError(RuntimeError):
    pass


@dataclass(frozen=True)
class ScheduledTaskState:
    exists: bool
    enabled: bool | None = None
    state: str | None = None
    time: str | None = None
    days: tuple[str, ...] = ()
    last_run_time: str | None = None
    last_result: int | None = None
    next_run_time: str | None = None

    def to_dict(self) -> dict:
        return {
            "task_name": TASK_NAME,
            "exists": self.exists,
            "enabled": self.enabled,
            "state": self.state,
            "time": self.time,
            "days": list(self.days),
            "last_run_time": self.last_run_time,
            "last_result": self.last_result,
            "next_run_time": self.next_run_time,
        }


class TaskScheduler(Protocol):
    def query(self) -> ScheduledTaskState: ...
    def register(self, time_str: str) -> ScheduledTaskState: ...
    def set_enabled(self, enabled: bool) -> ScheduledTaskState: ...
    def delete(self) -> None: ...


class WindowsTaskScheduler:
    def __init__(self, repo_root: str | Path) -> None:
        self.repo_root = Path(repo_root)
        self._shell = shutil.which("pwsh") or shutil.which("powershell")
        if self._shell is None:
            raise TaskSchedulerError("Neither pwsh nor powershell was found on PATH")

    def _run(self, script: str) -> str:
        try:
            completed = subprocess.run(
                (self._shell, "-NoProfile", "-NonInteractive", "-Command", script),
                capture_output=True,
                text=True,
                timeout=60,
                cwd=self.repo_root,
            )
        except subprocess.TimeoutExpired as exc:
            raise TaskSchedulerError("PowerShell query timed out after 60s") from exc
        if completed.returncode != 0:
            message = (completed.stderr or completed.stdout or "").strip()
            raise TaskSchedulerError(message or f"PowerShell exited {completed.returncode}")
        return completed.stdout.strip()

    def query(self) -> ScheduledTaskState:
        output = self._run(
            "$t = Get-ScheduledTask -TaskName '" + TASK_NAME + "' -ErrorAction SilentlyContinue; "
            "if (-not $t) { '{\"exists\": false}' } else { "
            "$i = Get-ScheduledTaskInfo -TaskName '" + TASK_NAME + "'; "
            "$trig = @($t.Triggers)[0]; "
            "@{ exists = $true; state = [string]$t.State; "
            "enabled = ($t.State -ne 'Disabled'); "
            "start = [string]$trig.StartBoundary; "
            "days_mask = [int]$trig.DaysOfWeek; "
            "last_run = [string]$i.LastRunTime; "
            "last_result = [int64]$i.LastTaskResult; "
            "next_run = [string]$i.NextRunTime } | ConvertTo-Json -Compress }"
        )
        try:
            payload = json.loads(output)
        except json.JSONDecodeError as exc:
            raise TaskSchedulerError(f"Unexpected scheduler output: {output[:200]}") from exc
        if not payload.get("exists"):
            return ScheduledTaskState(exists=False)
        start = str(payload.get("start") or "")
        time_str = start[11:16] if len(start) >= 16 else None
        mask = int(payload.get("days_mask") or 0)
        days = tuple(name for bit, name in _DAY_BITS if mask & bit)
        return ScheduledTaskState(
            exists=True,
            enabled=bool(payload.get("enabled")),
            state=payload.get("state"),
            time=time_str,
            days=days,
            last_run_time=payload.get("last_run") or None,
            last_result=payload.get("last_result"),
            next_run_time=payload.get("next_run") or None,
        )

    def register(self, time_str: str) -> ScheduledTaskState:
        script = self.repo_root / "scripts" / "register-daily-task.ps1"
        if not script.is_file():
            raise TaskSchedulerError(f"Missing registration script: {script}")
        try:
            completed = subprocess.run(
                (
                    self._shell, "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass",
                    "-File", str(script), "-Time", time_str,
                ),
                capture_output=True,
                text=True,
                timeout=120,
                cwd=self.repo_root,
            )
        except subprocess.TimeoutExpired as exc:
            raise TaskSchedulerError("Task registration timed out after 120s") from exc
        if completed.returncode != 0:
            message = (completed.stderr or completed.stdout or "").strip()
            raise TaskSchedulerError(message or f"Registration exited {completed.returncode}")
        return self.query()

    def set_enabled(self, enabled: bool) -> ScheduledTaskState:
        verb = "Enable-ScheduledTask" if enabled else "Disable-ScheduledTask"
        self._run(f"{verb} -TaskName '{TASK_NAME}' | Out-Null")
        return self.query()

    def delete(self) -> None:
        self._run(
            f"Unregister-ScheduledTask -TaskName '{TASK_NAME}' -Confirm:$false"
        )
