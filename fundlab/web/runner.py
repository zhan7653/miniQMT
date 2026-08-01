"""Single-flight background launcher for `fundlab daily run`.

The dashboard triggers the same subprocess the scheduled task runs, so a
manual run and an automated run are indistinguishable in evidence. Only one
run may be in flight; the underlying pipeline is idempotent, so a retry after
any outcome is always safe.
"""

from __future__ import annotations

import subprocess
import sys
import threading
from pathlib import Path

from fundlab.common.dates import audit_now


class DailyRunLauncher:
    def __init__(
        self,
        repo_root: str | Path,
        log_dir: str | Path,
        config_path: str | Path | None = None,
        command_override: list[str] | None = None,
    ) -> None:
        self.repo_root = Path(repo_root)
        self.log_dir = Path(log_dir)
        self.config_path = None if config_path is None else str(config_path)
        self.command_override = command_override
        self._lock = threading.Lock()
        self._process: subprocess.Popen | None = None
        self._log_path: Path | None = None
        self._log_handle = None
        self._started_at: str | None = None
        self._last: dict | None = None

    def _command(self, *, skip_data: bool, skip_accounts: bool) -> list[str]:
        if self.command_override is not None:
            return list(self.command_override)
        command = [sys.executable, "-m", "fundlab.cli"]
        if self.config_path:
            command.extend(("--config", self.config_path))
        command.extend(("daily", "run"))
        if skip_data:
            command.append("--skip-data")
        if skip_accounts:
            command.append("--skip-accounts")
        return command

    def start(self, *, skip_data: bool = False, skip_accounts: bool = False) -> dict:
        with self._lock:
            if self._process is not None and self._process.poll() is None:
                return {"started": False, "reason": "already_running", **self._snapshot_locked()}
            self._collect_finished_locked()
            command = self._command(skip_data=skip_data, skip_accounts=skip_accounts)
            self.log_dir.mkdir(parents=True, exist_ok=True)
            stamp = audit_now().strftime("%Y-%m-%d_%H%M%S")
            self._log_path = self.log_dir / f"web-run-{stamp}.log"
            self._log_handle = self._log_path.open("w", encoding="utf-8")
            try:
                self._log_handle.write(f"$ {' '.join(command)}\n")
                self._log_handle.flush()
                self._started_at = audit_now().isoformat(timespec="seconds")
                self._process = subprocess.Popen(
                    command,
                    cwd=self.repo_root,
                    stdout=self._log_handle,
                    stderr=subprocess.STDOUT,
                )
            except Exception:
                self._log_handle.close()
                self._log_handle = None
                self._process = None
                raise
            return {"started": True, **self._snapshot_locked()}

    def status(self, *, tail_lines: int = 60) -> dict:
        with self._lock:
            self._collect_finished_locked()
            snapshot = self._snapshot_locked()
        log_path = snapshot.get("log_path")
        if log_path:
            snapshot["log_tail"] = _tail(Path(log_path), tail_lines)
        return snapshot

    def _collect_finished_locked(self) -> None:
        if self._process is not None and self._process.poll() is not None:
            if self._log_handle is not None:
                self._log_handle.close()
                self._log_handle = None
            self._last = {
                "exit_code": self._process.returncode,
                "started_at": self._started_at,
                "finished_at": audit_now().isoformat(timespec="seconds"),
                "log_path": None if self._log_path is None else str(self._log_path),
            }
            self._process = None

    def _snapshot_locked(self) -> dict:
        running = self._process is not None and self._process.poll() is None
        if running:
            return {
                "running": True,
                "started_at": self._started_at,
                "log_path": None if self._log_path is None else str(self._log_path),
                "last": self._last,
            }
        return {
            "running": False,
            "started_at": None,
            "log_path": None if self._last is None else self._last.get("log_path"),
            "last": self._last,
        }


def _tail(path: Path, lines: int) -> list[str]:
    try:
        content = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return []
    return content.splitlines()[-max(1, lines):]
