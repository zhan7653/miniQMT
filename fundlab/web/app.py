"""FastAPI application for the local FundLab dashboard.

Binds to localhost by default; there is no authentication because the
dashboard is a single-user local tool. Mutating endpoints only touch the
scheduled task, the manual-run launcher, and agent decision files — the
canonical data stores stay CLI/pipeline-owned.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import Body, FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from fundlab.settings import FoundationSettings
from fundlab.web.runner import DailyRunLauncher
from fundlab.web.schedule import TaskScheduler, TaskSchedulerError, WindowsTaskScheduler
from fundlab.web.service import DashboardError, DashboardService

STATIC_ROOT = Path(__file__).parent / "static"


def create_app(
    settings: FoundationSettings,
    *,
    repo_root: str | Path | None = None,
    config_path: str | Path | None = None,
    scheduler: TaskScheduler | None = None,
    launcher: DailyRunLauncher | None = None,
) -> FastAPI:
    root = Path(repo_root) if repo_root is not None else Path.cwd()
    service = DashboardService(settings)
    task_scheduler = scheduler if scheduler is not None else WindowsTaskScheduler(root)
    run_launcher = launcher if launcher is not None else DailyRunLauncher(
        root, root / "logs" / "daily", config_path=config_path,
    )

    app = FastAPI(title="FundLab Dashboard", docs_url=None, redoc_url=None)

    # ------------------------------------------------------------- reads

    @app.get("/api/overview")
    def overview() -> dict[str, Any]:
        schedule: dict[str, Any]
        try:
            schedule = task_scheduler.query().to_dict()
        except TaskSchedulerError as exc:
            schedule = {"error": str(exc)}
        reports = service.daily_reports(limit=1)
        return {
            "market": service.market_summary(),
            "accounts": service.accounts(),
            "schedule": schedule,
            "last_report": reports[0] if reports else None,
            "run": run_launcher.status(tail_lines=1),
            "config": {
                "session_cutoff_local": settings.daily.session_cutoff.isoformat(timespec="minutes"),
                "agent_decision_dir": str(settings.daily.agent_decision_root),
            },
        }

    @app.get("/api/accounts")
    def accounts() -> list[dict[str, Any]]:
        return service.accounts()

    @app.get("/api/accounts/{account_id}")
    def account_detail(account_id: str, events_limit: int = 200) -> dict[str, Any]:
        try:
            return service.account_detail(
                account_id, events_limit=max(1, min(events_limit, 1000)),
            )
        except DashboardError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.get("/api/runs")
    def runs() -> list[dict[str, Any]]:
        return service.daily_reports()

    @app.get("/api/runs/{file_name}")
    def run_report(file_name: str) -> dict[str, Any]:
        try:
            return service.daily_report(file_name)
        except DashboardError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    # ------------------------------------------------------ manual trigger

    @app.post("/api/daily/run")
    def trigger_run(payload: dict[str, Any] = Body(default={})) -> dict[str, Any]:
        return run_launcher.start(
            skip_data=bool(payload.get("skip_data")),
            skip_accounts=bool(payload.get("skip_accounts")),
        )

    @app.get("/api/daily/run/status")
    def run_status(tail_lines: int = 60) -> dict[str, Any]:
        return run_launcher.status(tail_lines=max(1, min(tail_lines, 500)))

    # ----------------------------------------------------------- schedule

    @app.get("/api/schedule")
    def schedule_state() -> dict[str, Any]:
        try:
            return task_scheduler.query().to_dict()
        except TaskSchedulerError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

    @app.put("/api/schedule")
    def schedule_update(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
        time_str = str(payload.get("time") or "").strip()
        enabled = payload.get("enabled")
        try:
            if time_str:
                if not _valid_time(time_str):
                    raise HTTPException(status_code=422, detail=f"时间格式应为 HH:MM: {time_str}")
                state = task_scheduler.register(time_str)
            else:
                state = task_scheduler.query()
                if not state.exists:
                    raise HTTPException(status_code=404, detail="计划任务不存在，请先设置时间创建")
            if enabled is not None:
                state = task_scheduler.set_enabled(bool(enabled))
            return state.to_dict()
        except TaskSchedulerError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

    @app.delete("/api/schedule")
    def schedule_delete() -> dict[str, Any]:
        try:
            task_scheduler.delete()
        except TaskSchedulerError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        return {"deleted": True}

    # -------------------------------------------------------------- agent

    @app.get("/api/agent/accounts")
    def agent_accounts() -> list[dict[str, Any]]:
        return service.agent_accounts()

    @app.get("/api/agent/decisions/{account_id}")
    def agent_decisions(account_id: str) -> list[dict[str, Any]]:
        try:
            return service.agent_decisions(account_id)
        except DashboardError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.post("/api/agent/decisions/{account_id}")
    def submit_decision(account_id: str, payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
        try:
            return service.submit_agent_decision(
                account_id,
                decision_date=str(payload.get("decision_date") or ""),
                target_weights=payload.get("target_weights") or {},
                reason=str(payload.get("reason") or ""),
                agent_id=str(payload.get("agent_id") or "dashboard"),
                overwrite=bool(payload.get("overwrite")),
            )
        except DashboardError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @app.post("/api/agent/decide/{account_id}")
    def run_agent_decision(account_id: str, payload: dict[str, Any] = Body(default={})) -> dict[str, Any]:
        try:
            return service.run_agent_decision(
                account_id,
                overwrite=bool(payload.get("overwrite")),
                dry_run=bool(payload.get("dry_run")),
                force_review=bool(payload.get("force_review")),
            )
        except DashboardError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    # ------------------------------------------------------------- static

    app.mount("/static", StaticFiles(directory=STATIC_ROOT), name="static")

    @app.get("/", include_in_schema=False)
    def index() -> FileResponse:
        return FileResponse(STATIC_ROOT / "index.html")

    return app


def _valid_time(value: str) -> bool:
    parts = value.split(":")
    if len(parts) != 2:
        return False
    try:
        hour, minute = int(parts[0]), int(parts[1])
    except ValueError:
        return False
    return 0 <= hour <= 23 and 0 <= minute <= 59
