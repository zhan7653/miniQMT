"""FastAPI application for the local FundLab dashboard.

Binds to localhost by default; there is no authentication because the
dashboard is a single-user local tool. Mutating endpoints only touch the
scheduled task, the manual-run launcher, and agent decision files — the
canonical data stores stay CLI/pipeline-owned.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fastapi import Body, FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles

from fundlab.settings import FoundationSettings
from fundlab.web.read_cache import SingleFlightReadCache
from fundlab.web.runner import DailyRunLauncher
from fundlab.web.schedule import TaskScheduler, TaskSchedulerError, WindowsTaskScheduler
from fundlab.web.schedule_cache import CachedTaskScheduler
from fundlab.web.service import DashboardError, DashboardService
from fundlab.web.snapshot_cache import InstrumentNameResolver

STATIC_ROOT = Path(__file__).parent / "static"


def create_app(
    settings: FoundationSettings,
    *,
    repo_root: str | Path | None = None,
    config_path: str | Path | None = None,
    scheduler: TaskScheduler | None = None,
    launcher: DailyRunLauncher | None = None,
    snapshot_resolver: InstrumentNameResolver | None = None,
) -> FastAPI:
    root = Path(repo_root) if repo_root is not None else Path.cwd()
    owns_snapshot_resolver = snapshot_resolver is None
    name_resolver = snapshot_resolver or InstrumentNameResolver(
        settings.paths.market_data,
    )
    service = DashboardService(settings, name_resolver=name_resolver)
    owns_scheduler_cache = scheduler is None
    task_scheduler: TaskScheduler = (
        scheduler
        if scheduler is not None
        else CachedTaskScheduler(WindowsTaskScheduler(root))
    )
    run_launcher = launcher if launcher is not None else DailyRunLauncher(
        root, root / "logs" / "daily", config_path=config_path,
    )
    accounts_cache = SingleFlightReadCache[
        tuple[list[dict[str, Any]], bytes]
    ](ttl_seconds=1.0)

    app = FastAPI(title="FundLab Dashboard", docs_url=None, redoc_url=None)

    if owns_snapshot_resolver:
        app.router.add_event_handler("shutdown", name_resolver.close)
    if owns_scheduler_cache:
        app.router.add_event_handler("shutdown", task_scheduler.close)

    @app.middleware("http")
    async def prevent_stale_dashboard_assets(request: Request, call_next):
        response = await call_next(request)
        if request.url.path in {"/", "/static/app.js", "/static/style.css"}:
            response.headers["Cache-Control"] = "no-store"
        return response

    # ------------------------------------------------------------- reads

    def load_accounts() -> tuple[list[dict[str, Any]], bytes]:
        payload = service.accounts()
        return payload, _json_bytes(payload)

    def load_overview() -> dict[str, Any]:
        schedule: dict[str, Any]
        try:
            schedule = task_scheduler.query().to_dict()
        except TaskSchedulerError as exc:
            schedule = {"error": str(exc)}
        reports = service.daily_reports(limit=1)
        return {
            "market": service.market_summary(),
            "accounts": accounts_cache.get(load_accounts)[0],
            "schedule": schedule,
            "last_report": reports[0] if reports else None,
            "run": run_launcher.status(tail_lines=1),
            "config": {
                "session_cutoff_local": settings.daily.session_cutoff.isoformat(
                    timespec="minutes",
                ),
                "agent_decision_dir": str(settings.daily.agent_decision_root),
            },
        }
    @app.get("/api/overview")
    def overview() -> dict[str, Any]:
        return load_overview()

    @app.get("/api/accounts")
    def accounts() -> Response:
        return Response(
            accounts_cache.get(load_accounts)[1],
            media_type="application/json",
        )

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

    @app.get("/api/agent/monitor")
    def crisis_monitor() -> dict[str, Any]:
        return service.crisis_monitor()

    @app.get("/api/agent/evaluations/{account_id}")
    def crisis_evaluations(account_id: str, limit: int = 90) -> dict[str, Any]:
        try:
            return service.crisis_evaluations(
                account_id, limit=max(1, min(limit, 2000)),
            )
        except DashboardError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

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
        return FileResponse(
            STATIC_ROOT / "index.html",
            headers={"Cache-Control": "no-store"},
        )

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


def _json_bytes(payload: Any) -> bytes:
    return json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
