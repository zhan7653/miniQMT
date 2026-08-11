from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
import json
from pathlib import Path
import shutil
import subprocess
from types import SimpleNamespace

import pandas as pd
import pytest

from fundlab.common.canonical import canonical_json, stable_digest
from fundlab.marketdata import (
    CanonicalMarketData,
    CURRENT_SH_SZ_STOCK_ETF_UNIVERSE,
    CoverageClaim,
    MarketDataWarehouse,
    MarketTable,
    ObservationPayload,
    ProviderCapability,
    ProviderRegistry,
    ProviderRequest,
    SnapshotPlan,
    SourceSlice,
    TradeRuleError,
    UniverseScope,
)
from fundlab.marketdata.incremental import IncrementalCanonicalPublisher
from fundlab.marketdata.contracts import EXECUTION_EVIDENCE_GAP_RULE_ID
from fundlab.marketdata.schema import empty_table
from fundlab.marketdata.sources.cninfo import (
    CninfoAnnouncementPageEvidence,
    CninfoAnnouncementScan,
)
from fundlab.marketdata.sources.eastmoney_fund import EASTMONEY_ETF_ACTION_POLICY
from fundlab.pipeline import DailyPipeline
from fundlab.pipeline.daily import (
    DailyPipelineBlocked,
    DailyRunResult,
    DailyStage,
    PIPELINE_VERSION,
)
from fundlab.settings import (
    AgentPolicySettings,
    AgentSettings,
    DailyAccountSettings,
    DailySettings,
    FoundationPaths,
    FoundationSettings,
)
from fundlab.trading import AccountStatus, PortfolioState, TradingRepository
from tests.canonical.fixtures import (
    DAYS,
    FUTURE_DAYS,
    commit_test_snapshot,
    market_frames,
    ready_market,
)
from tests.canonical.exchange_fixtures import (
    OFFICIAL_COMPONENT_ENDPOINTS as _OFFICIAL_COMPONENT_ENDPOINTS,
    OFFICIAL_COMPONENT_SCOPES as _OFFICIAL_COMPONENT_SCOPES,
    component_for_row as _daily_component_for_row,
    with_component_closure as _fixture_component_closure,
)
from tests.canonical.test_trading_kernel import fees, policies


def test_degraded_daily_result_is_success_without_instrument_count_cutoff():
    result = DailyRunResult("degraded", None, None, [], [], None)
    assert result.exit_code == 0

    transient_action_gap = (
        "batch:SnapshotNotReadyError:etf-actions evidence remains unresolved: "
        "count=1 ids=510050.SH invalid={} "
        "request_errors=510050.SH:TimeoutError:timed out",
        "missing_etf-actions_instruments:1",
    )
    assert DailyPipeline._quarantinable_action_collection(
        transient_action_gap, ("510050.SH",),
    )
    pending_stock_action = (
        "batch:SnapshotNotReadyError:stock-actions evidence remains unresolved: "
        "count=1 ids=600000.SH invalid={} "
        "request_errors=600000.SH:PendingAnnouncement:structured lifecycle not available",
        "missing_stock-actions_instruments:1",
    )
    assert DailyPipeline._quarantinable_action_collection(
        pending_stock_action, ("600000.SH",),
    )
    assert not DailyPipeline._quarantinable_action_collection((
        "batch:HistoricalActionCorrectionError:stock-actions historical correction "
        "detected: 600000.SH/2026-06-01/cash_dividend_per_share",
        "missing_stock-actions_instruments:1",
    ), ("600000.SH",))
    assert not DailyPipeline._quarantinable_action_collection((
        transient_action_gap[0].replace(
            "invalid={}", 'invalid={"510050.SH":{"reason":"bad lifecycle"}}',
        ),
        transient_action_gap[1],
    ), ("510050.SH",))
    assert not DailyPipeline._quarantinable_action_collection((
        transient_action_gap[0].replace(
            "TimeoutError:timed out", "ValueError:unexpected schema",
        ),
        transient_action_gap[1],
    ), ("510050.SH",))
    assert not DailyPipeline._quarantinable_action_collection((
        transient_action_gap[0].replace(
            "request_errors=510050.SH:TimeoutError:timed out",
            "request_errors=",
        ),
        transient_action_gap[1],
    ), ("510050.SH",))
    assert not DailyPipeline._quarantinable_action_collection(
        transient_action_gap, ("510050.SH", "510300.SH"),
    )

    assert DailyPipeline._action_gap_instrument_ids({
        "reasons_by_instrument": {
            "600000.SH": ("status:xtquant:timeout",),
            "510050.SH": ("evidence:etf-actions:timeout",),
        },
    }) == {"510050.SH"}


def test_daily_report_half_publish_recovers_then_runs_requested_business(tmp_path, monkeypatch):
    import fundlab.pipeline.daily as daily_module

    ready_market(tmp_path / "market")
    pipeline = DailyPipeline(
        build_settings(tmp_path, ()), now_fn=lambda: evening_of(FUTURE_DAYS[0]),
    )
    stage = DailyStage("accounts", "skipped", {"reason": "test"})
    real_publish = daily_module.publish_immutable_bytes
    fail_json = True

    def publish(path, payload):
        if fail_json and path.suffix == ".json":
            raise OSError("simulated JSON commit failure")
        real_publish(path, payload)

    monkeypatch.setattr(daily_module, "publish_immutable_bytes", publish)
    with pytest.raises(OSError, match="JSON commit failure"):
        pipeline._write_report("ok", FUTURE_DAYS[0], "snap-test", [stage], [])

    assert not list(pipeline.daily_report_root.glob("daily-*.json"))
    pending_name = next(
        (pipeline.daily_report_root / ".pending").glob("daily-*.json")
    ).name
    fail_json = False
    advanced = []
    monkeypatch.setattr(
        pipeline, "_advance_accounts", lambda stages: advanced.append(stages) or [],
    )
    result = pipeline.run(target_date=FUTURE_DAYS[1], skip_data=True)

    old_report = pipeline.daily_report_root / pending_name
    assert old_report.exists()
    assert result.status == "ok"
    assert result.target_date == FUTURE_DAYS[1]
    assert result.report_path is not None and result.report_path != old_report
    assert len(advanced) == 1
    assert not list((pipeline.daily_report_root / ".pending").glob("daily-*"))


def test_daily_report_missing_pending_markdown_recovers_without_public_json(
    tmp_path, monkeypatch,
):
    import fundlab.pipeline.daily as daily_module

    ready_market(tmp_path / "market")
    pipeline = DailyPipeline(
        build_settings(tmp_path, ()), now_fn=lambda: evening_of(FUTURE_DAYS[0]),
    )
    real_stage = daily_module.atomic_replace_bytes
    fail_markdown = True

    def stage(path, payload):
        if fail_markdown and path.suffix == ".md":
            raise OSError("simulated pending markdown failure")
        real_stage(path, payload)

    monkeypatch.setattr(daily_module, "atomic_replace_bytes", stage)
    with pytest.raises(OSError, match="pending markdown failure"):
        pipeline._write_report("ok", FUTURE_DAYS[0], "snap-test", [], [])

    assert not list(pipeline.daily_report_root.glob("daily-*.json"))
    assert list((pipeline.daily_report_root / ".pending").glob("daily-*.json"))
    fail_markdown = False
    result = pipeline.run(target_date=FUTURE_DAYS[1], skip_data=True, skip_accounts=True)

    assert result.target_date == FUTURE_DAYS[1]
    assert result.report_path is not None and result.report_path.exists()


def test_daily_run_finalizes_all_verified_pending_reports_then_continues(tmp_path, monkeypatch):
    import fundlab.pipeline.daily as daily_module

    ready_market(tmp_path / "market")
    pipeline = DailyPipeline(
        build_settings(tmp_path, ()), now_fn=lambda: evening_of(FUTURE_DAYS[0]),
    )
    real_publish = daily_module.publish_immutable_bytes
    fail_json = True

    def publish(path, payload):
        if fail_json and path.suffix == ".json":
            raise OSError("simulated JSON commit failure")
        real_publish(path, payload)

    monkeypatch.setattr(daily_module, "publish_immutable_bytes", publish)
    for target in FUTURE_DAYS[:2]:
        with pytest.raises(OSError, match="JSON commit failure"):
            pipeline._write_report("ok", target, "snap-test", [], [])
    pending_names = {
        path.name for path in (pipeline.daily_report_root / ".pending").glob("daily-*.json")
    }
    assert len(pending_names) == 2

    fail_json = False
    advanced = []
    monkeypatch.setattr(
        pipeline, "_advance_accounts", lambda stages: advanced.append(stages) or [],
    )
    result = pipeline.run(target_date=FUTURE_DAYS[-1], skip_data=True)

    assert result.target_date == FUTURE_DAYS[-1]
    assert len(advanced) == 1
    assert not list((pipeline.daily_report_root / ".pending").glob("daily-*"))
    assert pending_names <= {
        path.name for path in pipeline.daily_report_root.glob("daily-*.json")
    }


def test_daily_run_repairs_missing_or_corrupt_markdown_from_json(tmp_path):
    ready_market(tmp_path / "market")
    pipeline = DailyPipeline(
        build_settings(tmp_path, ()), now_fn=lambda: evening_of(FUTURE_DAYS[0]),
    )
    path = pipeline._write_report("ok", None, "snap-test", [], [])
    markdown = path.with_suffix(".md")
    markdown.write_bytes(b"corrupt")

    pipeline.run(skip_data=True, skip_accounts=True)

    payload = json.loads(path.read_text(encoding="utf-8"))
    assert markdown.read_text(encoding="utf-8") == pipeline._markdown_summary(payload)


def test_daily_run_repairs_pending_current_report_markdown_after_json_commit(tmp_path):
    ready_market(tmp_path / "market")
    pipeline = DailyPipeline(
        build_settings(tmp_path, ()), now_fn=lambda: evening_of(FUTURE_DAYS[0]),
    )
    committed_json = pipeline._write_report("ok", FUTURE_DAYS[0], "snap-test", [], [])
    committed_markdown = committed_json.with_suffix(".md")
    pending_json = pipeline.daily_report_root / ".pending" / committed_json.name
    pending_json.parent.mkdir(parents=True, exist_ok=True)
    pending_json.write_bytes(committed_json.read_bytes())
    committed_markdown.write_bytes(b"corrupt")

    result = pipeline.run(target_date=FUTURE_DAYS[1], skip_data=True, skip_accounts=True)

    payload = json.loads(committed_json.read_text(encoding="utf-8"))
    assert result.target_date == FUTURE_DAYS[1]
    assert committed_markdown.read_text(encoding="utf-8") == pipeline._markdown_summary(payload)
    assert not pending_json.exists()


def test_daily_run_accepts_v1_published_report_and_writes_current_report(tmp_path):
    ready_market(tmp_path / "market")
    pipeline = DailyPipeline(
        build_settings(tmp_path, ()), now_fn=lambda: evening_of(FUTURE_DAYS[0]),
    )
    legacy_payload = {
        "pipeline": "daily-pipeline-v1",
        "generated_at": evening_of(FUTURE_DAYS[0]).isoformat(),
        "status": "ok",
        "target_date": FUTURE_DAYS[0].isoformat(),
        "snapshot_id": "snap-legacy",
        "stages": [{"name": "data", "status": "up_to_date", "detail": {}}],
        "accounts": [],
    }
    json_name, markdown_name = pipeline._report_filename(legacy_payload)
    legacy_json = pipeline.daily_report_root / json_name
    legacy_json.parent.mkdir(parents=True)
    legacy_json.write_bytes((canonical_json(legacy_payload) + "\n").encode("utf-8"))
    legacy_markdown = pipeline.daily_report_root / markdown_name
    legacy_markdown.write_bytes(b"corrupt")

    result = pipeline.run(target_date=FUTURE_DAYS[1], skip_data=True, skip_accounts=True)

    assert result.target_date == FUTURE_DAYS[1]
    assert json.loads(result.report_path.read_text(encoding="utf-8"))["pipeline"] == PIPELINE_VERSION
    assert legacy_markdown.read_text(encoding="utf-8") == pipeline._markdown_summary(legacy_payload)


def test_daily_run_rejects_corrupt_current_published_report(tmp_path):
    ready_market(tmp_path / "market")
    pipeline = DailyPipeline(
        build_settings(tmp_path, ()), now_fn=lambda: evening_of(FUTURE_DAYS[0]),
    )
    path = pipeline._write_report("ok", None, "snap-test", [], [])
    corrupt = json.loads(path.read_text(encoding="utf-8"))
    corrupt["status"] = "blocked"
    path.write_text(canonical_json(corrupt), encoding="utf-8")

    with pytest.raises(ValueError, match="filename or payload verification failed"):
        pipeline.run(target_date=FUTURE_DAYS[1], skip_data=True, skip_accounts=True)


def test_daily_run_rejects_unverified_pending_report(tmp_path):
    pipeline = DailyPipeline(
        build_settings(tmp_path, ()), now_fn=lambda: evening_of(FUTURE_DAYS[0]),
    )
    pending = pipeline.daily_report_root / ".pending"
    pending.mkdir(parents=True)
    (pending / "daily-unknown-0000000000.json").write_text("{}", encoding="utf-8")

    with pytest.raises(ValueError, match="Invalid daily report payload"):
        pipeline.run(skip_data=True, skip_accounts=True)


def test_daily_unexpected_business_exception_writes_blocked_report(tmp_path, monkeypatch):
    ready_market(tmp_path / "market")
    pipeline = DailyPipeline(
        build_settings(tmp_path, ()), now_fn=lambda: evening_of(FUTURE_DAYS[0]),
    )
    monkeypatch.setattr(
        pipeline, "_validated_calendar",
        lambda _start: (_ for _ in ()).throw(RuntimeError("unexpected provider bug")),
    )

    result = pipeline.run(skip_accounts=True)

    assert result.status == "blocked" and result.exit_code == 2
    assert result.stages[-1].name == "run"
    assert result.stages[-1].detail["error_type"] == "RuntimeError"
    assert result.report_path is not None
    assert json.loads(result.report_path.read_text(encoding="utf-8"))["status"] == "blocked"


def test_daily_report_commit_failure_does_not_return_success_result(tmp_path, monkeypatch):
    import fundlab.pipeline.daily as daily_module

    ready_market(tmp_path / "market")
    pipeline = DailyPipeline(
        build_settings(tmp_path, ()), now_fn=lambda: evening_of(FUTURE_DAYS[0]),
    )
    real_publish = daily_module.publish_immutable_bytes

    def fail_json(path, payload):
        if path.suffix == ".json":
            raise OSError("cannot commit report JSON")
        real_publish(path, payload)

    monkeypatch.setattr(daily_module, "publish_immutable_bytes", fail_json)
    with pytest.raises(OSError, match="cannot commit report JSON"):
        pipeline.run(skip_data=True, skip_accounts=True)
    assert not list(pipeline.daily_report_root.glob("daily-*.json"))


def test_daily_report_repeated_content_is_immutable_and_idempotent(tmp_path):
    pipeline = DailyPipeline(
        build_settings(tmp_path, ()), now_fn=lambda: evening_of(FUTURE_DAYS[0]),
    )
    stage = DailyStage("accounts", "skipped", {"reason": "test"})

    first = pipeline._write_report("ok", FUTURE_DAYS[0], "snap-test", [stage], [])
    second = pipeline._write_report("ok", FUTURE_DAYS[0], "snap-test", [stage], [])

    assert first == second
    assert len(list(pipeline.daily_report_root.glob("daily-*.json"))) == 1
    assert len(list(pipeline.daily_report_root.glob("daily-*.md"))) == 1


def test_daily_run_recovers_eod_receipts_even_when_data_is_up_to_date(
    tmp_path, monkeypatch,
):
    import fundlab.pipeline.daily as daily_module

    ready_market(tmp_path / "market")
    pipeline = DailyPipeline(
        build_settings(tmp_path, ()), now_fn=lambda: evening_of(FUTURE_DAYS[0]),
    )
    recovered: list[Path] = []

    def recover(builder):
        recovered.append(builder.report_root)

    monkeypatch.setattr(
        daily_module.SimulationSnapshotBuilder, "recover_pending_reports", recover,
    )

    result = pipeline.run(skip_data=True, skip_accounts=True)

    assert result.exit_code == 0
    assert recovered == [pipeline.report_root]


def test_no_trade_active_source_conflict_becomes_bounded_quarantine(monkeypatch):
    import fundlab.marketdata.history as history_module
    import fundlab.pipeline.daily as daily_module

    target = ("510050.SH", "600000.SH")
    pipeline = object.__new__(DailyPipeline)
    pipeline.warehouse = SimpleNamespace(
        matching_observations=lambda **kwargs: (),
        record_observation=lambda payload: SimpleNamespace(
            observation_id=f"obs-{payload['provider']}"
        ),
    )
    pipeline.registry = SimpleNamespace(
        observe=lambda provider, request: {"provider": provider, "request": request},
    )
    monkeypatch.setattr(
        daily_module, "find_no_trade_research_partition", lambda *args, **kwargs: None,
    )
    calls = []

    def record_partition(warehouse, **kwargs):
        calls.append(kwargs)
        if kwargs["instrument_ids"] == target:
            raise history_module.NoTradeSourceActiveError({
                "510050.SH": ("obs-baostock",),
            })
        assert kwargs["instrument_ids"] == ("600000.SH",)
        return SimpleNamespace(observation_id="obs-confirmed-no-trade")

    monkeypatch.setattr(
        daily_module, "record_no_trade_research_partition", record_partition,
    )
    reasons = {}

    resolution = pipeline._record_no_trade(
        predecessor_snapshot_id="snap-predecessor",
        universe_observation_id="obs-universe",
        calendar_observation_id="obs-calendar",
        start=FUTURE_DAYS[0],
        end=FUTURE_DAYS[0],
        instrument_ids=target,
        quarantine_reasons=reasons,
        universe_size=100,
    )

    assert resolution.observation_id == "obs-confirmed-no-trade"
    assert resolution.confirmed_instrument_ids == ("600000.SH",)
    assert resolution.active_conflicts == {
        "510050.SH": ("obs-baostock",),
    }
    assert "obs-baostock" in reasons["510050.SH"][0]
    assert [item["instrument_ids"] for item in calls] == [
        target, ("600000.SH",),
    ]

    repeated = pipeline._record_no_trade(
        predecessor_snapshot_id="snap-predecessor",
        universe_observation_id="obs-universe",
        calendar_observation_id="obs-calendar",
        start=FUTURE_DAYS[0],
        end=FUTURE_DAYS[0],
        instrument_ids=target,
        quarantine_reasons={},
        universe_size=1,
    )
    assert repeated.confirmed_instrument_ids == ("600000.SH",)


def test_quarantine_remains_non_blocking_after_six_consecutive_sessions():
    from fundlab.marketdata.contracts import DATA_GAP_QUARANTINE_RULE_ID

    instrument_id = "600000.SH"
    target = date(2026, 7, 30)
    previous_end = target - timedelta(days=1)
    prior_rows = pd.DataFrame({
        "instrument_id": [instrument_id] * 4,
        "session_date": [
            "2026-07-24", "2026-07-25", "2026-07-28", "2026-07-29",
        ],
        "trade_rule_id": [DATA_GAP_QUARANTINE_RULE_ID] * 4,
    })
    pipeline = object.__new__(DailyPipeline)
    pipeline.warehouse = SimpleNamespace(
        query_loaded_snapshot_table=lambda *args, **kwargs: prior_rows,
    )
    predecessor = SimpleNamespace(plan=SimpleNamespace(universe_scope=UniverseScope(
        CURRENT_SH_SZ_STOCK_ETF_UNIVERSE,
        previous_end,
        date(2026, 1, 1),
        previous_end,
        survivorship_bias=True,
        instrument_ids=(instrument_id,),
    )))
    instruments = pd.DataFrame([{
        "instrument_id": instrument_id,
        "exchange": "SH",
        "listed_date": "2000-01-01",
        "delisted_date": None,
    }])
    calendar = pd.DataFrame([{
        "exchange": "SH", "session_date": target.isoformat(), "is_open": True,
    }])
    kwargs = {
        "predecessor": predecessor,
        "official_frame": instruments,
        "calendar_frame": calendar,
        "increment_start": target,
        "target": target,
        "reasons": {instrument_id: ["fixture-gap"]},
        "universe_size": 100,
    }

    detail = pipeline._finalize_quarantine(**kwargs)
    assert detail["consecutive_sessions"][instrument_id] == 5

    prior_rows.loc[len(prior_rows)] = {
        "instrument_id": instrument_id,
        "session_date": "2026-07-23",
        "trade_rule_id": DATA_GAP_QUARANTINE_RULE_ID,
    }
    detail = pipeline._finalize_quarantine(**kwargs)
    assert detail["consecutive_sessions"][instrument_id] == 6
    assert detail["blocking_thresholds_enforced"] is False


def test_persistent_quarantine_uses_current_contiguous_tail_not_historical_max():
    from fundlab.marketdata.contracts import DATA_GAP_QUARANTINE_RULE_ID

    instrument_id = "600000.SH"
    sessions = tuple(date(2026, 7, day) for day in (21, 22, 23, 24, 25, 28, 29))
    old = {
        "instrument_ids": (instrument_id,),
        "increment_sessions": {
            instrument_id: tuple(item.isoformat() for item in sessions[:5]),
        },
        "consecutive_sessions": {instrument_id: 5},
        "reasons_by_instrument": {instrument_id: ("old-gap",)},
    }
    current = {
        "instrument_ids": (instrument_id,),
        "increment_sessions": {instrument_id: (sessions[-1].isoformat(),)},
        "consecutive_sessions": {instrument_id: 1},
        "reasons_by_instrument": {instrument_id: ("new-gap",)},
    }
    rules = (
        (DATA_GAP_QUARANTINE_RULE_ID,) * 5
        + ("cn-stock-main-v1", DATA_GAP_QUARANTINE_RULE_ID)
    )
    bars = pd.DataFrame({
        "instrument_id": [instrument_id] * len(sessions),
        "session_date": [item.isoformat() for item in sessions],
        "trade_rule_id": rules,
    })
    manifests = {
        "obs-old": SimpleNamespace(source_metadata={"degraded_quarantine": old}),
        "obs-current": SimpleNamespace(source_metadata={"degraded_quarantine": current}),
    }
    pipeline = object.__new__(DailyPipeline)
    pipeline.warehouse = SimpleNamespace(
        load_observation=lambda observation_id: manifests[observation_id],
        query_loaded_snapshot_table=lambda *args, **kwargs: bars,
    )
    snapshot = SimpleNamespace(plan=SimpleNamespace(
        selections=(
            SimpleNamespace(observation_id="obs-old"),
            SimpleNamespace(observation_id="obs-current"),
        ),
        universe_scope=UniverseScope(
            CURRENT_SH_SZ_STOCK_ETF_UNIVERSE,
            sessions[-1],
            sessions[0],
            sessions[-1],
            survivorship_bias=True,
            instrument_ids=(instrument_id,),
        ),
    ))

    detail = pipeline._snapshot_degraded_quarantine(snapshot)

    assert detail["instrument_ids"] == (instrument_id,)
    assert detail["consecutive_sessions"] == {instrument_id: 1}
    assert detail["reasons_by_instrument"] == {instrument_id: ("new-gap",)}


class CalendarProvider:
    capabilities = frozenset({ProviderCapability.TRADING_CALENDAR})

    def __init__(self, name: str, frame: pd.DataFrame):
        self.name = name
        self.frame = frame
        self.calls = 0

    def observe(self, request: ProviderRequest) -> ObservationPayload:
        self.calls += 1
        return ObservationPayload(
            self.name,
            datetime(2026, 7, 16, 12, 0, tzinfo=timezone.utc),
            request,
            {MarketTable.CALENDAR: self.frame},
            (CoverageClaim(
                MarketTable.CALENDAR, True, request.start_date, request.end_date,
            ),),
            {"backend_group": self.name},
        )


def calendar_frame(extra_open: date | None = None) -> pd.DataFrame:
    frame = market_frames()[MarketTable.CALENDAR].copy()
    if extra_open is not None:
        frame = pd.concat((frame, pd.DataFrame([{
            "exchange": "SH",
            "session_date": extra_open.isoformat(),
            "is_open": True,
            "source_payload": None,
        }])), ignore_index=True)
    return frame


def build_settings(tmp_path, accounts: tuple[DailyAccountSettings, ...]) -> FoundationSettings:
    execution, risk = policies()
    return FoundationSettings(
        FoundationPaths(
            market_data=tmp_path / "market",
            trading_database=tmp_path / "trading.sqlite3",
            report_root=tmp_path / "reports",
        ),
        execution,
        risk,
        fees(),
        DailySettings(
            session_cutoff=time(19, 0),
            agent_decision_root=tmp_path / "decisions",
            report_root=tmp_path / "daily-reports",
            accounts=accounts,
        ),
    )


def registry_with_calendars(second_frame: pd.DataFrame | None = None) -> ProviderRegistry:
    registry = ProviderRegistry()
    registry.register(CalendarProvider("baostock", calendar_frame()))
    registry.register(CalendarProvider(
        "sina-calendar", calendar_frame() if second_frame is None else second_frame,
    ))
    return registry


def evening_of(day: date) -> datetime:
    return datetime(day.year, day.month, day.day, 20, 0)


def test_daily_run_is_idempotent_and_advances_static_account(tmp_path):
    ready_market(tmp_path / "market")
    settings = build_settings(tmp_path, (
        DailyAccountSettings(
            "paper-1", "Paper 1", Decimal("100000"), "static",
            {"600000.SH": Decimal("0.5")},
        ),
    ))
    pipeline = DailyPipeline(
        settings,
        registry=registry_with_calendars(),
        now_fn=lambda: evening_of(DAYS[-1]),
    )

    first = pipeline.run()
    assert first.status == "ok" and first.exit_code == 0
    assert first.target_date == DAYS[-1]
    stage_status = {item.name: item.status for item in first.stages}
    assert stage_status["data"] == "up_to_date"
    assert stage_status["accounts"] == "ok"
    assert first.accounts[0]["status"] == "ok"
    assert first.accounts[0]["sessions_advanced"] == 1
    assert first.report_path is not None and first.report_path.is_file()
    payload = json.loads(first.report_path.read_text(encoding="utf-8"))
    assert payload["status"] == "ok"

    repository = TradingRepository(settings.paths.trading_database)
    _, head_run = repository.selected_state("paper-1")
    assert head_run is not None
    # The account clock advances to the published data head; the snapshot
    # calendar carries exchange-announced future sessions, so the close-of-head
    # intent schedules its T+1 order into the first future trading day.
    assert repository.run(head_run).binding.end_date == DAYS[-1]
    final = repository.final_state(head_run)
    assert final.pending_orders, "the static intent must actually schedule an order"
    assert final.pending_orders[0].execution_date == FUTURE_DAYS[0]

    second = pipeline.run()
    assert second.status == "ok"
    assert second.accounts[0]["sessions_advanced"] == 0


def test_daily_run_skips_disabled_account_without_creating_it(tmp_path):
    ready_market(tmp_path / "market")
    account = DailyAccountSettings(
        "disabled-paper",
        "Disabled Paper",
        Decimal("100000"),
        "static",
        {"600000.SH": Decimal("0.5")},
        enabled=False,
    )
    settings = build_settings(tmp_path, (account,))

    result = DailyPipeline(
        settings,
        registry=registry_with_calendars(),
        now_fn=lambda: evening_of(DAYS[-1]),
    ).run()

    assert result.status == "ok"
    assert result.accounts == [{
        "account_id": account.account_id,
        "status": "disabled",
        "strategy": "static",
        "sessions_advanced": 0,
        "head": None,
    }]
    with pytest.raises(KeyError, match="Unknown trading account"):
        TradingRepository(settings.paths.trading_database).account(account.account_id)


def test_daily_run_skips_persistently_paused_account(tmp_path):
    ready_market(tmp_path / "market")
    account = DailyAccountSettings(
        "paused-paper",
        "Paused Paper",
        Decimal("100000"),
        "static",
        {"600000.SH": Decimal("0.5")},
    )
    settings = build_settings(tmp_path, (account,))
    repository = TradingRepository(settings.paths.trading_database)
    repository.create_account(
        account.account_id,
        account.name,
        PortfolioState.with_cash(account.initial_cash),
    )
    repository.set_account_status(account.account_id, AccountStatus.PAUSED)

    result = DailyPipeline(
        settings,
        registry=registry_with_calendars(),
        now_fn=lambda: evening_of(DAYS[-1]),
    ).run()

    assert result.status == "ok"
    assert result.accounts[0]["status"] == "paused"
    assert result.accounts[0]["sessions_advanced"] == 0
    assert repository.account(account.account_id).selected_run_id is None


@pytest.mark.parametrize("statuses", [
    ("ok", "blocked"),
    ("blocked", "ok"),
])
def test_accounts_stage_degrades_when_one_real_account_commits_and_one_blocks(
    tmp_path, monkeypatch, statuses,
):
    import fundlab.pipeline.daily as daily_module

    accounts = tuple(
        DailyAccountSettings(
            f"paper-{index}", f"Paper {index}", Decimal("100000"), "static",
            {"600000.SH": Decimal("0.5")},
        )
        for index in range(2)
    )
    scope = UniverseScope(
        CURRENT_SH_SZ_STOCK_ETF_UNIVERSE, DAYS[-1], DAYS[0], DAYS[-1],
        instrument_ids=_DAILY_BASE_IDS,
    )
    market = SimpleNamespace(
        snapshot_id="snapshot-published",
        manifest=SimpleNamespace(plan=SimpleNamespace(universe_scope=scope)),
    )
    monkeypatch.setattr(daily_module.CanonicalMarketData, "open", lambda path: market)
    monkeypatch.setattr(daily_module, "TradingRepository", lambda path: object())
    monkeypatch.setattr(daily_module, "SimulationService", lambda **kwargs: object())
    pipeline = DailyPipeline(build_settings(tmp_path, accounts))
    responses = [
        {
            "account_id": account.account_id,
            "status": status,
            "head": DAYS[-1].isoformat() if status == "ok" else None,
            "sessions_advanced": 1 if status == "ok" else 0,
        }
        for account, status in zip(accounts, statuses, strict=True)
    ]
    monkeypatch.setattr(pipeline, "_advance_one_account", lambda *args: responses.pop(0))

    stages: list[DailyStage] = []
    result = pipeline._advance_accounts(stages)

    assert [item["status"] for item in result] == list(statuses)
    assert stages[-1].status == "degraded"
    assert stages[-1].detail["advanced"] == 1
    assert len(stages[-1].detail["blocked"]) == 1
    assert next(item for item in result if item["status"] == "ok")["head"] == DAYS[-1].isoformat()


def test_accounts_stage_blocks_when_up_to_date_account_has_no_new_commit_and_peer_blocks(
    tmp_path, monkeypatch,
):
    import fundlab.pipeline.daily as daily_module

    accounts = tuple(
        DailyAccountSettings(
            f"paper-{index}", f"Paper {index}", Decimal("100000"), "static",
            {"600000.SH": Decimal("0.5")},
        )
        for index in range(2)
    )
    scope = UniverseScope(
        CURRENT_SH_SZ_STOCK_ETF_UNIVERSE, DAYS[-1], DAYS[0], DAYS[-1],
        instrument_ids=_DAILY_BASE_IDS,
    )
    market = SimpleNamespace(
        snapshot_id="snapshot-published",
        manifest=SimpleNamespace(plan=SimpleNamespace(universe_scope=scope)),
    )
    monkeypatch.setattr(daily_module.CanonicalMarketData, "open", lambda path: market)
    monkeypatch.setattr(daily_module, "TradingRepository", lambda path: object())
    monkeypatch.setattr(daily_module, "SimulationService", lambda **kwargs: object())
    pipeline = DailyPipeline(build_settings(tmp_path, accounts))
    responses = [
        {"account_id": accounts[0].account_id, "status": "ok", "sessions_advanced": 0},
        {"account_id": accounts[1].account_id, "status": "blocked", "sessions_advanced": 0},
    ]
    monkeypatch.setattr(pipeline, "_advance_one_account", lambda *args: responses.pop(0))

    stages: list[DailyStage] = []
    pipeline._advance_accounts(stages)

    assert stages[-1].status == "blocked"
    assert stages[-1].detail["advanced"] == 0


def test_accounts_stage_blocks_only_when_every_attempted_account_blocks(tmp_path, monkeypatch):
    import fundlab.pipeline.daily as daily_module

    accounts = tuple(
        DailyAccountSettings(
            f"paper-{index}", f"Paper {index}", Decimal("100000"), "static",
            {"600000.SH": Decimal("0.5")},
        )
        for index in range(2)
    )
    scope = UniverseScope(
        CURRENT_SH_SZ_STOCK_ETF_UNIVERSE, DAYS[-1], DAYS[0], DAYS[-1],
        instrument_ids=_DAILY_BASE_IDS,
    )
    market = SimpleNamespace(
        snapshot_id="snapshot-published",
        manifest=SimpleNamespace(plan=SimpleNamespace(universe_scope=scope)),
    )
    monkeypatch.setattr(daily_module.CanonicalMarketData, "open", lambda path: market)
    monkeypatch.setattr(daily_module, "TradingRepository", lambda path: object())
    monkeypatch.setattr(daily_module, "SimulationService", lambda **kwargs: object())
    pipeline = DailyPipeline(build_settings(tmp_path, accounts))
    monkeypatch.setattr(
        pipeline, "_advance_one_account",
        lambda account, *args: {"account_id": account.account_id, "status": "blocked"},
    )

    stages: list[DailyStage] = []
    pipeline._advance_accounts(stages)

    assert stages[-1].status == "blocked"
    assert stages[-1].detail["advanced"] == 0


def test_account_partial_sessions_and_feedback_failure_are_degraded(tmp_path, monkeypatch):
    import fundlab.pipeline.daily as daily_module

    account = DailyAccountSettings(
        "paper-partial", "Paper Partial", Decimal("100000"), "static",
        {"600000.SH": Decimal("0.5")},
    )

    class Repository:
        def __init__(self, selected_run_id="prior"):
            self.selected_run_id = selected_run_id
            self.dates = {"prior": DAYS[-1]}

        def account(self, account_id):
            return SimpleNamespace(status=AccountStatus.ACTIVE)

        def create_account(self, *args):  # pragma: no cover - active fixture
            raise AssertionError("existing account must be used")

        def selected_state(self, account_id):
            return None, self.selected_run_id

        def run(self, run_id):
            return SimpleNamespace(binding=SimpleNamespace(end_date=self.dates[run_id]))

    class Market:
        def trading_days(self, start, end):
            return (FUTURE_DAYS[0], FUTURE_DAYS[1])

    class PartialService:
        def __init__(self, repository):
            self.repository = repository
            self.calls = 0

        def run_daily(self, account_id, session, source):
            self.calls += 1
            if self.calls == 2:
                raise RuntimeError("second session failed")
            self.repository.dates["committed"] = session
            self.repository.selected_run_id = "committed"
            return SimpleNamespace(run=SimpleNamespace(run_id="committed"))

    pipeline = DailyPipeline(build_settings(tmp_path, (account,)))
    repository = Repository()
    partial = pipeline._advance_one_account(
        account, Market(), repository, PartialService(repository), FUTURE_DAYS[1],
    )
    assert partial["status"] == "degraded"
    assert partial["sessions_advanced"] == 1
    assert partial["head"] == FUTURE_DAYS[0].isoformat()

    class PostCommitFailureService:
        def __init__(self, repository):
            self.repository = repository

        def run_daily(self, account_id, session, source):
            # Mirrors SimulationService's COMPLETE/head transaction followed
            # by a failing verify_run call.
            self.repository.dates["committed-after-error"] = session
            self.repository.selected_run_id = "committed-after-error"
            raise RuntimeError("post-promotion verification failed")

    post_commit_repository = Repository()
    post_commit = pipeline._advance_one_account(
        account,
        Market(),
        post_commit_repository,
        PostCommitFailureService(post_commit_repository),
        FUTURE_DAYS[1],
    )
    assert post_commit["status"] == "degraded"
    assert post_commit["sessions_advanced"] == 1
    assert post_commit["head"] == FUTURE_DAYS[0].isoformat()
    assert "post-promotion verification failed" in post_commit["error"]

    class OneSessionMarket:
        def trading_days(self, start, end):
            return (FUTURE_DAYS[1],)

    feedback_repository = Repository()
    feedback_service = PartialService(feedback_repository)
    monkeypatch.setattr(
        daily_module, "build_simulation_feedback",
        lambda *args: (_ for _ in ()).throw(RuntimeError("feedback unavailable")),
    )
    feedback = pipeline._advance_one_account(
        account, OneSessionMarket(), feedback_repository, feedback_service, FUTURE_DAYS[1],
    )
    assert feedback["status"] == "degraded"
    assert feedback["sessions_advanced"] == 1
    assert feedback["head"] == FUTURE_DAYS[1].isoformat()


def test_account_repository_failure_does_not_create_a_replacement_account(tmp_path):
    account = DailyAccountSettings(
        "paper-repository", "Paper Repository", Decimal("100000"), "static",
        {"600000.SH": Decimal("0.5")},
    )

    class Repository:
        def account(self, account_id):
            raise RuntimeError("trading database unavailable")

        def create_account(self, *args):  # pragma: no cover - must not run
            raise AssertionError("repository fault must not create a replacement account")

    pipeline = DailyPipeline(build_settings(tmp_path, (account,)))
    result = pipeline._advance_one_account(
        account, SimpleNamespace(), Repository(), SimpleNamespace(), FUTURE_DAYS[0],
    )

    assert result["status"] == "blocked"
    assert result["error_type"] == "RuntimeError"


def test_account_completed_run_without_selected_head_promotion_is_blocked(tmp_path):
    account = DailyAccountSettings(
        "paper-unpromoted", "Paper Unpromoted", Decimal("100000"), "static",
        {"600000.SH": Decimal("0.5")},
    )

    class Repository:
        def account(self, account_id):
            return SimpleNamespace(status=AccountStatus.ACTIVE)

        def selected_state(self, account_id):
            return None, "prior"

        def run(self, run_id):
            return SimpleNamespace(binding=SimpleNamespace(end_date=DAYS[-1]))

    class Market:
        def trading_days(self, start, end):
            return (FUTURE_DAYS[0],)

    class UnpromotedService:
        def run_daily(self, account_id, session, source):
            return SimpleNamespace(run=SimpleNamespace(run_id="complete-not-selected"))

    result = DailyPipeline(build_settings(tmp_path, (account,)))._advance_one_account(
        account, Market(), Repository(), UnpromotedService(), FUTURE_DAYS[0],
    )

    assert result["status"] == "blocked"
    assert "without promoting" in result["error"]


def test_daily_ma_grid_close_signal_schedules_the_next_session_without_file_lag(
    tmp_path,
):
    ready_market(tmp_path / "market")
    account = DailyAccountSettings(
        "paper-grid", "Paper Grid", Decimal("100000"), "moving-average-grid"
    )
    settings = replace(
        build_settings(tmp_path, (account,)),
        agent=AgentSettings(policies={
            account.account_id: AgentPolicySettings(
                account.account_id,
                "moving-average-grid",
                {
                    "instrument": "600000.SH",
                    "activation_date": DAYS[-1].isoformat(),
                    "max_weight": "0.80",
                    "minimum_grid_step": "0.01",
                    "moving_average_days": 2,
                    "trend_average_days": 3,
                    "trend_slope_days": 1,
                    "residual_window_days": 2,
                    "startup_ramp_days": 1,
                },
            ),
        }),
    )
    result = DailyPipeline(
        settings,
        registry=registry_with_calendars(),
        now_fn=lambda: evening_of(DAYS[-1]),
    ).run()

    assert result.status == "ok"
    repository = TradingRepository(settings.paths.trading_database)
    _, run_id = repository.selected_state(account.account_id)
    assert run_id is not None
    run = repository.run(run_id)
    assert run.binding.strategy_id == "moving-average-grid"
    assert run.binding.end_date == DAYS[-1]
    pending = repository.final_state(run_id).pending_orders
    assert len(pending) == 1
    assert pending[0].execution_date == FUTURE_DAYS[0]
    assert not (settings.daily.agent_decision_root / account.account_id).exists()


def test_daily_run_blocks_when_calendar_sources_disagree(tmp_path):
    ready_market(tmp_path / "market")
    account = DailyAccountSettings(
        "paper-calendar", "Paper Calendar", Decimal("100000"), "static",
        {"600000.SH": Decimal("0.5")},
    )
    settings = build_settings(tmp_path, (account,))
    healthy = DailyPipeline(
        settings,
        registry=registry_with_calendars(),
        now_fn=lambda: evening_of(DAYS[-1]),
    )
    assert healthy.run(skip_data=True).status == "ok"
    warehouse = MarketDataWarehouse(tmp_path / "market")
    snapshot_before = warehouse.current_snapshot_id()
    repository = TradingRepository(settings.paths.trading_database)
    _, head_before = repository.selected_state(account.account_id)
    pipeline = DailyPipeline(
        settings,
        registry=registry_with_calendars(calendar_frame(extra_open=date(2026, 7, 21))),
        now_fn=lambda: evening_of(DAYS[-1]),
    )

    result = pipeline.run()

    assert result.status == "blocked" and result.exit_code == 2
    blocked = [item for item in result.stages if item.status == "blocked"]
    assert blocked and blocked[0].detail["reason"] == "calendar sources disagree"
    assert result.report_path is not None
    assert result.snapshot_id is None
    assert warehouse.current_snapshot_id() == snapshot_before
    assert repository.selected_state(account.account_id)[1] == head_before


def test_daily_run_agent_account_holds_without_decision_and_executes_with_one(tmp_path):
    ready_market(tmp_path / "market")
    settings = build_settings(tmp_path, (
        DailyAccountSettings("paper-agent", "Agent", Decimal("100000"), "agent-file"),
    ))
    pipeline = DailyPipeline(
        settings,
        registry=registry_with_calendars(),
        now_fn=lambda: evening_of(DAYS[-1]),
    )

    held = pipeline.run()
    assert held.status == "ok"
    assert held.accounts[0]["status"] == "ok"
    repository = TradingRepository(settings.paths.trading_database)
    _, run_id = repository.selected_state("paper-agent")
    assert run_id is not None
    assert not repository.final_state(run_id).lots, "no decision file means hold"


def test_daily_run_respects_skip_flags(tmp_path):
    ready_market(tmp_path / "market")
    settings = build_settings(tmp_path, ())
    pipeline = DailyPipeline(
        settings,
        registry=registry_with_calendars(),
        now_fn=lambda: evening_of(DAYS[-1]),
    )

    result = pipeline.run(skip_data=True, skip_accounts=True)

    assert result.status == "ok"
    stage_status = {item.name: item.status for item in result.stages}
    assert stage_status["data"] == "skipped"
    assert stage_status["accounts"] == "skipped"


def test_cutoff_targets_previous_session_before_evening(tmp_path):
    ready_market(tmp_path / "market")
    settings = build_settings(tmp_path, ())
    pipeline = DailyPipeline(
        settings,
        registry=registry_with_calendars(),
        now_fn=lambda: datetime(
            DAYS[-1].year, DAYS[-1].month, DAYS[-1].day, 10, 0,
        ),
    )

    result = pipeline.run(skip_accounts=True)

    assert result.status == "ok"
    assert result.target_date == DAYS[-2]


def test_concurrent_daily_runs_are_excluded_by_the_lock(tmp_path):
    from fundlab.pipeline.daily import _exclusive_daily_lock

    ready_market(tmp_path / "market")
    settings = build_settings(tmp_path, ())
    pipeline = DailyPipeline(
        settings,
        registry=registry_with_calendars(),
        now_fn=lambda: evening_of(DAYS[-1]),
    )
    lock_path = tmp_path / "market" / "builds" / ".locks" / "daily-run.lock"

    with _exclusive_daily_lock(lock_path):
        result = pipeline.run(skip_data=True, skip_accounts=True)

    assert result.status == "blocked" and result.exit_code == 2
    assert result.stages[-1].name == "lock"

    released = pipeline.run(skip_data=True, skip_accounts=True)
    assert released.status == "ok"


def test_scoped_events_and_missing_source_reasons():
    frame = pd.DataFrame([
        {"instrument_id": "600000.SH", "ex_date": DAYS[1].isoformat(), "value": 1},
        {"instrument_id": "600000.SH", "ex_date": "2027-01-01", "value": 2},
        {"instrument_id": "999999.SZ", "ex_date": DAYS[1].isoformat(), "value": 3},
    ])
    from tests.canonical.fixtures import fixture_universe_scope

    scoped = DailyPipeline._scoped_events(
        frame, date_column="ex_date", scope=fixture_universe_scope(),
    )
    assert list(scoped["value"]) == [1]

    assert DailyPipeline._only_missing_sources(["missing_source:tickflow", "missing_source:xtquant"])
    assert not DailyPipeline._only_missing_sources(["missing_source:tickflow", "critical_conflict:close"])
    assert not DailyPipeline._only_missing_sources([])
    assert not DailyPipeline._only_missing_sources(None)
    assert DailyPipeline._only_not_in_historical_master(["not_in_historical_master"])
    assert not DailyPipeline._only_not_in_historical_master([
        "not_in_historical_master", "missing_source:tickflow",
    ])
    assert DailyPipeline._only_retryable_provider_errors([
        "provider_error:tickflow=ObservationError:Source request failed: "
        "_ssl.c:1011: The handshake operation timed out",
    ])
    assert DailyPipeline._only_retryable_provider_errors([
        "provider_error:tickflow=ObservationError:Source HTTP 503: unavailable",
    ])
    assert not DailyPipeline._only_retryable_provider_errors([
        "provider_error:tickflow=ObservationError:Source returned invalid UTF-8 JSON",
    ])
    assert not DailyPipeline._only_retryable_provider_errors([
        "provider_error:tickflow=ObservationError:Source request failed: timeout;"
        "xtquant=ValueError:unexpected schema",
    ])
    assert DailyPipeline._only_retryable_capture_errors([
        "688808.SH:PermissionError:[WinError 5] access denied",
    ])
    assert not DailyPipeline._only_retryable_capture_errors([
        "688808.SH:ValueError:unexpected schema",
    ])
    assert DailyPipeline._only_retryable_provider_errors([
        "provider_error:tickflow=PermissionError:[WinError 5] scanner lock",
    ])
    assert DailyPipeline._only_retryable_collection_blockers(
        [
            "batch-1:SnapshotNotReadyError:request_errors=ObservationError: "
            "Source HTTP 514: upstream",
            "missing_etf-actions_instruments:4",
        ],
        consequence_prefixes=("missing_etf-actions_instruments:",),
    )
    assert not DailyPipeline._only_retryable_collection_blockers(
        [
            "batch-1:SnapshotNotReadyError:request_errors=ObservationError: "
            "Source HTTP 514: upstream",
            "batch-2:ValueError:unexpected schema",
            "missing_etf-actions_instruments:4",
        ],
        consequence_prefixes=("missing_etf-actions_instruments:",),
    )
    assert not DailyPipeline._only_retryable_collection_blockers(
        [
            "batch-1:SnapshotNotReadyError:evidence remains unresolved "
            "invalid={} request_errors=000001.SZ:ObservationError: Source HTTP 514: "
            "upstream;000002.SZ:ValueError:unexpected schema",
            "missing_etf-actions_instruments:2",
        ],
        consequence_prefixes=("missing_etf-actions_instruments:",),
    )

    direct_results = {
        "xtquant": {
            "_covered_instrument_ids": ("600000.SH",),
            "_errors_by_instrument": {},
        },
        "eastmoney-efinance": {
            "_covered_instrument_ids": (),
            "_errors_by_instrument": {
                "600000.SH": ("ObservationError: Source HTTP 503: unavailable",),
            },
        },
    }
    validation_reason = (
        "Price-limit audit needs two direct provider limit values for "
        "600000.SH/2026-07-30"
    )
    assert DailyPipeline._retryable_direct_limit_validation_failure(
        validation_reason, direct_results,
    )
    direct_results["eastmoney-efinance"]["_errors_by_instrument"]["600000.SH"] = (
        "ValueError:unexpected schema",
    )
    assert not DailyPipeline._retryable_direct_limit_validation_failure(
        validation_reason, direct_results,
    )


def test_daily_wrapper_reads_structured_retryable_report(tmp_path):
    pwsh = shutil.which("pwsh")
    if pwsh is None:
        pytest.skip("PowerShell is unavailable")
    report_dir = tmp_path / "reports"
    report_dir.mkdir()
    report = report_dir / "daily-test.json"
    script = Path(__file__).parents[2] / "scripts" / "run-daily.ps1"

    def classify(retryable: bool) -> str:
        report.write_text(json.dumps({
            "stages": [{
                "name": "evidence",
                "status": "blocked",
                "detail": {"retryable": retryable},
            }],
        }), encoding="utf-8")
        command = (
            f". '{str(script).replace(chr(39), chr(39) * 2)}' -FunctionsOnly; "
            "Test-DailyFailureRetryable "
            f"-DailyReportDir '{str(report_dir).replace(chr(39), chr(39) * 2)}' "
            "-AttemptStarted (Get-Date).AddMinutes(-1)"
        )
        completed = subprocess.run(
            [pwsh, "-NoProfile", "-Command", command],
            check=True,
            capture_output=True,
            text=True,
        )
        return completed.stdout.strip()

    assert classify(True) == "True"
    assert classify(False) == "False"


def _new_listing_frame(listed_date: date) -> pd.DataFrame:
    return pd.DataFrame([{
        "instrument_id": "688825.SH",
        "exchange": "SH",
        "local_code": "688825",
        "asset_type": "stock",
        "name": "New STAR",
        "currency": "CNY",
        "listed_date": listed_date.isoformat(),
        "delisted_date": None,
        "board": "star",
        "exchange_product_class": None,
        "buy_lot": 100,
        "quantity_step": None,
        "odd_lot_sell_all": None,
        "price_tick": 0.01,
        "sell_delay_sessions": None,
        "price_limit_ratio": None,
        "field_lineage": None,
        "source_payload": None,
    }])


_DAILY_BASE_IDS = ("000001.SZ", "159001.SZ", "510050.SH", "600000.SH")
_DAILY_NEW_ID = "688825.SH"
_DAILY_ENDPOINT_COUNTS = {
    "sse-main-stock-list": 1,
    "sse-star-stock-list": 1,
    "szse-a-stock-list": 1,
    "sse-etf-scale-list": 1,
    "sse-current-full-etf-list": 1,
    "szse-etf-scale-daily": 1,
    "szse-current-etf-list": 1,
}


def _daily_base_instruments() -> pd.DataFrame:
    template = market_frames()[MarketTable.INSTRUMENTS].iloc[0].to_dict()
    rows = []
    for instrument_id, exchange, asset_type, name, listed_date in (
        ("600000.SH", "SH", "stock", "Bank A", "1999-11-10"),
        ("000001.SZ", "SZ", "stock", "Bank B", "1991-04-03"),
        ("510050.SH", "SH", "etf", "SH ETF", "2005-02-23"),
        ("159001.SZ", "SZ", "etf", "SZ ETF", "2006-02-21"),
    ):
        row = dict(template)
        row.update({
            "instrument_id": instrument_id,
            "exchange": exchange,
            "local_code": instrument_id.split(".", 1)[0],
            "asset_type": asset_type,
            "name": name,
            "listed_date": listed_date,
            "board": "main",
            "price_tick": 0.001 if asset_type == "etf" else 0.01,
            "exchange_product_class": (
                "sse-fund-subclass-03" if instrument_id == "510050.SH" else
                "szse-ETF|股票基金" if instrument_id == "159001.SZ" else None
            ),
        })
        rows.append(row)
    return pd.DataFrame(rows).sort_values("instrument_id", kind="stable").reset_index(drop=True)


def _daily_official_instruments() -> pd.DataFrame:
    return pd.concat((
        _daily_base_instruments(),
        _new_listing_frame(FUTURE_DAYS[0]),
    ), ignore_index=True).sort_values("instrument_id", kind="stable").reset_index(drop=True)


def _daily_calendar() -> pd.DataFrame:
    open_dates = {*DAYS, *FUTURE_DAYS}
    days = tuple(
        DAYS[0] + timedelta(days=offset)
        for offset in range((FUTURE_DAYS[-1] - DAYS[0]).days + 1)
    )
    return pd.DataFrame([
        {
            "exchange": exchange,
            "session_date": day.isoformat(),
            "is_open": day in open_dates,
            "source_payload": None,
        }
        for exchange in ("SH", "SZ")
        for day in days
    ])


def _daily_increment_bars(request: ProviderRequest) -> pd.DataFrame:
    sessions = tuple(
        day for day in (*DAYS, *FUTURE_DAYS)
        if request.start_date <= day <= request.end_date
    )
    rows = []
    base_prices = {
        "000001.SZ": 10.0,
        "159001.SZ": 11.0,
        "510050.SH": 12.0,
        "600000.SH": 13.0,
        "688825.SH": 14.0,
    }
    for instrument_id in request.instrument_ids:
        for session_ordinal, day in enumerate(sessions):
            close = base_prices[instrument_id] + session_ordinal
            previous_close = close - 0.5
            tick = Decimal("0.001") if instrument_id in {
                "159001.SZ", "510050.SH",
            } else Decimal("0.01")
            bounded = instrument_id != _DAILY_NEW_ID
            limit_up = limit_down = None
            if bounded:
                previous = Decimal(str(previous_close))
                limit_up = float(
                    (previous * Decimal("1.10") / tick).quantize(
                        Decimal("1"), rounding=ROUND_HALF_UP,
                    ) * tick
                )
                limit_down = float(
                    (previous * Decimal("0.90") / tick).quantize(
                        Decimal("1"), rounding=ROUND_HALF_UP,
                    ) * tick
                )
            rows.append({
                "instrument_id": instrument_id,
                "session_date": day.isoformat(),
                "price_mode": "raw",
                "open": close,
                "high": close + 0.1,
                "low": close - 0.1,
                "close": close,
                "volume": 1_000_000,
                "amount": close * 1_000_000,
                "suspended": False,
                "is_st": None,
                "price_limit_state": "unknown",
                "previous_close": previous_close,
                "limit_up": limit_up,
                "limit_down": limit_down,
                "source_payload": None,
            })
    return pd.DataFrame(rows)


class _DailyFixtureProvider:
    def __init__(self, name: str, capabilities: frozenset[ProviderCapability]):
        self.name = name
        self.capabilities = capabilities
        self.calls: list[ProviderRequest] = []

    def scan_announcements(self, start_date, end_date, **kwargs):
        if self.name != "cninfo-public":
            raise AssertionError(f"announcement scan routed to {self.name}")
        categories = (
            "category_qyfpxzcs_szsh",
            "category_pg_szsh",
            "category_bcgz_szsh",
        )
        payload = {"totalAnnouncement": 0, "announcements": None}
        page_evidence = tuple(
            CninfoAnnouncementPageEvidence(
                category=category,
                page_number=1,
                reported_total=0,
                record_count=0,
                response_hash=stable_digest(payload),
                response_json=canonical_json(payload),
                check=check,
            )
            for category in categories
            for check in ("initial", "recheck")
        )
        return CninfoAnnouncementScan(
            policy_version="cninfo-corporate-actions-v1",
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            categories=categories,
            page_evidence=page_evidence,
            records=(),
            affected_instrument_ids=(),
            complete=True,
        )

    def observe(self, request: ProviderRequest) -> ObservationPayload:
        self.calls.append(request)
        observed_at = datetime(2026, 7, 18, 1, 0, tzinfo=timezone.utc)
        if request.capability is ProviderCapability.TRADING_CALENDAR:
            return ObservationPayload(
                self.name,
                observed_at,
                request,
                {MarketTable.CALENDAR: _daily_calendar()},
                (CoverageClaim(
                    MarketTable.CALENDAR,
                    True,
                    request.start_date,
                    request.end_date,
                ),),
                {"backend_group": self.name},
            )
        if request.capability is ProviderCapability.INSTRUMENTS:
            frame = (
                _daily_official_instruments()
                if self.name == "exchange-public" else _daily_base_instruments()
            )
            metadata = {"backend_group": self.name}
            if self.name == "exchange-public":
                metadata.update({
                    "as_of_date": request.parameters["as_of_date"],
                    "requested_scope": {
                        "exchanges": tuple(request.parameters["exchanges"]),
                        "asset_types": tuple(request.parameters["asset_types"]),
                    },
                    "endpoint_counts": _DAILY_ENDPOINT_COUNTS,
                    "response_sha256": {
                        endpoint: stable_digest({"endpoint": endpoint})
                        for endpoint in _DAILY_ENDPOINT_COUNTS
                    },
                    "endpoint_response_counts": _DAILY_ENDPOINT_COUNTS,
                    "unavailable_components": {},
                    "pending_onboarding": {},
                    "as_of_excluded_future_instrument_ids": {},
                })
            return ObservationPayload(
                self.name,
                observed_at,
                request,
                {MarketTable.INSTRUMENTS: frame},
                (CoverageClaim(
                    MarketTable.INSTRUMENTS,
                    True,
                    instrument_ids=tuple(map(str, frame["instrument_id"])),
                ),),
                metadata,
            )
        if request.capability is ProviderCapability.DAILY_BARS_RAW:
            frame = _daily_increment_bars(request)
            return ObservationPayload(
                self.name,
                observed_at,
                request,
                {MarketTable.DAILY_BARS: frame},
                (CoverageClaim(
                    MarketTable.DAILY_BARS,
                    True,
                    request.start_date,
                    request.end_date,
                    request.instrument_ids,
                ),),
                {"backend_group": self.name},
            )
        if request.capability is ProviderCapability.DAILY_STATUS:
            frame = _daily_increment_bars(request)
            if request.parameters.get("instrument_limit_snapshot"):
                valid = frame.loc[
                    frame[["previous_close", "limit_up", "limit_down"]]
                    .notna().all(axis=1)
                ]
                completed = tuple(sorted(set(map(str, valid["instrument_id"]))))
                errors = {
                    instrument_id: "missing_limit_prices"
                    for instrument_id in request.instrument_ids
                    if instrument_id not in completed
                }
                frame = frame.copy()
                frame[["open", "high", "low", "close", "amount"]] = None
                frame["volume"] = 0
                frame["suspended"] = None
                return ObservationPayload(
                    self.name,
                    observed_at,
                    request,
                    {MarketTable.DAILY_BARS: frame},
                    (CoverageClaim(
                        MarketTable.DAILY_BARS,
                        set(completed) == set(request.instrument_ids),
                        request.start_date,
                        request.end_date,
                        request.instrument_ids,
                    ),),
                    {
                        "backend_group": self.name,
                        "response_sha256": {
                            instrument_id: f"sha256-limit-{instrument_id}"
                            for instrument_id in completed
                        },
                        "request_errors": errors,
                        "status_fields": (
                            "previous_close", "limit_up", "limit_down",
                        ),
                    },
                )
            if self.name == "baostock":
                frame[["open", "high", "low", "close", "amount"]] = None
                frame["volume"] = 0
                frame["is_st"] = False
                frame[["limit_up", "limit_down"]] = None
            return ObservationPayload(
                self.name,
                observed_at,
                request,
                {MarketTable.DAILY_BARS: frame},
                (CoverageClaim(
                    MarketTable.DAILY_BARS,
                    True,
                    request.start_date,
                    request.end_date,
                    request.instrument_ids,
                ),),
                {"backend_group": self.name},
            )
        if request.capability is ProviderCapability.CORPORATE_ACTIONS:
            metadata = {
                "backend_group": self.name,
                "response_sha256": {
                    instrument_id: {"fixture": f"sha256-{instrument_id}"}
                    for instrument_id in request.instrument_ids
                },
                "request_errors": {},
                "invalid_lifecycle": {},
            }
            if self.name == "eastmoney-fund-public":
                metadata["parser_policy"] = EASTMONEY_ETF_ACTION_POLICY
            return ObservationPayload(
                self.name,
                observed_at,
                request,
                {MarketTable.CORPORATE_ACTIONS: empty_table(
                    MarketTable.CORPORATE_ACTIONS, include_lineage=True,
                )},
                (CoverageClaim(
                    MarketTable.CORPORATE_ACTIONS,
                    True,
                    request.start_date,
                    request.end_date,
                    request.instrument_ids,
                ),),
                metadata,
            )
        if request.capability is ProviderCapability.ADJUSTMENT_FACTORS:
            return ObservationPayload(
                self.name,
                observed_at,
                request,
                {MarketTable.ADJUSTMENT_FACTORS: empty_table(
                    MarketTable.ADJUSTMENT_FACTORS, include_lineage=True,
                )},
                (CoverageClaim(
                    MarketTable.ADJUSTMENT_FACTORS,
                    True,
                    request.start_date,
                    request.end_date,
                    request.instrument_ids,
                ),),
                {"backend_group": self.name},
            )
        raise AssertionError(f"unsupported fixture request: {self.name}/{request.capability}")


def _daily_extension_registry() -> ProviderRegistry:
    registry = ProviderRegistry()
    for name, capabilities in (
        ("baostock", frozenset({
            ProviderCapability.TRADING_CALENDAR,
            ProviderCapability.INSTRUMENTS,
            ProviderCapability.DAILY_BARS_RAW,
            ProviderCapability.DAILY_STATUS,
        })),
        ("sina-calendar", frozenset({ProviderCapability.TRADING_CALENDAR})),
        ("exchange-public", frozenset({ProviderCapability.INSTRUMENTS})),
        ("tickflow", frozenset({ProviderCapability.DAILY_BARS_RAW})),
        ("xtquant", frozenset({
            ProviderCapability.DAILY_BARS_RAW,
            ProviderCapability.DAILY_STATUS,
            ProviderCapability.ADJUSTMENT_FACTORS,
        })),
        ("eastmoney-efinance", frozenset({ProviderCapability.DAILY_STATUS})),
        ("cninfo-public", frozenset({ProviderCapability.CORPORATE_ACTIONS})),
        ("eastmoney-fund-public", frozenset({ProviderCapability.CORPORATE_ACTIONS})),
    ):
        registry.register(_DailyFixtureProvider(name, capabilities))
    return registry


def _install_official_component_outage(monkeypatch, registry, components: tuple[str, ...]):
    provider = registry.provider("exchange-public")
    original_observe = provider.observe

    def partial_observe(request: ProviderRequest) -> ObservationPayload:
        payload = original_observe(request)
        if request.capability is not ProviderCapability.INSTRUMENTS:
            return payload
        frame = payload.tables[MarketTable.INSTRUMENTS]
        filtered = frame.loc[
            ~frame.apply(
                lambda row: _daily_component_for_row(row.to_dict()) in components,
                axis=1,
            )
        ].copy().reset_index(drop=True)
        metadata = dict(payload.source_metadata)
        metadata["unavailable_components"] = {
            component: {
                "component": component,
                "scope": _OFFICIAL_COMPONENT_SCOPES[component],
                "endpoints": list(_OFFICIAL_COMPONENT_ENDPOINTS[component]),
                "failed_endpoint": _OFFICIAL_COMPONENT_ENDPOINTS[component][0],
                "error_type": "ProviderUnavailableError",
                "message": f"fixture unavailable: {component}",
            }
            for component in components
        }
        unavailable_endpoints = {
            endpoint
            for component in components
            for endpoint in _OFFICIAL_COMPONENT_ENDPOINTS[component]
        }
        for evidence_key in (
            "response_sha256", "endpoint_counts", "endpoint_response_counts",
        ):
            metadata[evidence_key] = {
                endpoint: value
                for endpoint, value in metadata[evidence_key].items()
                if endpoint not in unavailable_endpoints
            }
        filtered = _fixture_component_closure(filtered, metadata, components)
        coverage = tuple(
            CoverageClaim(
                claim.table,
                False if claim.table is MarketTable.INSTRUMENTS else claim.complete,
                claim.start_date,
                claim.end_date,
                tuple(map(str, filtered["instrument_id"])),
                claim.detail,
            ) if claim.table is MarketTable.INSTRUMENTS else claim
            for claim in payload.coverage
        )
        return ObservationPayload(
            payload.provider,
            payload.observed_at,
            payload.request,
            {MarketTable.INSTRUMENTS: filtered},
            coverage,
            metadata,
        )

    monkeypatch.setattr(provider, "observe", partial_observe)


def _install_daily_etf_detail_client(monkeypatch) -> None:
    import fundlab.marketdata.simulation_data as simulation_data_module
    from fundlab.marketdata.etf_rules import EtfRuleEvidenceBuilder

    class EtfDetailClient:
        def get_instrument_detail(self, instrument_id, *, iscomplete):
            assert iscomplete
            opened, category = {
                "510050.SH": ("20050223", 70283376),
                "159001.SZ": ("20060221", 3203072),
            }[instrument_id]
            return {
                "OpenDate": opened, "secuCategory": category,
                "PreClose": 10.0, "UpStopPrice": 11.0,
                "DownStopPrice": 9.0, "PriceTick": 0.001,
            }

    monkeypatch.setattr(
        simulation_data_module,
        "EtfRuleEvidenceBuilder",
        lambda report_root: EtfRuleEvidenceBuilder(report_root, client=EtfDetailClient()),
    )


def _install_new_stock_direct_limit_evidence(monkeypatch, registry) -> None:
    """Keep the component-outage test focused on universe scope, not limits."""

    for provider_name in ("xtquant", "eastmoney-efinance"):
        provider = registry.provider(provider_name)
        original_observe = provider.observe

        def observe(request: ProviderRequest, *, original_observe=original_observe):
            payload = original_observe(request)
            if (
                request.capability is not ProviderCapability.DAILY_STATUS
                or not request.parameters.get("instrument_limit_snapshot")
                or _DAILY_NEW_ID not in request.instrument_ids
            ):
                return payload
            frame = payload.tables[MarketTable.DAILY_BARS].copy()
            new_rows = frame["instrument_id"].astype(str).eq(_DAILY_NEW_ID)
            previous = frame.loc[new_rows, "previous_close"]
            frame.loc[new_rows, "limit_up"] = previous * 1.10
            frame.loc[new_rows, "limit_down"] = previous * 0.90
            metadata = dict(payload.source_metadata)
            hashes = dict(metadata.get("response_sha256", {}))
            hashes[_DAILY_NEW_ID] = f"sha256-limit-{_DAILY_NEW_ID}"
            metadata["response_sha256"] = hashes
            errors = dict(metadata.get("request_errors", {}))
            errors.pop(_DAILY_NEW_ID, None)
            metadata["request_errors"] = errors
            coverage = tuple(
                CoverageClaim(
                    claim.table,
                    True,
                    claim.start_date,
                    claim.end_date,
                    claim.instrument_ids,
                    claim.detail,
                ) if claim.table is MarketTable.DAILY_BARS else claim
                for claim in payload.coverage
            )
            return ObservationPayload(
                payload.provider,
                payload.observed_at,
                payload.request,
                {MarketTable.DAILY_BARS: frame},
                coverage,
                metadata,
            )

        monkeypatch.setattr(provider, "observe", observe)


def _ready_multi_asset_market(path) -> None:
    warehouse = MarketDataWarehouse(path)
    instruments = _daily_base_instruments()
    calendar = _daily_calendar()
    original_bars = market_frames()[MarketTable.DAILY_BARS]
    bars = pd.concat((
        original_bars.assign(instrument_id=instrument_id)
        for instrument_id in _DAILY_BASE_IDS
    ), ignore_index=True)
    tables = {
        MarketTable.INSTRUMENTS: instruments,
        MarketTable.CALENDAR: calendar,
        MarketTable.DAILY_BARS: bars,
        MarketTable.CORPORATE_ACTIONS: empty_table(
            MarketTable.CORPORATE_ACTIONS, include_lineage=True,
        ),
        MarketTable.ADJUSTMENT_FACTORS: empty_table(
            MarketTable.ADJUSTMENT_FACTORS, include_lineage=True,
        ),
    }
    observed = warehouse.record_observation(ObservationPayload(
        "daily-extension-predecessor",
        datetime(2026, 7, 17, 1, 0, tzinfo=timezone.utc),
        ProviderRequest(
            ProviderCapability.CANONICAL_RECONCILIATION,
            DAYS[0],
            DAYS[-1],
            _DAILY_BASE_IDS,
        ),
        tables,
        (
            CoverageClaim(
                MarketTable.INSTRUMENTS,
                True,
                instrument_ids=_DAILY_BASE_IDS,
            ),
            CoverageClaim(MarketTable.CALENDAR, True, DAYS[0], FUTURE_DAYS[-1]),
            CoverageClaim(
                MarketTable.DAILY_BARS,
                True,
                DAYS[0],
                DAYS[-1],
                _DAILY_BASE_IDS,
            ),
            CoverageClaim(
                MarketTable.CORPORATE_ACTIONS,
                True,
                DAYS[0],
                DAYS[-1],
                _DAILY_BASE_IDS,
            ),
            CoverageClaim(
                MarketTable.ADJUSTMENT_FACTORS,
                True,
                DAYS[0],
                DAYS[-1],
                _DAILY_BASE_IDS,
            ),
        ),
        {"kind": "field_level_reconciliation", "reconciliation_ready": True},
    ))
    scope = UniverseScope(
        CURRENT_SH_SZ_STOCK_ETF_UNIVERSE,
        DAYS[-1],
        DAYS[0],
        DAYS[-1],
        survivorship_bias=True,
        instrument_ids=_DAILY_BASE_IDS,
    )
    legacy = commit_test_snapshot(warehouse, SnapshotPlan(
        (
            SourceSlice(
                observed.observation_id,
                MarketTable.INSTRUMENTS,
                "daily new-listing predecessor fixture",
                _DAILY_BASE_IDS,
            ),
            SourceSlice(
                observed.observation_id,
                MarketTable.CALENDAR,
                "daily new-listing predecessor fixture",
                start_date=DAYS[0],
                end_date=FUTURE_DAYS[-1],
            ),
            *(
                SourceSlice(
                    observed.observation_id,
                    table,
                    "daily new-listing predecessor fixture",
                    _DAILY_BASE_IDS,
                    DAYS[0],
                    DAYS[-1],
                )
                for table in (
                    MarketTable.DAILY_BARS,
                    MarketTable.CORPORATE_ACTIONS,
                    MarketTable.ADJUSTMENT_FACTORS,
                )
            ),
        ),
        "daily new-listing predecessor fixture",
        universe_scope=scope,
    ))
    predecessor = IncrementalCanonicalPublisher(warehouse).bootstrap(legacy.snapshot_id)
    warehouse._replace_current_pointer(predecessor)


def test_daily_new_listing_supplement_uses_official_exact_master(tmp_path):
    report = tmp_path / "new-listing-build.json"
    report.write_text(json.dumps({
        "included_instrument_ids": ["688825.SH"],
        "excluded": {},
        "canonical_observation_ids": ["obs-new-canonical"],
    }), encoding="utf-8")

    class Builder:
        seen = None

        def build(self, spec, *, universe_observation_id=None):
            self.seen = (spec, universe_observation_id)
            return SimpleNamespace(
                report=report,
                snapshot_id="snap-new-listing",
                blockers=(),
                build_id="history-new-listing",
            )

    builder = Builder()
    pipeline = DailyPipeline(
        build_settings(tmp_path, ()), registry=registry_with_calendars(),
    )

    snapshot_id, partitions, detail = pipeline._build_new_instrument_supplement(
        builder=builder,
        universe_observation_id="obs-exchange-official",
        official_frame=_new_listing_frame(FUTURE_DAYS[0]),
        instrument_ids=("688825.SH",),
        start=FUTURE_DAYS[0],
        end=FUTURE_DAYS[0],
    )

    assert snapshot_id == "snap-new-listing"
    assert partitions == ("obs-new-canonical",)
    assert detail["instrument_ids"] == ("688825.SH",)
    assert builder.seen[0].instrument_ids == ("688825.SH",)
    assert builder.seen[1] == "obs-exchange-official"


def test_daily_new_listing_supplement_fails_closed_outside_increment_window(tmp_path):
    class Builder:
        def build(self, spec, *, universe_observation_id=None):  # pragma: no cover
            raise AssertionError("metadata validation must happen before source capture")

    pipeline = DailyPipeline(
        build_settings(tmp_path, ()), registry=registry_with_calendars(),
    )

    with pytest.raises(DailyPipelineBlocked, match="exact onboarding scope"):
        pipeline._build_new_instrument_supplement(
            builder=Builder(),
            universe_observation_id="obs-exchange-official",
            official_frame=_new_listing_frame(DAYS[0]),
            instrument_ids=("688825.SH",),
            start=FUTURE_DAYS[0],
            end=FUTURE_DAYS[0],
        )


def test_daily_new_listing_supplement_fails_closed_without_two_source_evidence(tmp_path):
    report = tmp_path / "new-listing-incomplete.json"
    report.write_text(json.dumps({
        "included_instrument_ids": [],
        "excluded": {"688825.SH": ["missing_source:xtquant"]},
        "canonical_observation_ids": [],
    }), encoding="utf-8")

    class Builder:
        def build(self, spec, *, universe_observation_id=None):
            return SimpleNamespace(
                report=report,
                snapshot_id=None,
                blockers=("excluded_instruments:1",),
                build_id="history-new-listing-incomplete",
            )

    pipeline = DailyPipeline(
        build_settings(tmp_path, ()), registry=registry_with_calendars(),
    )

    with pytest.raises(TradeRuleError, match="exact reconciled partition") as exc_info:
        pipeline._build_new_instrument_supplement(
            builder=Builder(),
            universe_observation_id="obs-exchange-official",
            official_frame=_new_listing_frame(FUTURE_DAYS[0]),
            instrument_ids=("688825.SH",),
            start=FUTURE_DAYS[0],
            end=FUTURE_DAYS[0],
        )
    assert exc_info.value.instrument_ids == ("688825.SH",)


def test_daily_incomplete_new_listing_supplement_is_exactly_quarantined(
    tmp_path, monkeypatch,
):
    import fundlab.marketdata.simulation_data as simulation_data_module
    import fundlab.pipeline.daily as daily_module
    from fundlab.marketdata.contracts import DATA_GAP_QUARANTINE_RULE_ID
    from fundlab.marketdata.etf_rules import EtfRuleEvidenceBuilder

    class EtfDetailClient:
        def get_instrument_detail(self, instrument_id, *, iscomplete):
            assert iscomplete
            opened, category = {
                "510050.SH": ("20050223", 70283376),
                "159001.SZ": ("20060221", 3203072),
            }[instrument_id]
            return {
                "OpenDate": opened, "secuCategory": category,
                "PreClose": 10.0, "UpStopPrice": 11.0,
                "DownStopPrice": 9.0, "PriceTick": 0.001,
            }

    monkeypatch.setattr(
        simulation_data_module, "EtfRuleEvidenceBuilder",
        lambda report_root: EtfRuleEvidenceBuilder(report_root, client=EtfDetailClient()),
    )
    report = tmp_path / "incomplete-new-listing.json"
    report.write_text(json.dumps({
        "included_instrument_ids": [],
        "excluded": {_DAILY_NEW_ID: ["missing_source:xtquant"]},
        "canonical_observation_ids": [],
    }), encoding="utf-8")
    real_build = daily_module.HistoryDatabaseBuilder.build

    def incomplete_supplement(self, spec, **kwargs):
        if spec.instrument_ids == (_DAILY_NEW_ID,):
            return SimpleNamespace(
                report=report,
                snapshot_id=None,
                blockers=("excluded_instruments:1",),
                build_id="incomplete-new-listing",
            )
        return real_build(self, spec, **kwargs)

    monkeypatch.setattr(daily_module.HistoryDatabaseBuilder, "build", incomplete_supplement)
    _ready_multi_asset_market(tmp_path / "market")
    pipeline = DailyPipeline(
        build_settings(tmp_path, ()), registry=_daily_extension_registry(),
        now_fn=lambda: evening_of(FUTURE_DAYS[0]),
    )

    result = pipeline.run(target_date=FUTURE_DAYS[0], skip_accounts=True)

    assert result.status == "degraded"
    snapshot = pipeline.warehouse.load_snapshot(result.snapshot_id)
    rows = pipeline.warehouse.query_loaded_snapshot_table(
        snapshot,
        instrument_ids=(_DAILY_NEW_ID,),
        table=MarketTable.DAILY_BARS,
        start_date=FUTURE_DAYS[0],
        end_date=FUTURE_DAYS[0],
        price_mode="raw",
    )
    assert rows.iloc[0]["trade_rule_id"] == DATA_GAP_QUARANTINE_RULE_ID


def test_daily_new_listing_supplement_persistence_failure_remains_globally_blocked(
    tmp_path, monkeypatch,
):
    import fundlab.pipeline.daily as daily_module
    from fundlab.marketdata import IntegrityError

    _install_daily_etf_detail_client(monkeypatch)
    real_build = daily_module.HistoryDatabaseBuilder.build

    def persistence_failure(self, spec, **kwargs):
        if spec.instrument_ids == (_DAILY_NEW_ID,):
            raise IntegrityError("canonical supplement observation write failed")
        return real_build(self, spec, **kwargs)

    monkeypatch.setattr(
        daily_module.HistoryDatabaseBuilder, "build", persistence_failure,
    )
    _ready_multi_asset_market(tmp_path / "market")
    pipeline = DailyPipeline(
        build_settings(tmp_path, ()), registry=_daily_extension_registry(),
        now_fn=lambda: evening_of(FUTURE_DAYS[0]),
    )
    before = pipeline.warehouse.current_snapshot_id()

    result = pipeline.run(target_date=FUTURE_DAYS[0], skip_accounts=True)

    assert result.status == "blocked"
    blocked = next(stage for stage in result.stages if stage.name == "new_instruments")
    assert blocked.detail["error_type"] == "IntegrityError"
    assert pipeline.warehouse.current_snapshot_id() == before


def test_daily_all_exact_history_exclusions_publish_data_gap_quarantine(tmp_path, monkeypatch):
    import fundlab.marketdata.simulation_data as simulation_data_module
    import fundlab.pipeline.daily as daily_module
    from fundlab.marketdata.contracts import DATA_GAP_QUARANTINE_RULE_ID
    from fundlab.marketdata.etf_rules import EtfRuleEvidenceBuilder

    class EtfDetailClient:
        def get_instrument_detail(self, instrument_id, *, iscomplete):
            assert iscomplete
            opened, category = {
                "510050.SH": ("20050223", 70283376),
                "159001.SZ": ("20060221", 3203072),
            }[instrument_id]
            return {
                "OpenDate": opened, "secuCategory": category,
                "PreClose": 10.0, "UpStopPrice": 11.0,
                "DownStopPrice": 9.0, "PriceTick": 0.001,
            }

    monkeypatch.setattr(
        simulation_data_module, "EtfRuleEvidenceBuilder",
        lambda report_root: EtfRuleEvidenceBuilder(report_root, client=EtfDetailClient()),
    )
    report = tmp_path / "all-excluded-history.json"
    report.write_text(json.dumps({
        "included_instrument_ids": [],
        "excluded": {
            instrument_id: ["provider_unavailable:bounded"]
            for instrument_id in (*_DAILY_BASE_IDS, _DAILY_NEW_ID)
        },
        "canonical_observation_ids": [],
    }), encoding="utf-8")

    def all_excluded(self, spec, **kwargs):
        return SimpleNamespace(
            report=report, snapshot_id=None, blockers=("all sources unavailable",),
            build_id="all-excluded-history",
        )

    monkeypatch.setattr(daily_module.HistoryDatabaseBuilder, "build", all_excluded)
    _ready_multi_asset_market(tmp_path / "market")
    pipeline = DailyPipeline(
        build_settings(tmp_path, ()), registry=_daily_extension_registry(),
        now_fn=lambda: evening_of(FUTURE_DAYS[0]),
    )

    result = pipeline.run(target_date=FUTURE_DAYS[0], skip_accounts=True)

    assert result.status == "degraded" and result.exit_code == 0, [
        (stage.name, stage.status, stage.detail) for stage in result.stages
    ]
    assert result.snapshot_id is not None
    snapshot = pipeline.warehouse.load_snapshot(result.snapshot_id)
    rows = pipeline.warehouse.query_loaded_snapshot_table(
        snapshot, MarketTable.DAILY_BARS, start_date=FUTURE_DAYS[0],
        end_date=FUTURE_DAYS[0], price_mode="raw",
    )
    assert set(rows["instrument_id"].astype(str)) == {*_DAILY_BASE_IDS, _DAILY_NEW_ID}
    assert set(rows["trade_rule_id"].astype(str)) == {DATA_GAP_QUARANTINE_RULE_ID}


def test_daily_malformed_history_exclusion_scope_remains_blocked(tmp_path, monkeypatch):
    import fundlab.pipeline.daily as daily_module

    report = tmp_path / "malformed-history.json"
    report.write_text(json.dumps({
        "included_instrument_ids": [],
        "excluded": {"600000.SH": ["provider_unavailable:bounded"]},
        "canonical_observation_ids": [],
    }), encoding="utf-8")

    def malformed(self, spec, **kwargs):
        return SimpleNamespace(
            report=report, snapshot_id=None, blockers=("bad report",),
            build_id="malformed-history",
        )

    monkeypatch.setattr(daily_module.HistoryDatabaseBuilder, "build", malformed)
    _ready_multi_asset_market(tmp_path / "market")
    pipeline = DailyPipeline(
        build_settings(tmp_path, ()), registry=_daily_extension_registry(),
        now_fn=lambda: evening_of(FUTURE_DAYS[0]),
    )
    before = pipeline.warehouse.current_snapshot_id()

    result = pipeline.run(target_date=FUTURE_DAYS[0], skip_accounts=True)

    assert result.status == "blocked"
    assert next(stage for stage in result.stages if stage.name == "bars").status == "blocked"
    assert pipeline.warehouse.current_snapshot_id() == before


def test_daily_new_listing_without_trusted_metadata_stays_pending_onboarding(
    tmp_path, monkeypatch,
):
    import fundlab.marketdata.simulation_data as simulation_data_module
    from fundlab.marketdata.etf_rules import EtfRuleEvidenceBuilder

    class EtfDetailClient:
        def get_instrument_detail(self, instrument_id, *, iscomplete):
            assert iscomplete
            opened, category = {
                "510050.SH": ("20050223", 70283376),
                "159001.SZ": ("20060221", 3203072),
            }[instrument_id]
            return {
                "OpenDate": opened, "secuCategory": category,
                "PreClose": 10.0, "UpStopPrice": 11.0,
                "DownStopPrice": 9.0, "PriceTick": 0.001,
            }

    monkeypatch.setattr(
        simulation_data_module, "EtfRuleEvidenceBuilder",
        lambda report_root: EtfRuleEvidenceBuilder(report_root, client=EtfDetailClient()),
    )
    _ready_multi_asset_market(tmp_path / "market")
    pipeline = DailyPipeline(
        build_settings(tmp_path, ()), registry=_daily_extension_registry(),
        now_fn=lambda: evening_of(FUTURE_DAYS[0]),
    )
    real_official = pipeline._official_universe

    def incomplete_new_master(target, *, predecessor):
        resolved = real_official(target, predecessor=predecessor)
        frame = resolved.frame.copy()
        new_row = frame["instrument_id"].eq(_DAILY_NEW_ID)
        frame.loc[new_row, "asset_type"] = "etf"
        frame.loc[new_row, "exchange_product_class"] = None
        return replace(resolved, frame=frame)

    monkeypatch.setattr(pipeline, "_official_universe", incomplete_new_master)

    result = pipeline.run(target_date=FUTURE_DAYS[0], skip_accounts=True)

    assert result.status == "degraded", [
        (stage.name, stage.status, stage.detail) for stage in result.stages
    ]
    universe = next(stage for stage in result.stages if stage.name == "universe")
    assert tuple(universe.detail["pending_onboarding"]) == (_DAILY_NEW_ID,)
    assert universe.detail["pending_onboarding"][_DAILY_NEW_ID]["missing_fields"] == (
        "exchange_product_class",
    )
    snapshot = pipeline.warehouse.load_snapshot(result.snapshot_id)
    assert _DAILY_NEW_ID not in snapshot.plan.universe_scope.instrument_ids


def test_daily_pending_onboarding_and_exact_no_trade_do_not_block_publication(
    tmp_path, monkeypatch,
):
    _install_daily_etf_detail_client(monkeypatch)
    real_official = _daily_official_instruments
    real_bars = _daily_increment_bars
    no_trade_id = "600000.SH"
    carried_id = "000001.SZ"

    def official_with_future_listing():
        frame = real_official().copy()
        frame.loc[
            frame["instrument_id"].eq(_DAILY_NEW_ID), "listed_date"
        ] = FUTURE_DAYS[1].isoformat()
        return frame.loc[
            ~frame["instrument_id"].eq(carried_id)
        ].reset_index(drop=True)

    def bars_without_exact_no_trade_candidate(request):
        frame = real_bars(request)
        return frame.loc[
            ~frame["instrument_id"].astype(str).eq(no_trade_id)
        ].reset_index(drop=True)

    monkeypatch.setitem(globals(), "_daily_official_instruments", official_with_future_listing)
    monkeypatch.setitem(globals(), "_daily_increment_bars", bars_without_exact_no_trade_candidate)
    _ready_multi_asset_market(tmp_path / "market")
    pipeline = DailyPipeline(
        build_settings(tmp_path, ()), registry=_daily_extension_registry(),
        now_fn=lambda: evening_of(FUTURE_DAYS[0]),
    )

    result = pipeline.run(target_date=FUTURE_DAYS[0], skip_accounts=True)

    assert result.status == "degraded" and result.exit_code == 0, [
        (stage.name, stage.status, stage.detail) for stage in result.stages
    ]
    assert result.snapshot_id is not None
    universe = next(stage for stage in result.stages if stage.name == "universe")
    assert tuple(universe.detail["pending_onboarding"]) == (_DAILY_NEW_ID,)
    pending_manifest = pipeline.warehouse.load_observation(
        universe.detail["universe_observation_id"]
    )
    carry_manifest = pipeline.warehouse.load_observation(
        pending_manifest.request.parameters["official_observation_id"]
    )
    assert carry_manifest.provider == "canonical-universe-carry-forward-daily-pipeline-v2"
    assert (
        carry_manifest.request.parameters["reason"]
        == "official_universe_removed_predecessor_instruments"
    )
    no_trade = next(stage for stage in result.stages if stage.name == "no_trade")
    assert no_trade.status == "ok"
    assert no_trade.detail["instruments"] == (no_trade_id,)
    snapshot = pipeline.warehouse.load_snapshot(result.snapshot_id)
    assert _DAILY_NEW_ID not in snapshot.plan.universe_scope.instrument_ids
    assert no_trade_id in snapshot.plan.universe_scope.instrument_ids
    assert carried_id in snapshot.plan.universe_scope.instrument_ids
    rows = pipeline.warehouse.query_loaded_snapshot_table(
        snapshot,
        MarketTable.DAILY_BARS,
        instrument_ids=(no_trade_id,),
        start_date=FUTURE_DAYS[0],
        end_date=FUTURE_DAYS[0],
        price_mode="raw",
    )
    assert len(rows) == 1
    assert pd.isna(rows.iloc[0]["open"])


def test_daily_verified_research_missing_ids_are_repaired_with_exact_quarantine(
    tmp_path, monkeypatch,
):
    import fundlab.marketdata.simulation_data as simulation_data_module
    import fundlab.pipeline.daily as daily_module
    from fundlab.marketdata.etf_rules import EtfRuleEvidenceBuilder

    class EtfDetailClient:
        def get_instrument_detail(self, instrument_id, *, iscomplete):
            assert iscomplete
            opened, category = {
                "510050.SH": ("20050223", 70283376),
                "159001.SZ": ("20060221", 3203072),
            }[instrument_id]
            return {
                "OpenDate": opened, "secuCategory": category,
                "PreClose": 10.0, "UpStopPrice": 11.0,
                "DownStopPrice": 9.0, "PriceTick": 0.001,
            }

    monkeypatch.setattr(
        simulation_data_module, "EtfRuleEvidenceBuilder",
        lambda report_root: EtfRuleEvidenceBuilder(report_root, client=EtfDetailClient()),
    )
    real_derive = daily_module.derive_current_research_snapshot
    derive_calls = 0

    def missing_once(*args, **kwargs):
        nonlocal derive_calls
        result = real_derive(*args, **kwargs)
        derive_calls += 1
        if derive_calls == 1:
            return replace(
                result,
                status="ready_scoped",
                missing_instrument_ids=("600000.SH",),
            )
        return result

    monkeypatch.setattr(daily_module, "derive_current_research_snapshot", missing_once)
    _ready_multi_asset_market(tmp_path / "market")
    pipeline = DailyPipeline(
        build_settings(tmp_path, ()), registry=_daily_extension_registry(),
        now_fn=lambda: evening_of(FUTURE_DAYS[0]),
    )

    result = pipeline.run(target_date=FUTURE_DAYS[0], skip_accounts=True)

    assert result.status == "degraded", [
        (stage.name, stage.status, stage.detail) for stage in result.stages
    ]
    research = next(stage for stage in result.stages if stage.name == "research")
    assert research.status == "degraded"
    assert research.detail["quarantine_instrument_ids"] == ("600000.SH",)


def test_execution_guard_drops_duplicate_research_key_to_no_price_guard():
    target = FUTURE_DAYS[0]
    instruments = _daily_base_instruments()
    duplicate_rows = pd.DataFrame([
        {
            "instrument_id": "600000.SH",
            "session_date": target.isoformat(),
            "price_mode": "raw",
            "open": 10.0,
            "high": 10.1,
            "low": 9.9,
            "close": 10.0,
        },
        {
            "instrument_id": "600000.SH",
            "session_date": target.isoformat(),
            "price_mode": "raw",
            "open": 20.0,
            "high": 20.1,
            "low": 19.9,
            "close": 20.0,
        },
    ])
    scope = UniverseScope(
        CURRENT_SH_SZ_STOCK_ETF_UNIVERSE, target, target, target,
        instrument_ids=("600000.SH",),
    )

    bars = DailyPipeline._build_execution_guard_bars(
        instruments=instruments,
        research_bars=duplicate_rows,
        calendar=_daily_calendar(),
        scope=scope,
        instrument_ids=("600000.SH",),
        reasons={"600000.SH": ("status:source_gap",)},
        source_observation_id="obs-source",
    )

    row = bars.iloc[0]
    assert row["trade_rule_id"] == EXECUTION_EVIDENCE_GAP_RULE_ID
    assert pd.isna(row["open"]) and pd.isna(row["close"])
    lineage = json.loads(row["field_lineage"])
    assert lineage["duplicate_research_price_key"] is True
    assert "duplicate_research_price_key" in lineage["reasons"]


def test_daily_new_listing_runs_through_componentized_increment_and_publication(
    tmp_path, monkeypatch,
):
    import fundlab.marketdata.simulation_data as simulation_data_module
    from fundlab.marketdata.etf_rules import EtfRuleEvidenceBuilder

    class EtfDetailClient:
        def get_instrument_detail(self, instrument_id, *, iscomplete):
            assert iscomplete
            opened, category = {
                "510050.SH": ("20050223", 70283376),
                "159001.SZ": ("20060221", 3203072),
            }[instrument_id]
            return {
                "OpenDate": opened,
                "secuCategory": category,
                "PreClose": 10.0,
                "UpStopPrice": 11.0,
                "DownStopPrice": 9.0,
                "PriceTick": 0.001,
            }

    monkeypatch.setattr(
        simulation_data_module,
        "EtfRuleEvidenceBuilder",
        lambda report_root: EtfRuleEvidenceBuilder(
            report_root, client=EtfDetailClient(),
        ),
    )
    _ready_multi_asset_market(tmp_path / "market")
    registry = _daily_extension_registry()
    pipeline = DailyPipeline(
        build_settings(tmp_path, ()),
        registry=registry,
        now_fn=lambda: evening_of(FUTURE_DAYS[0]),
    )

    first = pipeline.run(target_date=FUTURE_DAYS[0], skip_accounts=True)

    assert first.status == "degraded", [
        (stage.name, stage.status, stage.detail) for stage in first.stages
    ]
    limit_stage = next(stage for stage in first.stages if stage.name == "limits")
    assert limit_stage.detail["execution_guard_sample"] == ("688825.SH",)
    assert first.snapshot_id is not None
    by_name = {stage.name: stage for stage in first.stages}
    assert by_name["bars"].detail["included"] == len(_DAILY_BASE_IDS)
    assert by_name["bars"].detail["excluded"] == 1
    assert by_name["new_instruments"].detail["instrument_ids"] == (_DAILY_NEW_ID,)
    for stage in (
        "research", "status", "evidence", "factor_reconciliation",
        "candidate", "validate", "extend",
    ):
        assert by_name[stage].status == "ok"
    assert by_name["limits"].status == "degraded"

    second = pipeline.run(target_date=FUTURE_DAYS[1], skip_accounts=True)

    assert second.status == "degraded", [
        (stage.name, stage.status, stage.detail) for stage in second.stages
    ]
    assert second.snapshot_id is not None and second.snapshot_id != first.snapshot_id
    second_by_name = {stage.name: stage for stage in second.stages}
    assert "new_instruments" not in second_by_name
    assert second_by_name["carried_instruments"].detail[
        "carried_instrument_ids"
    ] == (_DAILY_NEW_ID,)
    assert second_by_name["carried_instruments"].detail[
        "new_instrument_ids"
    ] == ()
    assert second_by_name["carried_instruments"].detail[
        "trusted_predecessor_snapshot_id"
    ] == first.snapshot_id
    carried_observation_id = second_by_name["carried_instruments"].detail[
        "canonical_observation_ids"
    ][0]
    carried_observation = MarketDataWarehouse(tmp_path / "market").load_observation(
        carried_observation_id
    )
    assert carried_observation.request.parameters[
        "trusted_predecessor_snapshot_id"
    ] == first.snapshot_id
    assert carried_observation.source_metadata["partition_quality"][
        "trusted_predecessor_snapshot_id"
    ] == first.snapshot_id
    baostock = registry.provider("baostock")
    assert sum(
        request.capability is ProviderCapability.INSTRUMENTS
        for request in baostock.calls
    ) == 2
    warehouse = MarketDataWarehouse(tmp_path / "market")
    assert warehouse.current_snapshot_id() == second.snapshot_id
    published = warehouse.load_snapshot(second.snapshot_id)
    scope = published.plan.universe_scope
    assert scope is not None
    assert scope.as_of_date == scope.history_end == FUTURE_DAYS[1]
    assert set(scope.instrument_ids) == {*_DAILY_BASE_IDS, _DAILY_NEW_ID}
    increment_bars = warehouse.query_loaded_snapshot_table(
        published,
        MarketTable.DAILY_BARS,
        instrument_ids=scope.instrument_ids,
        start_date=FUTURE_DAYS[1],
        end_date=FUTURE_DAYS[1],
        price_mode="raw",
    )
    assert set(map(str, increment_bars["instrument_id"])) == set(scope.instrument_ids)


def test_daily_preserves_price_and_disables_execution_for_evidence_gap(
    tmp_path, monkeypatch,
):
    import fundlab.marketdata.simulation_data as simulation_data_module
    import fundlab.pipeline.daily as daily_module
    from fundlab.marketdata.etf_rules import EtfRuleEvidenceBuilder

    class EtfDetailClient:
        def get_instrument_detail(self, instrument_id, *, iscomplete):
            assert iscomplete
            opened, category = {
                "510050.SH": ("20050223", 70283376),
                "159001.SZ": ("20060221", 3203072),
            }[instrument_id]
            return {
                "OpenDate": opened,
                "secuCategory": category,
                "PreClose": 10.0,
                "UpStopPrice": 11.0,
                "DownStopPrice": 9.0,
                "PriceTick": 0.001,
            }

    monkeypatch.setattr(
        simulation_data_module,
        "EtfRuleEvidenceBuilder",
        lambda report_root: EtfRuleEvidenceBuilder(
            report_root, client=EtfDetailClient(),
        ),
    )
    real_collector = daily_module.SimulationEvidenceCollector

    class OneInstrumentGapCollector:
        def __init__(self, *args, **kwargs):
            self.delegate = real_collector(*args, **kwargs)

        def collect(self, spec):
            result = self.delegate.collect(spec)
            if spec.kind != "stock-actions":
                return result
            return SimpleNamespace(
                status="incomplete",
                blockers=(
                    "batch:SnapshotNotReadyError:stock-actions evidence remains unresolved: "
                    "count=1 ids=600000.SH invalid={} "
                    "request_errors=600000.SH:TimeoutError:timed out",
                    "missing_stock-actions_instruments:1",
                ),
                observation_ids=result.observation_ids,
                unresolved_instrument_ids=("600000.SH",),
            )

    monkeypatch.setattr(
        daily_module, "SimulationEvidenceCollector", OneInstrumentGapCollector,
    )
    _ready_multi_asset_market(tmp_path / "market")
    pipeline = DailyPipeline(
        build_settings(tmp_path, ()),
        registry=_daily_extension_registry(),
        now_fn=lambda: evening_of(FUTURE_DAYS[0]),
    )

    result = pipeline.run(target_date=FUTURE_DAYS[0], skip_accounts=True)

    assert result.status == "degraded", [
        (stage.name, stage.status, stage.detail) for stage in result.stages[-3:]
    ]
    assert result.exit_code == 0
    evidence_stage = next(stage for stage in result.stages if stage.name == "evidence")
    assert evidence_stage.status == "degraded"
    candidate_id = next(
        stage for stage in result.stages if stage.name == "candidate"
    ).detail["observation_id"]
    candidate = pipeline.warehouse.load_observation(candidate_id)
    incomplete_event_claims = [
        claim for claim in candidate.coverage
        if claim.table in {
            MarketTable.CORPORATE_ACTIONS,
            MarketTable.ADJUSTMENT_FACTORS,
        }
        and not claim.complete
    ]
    assert incomplete_event_claims
    assert all("600000.SH" in claim.instrument_ids for claim in incomplete_event_claims)
    snapshot = pipeline.warehouse.load_snapshot(result.snapshot_id)
    assert snapshot.plan.require_complete_coverage is False
    rows = pipeline.warehouse.query_loaded_snapshot_table(
        snapshot,
        MarketTable.DAILY_BARS,
        instrument_ids=("600000.SH",),
        start_date=FUTURE_DAYS[0],
        end_date=FUTURE_DAYS[0],
        price_mode="raw",
    )
    assert len(rows) == 1
    assert rows.iloc[0]["trade_rule_id"] == EXECUTION_EVIDENCE_GAP_RULE_ID
    assert pd.isna(rows.iloc[0]["suspended"])
    assert not pd.isna(rows.iloc[0]["close"])
    market = CanonicalMarketData.open(
        pipeline.settings.paths.market_data,
        snapshot_id=result.snapshot_id,
    )
    guarded_bar = market.session(FUTURE_DAYS[0]).bars["600000.SH"]
    assert guarded_bar.suspended is False
    assert guarded_bar.trade_rule_id == EXECUTION_EVIDENCE_GAP_RULE_ID


def test_daily_factor_failure_guards_only_named_instrument(tmp_path, monkeypatch):
    import fundlab.marketdata.simulation_data as simulation_data_module
    from fundlab.marketdata.etf_rules import EtfRuleEvidenceBuilder

    class EtfDetailClient:
        def get_instrument_detail(self, instrument_id, *, iscomplete):
            assert iscomplete
            opened, category = {
                "510050.SH": ("20050223", 70283376),
                "159001.SZ": ("20060221", 3203072),
            }[instrument_id]
            return {
                "OpenDate": opened,
                "secuCategory": category,
                "PreClose": 10.0,
                "UpStopPrice": 11.0,
                "DownStopPrice": 9.0,
                "PriceTick": 0.001,
            }

    monkeypatch.setattr(
        simulation_data_module,
        "EtfRuleEvidenceBuilder",
        lambda report_root: EtfRuleEvidenceBuilder(
            report_root, client=EtfDetailClient(),
        ),
    )
    _ready_multi_asset_market(tmp_path / "market")
    pipeline = DailyPipeline(
        build_settings(tmp_path, ()),
        registry=_daily_extension_registry(),
        now_fn=lambda: evening_of(FUTURE_DAYS[0]),
    )
    real_reconcile = pipeline._reconcile_action_factor_evidence
    failed = False

    def fail_one_instrument(**kwargs):
        nonlocal failed
        ids = set(map(str, kwargs["instruments"]["instrument_id"]))
        if not failed and "600000.SH" in ids:
            failed = True
            raise TradeRuleError(
                "factor evidence unavailable",
                instrument_ids=("600000.SH",),
            )
        return real_reconcile(**kwargs)

    monkeypatch.setattr(
        pipeline, "_reconcile_action_factor_evidence", fail_one_instrument,
    )

    result = pipeline.run(target_date=FUTURE_DAYS[0], skip_accounts=True)

    assert result.status == "degraded"
    factor = next(
        stage for stage in result.stages if stage.name == "factor_reconciliation"
    )
    assert factor.status == "degraded"
    assert factor.detail["failures"][0]["instrument_ids"] == ("600000.SH",)
    snapshot = pipeline.warehouse.load_snapshot(result.snapshot_id)
    rows = pipeline.warehouse.query_loaded_snapshot_table(
        snapshot,
        MarketTable.DAILY_BARS,
        start_date=FUTURE_DAYS[0],
        end_date=FUTURE_DAYS[0],
        price_mode="raw",
    )
    guarded = rows.loc[
        rows["trade_rule_id"].astype(str).eq(EXECUTION_EVIDENCE_GAP_RULE_ID),
        "instrument_id",
    ]
    assert set(guarded) == {"600000.SH", _DAILY_NEW_ID}


def test_daily_publishes_prices_with_execution_guard_when_direct_limits_are_missing(
    tmp_path, monkeypatch,
):
    import fundlab.marketdata.simulation_data as simulation_data_module
    from fundlab.marketdata.contracts import EXECUTION_EVIDENCE_GAP_RULE_ID
    from fundlab.marketdata.etf_rules import EtfRuleEvidenceBuilder

    class EtfDetailClient:
        def get_instrument_detail(self, instrument_id, *, iscomplete):
            assert iscomplete
            opened, category = {
                "510050.SH": ("20050223", 70283376),
                "159001.SZ": ("20060221", 3203072),
            }[instrument_id]
            return {
                "OpenDate": opened,
                "secuCategory": category,
                "PreClose": 10.0,
                "UpStopPrice": 11.0,
                "DownStopPrice": 9.0,
                "PriceTick": 0.001,
            }

    monkeypatch.setattr(
        simulation_data_module,
        "EtfRuleEvidenceBuilder",
        lambda report_root: EtfRuleEvidenceBuilder(
            report_root, client=EtfDetailClient(),
        ),
    )
    _ready_multi_asset_market(tmp_path / "market")
    pipeline = DailyPipeline(
        build_settings(tmp_path, ()),
        registry=_daily_extension_registry(),
        now_fn=lambda: evening_of(FUTURE_DAYS[1]),
    )

    def no_direct_limits(*, provider, target, instrument_ids):
        return {
            "observation_ids": (),
            "covered_instruments": 0,
            "unresolved_instrument_ids": instrument_ids,
            "request_errors": (),
            "provider_errors": (f"{provider}:target_unavailable:{target}",),
            "_covered_instrument_ids": (),
            "_errors_by_instrument": {},
        }

    monkeypatch.setattr(
        pipeline, "_collect_direct_limit_observations", no_direct_limits,
    )

    result = pipeline.run(target_date=FUTURE_DAYS[1], skip_accounts=True)

    assert result.status == "degraded"
    assert result.snapshot_id is not None
    limits = next(stage for stage in result.stages if stage.name == "limits")
    assert limits.status == "degraded"
    assert limits.detail["execution_guard_instruments"] == len(
        pipeline.warehouse.load_snapshot(result.snapshot_id).plan.universe_scope.instrument_ids
    )
    snapshot = pipeline.warehouse.load_snapshot(result.snapshot_id)
    candidate_id = next(
        stage for stage in result.stages if stage.name == "candidate"
    ).detail["observation_id"]
    assert pipeline.warehouse.load_observation(candidate_id).source_metadata[
        "degraded_auxiliary_evidence"
    ] is None
    validated_id = next(
        stage for stage in result.stages if stage.name == "validate"
    ).detail["observation_id"]
    validated_report = pipeline.warehouse.load_observation(validated_id).source_metadata[
        "report"
    ]
    assert "auxiliary_event_evidence_incomplete" not in validated_report[
        "non_blocking_degradations"
    ]
    target_rows = pipeline.warehouse.query_loaded_snapshot_table(
        snapshot,
        MarketTable.DAILY_BARS,
        start_date=FUTURE_DAYS[1],
        end_date=FUTURE_DAYS[1],
        price_mode="raw",
    )
    prior_rows = pipeline.warehouse.query_loaded_snapshot_table(
        snapshot,
        MarketTable.DAILY_BARS,
        start_date=FUTURE_DAYS[0],
        end_date=FUTURE_DAYS[0],
        price_mode="raw",
    )
    assert target_rows[["open", "high", "low", "close"]].notna().all().all()
    assert set(target_rows["trade_rule_id"]) == {EXECUTION_EVIDENCE_GAP_RULE_ID}
    assert EXECUTION_EVIDENCE_GAP_RULE_ID not in set(prior_rows["trade_rule_id"])


def test_daily_status_collector_top_level_failure_blocks(tmp_path, monkeypatch):
    import fundlab.marketdata.simulation_data as simulation_data_module
    import fundlab.pipeline.daily as daily_module
    from fundlab.marketdata.etf_rules import EtfRuleEvidenceBuilder

    class EtfDetailClient:
        def get_instrument_detail(self, instrument_id, *, iscomplete):
            assert iscomplete
            opened, category = {
                "510050.SH": ("20050223", 70283376),
                "159001.SZ": ("20060221", 3203072),
            }[instrument_id]
            return {
                "OpenDate": opened,
                "secuCategory": category,
                "PreClose": 10.0,
                "UpStopPrice": 11.0,
                "DownStopPrice": 9.0,
                "PriceTick": 0.001,
            }

    monkeypatch.setattr(
        simulation_data_module,
        "EtfRuleEvidenceBuilder",
        lambda report_root: EtfRuleEvidenceBuilder(
            report_root, client=EtfDetailClient(),
        ),
    )
    real_collector = daily_module.SimulationStatusCollector

    class FailingXtquantStatusCollector:
        def __init__(self, *args, **kwargs):
            self.delegate = real_collector(*args, **kwargs)

        def collect(self, spec):
            if spec.provider_name == "xtquant":
                raise ConnectionError("MiniQMT status endpoint unavailable")
            return self.delegate.collect(spec)

    monkeypatch.setattr(
        daily_module, "SimulationStatusCollector", FailingXtquantStatusCollector,
    )
    _ready_multi_asset_market(tmp_path / "market")
    pipeline = DailyPipeline(
        build_settings(tmp_path, ()),
        registry=_daily_extension_registry(),
        now_fn=lambda: evening_of(FUTURE_DAYS[0]),
    )

    result = pipeline.run(target_date=FUTURE_DAYS[0], skip_accounts=True)

    assert result.status == "blocked"
    status = next(stage for stage in result.stages if stage.name == "status")
    assert status.status == "blocked"
    assert status.detail["error_type"] == "ConnectionError"


def test_daily_etf_rule_detail_failure_guards_only_affected_etf(tmp_path, monkeypatch):
    import fundlab.marketdata.simulation_data as simulation_data_module
    from fundlab.marketdata.contracts import EXECUTION_EVIDENCE_GAP_RULE_ID
    from fundlab.marketdata.etf_rules import EtfRuleEvidenceBuilder

    class PartialEtfDetailClient:
        def get_instrument_detail(self, instrument_id, *, iscomplete):
            assert iscomplete
            if instrument_id == "510050.SH":
                return {}
            assert instrument_id == "159001.SZ"
            return {
                "OpenDate": "20060221",
                "secuCategory": 3203072,
                "PreClose": 10.0,
                "UpStopPrice": 11.0,
                "DownStopPrice": 9.0,
                "PriceTick": 0.001,
            }

    monkeypatch.setattr(
        simulation_data_module,
        "EtfRuleEvidenceBuilder",
        lambda report_root: EtfRuleEvidenceBuilder(
            report_root, client=PartialEtfDetailClient(),
        ),
    )
    _ready_multi_asset_market(tmp_path / "market")
    pipeline = DailyPipeline(
        build_settings(tmp_path, ()),
        registry=_daily_extension_registry(),
        now_fn=lambda: evening_of(FUTURE_DAYS[0]),
    )

    result = pipeline.run(target_date=FUTURE_DAYS[0], skip_accounts=True)

    assert result.status == "degraded", [
        (stage.name, stage.status, stage.detail) for stage in result.stages[-3:]
    ]
    validate = next(stage for stage in result.stages if stage.name == "validate")
    assert validate.status == "degraded"
    assert tuple(validate.detail["validator_added_execution_guard"]) == (
        "510050.SH",
    )
    snapshot = pipeline.warehouse.load_snapshot(result.snapshot_id)
    rows = pipeline.warehouse.query_loaded_snapshot_table(
        snapshot,
        MarketTable.DAILY_BARS,
        instrument_ids=("159001.SZ", "510050.SH"),
        start_date=FUTURE_DAYS[0],
        end_date=FUTURE_DAYS[0],
        price_mode="raw",
    ).set_index("instrument_id")
    assert rows[["open", "high", "low", "close"]].notna().all().all()
    assert rows.loc["510050.SH", "trade_rule_id"] == EXECUTION_EVIDENCE_GAP_RULE_ID
    assert rows.loc["159001.SZ", "trade_rule_id"] != EXECUTION_EVIDENCE_GAP_RULE_ID


def test_daily_carries_last_trusted_universe_when_official_endpoint_is_unavailable(
    tmp_path, monkeypatch,
):
    import fundlab.marketdata.simulation_data as simulation_data_module
    from fundlab.marketdata import MarketIngestionService, ProviderUnavailableError
    from fundlab.marketdata.contracts import EXECUTION_EVIDENCE_GAP_RULE_ID
    from fundlab.marketdata.etf_rules import EtfRuleEvidenceBuilder

    class EtfDetailClient:
        def get_instrument_detail(self, instrument_id, *, iscomplete):
            assert iscomplete
            opened, category = {
                "510050.SH": ("20050223", 70283376),
                "159001.SZ": ("20060221", 3203072),
            }[instrument_id]
            return {
                "OpenDate": opened,
                "secuCategory": category,
                "PreClose": 10.0,
                "UpStopPrice": 11.0,
                "DownStopPrice": 9.0,
                "PriceTick": 0.001,
            }

    monkeypatch.setattr(
        simulation_data_module,
        "EtfRuleEvidenceBuilder",
        lambda report_root: EtfRuleEvidenceBuilder(
            report_root, client=EtfDetailClient(),
        ),
    )
    real_capture = MarketIngestionService.capture_resumable

    def capture_with_missing_official(self, provider_name, request, *, refresh=False):
        if provider_name == "exchange-public":
            raise ProviderUnavailableError("official endpoint unavailable")
        return real_capture(self, provider_name, request, refresh=refresh)

    monkeypatch.setattr(
        MarketIngestionService, "capture_resumable", capture_with_missing_official,
    )
    _ready_multi_asset_market(tmp_path / "market")
    pipeline = DailyPipeline(
        build_settings(tmp_path, ()),
        registry=_daily_extension_registry(),
        now_fn=lambda: evening_of(FUTURE_DAYS[0]),
    )

    result = pipeline.run(target_date=FUTURE_DAYS[0], skip_accounts=True)

    assert result.status == "degraded"
    assert result.snapshot_id is not None
    universe = next(stage for stage in result.stages if stage.name == "universe")
    assert universe.status == "degraded"
    assert universe.detail["reason"] == "official_universe_unavailable"
    assert universe.detail["carried_instruments"] == len(_DAILY_BASE_IDS)
    published = pipeline.warehouse.load_snapshot(result.snapshot_id)
    assert set(published.plan.universe_scope.instrument_ids) == set(_DAILY_BASE_IDS)
    rows = pipeline.warehouse.query_loaded_snapshot_table(
        published,
        MarketTable.DAILY_BARS,
        start_date=FUTURE_DAYS[0],
        end_date=FUTURE_DAYS[0],
        price_mode="raw",
    )
    assert rows[["open", "high", "low", "close"]].notna().all().all()
    assert set(rows["trade_rule_id"]) == {EXECUTION_EVIDENCE_GAP_RULE_ID}


def test_daily_carries_and_guards_only_prior_sz_etf_for_component_outage(
    tmp_path, monkeypatch,
):
    _ready_multi_asset_market(tmp_path / "market")
    registry = _daily_extension_registry()
    _install_official_component_outage(monkeypatch, registry, ("sz-etf",))
    _install_daily_etf_detail_client(monkeypatch)
    _install_new_stock_direct_limit_evidence(monkeypatch, registry)
    pipeline = DailyPipeline(
        build_settings(tmp_path, ()), registry=registry,
        now_fn=lambda: evening_of(FUTURE_DAYS[0]),
    )

    result = pipeline.run(target_date=FUTURE_DAYS[0], skip_accounts=True)

    assert result.status == "degraded", [
        (stage.name, stage.status, stage.detail) for stage in result.stages
    ]
    universe = next(stage for stage in result.stages if stage.name == "universe")
    assert universe.status == "degraded"
    assert tuple(universe.detail["unavailable_components"]) == ("sz-etf",)
    assert universe.detail["removed_sample"] == ("159001.SZ",)
    published = pipeline.warehouse.load_snapshot(result.snapshot_id)
    assert set((*_DAILY_BASE_IDS, _DAILY_NEW_ID)) == set(
        published.plan.universe_scope.instrument_ids
    )
    supplement = next(stage for stage in result.stages if stage.name == "new_instruments")
    assert supplement.status == "ok"
    assert supplement.detail["instrument_ids"] == (_DAILY_NEW_ID,)
    rows = pipeline.warehouse.query_loaded_snapshot_table(
        published,
        MarketTable.DAILY_BARS,
        instrument_ids=(*_DAILY_BASE_IDS, _DAILY_NEW_ID),
        start_date=FUTURE_DAYS[0],
        end_date=FUTURE_DAYS[0],
        price_mode="raw",
    ).set_index("instrument_id")
    assert rows.loc["159001.SZ", "trade_rule_id"] == EXECUTION_EVIDENCE_GAP_RULE_ID
    assert all(
        rows.loc[instrument_id, "trade_rule_id"] != EXECUTION_EVIDENCE_GAP_RULE_ID
        for instrument_id in ("000001.SZ", "510050.SH", "600000.SH", _DAILY_NEW_ID)
    )


def test_daily_component_outage_without_prior_members_still_publishes_admitted_scope(
    tmp_path, monkeypatch,
):
    _ready_multi_asset_market(tmp_path / "market")
    registry = _daily_extension_registry()
    _install_official_component_outage(monkeypatch, registry, ("sh-stock-star",))
    _install_daily_etf_detail_client(monkeypatch)
    pipeline = DailyPipeline(
        build_settings(tmp_path, ()), registry=registry,
        now_fn=lambda: evening_of(FUTURE_DAYS[0]),
    )

    result = pipeline.run(target_date=FUTURE_DAYS[0], skip_accounts=True)

    assert result.status == "degraded", [
        (stage.name, stage.status, stage.detail) for stage in result.stages
    ]
    universe = next(stage for stage in result.stages if stage.name == "universe")
    assert universe.detail["unavailable_components"]["sh-stock-star"]["scope"] == {
        "exchange": "SH", "asset_type": "stock", "board": "star",
    }
    assert universe.detail["carried_instruments"] == 0
    published = pipeline.warehouse.load_snapshot(result.snapshot_id)
    assert set(published.plan.universe_scope.instrument_ids) == set(_DAILY_BASE_IDS)


def test_daily_all_unavailable_official_components_carry_and_guard_prior_scope(
    tmp_path, monkeypatch,
):
    _ready_multi_asset_market(tmp_path / "market")
    registry = _daily_extension_registry()
    _install_official_component_outage(
        monkeypatch, registry, tuple(_OFFICIAL_COMPONENT_SCOPES),
    )
    _install_daily_etf_detail_client(monkeypatch)
    pipeline = DailyPipeline(
        build_settings(tmp_path, ()), registry=registry,
        now_fn=lambda: evening_of(FUTURE_DAYS[0]),
    )

    result = pipeline.run(target_date=FUTURE_DAYS[0], skip_accounts=True)

    assert result.status == "degraded", [
        (stage.name, stage.status, stage.detail) for stage in result.stages
    ]
    universe = next(stage for stage in result.stages if stage.name == "universe")
    assert universe.status == "degraded"
    assert set(universe.detail["unavailable_components"]) == set(_OFFICIAL_COMPONENT_SCOPES)
    assert set(universe.detail["removed_sample"]) == set(_DAILY_BASE_IDS)
    published = pipeline.warehouse.load_snapshot(result.snapshot_id)
    assert set(published.plan.universe_scope.instrument_ids) == set(_DAILY_BASE_IDS)
    rows = pipeline.warehouse.query_loaded_snapshot_table(
        published,
        MarketTable.DAILY_BARS,
        instrument_ids=_DAILY_BASE_IDS,
        start_date=FUTURE_DAYS[0],
        end_date=FUTURE_DAYS[0],
        price_mode="raw",
    )
    assert set(rows["trade_rule_id"]) == {EXECUTION_EVIDENCE_GAP_RULE_ID}


def test_daily_unknown_official_universe_observation_error_blocks_without_carry_forward(
    tmp_path, monkeypatch,
):
    from fundlab.marketdata import MarketIngestionService, ObservationError

    _ready_multi_asset_market(tmp_path / "market")
    account = DailyAccountSettings(
        "paper-official-parser", "Official Parser", Decimal("100000"), "static",
        {"600000.SH": Decimal("0.5")},
    )
    settings = build_settings(tmp_path, (account,))
    pipeline = DailyPipeline(
        settings, registry=_daily_extension_registry(),
        now_fn=lambda: evening_of(FUTURE_DAYS[0]),
    )
    assert pipeline.run(target_date=FUTURE_DAYS[0], skip_data=True).status == "ok"
    before = pipeline.warehouse.current_snapshot_id()
    repository = TradingRepository(settings.paths.trading_database)
    _, head_before = repository.selected_state(account.account_id)
    assert head_before is not None
    real_capture = MarketIngestionService.capture_resumable

    def malformed_official(self, provider_name, request, *, refresh=False):
        if provider_name == "exchange-public":
            raise ObservationError("instruments has unknown columns: bogus")
        return real_capture(self, provider_name, request, refresh=refresh)

    monkeypatch.setattr(MarketIngestionService, "capture_resumable", malformed_official)

    result = pipeline.run(target_date=FUTURE_DAYS[0], skip_accounts=True)

    assert result.status == "blocked"
    blocked = next(stage for stage in result.stages if stage.name == "universe")
    assert blocked.detail["error_type"] == "ObservationError"
    assert pipeline.warehouse.current_snapshot_id() == before
    assert repository.selected_state(account.account_id)[1] == head_before


def test_daily_tampered_official_endpoint_evidence_blocks_without_state_change(
    tmp_path, monkeypatch,
):
    _ready_multi_asset_market(tmp_path / "market")
    account = DailyAccountSettings(
        "paper-official-evidence", "Official Evidence", Decimal("100000"), "static",
        {"600000.SH": Decimal("0.5")},
    )
    settings = build_settings(tmp_path, (account,))
    registry = _daily_extension_registry()
    _install_official_component_outage(monkeypatch, registry, ("sz-etf",))
    pipeline = DailyPipeline(
        settings, registry=registry,
        now_fn=lambda: evening_of(FUTURE_DAYS[0]),
    )
    assert pipeline.run(target_date=FUTURE_DAYS[0], skip_data=True).status == "ok"
    before = pipeline.warehouse.current_snapshot_id()
    repository = TradingRepository(settings.paths.trading_database)
    _, head_before = repository.selected_state(account.account_id)
    assert head_before is not None

    provider = registry.provider("exchange-public")
    original_observe = provider.observe

    def tampered_observe(request: ProviderRequest) -> ObservationPayload:
        payload = original_observe(request)
        if request.capability is not ProviderCapability.INSTRUMENTS:
            return payload
        metadata = dict(payload.source_metadata)
        closure = json.loads(canonical_json(metadata["component_closure"]))
        closure["sh-etf"]["admitted_ids"] = []
        metadata["component_closure"] = closure
        return ObservationPayload(
            payload.provider,
            payload.observed_at,
            payload.request,
            payload.tables,
            payload.coverage,
            metadata,
        )

    monkeypatch.setattr(provider, "observe", tampered_observe)
    result = pipeline.run(target_date=FUTURE_DAYS[0], skip_accounts=True)

    assert result.status == "blocked"
    blocked = next(stage for stage in result.stages if stage.name == "universe")
    assert blocked.detail["reason"] == "official universe component closure is invalid"
    assert pipeline.warehouse.current_snapshot_id() == before
    assert repository.selected_state(account.account_id)[1] == head_before


def test_daily_tampered_partial_official_frame_lineage_blocks_without_state_change(
    tmp_path, monkeypatch,
):
    _ready_multi_asset_market(tmp_path / "market")
    account = DailyAccountSettings(
        "paper-official-lineage", "Official Lineage", Decimal("100000"), "static",
        {"600000.SH": Decimal("0.5")},
    )
    settings = build_settings(tmp_path, (account,))
    registry = _daily_extension_registry()
    _install_official_component_outage(monkeypatch, registry, ("sz-etf",))
    pipeline = DailyPipeline(
        settings, registry=registry,
        now_fn=lambda: evening_of(FUTURE_DAYS[0]),
    )
    assert pipeline.run(target_date=FUTURE_DAYS[0], skip_data=True).status == "ok"
    before = pipeline.warehouse.current_snapshot_id()
    repository = TradingRepository(settings.paths.trading_database)
    _, head_before = repository.selected_state(account.account_id)
    assert head_before is not None

    provider = registry.provider("exchange-public")
    original_observe = provider.observe

    def tampered_observe(request: ProviderRequest) -> ObservationPayload:
        payload = original_observe(request)
        if request.capability is not ProviderCapability.INSTRUMENTS:
            return payload
        frame = payload.tables[MarketTable.INSTRUMENTS].copy()
        frame.at[frame.index[0], "field_lineage"] = canonical_json({
            "endpoint": "tampered-unsuccessful-endpoint",
        })
        return ObservationPayload(
            payload.provider,
            payload.observed_at,
            payload.request,
            {MarketTable.INSTRUMENTS: frame},
            payload.coverage,
            payload.source_metadata,
        )

    monkeypatch.setattr(provider, "observe", tampered_observe)
    result = pipeline.run(target_date=FUTURE_DAYS[0], skip_accounts=True)

    assert result.status == "blocked"
    blocked = next(stage for stage in result.stages if stage.name == "universe")
    assert blocked.detail["reason"] == "official universe component closure is invalid"
    assert pipeline.warehouse.current_snapshot_id() == before
    assert repository.selected_state(account.account_id)[1] == head_before


def test_daily_validation_failure_is_reported_as_a_blocked_stage(tmp_path, monkeypatch):
    import fundlab.pipeline.daily as daily_module

    def fail_validation(self, **kwargs):
        raise TradeRuleError(
            "Price-limit audit needs two direct provider limit values for "
            "000001.SZ/2026-07-15"
        )

    monkeypatch.setattr(
        daily_module.SimulationIncrementValidator,
        "validate_and_record",
        fail_validation,
    )
    _ready_multi_asset_market(tmp_path / "market")
    pipeline = DailyPipeline(
        build_settings(tmp_path, ()),
        registry=_daily_extension_registry(),
        now_fn=lambda: evening_of(FUTURE_DAYS[0]),
    )

    result = pipeline.run(target_date=FUTURE_DAYS[0], skip_accounts=True)

    assert result.status == "blocked"
    assert result.stages[-1].name == "validate"
    assert result.stages[-1].detail["error_type"] == "TradeRuleError"
    assert "two direct provider limit values" in result.stages[-1].detail["reason"]
    assert result.report_path is not None
    report = json.loads(result.report_path.read_text(encoding="utf-8"))
    assert report["status"] == "blocked"
    assert report["stages"][-1]["name"] == "validate"


def test_daily_adjusted_price_factor_audit_only_fills_unmatched_candidates():
    candidates = {
        "510050.SH": {"2026-07-28": 0.5},
        "600000.SH": {"2026-07-28": 0.9},
    }
    baostock = pd.DataFrame([{
        "instrument_id": "600000.SH",
        "effective_date": "2026-07-28",
        "price_multiplier": 0.901,
    }])

    unresolved = DailyPipeline._unmatched_factor_candidates(candidates, baostock)

    assert unresolved == {"510050.SH": {"2026-07-28": 0.5}}
    raw = pd.DataFrame([
        {
            "instrument_id": "510050.SH", "session_date": "2026-07-27",
            "price_mode": "raw", "close": 2.0,
        },
        {
            "instrument_id": "510050.SH", "session_date": "2026-07-28",
            "price_mode": "raw", "close": 1.0,
        },
    ])
    adjusted = pd.DataFrame([
        {
            "instrument_id": "510050.SH", "session_date": "2026-07-27",
            "price_mode": "adjusted", "close": 1.0,
        },
        {
            "instrument_id": "510050.SH", "session_date": "2026-07-28",
            "price_mode": "adjusted", "close": 1.0,
        },
    ])

    factors = DailyPipeline._derive_adjusted_price_factor_rows(
        raw_bars=raw,
        adjusted_bars=adjusted,
        candidates=unresolved,
        raw_observation_id="obs-raw-factor-audit",
        adjusted_observation_id="obs-adjusted-factor-audit",
    )

    assert len(factors) == 1
    assert factors.iloc[0]["price_multiplier"] == pytest.approx(0.5)
    payload = json.loads(factors.iloc[0]["source_payload"])
    assert payload["adjusted_price_audit"]["prior_session"] == "2026-07-27"
    with pytest.raises(TradeRuleError, match="expected=0.4") as exc_info:
        DailyPipeline._derive_adjusted_price_factor_rows(
            raw_bars=raw,
            adjusted_bars=adjusted,
            candidates={"510050.SH": {"2026-07-28": 0.4}},
            raw_observation_id="obs-raw-factor-audit",
            adjusted_observation_id="obs-adjusted-factor-audit",
        )
    assert exc_info.value.instrument_ids == ("510050.SH",)


def test_daily_adjusted_factor_duplicate_keys_report_exact_instrument_scope():
    raw = pd.DataFrame([
        {
            "instrument_id": "510050.SH", "session_date": "2026-07-27",
            "price_mode": "raw", "close": 2.0,
        },
        {
            "instrument_id": "510050.SH", "session_date": "2026-07-27",
            "price_mode": "raw", "close": 2.1,
        },
        {
            "instrument_id": "600000.SH", "session_date": "2026-07-27",
            "price_mode": "raw", "close": 10.0,
        },
    ])
    adjusted = pd.DataFrame([
        {
            "instrument_id": "510050.SH", "session_date": "2026-07-27",
            "price_mode": "adjusted", "close": 1.0,
        },
        {
            "instrument_id": "600000.SH", "session_date": "2026-07-27",
            "price_mode": "adjusted", "close": 10.0,
        },
    ])

    with pytest.raises(TradeRuleError, match="duplicate daily keys") as exc_info:
        DailyPipeline._derive_adjusted_price_factor_rows(
            raw_bars=raw,
            adjusted_bars=adjusted,
            candidates={"510050.SH": {"2026-07-28": 0.5}},
            raw_observation_id="obs-raw",
            adjusted_observation_id="obs-adjusted",
        )

    assert exc_info.value.instrument_ids == ("510050.SH",)


def test_daily_factor_audit_without_prior_open_session_reports_unresolved_scope(
    monkeypatch,
):
    import fundlab.pipeline.daily as daily_module

    pipeline = object.__new__(DailyPipeline)
    pipeline.warehouse = SimpleNamespace(
        read_observation_table=lambda observation_id, table: empty_table(
            MarketTable.ADJUSTMENT_FACTORS, include_lineage=True,
        ),
    )
    pipeline._capture_exact_source = lambda **kwargs: (
        SimpleNamespace(observation_id="obs-baostock"), False,
    )
    monkeypatch.setattr(
        daily_module,
        "build_factor_audit_candidates",
        lambda **kwargs: {
            "510050.SH": {FUTURE_DAYS[0].isoformat(): 0.5},
            "600000.SH": {FUTURE_DAYS[0].isoformat(): 0.9},
        },
    )
    scope = UniverseScope(
        CURRENT_SH_SZ_STOCK_ETF_UNIVERSE,
        FUTURE_DAYS[0],
        FUTURE_DAYS[0],
        FUTURE_DAYS[0],
        instrument_ids=("510050.SH", "600000.SH"),
    )
    calendar = pd.DataFrame([{
        "exchange": "SH",
        "session_date": FUTURE_DAYS[0].isoformat(),
        "is_open": True,
    }])

    with pytest.raises(TradeRuleError, match="no prior open session") as exc_info:
        pipeline._reconcile_action_factor_evidence(
            instruments=_daily_base_instruments().loc[
                lambda frame: frame["instrument_id"].isin(scope.instrument_ids)
            ],
            actions=empty_table(MarketTable.CORPORATE_ACTIONS, include_lineage=True),
            primary_factors=empty_table(
                MarketTable.ADJUSTMENT_FACTORS, include_lineage=True,
            ),
            bars=empty_table(MarketTable.DAILY_BARS, include_lineage=True),
            calendar_frame=calendar,
            increment_scope=scope,
        )

    assert exc_info.value.instrument_ids == ("510050.SH", "600000.SH")


def test_daily_healthy_increment_retains_prior_incomplete_coverage_marker(
    tmp_path, monkeypatch,
):
    import fundlab.marketdata.simulation_data as simulation_data_module
    import fundlab.pipeline.daily as daily_module
    from fundlab.marketdata.etf_rules import EtfRuleEvidenceBuilder

    class EtfDetailClient:
        def get_instrument_detail(self, instrument_id, *, iscomplete):
            assert iscomplete
            opened, category = {
                "510050.SH": ("20050223", 70283376),
                "159001.SZ": ("20060221", 3203072),
            }[instrument_id]
            return {
                "OpenDate": opened,
                "secuCategory": category,
                "PreClose": 10.0,
                "UpStopPrice": 11.0,
                "DownStopPrice": 9.0,
                "PriceTick": 0.001,
            }

    monkeypatch.setattr(
        simulation_data_module,
        "EtfRuleEvidenceBuilder",
        lambda report_root: EtfRuleEvidenceBuilder(
            report_root, client=EtfDetailClient(),
        ),
    )
    real_collector = daily_module.SimulationEvidenceCollector

    class TransientEvidenceCollector:
        failed_once = False

        def __init__(self, *args, **kwargs):
            self.delegate = real_collector(*args, **kwargs)

        def collect(self, spec):
            if spec.kind == "stock-actions" and not self.failed_once:
                type(self).failed_once = True
                return SimpleNamespace(
                    status="incomplete",
                    blockers=("temporary provider failure",),
                    observation_ids=(),
                )
            return self.delegate.collect(spec)

    monkeypatch.setattr(
        daily_module, "SimulationEvidenceCollector", TransientEvidenceCollector,
    )
    _ready_multi_asset_market(tmp_path / "market")
    registry = _daily_extension_registry()
    pipeline = DailyPipeline(
        build_settings(tmp_path, ()),
        registry=registry,
        now_fn=lambda: evening_of(FUTURE_DAYS[0]),
    )

    first = pipeline.run(target_date=FUTURE_DAYS[0], skip_accounts=True)
    second = pipeline.run(target_date=FUTURE_DAYS[1], skip_accounts=True)

    assert first.status == "degraded"
    assert next(stage for stage in first.stages if stage.name == "evidence").status == (
        "degraded"
    )
    assert second.status == "degraded", [
        (stage.name, stage.status, stage.detail) for stage in second.stages
    ]
    assert next(stage for stage in second.stages if stage.name == "evidence").status == (
        "ok"
    )
    second_snapshot = pipeline.warehouse.load_snapshot(second.snapshot_id)
    assert second_snapshot.plan.require_complete_coverage is False
    assert "degraded_auxiliary_evidence_coverage" in second_snapshot.quality.warnings
    baostock = registry.provider("baostock")
    assert sum(
        request.capability is ProviderCapability.INSTRUMENTS
        for request in baostock.calls
    ) == 2
