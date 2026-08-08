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
    SnapshotNotReadyError,
    SourceSlice,
    TradeRuleError,
    UniverseScope,
)
from fundlab.marketdata.incremental import IncrementalCanonicalPublisher
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
)
from fundlab.settings import (
    AgentPolicySettings,
    AgentSettings,
    DailyAccountSettings,
    DailySettings,
    FoundationPaths,
    FoundationSettings,
)
from fundlab.trading import TradingRepository
from tests.canonical.fixtures import (
    DAYS,
    FUTURE_DAYS,
    commit_test_snapshot,
    market_frames,
    ready_market,
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


def test_no_trade_active_source_conflict_becomes_bounded_quarantine(monkeypatch):
    import fundlab.marketdata.history as history_module
    import fundlab.pipeline.daily as daily_module

    target = ("510050.SH", "600000.SH")
    pipeline = object.__new__(DailyPipeline)
    pipeline.warehouse = SimpleNamespace()
    pipeline.ingestion = SimpleNamespace(
        capture_resumable=lambda provider, request: (
            SimpleNamespace(observation_id=f"obs-{provider}"), False,
        ),
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
    settings = build_settings(tmp_path, ())
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
                        endpoint: f"sha256-{endpoint}"
                        for endpoint in _DAILY_ENDPOINT_COUNTS
                    },
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

    with pytest.raises(DailyPipelineBlocked, match="exact reconciled partition"):
        pipeline._build_new_instrument_supplement(
            builder=Builder(),
            universe_observation_id="obs-exchange-official",
            official_frame=_new_listing_frame(FUTURE_DAYS[0]),
            instrument_ids=("688825.SH",),
            start=FUTURE_DAYS[0],
            end=FUTURE_DAYS[0],
        )


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
        now_fn=lambda: evening_of(FUTURE_DAYS[0]),
    )
    real_reconcile = pipeline._reconcile_action_factor_evidence
    failed = False

    def fail_one_instrument(**kwargs):
        nonlocal failed
        ids = set(map(str, kwargs["instruments"]["instrument_id"]))
        if not failed and "600000.SH" in ids:
            failed = True
            raise SnapshotNotReadyError(
                "factor evidence unavailable: 600000.SH/2026-07-17"
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


def test_daily_status_provider_failure_preserves_prices_and_publishes(tmp_path, monkeypatch):
    import fundlab.marketdata.simulation_data as simulation_data_module
    import fundlab.pipeline.daily as daily_module
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
    real_collector = daily_module.SimulationStatusCollector

    class FailingXtquantStatusCollector:
        def __init__(self, *args, **kwargs):
            self.delegate = real_collector(*args, **kwargs)

        def collect(self, spec):
            if spec.provider_name == "xtquant":
                raise RuntimeError("MiniQMT status endpoint unavailable")
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

    assert result.status == "degraded"
    status = next(stage for stage in result.stages if stage.name == "status")
    assert status.status == "degraded"
    assert "MiniQMT status endpoint unavailable" in status.detail["xtquant"][
        "blockers"
    ][0]
    snapshot = pipeline.warehouse.load_snapshot(result.snapshot_id)
    rows = pipeline.warehouse.query_loaded_snapshot_table(
        snapshot,
        MarketTable.DAILY_BARS,
        start_date=FUTURE_DAYS[0],
        end_date=FUTURE_DAYS[0],
        price_mode="raw",
    )
    assert rows[["open", "high", "low", "close"]].notna().all().all()
    assert set(rows["trade_rule_id"]) == {EXECUTION_EVIDENCE_GAP_RULE_ID}


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
    from fundlab.marketdata import MarketIngestionService, ObservationError
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
            raise ObservationError("official endpoint returned no rows")
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
    with pytest.raises(SnapshotNotReadyError, match="expected=0.4"):
        DailyPipeline._derive_adjusted_price_factor_rows(
            raw_bars=raw,
            adjusted_bars=adjusted,
            candidates={"510050.SH": {"2026-07-28": 0.4}},
            raw_observation_id="obs-raw-factor-audit",
            adjusted_observation_id="obs-adjusted-factor-audit",
        )


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
