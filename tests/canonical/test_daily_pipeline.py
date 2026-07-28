from __future__ import annotations

from datetime import date, datetime, time, timezone
from decimal import Decimal
import json
from types import SimpleNamespace

import pandas as pd
import pytest

from fundlab.marketdata import (
    CoverageClaim,
    MarketTable,
    ObservationPayload,
    ProviderCapability,
    ProviderRegistry,
    ProviderRequest,
)
from fundlab.pipeline import DailyPipeline
from fundlab.pipeline.daily import DailyPipelineBlocked
from fundlab.settings import (
    DailyAccountSettings,
    DailySettings,
    FoundationPaths,
    FoundationSettings,
)
from fundlab.trading import TradingRepository
from tests.canonical.fixtures import DAYS, FUTURE_DAYS, market_frames, ready_market
from tests.canonical.test_trading_kernel import fees, policies


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
