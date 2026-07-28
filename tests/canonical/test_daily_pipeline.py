from __future__ import annotations

from datetime import date, datetime, time, timezone
from decimal import Decimal, ROUND_HALF_UP
import json
from types import SimpleNamespace

import pandas as pd
import pytest

from fundlab.marketdata import (
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
    UniverseScope,
)
from fundlab.marketdata.incremental import IncrementalCanonicalPublisher
from fundlab.marketdata.schema import empty_table
from fundlab.marketdata.sources.eastmoney_fund import EASTMONEY_ETF_ACTION_POLICY
from fundlab.pipeline import DailyPipeline
from fundlab.pipeline.daily import DailyPipelineBlocked
from fundlab.settings import (
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
    base = calendar_frame()
    return pd.concat((base, base.assign(exchange="SZ")), ignore_index=True)


def _daily_increment_bars(request: ProviderRequest) -> pd.DataFrame:
    sessions = tuple(
        day for day in (*DAYS, *FUTURE_DAYS)
        if request.start_date <= day <= request.end_date
    )
    rows = []
    for ordinal, instrument_id in enumerate(request.instrument_ids):
        for session_ordinal, day in enumerate(sessions):
            close = 10.0 + ordinal + session_ordinal
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

    def observe(self, request: ProviderRequest) -> ObservationPayload:
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
    pipeline = DailyPipeline(
        build_settings(tmp_path, ()),
        registry=_daily_extension_registry(),
        now_fn=lambda: evening_of(FUTURE_DAYS[0]),
    )

    result = pipeline.run(target_date=FUTURE_DAYS[0], skip_accounts=True)

    assert result.status == "ok", [
        (stage.name, stage.status, stage.detail) for stage in result.stages
    ]
    assert result.snapshot_id is not None
    by_name = {stage.name: stage for stage in result.stages}
    assert by_name["bars"].detail["included"] == len(_DAILY_BASE_IDS)
    assert by_name["bars"].detail["excluded"] == 1
    assert by_name["new_instruments"].detail["instrument_ids"] == (_DAILY_NEW_ID,)
    for stage in ("research", "status", "evidence", "candidate", "validate", "extend"):
        assert by_name[stage].status == "ok"

    warehouse = MarketDataWarehouse(tmp_path / "market")
    assert warehouse.current_snapshot_id() == result.snapshot_id
    published = warehouse.load_snapshot(result.snapshot_id)
    scope = published.plan.universe_scope
    assert scope is not None
    assert scope.as_of_date == scope.history_end == FUTURE_DAYS[0]
    assert set(scope.instrument_ids) == {*_DAILY_BASE_IDS, _DAILY_NEW_ID}
    increment_bars = warehouse.query_loaded_snapshot_table(
        published,
        MarketTable.DAILY_BARS,
        instrument_ids=scope.instrument_ids,
        start_date=FUTURE_DAYS[0],
        end_date=FUTURE_DAYS[0],
        price_mode="raw",
    )
    assert set(map(str, increment_bars["instrument_id"])) == set(scope.instrument_ids)
