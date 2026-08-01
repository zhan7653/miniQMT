from __future__ import annotations

from datetime import date, datetime, timezone
import json

import pandas as pd
import pytest

from fundlab.marketdata import (
    CURRENT_SH_SZ_STOCK_ETF_UNIVERSE,
    CoverageClaim,
    ETF_ACTION_CANONICAL_PROVIDER,
    EvidenceCollectionSpec,
    MarketDataWarehouse,
    MarketTable,
    ObservationPayload,
    ProviderCapability,
    ProviderRequest,
    ReadinessProfile,
    SIMULATION_STATUS_CANONICAL_PROVIDER,
    STOCK_ACTION_CANONICAL_PROVIDER,
    SimulationEvidenceCollector,
    SimulationStatusCollector,
    StatusCollectionSpec,
    SnapshotPlan,
    SourceSlice,
    UniverseScope,
    build_dense_simulation_bars,
    canonicalize_suspension_status,
    reconcile_corporate_action_factors,
    reconcile_simulation_status,
)
from fundlab.marketdata.simulation_data import _collapse_same_lifecycle_cash_components
from fundlab.marketdata.sources.cninfo import (
    CninfoAnnouncementRecord,
    CninfoAnnouncementScan,
)
from fundlab.marketdata.sources.eastmoney_fund import EASTMONEY_ETF_ACTION_POLICY
from tests.canonical.fixtures import DAYS, market_frames


def test_same_lifecycle_cash_components_are_summed_with_conservative_known_date():
    rows = pd.DataFrame([
        {
            "action_id": "component-a",
            "instrument_id": "600000.SH",
            "action_type": "cash_dividend",
            "known_date": "2026-05-01",
            "record_date": "2026-05-10",
            "ex_date": "2026-05-11",
            "pay_date": "2026-05-12",
            "listing_date": None,
            "cash_per_share": 0.1,
            "share_ratio": None,
            "rights_price": None,
            "quantity_multiplier": None,
            "field_lineage": None,
            "source_payload": "component-a-payload",
            "source_provider": "cninfo-public",
            "source_observation_id": "obs-source",
            "observed_at": "2026-07-17T00:00:00+00:00",
        },
        {
            "action_id": "component-b",
            "instrument_id": "600000.SH",
            "action_type": "cash_dividend",
            "known_date": "2026-05-02",
            "record_date": "2026-05-10",
            "ex_date": "2026-05-11",
            "pay_date": "2026-05-12",
            "listing_date": None,
            "cash_per_share": 0.002064,
            "share_ratio": None,
            "rights_price": None,
            "quantity_multiplier": None,
            "field_lineage": None,
            "source_payload": "component-b-payload",
            "source_provider": "cninfo-public",
            "source_observation_id": "obs-source",
            "observed_at": "2026-07-17T00:00:00+00:00",
        },
    ])

    collapsed, evidence = _collapse_same_lifecycle_cash_components(rows)

    assert len(collapsed) == 1
    assert collapsed.iloc[0]["known_date"] == "2026-05-02"
    assert collapsed.iloc[0]["cash_per_share"] == pytest.approx(0.102064)
    assert evidence["600000.SH|cash_dividend|2026-05-11"]["component_count"] == 2


def test_official_etf_cash_terms_quarantine_exact_conflicting_vendor_factor():
    observed_at = "2026-07-17T00:00:00+00:00"
    instruments = pd.DataFrame([{
        "instrument_id": "510720.SH",
        "asset_type": "etf",
        "listed_date": "2024-04-23",
        "delisted_date": None,
    }])
    actions = pd.DataFrame([{
        "action_id": "official-cash",
        "instrument_id": "510720.SH",
        "action_type": "cash_dividend",
        "known_date": "2026-05-08",
        "record_date": "2026-05-12",
        "ex_date": "2026-05-13",
        "pay_date": "2026-05-18",
        "listing_date": None,
        "cash_per_share": 0.0031,
        "share_ratio": None,
        "rights_price": None,
        "quantity_multiplier": None,
        "field_lineage": None,
        "source_payload": "official implementation notice",
        "source_provider": "canonical-etf-actions-r2-v5",
        "source_observation_id": "obs-official",
        "observed_at": observed_at,
    }])
    factors = pd.DataFrame([{
        "factor_id": "bad-xt-factor",
        "instrument_id": "510720.SH",
        "effective_date": "2026-05-13",
        "known_date": "2026-05-13",
        "price_multiplier": 0.9718172983,
        "field_lineage": None,
        "source_payload": '{"raw":{"interest":0.031,"stockBonus":0,"stockGift":0,"allotNum":0,"allotPrice":0}}',
        "source_provider": "xtquant",
        "source_observation_id": "obs-xt",
        "observed_at": observed_at,
    }])
    bars = pd.DataFrame([{
        "instrument_id": "510720.SH",
        "session_date": "2026-05-13",
        "previous_close": 1.1,
    }])
    scope = UniverseScope(
        CURRENT_SH_SZ_STOCK_ETF_UNIVERSE,
        date(2026, 7, 17),
        date(2024, 1, 1),
        date(2026, 7, 17),
        survivorship_bias=True,
        instrument_ids=("510720.SH",),
    )

    result = reconcile_corporate_action_factors(
        instruments=instruments,
        actions=actions,
        factors=factors,
        universe_scope=scope,
        daily_bars=bars,
    )

    assert result.actions.iloc[0]["cash_per_share"] == pytest.approx(0.0031)
    assert result.factors.iloc[0]["price_multiplier"] == pytest.approx(
        (1.1 - 0.0031) / 1.1
    )
    assert result.report["official_etf_cash_factor_recovery"][0][
        "quarantined_factor_id"
    ] == "bad-xt-factor"


def test_dense_simulation_bars_keep_reconciled_prices_and_explicit_suspension_state():
    frames = market_frames(suspended_on=DAYS[1])
    instruments = frames[MarketTable.INSTRUMENTS]
    calendar = frames[MarketTable.CALENDAR]
    research = frames[MarketTable.DAILY_BARS].loc[
        frames[MarketTable.DAILY_BARS]["price_mode"].eq("raw")
        & ~frames[MarketTable.DAILY_BARS]["suspended"]
    ].copy()
    research["source_observation_id"] = "obs-research"
    status = frames[MarketTable.DAILY_BARS].loc[
        frames[MarketTable.DAILY_BARS]["price_mode"].eq("raw")
    ].copy()
    status["source_observation_id"] = "obs-status"
    status.loc[~status["suspended"], "open"] += 0.01
    scope = UniverseScope(
        CURRENT_SH_SZ_STOCK_ETF_UNIVERSE,
        DAYS[-1], DAYS[0], DAYS[-1], survivorship_bias=True,
        instrument_ids=("600000.SH",),
    )

    dense = build_dense_simulation_bars(
        instruments=instruments,
        research_bars=research,
        status_bars=status,
        calendar=calendar,
        universe_scope=scope,
    )

    suspended = dense.loc[dense["session_date"].eq(DAYS[1].isoformat())].iloc[0]
    active = dense.loc[dense["session_date"].eq(DAYS[0].isoformat())].iloc[0]
    assert len(dense) == len(DAYS)
    assert suspended["suspended"] and suspended["volume"] == 0
    assert suspended[["open", "high", "low", "close"]].isna().all()
    assert active["open"] == research.iloc[0]["open"]
    assert '"status_observation_id":"obs-status"' in active["field_lineage"]


def test_dense_simulation_bars_reject_missing_daily_status():
    frames = market_frames()
    raw = frames[MarketTable.DAILY_BARS].loc[
        frames[MarketTable.DAILY_BARS]["price_mode"].eq("raw")
    ].copy()
    raw["source_observation_id"] = "obs-source"
    scope = UniverseScope(
        CURRENT_SH_SZ_STOCK_ETF_UNIVERSE,
        DAYS[-1], DAYS[0], DAYS[-1], survivorship_bias=True,
        instrument_ids=("600000.SH",),
    )

    with pytest.raises(Exception, match="Status daily coverage mismatch"):
        build_dense_simulation_bars(
            instruments=frames[MarketTable.INSTRUMENTS],
            research_bars=raw,
            status_bars=raw.iloc[:-1],
            calendar=frames[MarketTable.CALENDAR],
            universe_scope=scope,
        )


def test_sparse_trading_presence_only_infers_suspension_when_research_also_has_no_bar():
    frames = market_frames(suspended_on=DAYS[1])
    raw = frames[MarketTable.DAILY_BARS].loc[
        frames[MarketTable.DAILY_BARS]["price_mode"].eq("raw")
    ].copy()
    research = raw.loc[~raw["suspended"]].copy()
    research["source_observation_id"] = "obs-research"
    presence = raw.loc[~raw["suspended"]].copy()
    presence["is_st"] = None
    scope = UniverseScope(
        CURRENT_SH_SZ_STOCK_ETF_UNIVERSE,
        DAYS[-1], DAYS[0], DAYS[-1], survivorship_bias=True,
        instrument_ids=("600000.SH",),
    )

    dense, quality = canonicalize_suspension_status(
        instruments=frames[MarketTable.INSTRUMENTS],
        research_bars=research,
        status_bars=presence,
        calendar=frames[MarketTable.CALENDAR],
        universe_scope=scope,
        status_observation_id="obs-xt",
        source_snapshot_id="snap-research",
        calendar_observation_id="obs-calendar",
    )

    inferred = dense.loc[dense["session_date"].eq(DAYS[1].isoformat())].iloc[0]
    assert len(dense) == len(DAYS)
    assert inferred["suspended"]
    assert inferred[["open", "high", "low", "close", "previous_close"]].isna().all()
    assert quality["inferred_suspension_count"] == 1
    assert quality["active_session_count"] == len(DAYS) - 1
    assert "calendar_open_minus_two_source_active_presence" in inferred["field_lineage"]


def test_sparse_trading_presence_never_hides_a_reconciled_active_price():
    frames = market_frames()
    raw = frames[MarketTable.DAILY_BARS].loc[
        frames[MarketTable.DAILY_BARS]["price_mode"].eq("raw")
    ].copy()
    raw["source_observation_id"] = "obs-research"
    scope = UniverseScope(
        CURRENT_SH_SZ_STOCK_ETF_UNIVERSE,
        DAYS[-1], DAYS[0], DAYS[-1], survivorship_bias=True,
        instrument_ids=("600000.SH",),
    )

    with pytest.raises(Exception, match="price-key mismatch"):
        canonicalize_suspension_status(
            instruments=frames[MarketTable.INSTRUMENTS],
            research_bars=raw,
            status_bars=raw.iloc[:-1],
            calendar=frames[MarketTable.CALENDAR],
            universe_scope=scope,
            status_observation_id="obs-xt",
            source_snapshot_id="snap-research",
            calendar_observation_id="obs-calendar",
        )


def test_status_reconciliation_uses_sparse_st_only_on_active_stock_sessions():
    frames = market_frames(suspended_on=DAYS[1])
    raw = frames[MarketTable.DAILY_BARS].loc[
        frames[MarketTable.DAILY_BARS]["price_mode"].eq("raw")
    ].copy()
    research = raw.loc[~raw["suspended"]].copy()
    research["source_observation_id"] = "obs-research"
    dense = raw.copy()
    dense["source_observation_id"] = "obs-xt"
    dense.loc[dense["suspended"], "previous_close"] = None
    dense["is_st"] = None
    stock_st = raw.loc[~raw["suspended"]].copy()
    stock_st["source_observation_id"] = "obs-bao"
    stock_st.loc[stock_st["session_date"].eq(DAYS[2].isoformat()), "is_st"] = True
    scope = UniverseScope(
        CURRENT_SH_SZ_STOCK_ETF_UNIVERSE,
        DAYS[-1], DAYS[0], DAYS[-1], survivorship_bias=True,
        instrument_ids=("600000.SH",),
    )

    status = reconcile_simulation_status(
        instruments=frames[MarketTable.INSTRUMENTS],
        research_bars=research,
        dense_status_bars=dense,
        stock_st_bars=stock_st,
        calendar=frames[MarketTable.CALENDAR],
        universe_scope=scope,
    )

    suspended = status.loc[status["session_date"].eq(DAYS[1].isoformat())].iloc[0]
    changed = status.loc[status["session_date"].eq(DAYS[2].isoformat())].iloc[0]
    assert suspended["suspended"] and suspended["is_st"] == False  # noqa: E712
    assert suspended["previous_close"] == research.iloc[0]["close"]
    assert changed["is_st"] == True  # noqa: E712
    assert '"suspended_st_semantics":"last_point_in_time_observation"' in suspended["field_lineage"]


def test_status_reconciliation_carries_prior_st_across_one_isolated_source_hole():
    frames = market_frames()
    raw = frames[MarketTable.DAILY_BARS].loc[
        frames[MarketTable.DAILY_BARS]["price_mode"].eq("raw")
    ].copy()
    research = raw.copy()
    research["source_observation_id"] = "obs-research"
    dense = raw.copy()
    dense["source_observation_id"] = "obs-xt"
    dense["is_st"] = None
    stock_st = raw.loc[~raw["session_date"].eq(DAYS[1].isoformat())].copy()
    stock_st["source_observation_id"] = "obs-bao"
    scope = UniverseScope(
        CURRENT_SH_SZ_STOCK_ETF_UNIVERSE,
        DAYS[-1], DAYS[0], DAYS[-1], survivorship_bias=True,
        instrument_ids=("600000.SH",),
    )

    status = reconcile_simulation_status(
        instruments=frames[MarketTable.INSTRUMENTS],
        research_bars=research,
        dense_status_bars=dense,
        stock_st_bars=stock_st,
        calendar=frames[MarketTable.CALENDAR],
        universe_scope=scope,
    )

    carried = status.loc[status["session_date"].eq(DAYS[1].isoformat())].iloc[0]
    assert carried["is_st"] == False  # noqa: E712
    assert not carried["suspended"]
    assert '"st_observation_id":"obs-bao"' in carried["field_lineage"]


def test_baostock_suspension_removes_only_zero_volume_price_placeholder():
    frames = market_frames()
    raw = frames[MarketTable.DAILY_BARS].loc[
        frames[MarketTable.DAILY_BARS]["price_mode"].eq("raw")
    ].copy()
    research = raw.copy()
    research["source_observation_id"] = "obs-research"
    placeholder = research["session_date"].eq(DAYS[1].isoformat())
    research.loc[placeholder, "volume"] = 0
    research.loc[placeholder, ["open", "high", "low", "close"]] = research.loc[
        placeholder, "previous_close"
    ].iloc[0]
    dense = raw.copy()
    dense["source_observation_id"] = "obs-xt"
    dense["is_st"] = None
    stock_st = raw.copy()
    stock_st["source_observation_id"] = "obs-bao"
    stock_st.loc[
        stock_st["session_date"].isin((DAYS[1].isoformat(), DAYS[2].isoformat())),
        "suspended",
    ] = True
    scope = UniverseScope(
        CURRENT_SH_SZ_STOCK_ETF_UNIVERSE,
        DAYS[-1], DAYS[0], DAYS[-1], survivorship_bias=True,
        instrument_ids=("600000.SH",),
    )

    status = reconcile_simulation_status(
        instruments=frames[MarketTable.INSTRUMENTS],
        research_bars=research,
        dense_status_bars=dense,
        stock_st_bars=stock_st,
        calendar=frames[MarketTable.CALENDAR],
        universe_scope=scope,
    )
    bars = build_dense_simulation_bars(
        instruments=frames[MarketTable.INSTRUMENTS],
        research_bars=research,
        status_bars=status,
        calendar=frames[MarketTable.CALENDAR],
        universe_scope=scope,
    )

    removed = bars.loc[bars["session_date"].eq(DAYS[1].isoformat())].iloc[0]
    positive_volume = bars.loc[bars["session_date"].eq(DAYS[2].isoformat())].iloc[0]
    assert removed["suspended"] and removed["volume"] == 0
    assert removed[["open", "high", "low", "close"]].isna().all()
    assert "baostock_suspended_plus_zero_volume_price_placeholder" in status.loc[
        status["session_date"].eq(DAYS[1].isoformat()), "field_lineage"
    ].iloc[0]
    assert not positive_volume["suspended"]
    assert positive_volume["volume"] > 0


def test_status_collection_is_shardable_validated_and_resumable(tmp_path):
    frames = market_frames()
    observed_at = datetime(2026, 7, 17, tzinfo=timezone.utc)
    warehouse = MarketDataWarehouse(tmp_path / "market")
    source = warehouse.record_observation(ObservationPayload(
        "canonical-test",
        observed_at,
        ProviderRequest(
            ProviderCapability.CANONICAL_RECONCILIATION,
            DAYS[0], DAYS[-1], ("600000.SH",),
        ),
        {
            MarketTable.INSTRUMENTS: frames[MarketTable.INSTRUMENTS],
            MarketTable.DAILY_BARS: frames[MarketTable.DAILY_BARS].loc[
                frames[MarketTable.DAILY_BARS]["price_mode"].eq("raw")
            ],
        },
        (
            CoverageClaim(MarketTable.INSTRUMENTS, True, instrument_ids=("600000.SH",)),
            CoverageClaim(
                MarketTable.DAILY_BARS, True, DAYS[0], DAYS[-1], ("600000.SH",),
            ),
        ),
        {
            "kind": "field_level_reconciliation",
            "reconciliation_ready": True,
        },
    ))
    scope = UniverseScope(
        CURRENT_SH_SZ_STOCK_ETF_UNIVERSE,
        DAYS[-1], DAYS[0], DAYS[-1], survivorship_bias=True,
        instrument_ids=("600000.SH",),
    )
    research = warehouse.build_snapshot(SnapshotPlan(
        (
            SourceSlice(source.observation_id, MarketTable.INSTRUMENTS, "fixture"),
            SourceSlice(source.observation_id, MarketTable.DAILY_BARS, "fixture"),
        ),
        "status collector research fixture",
        readiness=ReadinessProfile.RESEARCH_PRICE,
        universe_scope=scope,
    ))
    calendar_frame = pd.concat((
        frames[MarketTable.CALENDAR],
        frames[MarketTable.CALENDAR].assign(exchange="SZ"),
    ), ignore_index=True)
    calendar = warehouse.record_observation(ObservationPayload(
        "canonical-calendar-test",
        observed_at,
        ProviderRequest(ProviderCapability.CANONICAL_RECONCILIATION, DAYS[0], DAYS[-1]),
        {MarketTable.CALENDAR: calendar_frame},
        (CoverageClaim(MarketTable.CALENDAR, True, DAYS[0], DAYS[-1]),),
        {
            "kind": "field_level_reconciliation",
            "reconciliation_ready": True,
            "calendar_quality": {"validated": True},
        },
    ))

    class StatusProvider:
        name = "baostock"
        capabilities = frozenset({ProviderCapability.DAILY_STATUS})

        def __init__(self):
            self.calls = 0

        def observe(self, request):
            self.calls += 1
            status = frames[MarketTable.DAILY_BARS].loc[
                frames[MarketTable.DAILY_BARS]["price_mode"].eq("raw")
            ].copy()
            status[["open", "high", "low", "close"]] = None
            status["volume"] = 0
            return ObservationPayload(
                self.name, observed_at, request,
                {MarketTable.DAILY_BARS: status},
                (CoverageClaim(
                    MarketTable.DAILY_BARS, True, DAYS[0], DAYS[-1], request.instrument_ids,
                ),),
                {"backend_group": "baostock"},
            )

    from fundlab.marketdata import ProviderRegistry

    provider = StatusProvider()
    registry = ProviderRegistry()
    registry.register(provider)
    collector = SimulationStatusCollector(
        warehouse, tmp_path / "reports", registry=registry,
    )
    spec = StatusCollectionSpec(research.snapshot_id, calendar.observation_id, batch_size=1)

    first = collector.collect(spec)
    second = collector.collect(spec)

    assert first.status == second.status == "complete"
    assert first.completed_instruments == 1
    assert first.observation_ids == second.observation_ids
    assert provider.calls == 1
    assert first.checkpoint.is_file() and first.report.is_file()


def test_xt_status_collection_records_dense_canonical_evidence_and_resumes(tmp_path):
    frames = market_frames(suspended_on=DAYS[1])
    observed_at = datetime(2026, 7, 17, tzinfo=timezone.utc)
    raw = frames[MarketTable.DAILY_BARS].loc[
        frames[MarketTable.DAILY_BARS]["price_mode"].eq("raw")
        & ~frames[MarketTable.DAILY_BARS]["suspended"]
    ].copy()
    warehouse = MarketDataWarehouse(tmp_path / "market")
    source = warehouse.record_observation(ObservationPayload(
        "canonical-test",
        observed_at,
        ProviderRequest(
            ProviderCapability.CANONICAL_RECONCILIATION,
            DAYS[0], DAYS[-1], ("600000.SH",),
        ),
        {
            MarketTable.INSTRUMENTS: frames[MarketTable.INSTRUMENTS],
            MarketTable.DAILY_BARS: raw,
        },
        (
            CoverageClaim(MarketTable.INSTRUMENTS, True, instrument_ids=("600000.SH",)),
            CoverageClaim(
                MarketTable.DAILY_BARS, True, DAYS[0], DAYS[-1], ("600000.SH",),
            ),
        ),
        {"kind": "field_level_reconciliation", "reconciliation_ready": True},
    ))
    scope = UniverseScope(
        CURRENT_SH_SZ_STOCK_ETF_UNIVERSE,
        DAYS[-1], DAYS[0], DAYS[-1], survivorship_bias=True,
        instrument_ids=("600000.SH",),
    )
    research = warehouse.build_snapshot(SnapshotPlan(
        (
            SourceSlice(source.observation_id, MarketTable.INSTRUMENTS, "fixture"),
            SourceSlice(source.observation_id, MarketTable.DAILY_BARS, "fixture"),
        ),
        "sparse research fixture",
        readiness=ReadinessProfile.RESEARCH_PRICE,
        universe_scope=scope,
    ))
    calendar_frame = pd.concat((
        frames[MarketTable.CALENDAR],
        frames[MarketTable.CALENDAR].assign(exchange="SZ"),
    ), ignore_index=True)
    calendar = warehouse.record_observation(ObservationPayload(
        "canonical-calendar-test",
        observed_at,
        ProviderRequest(ProviderCapability.CANONICAL_RECONCILIATION, DAYS[0], DAYS[-1]),
        {MarketTable.CALENDAR: calendar_frame},
        (CoverageClaim(MarketTable.CALENDAR, True, DAYS[0], DAYS[-1]),),
        {
            "kind": "field_level_reconciliation",
            "reconciliation_ready": True,
            "calendar_quality": {"validated": True},
        },
    ))

    class XtPresenceProvider:
        name = "xtquant"
        capabilities = frozenset({ProviderCapability.DAILY_STATUS})

        def __init__(self):
            self.calls = 0

        def observe(self, request):
            self.calls += 1
            status = raw.copy()
            status["is_st"] = None
            return ObservationPayload(
                self.name,
                observed_at,
                request,
                {MarketTable.DAILY_BARS: status},
                (CoverageClaim(
                    MarketTable.DAILY_BARS,
                    True,
                    DAYS[0], DAYS[-1], request.instrument_ids,
                ),),
                {"backend_group": "xtquant"},
            )

    from fundlab.marketdata import ProviderRegistry

    provider = XtPresenceProvider()
    registry = ProviderRegistry()
    registry.register(provider)
    collector = SimulationStatusCollector(
        warehouse, tmp_path / "reports", registry=registry,
    )
    spec = StatusCollectionSpec(
        research.snapshot_id,
        calendar.observation_id,
        provider_name="xtquant",
        batch_size=1,
    )

    first = collector.collect(spec)
    second = collector.collect(spec)

    assert first.status == second.status == "complete"
    assert first.observation_ids == second.observation_ids
    assert provider.calls == 1
    manifest = warehouse.load_observation(first.observation_ids[0])
    dense = warehouse.read_observation_table(
        manifest.observation_id, MarketTable.DAILY_BARS,
    )
    assert manifest.provider == SIMULATION_STATUS_CANONICAL_PROVIDER
    assert manifest.source_metadata["status_quality"]["inferred_suspension_count"] == 1
    assert len(dense) == len(DAYS)
    assert dense.loc[dense["session_date"].eq(DAYS[1].isoformat()), "suspended"].item()


def test_factor_evidence_collection_is_validated_and_resumable(tmp_path):
    frames = market_frames()
    observed_at = datetime(2026, 7, 17, tzinfo=timezone.utc)
    warehouse = MarketDataWarehouse(tmp_path / "market")
    source = warehouse.record_observation(ObservationPayload(
        "canonical-test",
        observed_at,
        ProviderRequest(
            ProviderCapability.CANONICAL_RECONCILIATION,
            DAYS[0], DAYS[-1], ("600000.SH",),
        ),
        {
            MarketTable.INSTRUMENTS: frames[MarketTable.INSTRUMENTS],
            MarketTable.DAILY_BARS: frames[MarketTable.DAILY_BARS].loc[
                frames[MarketTable.DAILY_BARS]["price_mode"].eq("raw")
            ],
        },
        (
            CoverageClaim(MarketTable.INSTRUMENTS, True, instrument_ids=("600000.SH",)),
            CoverageClaim(
                MarketTable.DAILY_BARS, True, DAYS[0], DAYS[-1], ("600000.SH",),
            ),
        ),
        {"kind": "field_level_reconciliation", "reconciliation_ready": True},
    ))
    scope = UniverseScope(
        CURRENT_SH_SZ_STOCK_ETF_UNIVERSE,
        DAYS[-1], DAYS[0], DAYS[-1], survivorship_bias=True,
        instrument_ids=("600000.SH",),
    )
    research = warehouse.build_snapshot(SnapshotPlan(
        (
            SourceSlice(source.observation_id, MarketTable.INSTRUMENTS, "fixture"),
            SourceSlice(source.observation_id, MarketTable.DAILY_BARS, "fixture"),
        ),
        "evidence collector research fixture",
        readiness=ReadinessProfile.RESEARCH_PRICE,
        universe_scope=scope,
    ))

    class FactorProvider:
        name = "xtquant"
        capabilities = frozenset({ProviderCapability.ADJUSTMENT_FACTORS})

        def __init__(self):
            self.calls = 0

        def observe(self, request):
            self.calls += 1
            factors = pd.DataFrame(columns=(
                "factor_id", "instrument_id", "effective_date", "known_date",
                "price_multiplier", "source_payload",
            ))
            return ObservationPayload(
                self.name, observed_at, request,
                {MarketTable.ADJUSTMENT_FACTORS: factors},
                (CoverageClaim(
                    MarketTable.ADJUSTMENT_FACTORS, True,
                    DAYS[0], DAYS[-1], request.instrument_ids,
                ),),
                {"backend_group": "xtquant"},
            )

    from fundlab.marketdata import ProviderRegistry

    provider = FactorProvider()
    registry = ProviderRegistry()
    registry.register(provider)
    collector = SimulationEvidenceCollector(
        warehouse, tmp_path / "reports", registry=registry,
    )
    spec = EvidenceCollectionSpec(research.snapshot_id, "factors", batch_size=1)

    first = collector.collect(spec)
    second = collector.collect(spec)

    assert first.status == second.status == "complete"
    assert first.observation_ids == second.observation_ids
    assert first.completed_instruments == 1
    assert provider.calls == 1


def test_etf_action_collection_accumulates_successes_and_retries_only_failures(tmp_path):
    frames = market_frames()
    observed_at = datetime(2026, 7, 17, tzinfo=timezone.utc)
    instrument_rows = []
    bar_rows = []
    for instrument_id, exchange in (("159919.SZ", "SZ"), ("510050.SH", "SH")):
        item = frames[MarketTable.INSTRUMENTS].iloc[0].copy()
        item["instrument_id"] = instrument_id
        item["local_code"] = instrument_id.split(".")[0]
        item["exchange"] = exchange
        item["asset_type"] = "etf"
        item["listed_date"] = "2012-05-28"
        item["board"] = None
        instrument_rows.append(item)
        bars = frames[MarketTable.DAILY_BARS].loc[
            frames[MarketTable.DAILY_BARS]["price_mode"].eq("raw")
        ].copy()
        bars["instrument_id"] = instrument_id
        bar_rows.append(bars)
    instruments = pd.DataFrame(instrument_rows).reset_index(drop=True)
    bars = pd.concat(bar_rows, ignore_index=True)
    warehouse = MarketDataWarehouse(tmp_path / "market")
    source = warehouse.record_observation(ObservationPayload(
        "canonical-test",
        observed_at,
        ProviderRequest(
            ProviderCapability.CANONICAL_RECONCILIATION,
            DAYS[0], DAYS[-1], ("159919.SZ", "510050.SH"),
        ),
        {MarketTable.INSTRUMENTS: instruments, MarketTable.DAILY_BARS: bars},
        (
            CoverageClaim(
                MarketTable.INSTRUMENTS,
                True,
                instrument_ids=("159919.SZ", "510050.SH"),
            ),
            CoverageClaim(
                MarketTable.DAILY_BARS,
                True,
                DAYS[0], DAYS[-1], ("159919.SZ", "510050.SH"),
            ),
        ),
        {"kind": "field_level_reconciliation", "reconciliation_ready": True},
    ))
    scope = UniverseScope(
        CURRENT_SH_SZ_STOCK_ETF_UNIVERSE,
        DAYS[-1], DAYS[0], DAYS[-1], survivorship_bias=True,
        instrument_ids=("159919.SZ", "510050.SH"),
    )
    research = warehouse.build_snapshot(SnapshotPlan(
        (
            SourceSlice(source.observation_id, MarketTable.INSTRUMENTS, "fixture"),
            SourceSlice(source.observation_id, MarketTable.DAILY_BARS, "fixture"),
        ),
        "ETF evidence recovery fixture",
        readiness=ReadinessProfile.RESEARCH_PRICE,
        universe_scope=scope,
    ))

    class PartialEtfProvider:
        name = "eastmoney-fund-public"
        capabilities = frozenset({ProviderCapability.CORPORATE_ACTIONS})

        def __init__(self):
            self.calls = []

        def observe(self, request):
            self.calls.append(request.instrument_ids)
            completed = request.instrument_ids[:1]
            failed = request.instrument_ids[1:]
            actions = pd.DataFrame(columns=(
                "action_id", "instrument_id", "action_type", "known_date",
                "record_date", "ex_date", "pay_date", "listing_date",
                "cash_per_share", "share_ratio", "rights_price",
                "quantity_multiplier", "source_payload",
            ))
            return ObservationPayload(
                self.name,
                observed_at,
                request,
                {MarketTable.CORPORATE_ACTIONS: actions},
                (CoverageClaim(
                    MarketTable.CORPORATE_ACTIONS,
                    not failed,
                    DAYS[0], DAYS[-1], request.instrument_ids,
                ),),
                    {
                        "parser_policy": EASTMONEY_ETF_ACTION_POLICY,
                        "response_sha256": {
                        instrument_id: {"fund_archive_page": f"hash-{instrument_id}"}
                        for instrument_id in completed
                    },
                    "request_errors": {
                        instrument_id: "temporary disconnect" for instrument_id in failed
                    },
                    "invalid_lifecycle": {},
                    "known_pending_after_cutoff": {},
                },
            )

    from fundlab.marketdata import ProviderRegistry

    provider = PartialEtfProvider()
    registry = ProviderRegistry()
    registry.register(provider)
    collector = SimulationEvidenceCollector(
        warehouse, tmp_path / "reports", registry=registry,
    )
    spec = EvidenceCollectionSpec(research.snapshot_id, "etf-actions", batch_size=2)

    first = collector.collect(spec)
    second = collector.collect(spec)

    assert first.status == second.status == "complete"
    assert provider.calls == [("159919.SZ", "510050.SH"), ("510050.SH",)]
    assert first.observation_ids == second.observation_ids
    manifest = warehouse.load_observation(first.observation_ids[0])
    assert manifest.provider == ETF_ACTION_CANONICAL_PROVIDER
    assert set(manifest.source_metadata["per_instrument_evidence"]) == {
        "159919.SZ", "510050.SH",
    }

    class PersistentlyPartialEtfProvider(PartialEtfProvider):
        def observe(self, request):
            self.calls.append(request.instrument_ids)
            completed = tuple(
                item for item in request.instrument_ids if item != "510050.SH"
            )
            failed = tuple(
                item for item in request.instrument_ids if item == "510050.SH"
            )
            actions = pd.DataFrame(columns=(
                "action_id", "instrument_id", "action_type", "known_date",
                "record_date", "ex_date", "pay_date", "listing_date",
                "cash_per_share", "share_ratio", "rights_price",
                "quantity_multiplier", "source_payload",
            ))
            return ObservationPayload(
                self.name,
                observed_at,
                request,
                {MarketTable.CORPORATE_ACTIONS: actions},
                (CoverageClaim(
                    MarketTable.CORPORATE_ACTIONS,
                    not failed,
                    DAYS[0], DAYS[-1], request.instrument_ids,
                ),),
                {
                    "parser_policy": EASTMONEY_ETF_ACTION_POLICY,
                    "response_sha256": {
                        instrument_id: {"fund_archive_page": f"hash-{instrument_id}"}
                        for instrument_id in completed
                    },
                    "request_errors": {
                        instrument_id: "temporary disconnect" for instrument_id in failed
                    },
                    "invalid_lifecycle": {},
                    "known_pending_after_cutoff": {},
                },
            )

    partial_provider = PersistentlyPartialEtfProvider()
    partial_registry = ProviderRegistry()
    partial_registry.register(partial_provider)
    incomplete = SimulationEvidenceCollector(
        warehouse, tmp_path / "partial-reports", registry=partial_registry,
    ).collect(EvidenceCollectionSpec(
        research.snapshot_id, "etf-actions", batch_size=2, refresh=True,
    ))

    assert incomplete.status == "incomplete"
    assert incomplete.completed_instruments == 1
    assert incomplete.unresolved_instrument_ids == ("510050.SH",)
    partial_manifest = warehouse.load_observation(incomplete.observation_ids[0])
    assert partial_manifest.request.instrument_ids == ("159919.SZ",)


def _stock_action_increment_fixture(
    tmp_path,
    *,
    instrument_ids: tuple[str, ...] = ("600000.SH", "600001.SH"),
    predecessor_actions: pd.DataFrame | None = None,
):
    """Create contiguous research snapshots for the announcement-index path."""

    frames = market_frames()
    observed_at = datetime(2026, 7, 17, tzinfo=timezone.utc)
    instrument_rows = []
    bar_rows = []
    raw = frames[MarketTable.DAILY_BARS].loc[
        frames[MarketTable.DAILY_BARS]["price_mode"].eq("raw")
    ]
    for index, instrument_id in enumerate(instrument_ids):
        item = frames[MarketTable.INSTRUMENTS].iloc[0].copy()
        item["instrument_id"] = instrument_id
        item["local_code"] = instrument_id.split(".")[0]
        item["name"] = f"Fixture Stock {index}"
        instrument_rows.append(item)
        bars = raw.copy()
        bars["instrument_id"] = instrument_id
        bar_rows.append(bars)
    instruments = pd.DataFrame(instrument_rows).reset_index(drop=True)
    bars = pd.concat(bar_rows, ignore_index=True)
    actions = predecessor_actions if predecessor_actions is not None else pd.DataFrame(
        columns=(
            "action_id", "instrument_id", "action_type", "known_date",
            "record_date", "ex_date", "pay_date", "listing_date",
            "cash_per_share", "share_ratio", "rights_price",
            "quantity_multiplier", "source_payload",
        )
    )
    warehouse = MarketDataWarehouse(tmp_path / "market")
    source = warehouse.record_observation(ObservationPayload(
        "canonical-stock-action-fixture",
        observed_at,
        ProviderRequest(
            ProviderCapability.CANONICAL_RECONCILIATION,
            DAYS[0], DAYS[-1], instrument_ids,
        ),
        {
            MarketTable.INSTRUMENTS: instruments,
            MarketTable.DAILY_BARS: bars,
            MarketTable.CORPORATE_ACTIONS: actions,
        },
        (
            CoverageClaim(MarketTable.INSTRUMENTS, True, instrument_ids=instrument_ids),
            CoverageClaim(
                MarketTable.DAILY_BARS, True, DAYS[0], DAYS[-1], instrument_ids,
            ),
            CoverageClaim(
                MarketTable.CORPORATE_ACTIONS, True, DAYS[0], DAYS[-1], instrument_ids,
            ),
        ),
        {"kind": "field_level_reconciliation", "reconciliation_ready": True},
    ))
    predecessor_scope = UniverseScope(
        CURRENT_SH_SZ_STOCK_ETF_UNIVERSE,
        DAYS[1], DAYS[0], DAYS[1], survivorship_bias=True,
        instrument_ids=instrument_ids,
    )
    predecessor = warehouse.build_snapshot(SnapshotPlan(
        (
            SourceSlice(source.observation_id, MarketTable.INSTRUMENTS, "fixture"),
            SourceSlice(
                source.observation_id, MarketTable.DAILY_BARS, "fixture",
                start_date=DAYS[0], end_date=DAYS[1],
            ),
            SourceSlice(
                source.observation_id, MarketTable.CORPORATE_ACTIONS, "fixture",
                start_date=DAYS[0], end_date=DAYS[1],
            ),
        ),
        "stock-action predecessor fixture",
        readiness=ReadinessProfile.RESEARCH_PRICE,
        universe_scope=predecessor_scope,
    ))
    scope = UniverseScope(
        CURRENT_SH_SZ_STOCK_ETF_UNIVERSE,
        DAYS[-1], DAYS[2], DAYS[-1], survivorship_bias=True,
        instrument_ids=instrument_ids,
    )
    current = warehouse.build_snapshot(SnapshotPlan(
        (
            SourceSlice(source.observation_id, MarketTable.INSTRUMENTS, "fixture"),
            SourceSlice(
                source.observation_id, MarketTable.DAILY_BARS, "fixture",
                start_date=DAYS[2], end_date=DAYS[-1],
            ),
        ),
        "stock-action increment fixture",
        readiness=ReadinessProfile.RESEARCH_PRICE,
        universe_scope=scope,
    ))
    return warehouse, predecessor, current, instruments, observed_at


def _stock_action_row(
    instrument_id: str,
    *,
    ex_date: date = DAYS[-1],
    cash_per_share: float = 0.1,
) -> dict[str, object]:
    return {
        "action_id": f"action-{instrument_id}-{ex_date.isoformat()}",
        "instrument_id": instrument_id,
        "action_type": "cash_dividend",
        "known_date": DAYS[2].isoformat(),
        "record_date": (ex_date if ex_date <= DAYS[2] else DAYS[2]).isoformat(),
        "ex_date": ex_date.isoformat(),
        "pay_date": ex_date.isoformat(),
        "listing_date": None,
        "cash_per_share": cash_per_share,
        "share_ratio": None,
        "rights_price": None,
        "quantity_multiplier": None,
        "source_payload": f"payload-{instrument_id}-{cash_per_share}",
    }


def _announcement_scan(*records: CninfoAnnouncementRecord) -> CninfoAnnouncementScan:
    return CninfoAnnouncementScan(
        policy_version="fixture-v1",
        start_date=DAYS[1].isoformat(),
        end_date=DAYS[-1].isoformat(),
        categories=(
            "category_qyfpxzcs_szsh",
            "category_pg_szsh",
            "category_bcgz_szsh",
        ),
        page_evidence=(),
        records=records,
        affected_instrument_ids=tuple(sorted({item.instrument_id for item in records})),
        complete=True,
    )


class _AnnouncementTargetedCninfoProvider:
    name = "cninfo-public"
    capabilities = frozenset({ProviderCapability.CORPORATE_ACTIONS})

    def __init__(self, observed_at, scan, actions_by_instrument):
        self.observed_at = observed_at
        self.scan = scan
        self.actions_by_instrument = actions_by_instrument
        self.scan_calls = 0
        self.observe_calls: list[tuple[str, ...]] = []

    def scan_announcements(self, start_date, end_date):
        self.scan_calls += 1
        assert start_date.isoformat() == self.scan.start_date
        assert end_date.isoformat() == self.scan.end_date
        return self.scan

    def observe(self, request):
        self.observe_calls.append(request.instrument_ids)
        rows = [
            row
            for instrument_id in request.instrument_ids
            for row in self.actions_by_instrument.get(instrument_id, ())
        ]
        columns = (
            "action_id", "instrument_id", "action_type", "known_date",
            "record_date", "ex_date", "pay_date", "listing_date",
            "cash_per_share", "share_ratio", "rights_price",
            "quantity_multiplier", "source_payload",
        )
        return ObservationPayload(
            self.name,
            self.observed_at,
            request,
            {MarketTable.CORPORATE_ACTIONS: pd.DataFrame(rows, columns=columns)},
            (CoverageClaim(
                MarketTable.CORPORATE_ACTIONS,
                True,
                request.start_date,
                request.end_date,
                request.instrument_ids,
            ),),
            {
                "response_sha256": {
                    instrument_id: f"hash-{instrument_id}"
                    for instrument_id in request.instrument_ids
                },
                "request_errors": {},
                "invalid_lifecycle": {},
                "known_pending_after_cutoff": {},
            },
        )


def _stock_action_collector(warehouse, tmp_path, provider):
    from fundlab.marketdata import ProviderRegistry

    registry = ProviderRegistry()
    registry.register(provider)
    return SimulationEvidenceCollector(warehouse, tmp_path / "reports", registry=registry)


def test_stock_action_empty_announcement_scan_covers_all_stocks_without_detail_calls(tmp_path):
    warehouse, predecessor, current, instruments, observed_at = _stock_action_increment_fixture(
        tmp_path,
    )
    provider = _AnnouncementTargetedCninfoProvider(
        observed_at, _announcement_scan(), {},
    )
    result = _stock_action_collector(warehouse, tmp_path, provider).collect(
        EvidenceCollectionSpec(
            current.snapshot_id, "stock-actions",
            predecessor_snapshot_id=predecessor.snapshot_id,
        ),
    )

    assert result.status == "complete"
    assert provider.scan_calls == 1
    assert provider.observe_calls == []
    manifest = warehouse.load_observation(result.observation_ids[0])
    assert manifest.provider == STOCK_ACTION_CANONICAL_PROVIDER
    assert manifest.request.instrument_ids == tuple(instruments["instrument_id"])


def test_stock_action_scan_targets_only_affected_stocks_and_keeps_current_actions(tmp_path):
    instrument_ids = ("600000.SH", "600001.SH", "600002.SH")
    warehouse, predecessor, current, _, observed_at = _stock_action_increment_fixture(
        tmp_path, instrument_ids=instrument_ids,
    )
    targets = instrument_ids[:2]
    provider = _AnnouncementTargetedCninfoProvider(
        observed_at,
        _announcement_scan(*(
            CninfoAnnouncementRecord(
                f"notice-{instrument_id}", instrument_id, "2026-07-15T08:00:00+08:00",
                "category_qyfpxzcs_szsh", "Dividend notice", "/notice.pdf",
            )
            for instrument_id in targets
        )),
        {instrument_id: (_stock_action_row(instrument_id),) for instrument_id in targets},
    )
    result = _stock_action_collector(warehouse, tmp_path, provider).collect(
        EvidenceCollectionSpec(
            current.snapshot_id, "stock-actions",
            predecessor_snapshot_id=predecessor.snapshot_id,
        ),
    )

    assert result.status == "complete"
    assert provider.observe_calls == [targets]
    canonical = warehouse.read_observation_table(
        result.observation_ids[0], MarketTable.CORPORATE_ACTIONS,
    )
    assert set(canonical["instrument_id"]) == set(targets)
    assert set(canonical["ex_date"].astype(str)) == {DAYS[-1].isoformat()}


def test_stock_action_actionable_announcement_with_empty_detail_is_exact_pending(tmp_path):
    warehouse, predecessor, current, _, observed_at = _stock_action_increment_fixture(tmp_path)
    target = "600000.SH"
    provider = _AnnouncementTargetedCninfoProvider(
        observed_at,
        _announcement_scan(CninfoAnnouncementRecord(
            "notice-pending", target, "2026-07-15T08:00:00+08:00",
            "category_qyfpxzcs_szsh", "Dividend notice", "/notice.pdf",
        )),
        {target: ()},
    )
    result = _stock_action_collector(warehouse, tmp_path, provider).collect(
        EvidenceCollectionSpec(
            current.snapshot_id, "stock-actions",
            predecessor_snapshot_id=predecessor.snapshot_id,
        ),
    )

    assert result.status == "incomplete"
    assert result.unresolved_instrument_ids == (target,)
    assert any("PendingAnnouncement:structured lifecycle not available for notice-pending" in item
               for item in result.blockers)
    assert result.observation_ids


def test_stock_action_persisted_pending_is_targeted_without_a_new_announcement(tmp_path):
    warehouse, predecessor, current, _, observed_at = _stock_action_increment_fixture(tmp_path)
    target = "600000.SH"
    pending_path = (
        warehouse.root / "indexes" / "cninfo-stock-actions" / "pending.json"
    )
    pending_path.parent.mkdir(parents=True, exist_ok=True)
    pending_path.write_text(json.dumps({
        "schema_version": 1,
        "instruments": {
            target: {
                "first_seen_date": DAYS[2].isoformat(),
                "last_attempt_date": DAYS[2].isoformat(),
                "state": "awaiting_structured_detail",
                "announcements": ({
                    "announcement_id": "notice-from-prior-run",
                    "instrument_id": target,
                    "announcement_time": "2026-07-14T08:00:00Z",
                    "category": "category_qyfpxzcs_szsh",
                    "title": "Dividend notice",
                    "document_url": "/notice.pdf",
                },),
            },
        },
    }), encoding="utf-8")
    provider = _AnnouncementTargetedCninfoProvider(
        observed_at, _announcement_scan(),
        {target: (_stock_action_row(target),)},
    )

    result = _stock_action_collector(warehouse, tmp_path, provider).collect(
        EvidenceCollectionSpec(
            current.snapshot_id, "stock-actions",
            predecessor_snapshot_id=predecessor.snapshot_id,
        ),
    )

    assert result.status == "complete"
    assert provider.observe_calls == [(target,)]
    assert json.loads(pending_path.read_text(encoding="utf-8"))["instruments"] == {}


def test_stock_action_historical_difference_blocks_without_canonical_observation(tmp_path):
    target = "600000.SH"
    predecessor_actions = pd.DataFrame([
        _stock_action_row(target, ex_date=DAYS[1], cash_per_share=0.1),
    ])
    warehouse, predecessor, current, _, observed_at = _stock_action_increment_fixture(
        tmp_path, predecessor_actions=predecessor_actions,
    )
    provider = _AnnouncementTargetedCninfoProvider(
        observed_at,
        _announcement_scan(CninfoAnnouncementRecord(
            "notice-correction", target, "2026-07-15T08:00:00+08:00",
            "category_qyfpxzcs_szsh", "Correction notice", "/correction.pdf",
        )),
        {target: (_stock_action_row(target, ex_date=DAYS[1], cash_per_share=0.2),)},
    )
    result = _stock_action_collector(warehouse, tmp_path, provider).collect(
        EvidenceCollectionSpec(
            current.snapshot_id, "stock-actions",
            predecessor_snapshot_id=predecessor.snapshot_id,
        ),
    )

    assert result.status == "incomplete"
    assert result.observation_ids == ()
    assert any(item.startswith("HistoricalActionCorrectionError:") for item in result.blockers)
    assert warehouse.observations(provider=STOCK_ACTION_CANONICAL_PROVIDER) == ()


def test_stock_action_historical_ex_date_move_reports_removed_and_added_keys(tmp_path):
    target = "600000.SH"
    old_ex_date = DAYS[0]
    corrected_ex_date = DAYS[1]
    warehouse, predecessor, current, _, observed_at = _stock_action_increment_fixture(
        tmp_path,
        predecessor_actions=pd.DataFrame([
            _stock_action_row(target, ex_date=old_ex_date),
        ]),
    )
    provider = _AnnouncementTargetedCninfoProvider(
        observed_at,
        _announcement_scan(CninfoAnnouncementRecord(
            "notice-ex-date-correction", target, "2026-07-15T08:00:00+08:00",
            "category_bcgz_szsh", "Correction notice", "/correction.pdf",
        )),
        {target: (_stock_action_row(target, ex_date=corrected_ex_date),)},
    )

    result = _stock_action_collector(warehouse, tmp_path, provider).collect(
        EvidenceCollectionSpec(
            current.snapshot_id, "stock-actions",
            predecessor_snapshot_id=predecessor.snapshot_id,
        ),
    )

    correction = next(
        item for item in result.blockers
        if item.startswith("HistoricalActionCorrectionError:")
    )
    assert f"cash_dividend|{old_ex_date.isoformat()}" in correction
    assert f"cash_dividend|{corrected_ex_date.isoformat()}" in correction
    assert result.observation_ids == ()


def test_stock_action_same_scope_reuses_announcement_scan_and_canonical_observation(tmp_path):
    warehouse, predecessor, current, _, observed_at = _stock_action_increment_fixture(tmp_path)
    target = "600000.SH"
    provider = _AnnouncementTargetedCninfoProvider(
        observed_at,
        _announcement_scan(CninfoAnnouncementRecord(
            "notice-reuse", target, "2026-07-15T08:00:00+08:00",
            "category_qyfpxzcs_szsh", "Dividend notice", "/notice.pdf",
        )),
        {target: (_stock_action_row(target),)},
    )
    collector = _stock_action_collector(warehouse, tmp_path, provider)
    spec = EvidenceCollectionSpec(
        current.snapshot_id, "stock-actions",
        predecessor_snapshot_id=predecessor.snapshot_id,
    )

    first = collector.collect(spec)
    second = collector.collect(spec)

    assert first.status == second.status == "complete"
    assert first.observation_ids == second.observation_ids
    assert provider.scan_calls == 1
    assert provider.observe_calls == [(target,)]
