from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
import json
from pathlib import Path
import subprocess
import sys
from threading import Barrier, Event, Lock
from types import SimpleNamespace

import pandas as pd
import pytest

import fundlab.marketdata.history as history_module
from fundlab.common.canonical import stable_digest
from fundlab.marketdata import (
    CanonicalMarketData,
    CoverageClaim,
    CURRENT_SH_SZ_STOCK_ETF_UNIVERSE,
    HistoryBuildSpec,
    HistoryDatabaseBuilder,
    IntegrityError,
    MarketDataWarehouse,
    MarketTable,
    ObservationError,
    ObservationPayload,
    PriceMode,
    ProviderCapability,
    ProviderSelectionError,
    ProviderRegistry,
    ProviderRequest,
    ReadinessProfile,
    SourceConflictError,
    UniverseScope,
    compose_history_snapshot,
    derive_current_research_snapshot,
)
from fundlab.marketdata.history import _exclusive_build_lock
from fundlab.marketdata.schema import empty_table
from tests.canonical.exchange_fixtures import (
    OFFICIAL_COMPONENT_ENDPOINTS,
    OFFICIAL_COMPONENT_SCOPES,
    with_component_closure,
)


START = date(2026, 7, 13)
END = date(2026, 7, 14)


def _master() -> pd.DataFrame:
    return pd.DataFrame([
        {
            "instrument_id": "510050.SH", "exchange": "SH", "local_code": "510050",
            "asset_type": "etf", "name": "ETF", "currency": "CNY",
            "listed_date": "2005-02-23", "delisted_date": None, "board": "main",
            "buy_lot": 100, "price_tick": 0.001, "sell_delay_sessions": 1,
            "price_limit_ratio": None, "source_payload": None,
        },
        {
            "instrument_id": "600000.SH", "exchange": "SH", "local_code": "600000",
            "asset_type": "stock", "name": "Bank", "currency": "CNY",
            "listed_date": "1999-11-10", "delisted_date": None, "board": "main",
            "buy_lot": 100, "price_tick": 0.01, "sell_delay_sessions": 1,
            "price_limit_ratio": None, "source_payload": None,
        },
    ])


def _bars(provider: str, requested: tuple[str, ...]) -> pd.DataFrame:
    rows = []
    supported = requested if provider == "baostock" else tuple(
        item for item in requested if item == "600000.SH"
    )
    for instrument_id in supported:
        for index, day in enumerate((START, END)):
            close = 10.0 + index
            rows.append({
                "instrument_id": instrument_id,
                "session_date": day.isoformat(),
                "price_mode": "raw",
                "open": close,
                "high": close + 0.2,
                "low": close - 0.2,
                "close": close,
                "volume": 1_000_000 + (100 if provider == "tickflow" else 0),
                "amount": close * 1_000_000,
                "suspended": None if provider == "tickflow" else False,
                "price_limit_state": None if provider == "tickflow" else "unknown",
                "previous_close": close - 0.5,
                "limit_up": None,
                "limit_down": None,
                "source_payload": None,
            })
    return pd.DataFrame(rows)


_OFFICIAL_ENDPOINTS = (
    "sse-main-stock-list",
    "sse-star-stock-list",
    "szse-a-stock-list",
    "sse-etf-scale-list",
    "sse-current-full-etf-list",
    "szse-etf-scale-daily",
    "szse-current-etf-list",
)


def _official_master(*, new_listed_date: date = START) -> pd.DataFrame:
    base = _master().copy()
    new_listing = base.loc[base["instrument_id"].eq("600000.SH")].copy()
    new_listing.loc[:, "instrument_id"] = "688825.SH"
    new_listing.loc[:, "local_code"] = "688825"
    new_listing.loc[:, "name"] = "New STAR"
    new_listing.loc[:, "listed_date"] = new_listed_date.isoformat()
    new_listing.loc[:, "board"] = "star"
    sz_stock = base.loc[base["instrument_id"].eq("600000.SH")].copy()
    sz_stock.loc[:, "instrument_id"] = "000001.SZ"
    sz_stock.loc[:, "exchange"] = "SZ"
    sz_stock.loc[:, "local_code"] = "000001"
    sz_stock.loc[:, "name"] = "SZ Bank"
    sz_etf = base.loc[base["instrument_id"].eq("510050.SH")].copy()
    sz_etf.loc[:, "instrument_id"] = "159001.SZ"
    sz_etf.loc[:, "exchange"] = "SZ"
    sz_etf.loc[:, "local_code"] = "159001"
    sz_etf.loc[:, "name"] = "SZ ETF"
    return pd.concat((base, new_listing, sz_stock, sz_etf), ignore_index=True)


def _record_official_master(
    warehouse: MarketDataWarehouse,
    frame: pd.DataFrame,
    *,
    claim_ids: tuple[str, ...] | None = None,
):
    endpoint_counts = {
        "sse-main-stock-list": 1,
        "sse-star-stock-list": 1,
        "szse-a-stock-list": 1,
        "sse-etf-scale-list": 1,
        "sse-current-full-etf-list": 1,
        "szse-etf-scale-daily": 1,
        "szse-current-etf-list": 1,
    }
    return warehouse.record_observation(ObservationPayload(
        "exchange-public",
        datetime(2026, 7, 18, 0, 3, tzinfo=timezone.utc),
        ProviderRequest(
            ProviderCapability.INSTRUMENTS,
            parameters={
                "exchanges": ("SH", "SZ"),
                "asset_types": ("stock", "etf"),
                "as_of_date": END.isoformat(),
            },
        ),
        {MarketTable.INSTRUMENTS: frame},
        (CoverageClaim(
            MarketTable.INSTRUMENTS,
            True,
            instrument_ids=(
                tuple(map(str, frame["instrument_id"]))
                if claim_ids is None else claim_ids
            ),
        ),),
        {
            "backend_group": "exchange-public",
            "as_of_date": END.isoformat(),
            "requested_scope": {
                "exchanges": ("SH", "SZ"),
                "asset_types": ("stock", "etf"),
            },
            "endpoint_counts": endpoint_counts,
            "response_sha256": {
                endpoint: f"sha256-{endpoint}" for endpoint in _OFFICIAL_ENDPOINTS
            },
        },
    ))


class _BaoProvider:
    name = "baostock"
    backend_group = "baostock"
    capabilities = frozenset({
        ProviderCapability.INSTRUMENTS,
        ProviderCapability.DAILY_BARS_RAW,
    })

    def observe(self, request: ProviderRequest) -> ObservationPayload:
        if request.capability is ProviderCapability.INSTRUMENTS:
            frame = _master()
            return ObservationPayload(
                self.name,
                datetime(2026, 7, 18, tzinfo=timezone.utc),
                request,
                {MarketTable.INSTRUMENTS: frame},
                (CoverageClaim(
                    MarketTable.INSTRUMENTS, True,
                    instrument_ids=tuple(frame["instrument_id"]),
                ),),
                {"backend_group": self.backend_group},
            )
        frame = _bars(self.name, request.instrument_ids)
        return ObservationPayload(
            self.name,
            datetime(2026, 7, 18, 0, 1, tzinfo=timezone.utc),
            request,
            {MarketTable.DAILY_BARS: frame},
            (CoverageClaim(
                MarketTable.DAILY_BARS, True, START, END, request.instrument_ids,
            ),),
            {"backend_group": self.backend_group},
        )


class _TickProvider:
    name = "tickflow"
    backend_group = "tickflow-unverified"
    capabilities = frozenset({ProviderCapability.DAILY_BARS_RAW})

    def observe(self, request: ProviderRequest) -> ObservationPayload:
        frame = _bars(self.name, request.instrument_ids)
        complete = set(frame["instrument_id"]) == set(request.instrument_ids)
        return ObservationPayload(
            self.name,
            datetime(2026, 7, 18, 0, 2, tzinfo=timezone.utc),
            request,
            {MarketTable.DAILY_BARS: frame},
            (CoverageClaim(
                MarketTable.DAILY_BARS, complete, START, END, request.instrument_ids,
            ),),
            {"backend_group": self.backend_group},
        )


class _AllTickProvider(_TickProvider):
    def observe(self, request: ProviderRequest) -> ObservationPayload:
        frame = _bars("baostock", request.instrument_ids).copy()
        frame["suspended"] = None
        frame["price_limit_state"] = None
        frame["volume"] = frame["volume"] + 100
        return ObservationPayload(
            self.name,
            datetime(2026, 7, 18, 0, 2, tzinfo=timezone.utc),
            request,
            {MarketTable.DAILY_BARS: frame},
            (CoverageClaim(
                MarketTable.DAILY_BARS, True, START, END, request.instrument_ids,
            ),),
            {"backend_group": self.backend_group},
        )


def test_history_builder_excludes_only_provider_declared_scoped_observation_error(tmp_path):
    class UnavailableTick(_AllTickProvider):
        def observe(self, request: ProviderRequest) -> ObservationPayload:
            if request.capability is ProviderCapability.DAILY_BARS_RAW:
                raise ObservationError("exact batch unavailable")
            return super().observe(request)

    registry = ProviderRegistry()
    registry.register(_BaoProvider())
    registry.register(UnavailableTick())
    warehouse = MarketDataWarehouse(tmp_path / "market")
    result = HistoryDatabaseBuilder(
        warehouse, tmp_path / "reports", registry=registry,
    ).build(HistoryBuildSpec(END, start_date=START, instrument_ids=("600000.SH",)))

    assert result.status == "incomplete"
    assert result.included_instruments == 0
    assert result.excluded_instruments == 1
    assert result.snapshot_id is None
    assert result.blockers


@pytest.mark.parametrize("error", (
    IntegrityError("observation commit integrity failure"),
    OSError("observation commit persistence failure"),
    RuntimeError("observation commit unexpected failure"),
))
def test_history_builder_propagates_durable_observation_commit_failures(
    tmp_path, monkeypatch, error,
):
    registry = ProviderRegistry()
    registry.register(_BaoProvider())
    registry.register(_AllTickProvider())
    warehouse = MarketDataWarehouse(tmp_path / "market")
    original_record = warehouse.record_observation

    def fail_daily_raw_commit(payload):
        if payload.request.capability is ProviderCapability.DAILY_BARS_RAW:
            raise error
        return original_record(payload)

    monkeypatch.setattr(warehouse, "record_observation", fail_daily_raw_commit)
    builder = HistoryDatabaseBuilder(warehouse, tmp_path / "reports", registry=registry)

    with pytest.raises(type(error), match=str(error)):
        builder.build(HistoryBuildSpec(
            END, start_date=START, instrument_ids=("600000.SH",),
        ))


def test_history_builder_propagates_unscoped_provider_selection_failure(tmp_path):
    registry = ProviderRegistry()
    registry.register(_BaoProvider())
    builder = HistoryDatabaseBuilder(
        MarketDataWarehouse(tmp_path / "market"), tmp_path / "reports", registry=registry,
    )

    with pytest.raises(ProviderSelectionError):
        builder.build(HistoryBuildSpec(
            END, start_date=START, instrument_ids=("600000.SH",),
        ))


def test_history_builder_propagates_unscoped_structural_provider_failure(tmp_path):
    class StructuralTick(_AllTickProvider):
        def observe(self, request: ProviderRequest) -> ObservationPayload:
            if request.capability is ProviderCapability.DAILY_BARS_RAW:
                raise SourceConflictError("provider returned a structural conflict")
            return super().observe(request)

    registry = ProviderRegistry()
    registry.register(_BaoProvider())
    registry.register(StructuralTick())
    builder = HistoryDatabaseBuilder(
        MarketDataWarehouse(tmp_path / "market"), tmp_path / "reports", registry=registry,
    )

    with pytest.raises(SourceConflictError, match="structural conflict"):
        builder.build(HistoryBuildSpec(
            END, start_date=START, instrument_ids=("600000.SH",),
        ))


@pytest.mark.parametrize("error", (
    IntegrityError("checkpoint observation integrity failure"),
    OSError("checkpoint observation persistence failure"),
    RuntimeError("checkpoint observation unexpected failure"),
))
def test_history_builder_propagates_checkpoint_reuse_failures(
    tmp_path, monkeypatch, error,
):
    registry = ProviderRegistry()
    registry.register(_BaoProvider())
    registry.register(_AllTickProvider())
    warehouse = MarketDataWarehouse(tmp_path / "market")
    builder = HistoryDatabaseBuilder(warehouse, tmp_path / "reports", registry=registry)
    spec = HistoryBuildSpec(END, start_date=START, instrument_ids=("600000.SH",))
    first = builder.build(spec)
    checkpoint = json.loads(first.checkpoint.read_text(encoding="utf-8"))
    canonical_id = next(iter(checkpoint["batches"].values()))["canonical_observation_id"]
    original_load = warehouse.load_observation

    def fail_checkpoint_reuse(observation_id):
        if observation_id == canonical_id:
            raise error
        return original_load(observation_id)

    monkeypatch.setattr(warehouse, "load_observation", fail_checkpoint_reuse)

    with pytest.raises(type(error), match=str(error)):
        builder.build(spec)


def test_history_builder_preserves_adjudicator_source_error_type_in_exclusion(tmp_path):
    class ConflictTick(_AllTickProvider):
        def observe(self, request: ProviderRequest) -> ObservationPayload:
            payload = super().observe(request)
            frame = payload.tables[MarketTable.DAILY_BARS].copy()
            frame.loc[:, "close"] = 12.5
            frame.loc[:, "high"] = 12.5
            return ObservationPayload(
                payload.provider,
                payload.observed_at,
                payload.request,
                {MarketTable.DAILY_BARS: frame},
                payload.coverage,
                payload.source_metadata,
            )

    class UnavailableAdjudicator:
        name = "xtquant"
        backend_group = "xtquant"
        capabilities = frozenset({ProviderCapability.DAILY_BARS_RAW})

        def observe(self, request: ProviderRequest) -> ObservationPayload:
            raise ObservationError("adjudicator unavailable")

    registry = ProviderRegistry()
    registry.register(_BaoProvider())
    registry.register(ConflictTick())
    registry.register(UnavailableAdjudicator())
    result = HistoryDatabaseBuilder(
        MarketDataWarehouse(tmp_path / "market"),
        tmp_path / "reports",
        registry=registry,
        adjudicator_provider="xtquant",
    ).build(HistoryBuildSpec(END, start_date=START, instrument_ids=("600000.SH",)))
    checkpoint = json.loads(result.checkpoint.read_text(encoding="utf-8"))
    excluded = next(iter(checkpoint["batches"].values()))["excluded"]["600000.SH"]

    assert "third_source_error:xtquant=ObservationError:adjudicator unavailable" in excluded


def _daily_partial_carry_universe(tmp_path):
    """Return an independently recorded daily partial master and its trust facade."""

    warehouse = MarketDataWarehouse(tmp_path / "market")
    target = END.isoformat()
    unavailable = {
        component: {
            "component": component,
            "scope": dict(scope),
            "endpoints": list(OFFICIAL_COMPONENT_ENDPOINTS[component]),
            "failed_endpoint": OFFICIAL_COMPONENT_ENDPOINTS[component][0],
            "error_type": "ProviderUnavailableError",
            "message": f"bounded {component} outage",
        }
        for component, scope in OFFICIAL_COMPONENT_SCOPES.items()
        if component != "sh-stock-star"
    }
    successful = {
        endpoint
        for component, endpoints in OFFICIAL_COMPONENT_ENDPOINTS.items()
        if component not in unavailable
        for endpoint in endpoints
    }
    raw_metadata = {
        "backend_group": "exchange-public",
        "as_of_date": target,
        "requested_scope": {"exchanges": ("SH", "SZ"), "asset_types": ("stock", "etf")},
        "unavailable_components": unavailable,
        "pending_onboarding": {},
        "response_sha256": {endpoint: "a" * 64 for endpoint in successful},
        "endpoint_counts": {endpoint: 1 for endpoint in successful},
        "endpoint_response_counts": {endpoint: 1 for endpoint in successful},
        "as_of_excluded_future_instrument_ids": {},
    }
    raw_frame = _official_master().loc[
        lambda value: value["instrument_id"].eq("688825.SH")
    ].reset_index(drop=True)
    raw_frame = with_component_closure(
        raw_frame, raw_metadata, tuple(sorted(unavailable)),
    )
    raw = warehouse.record_observation(ObservationPayload(
        "exchange-public",
        datetime(2026, 7, 18, 0, 3, tzinfo=timezone.utc),
        ProviderRequest(ProviderCapability.INSTRUMENTS, parameters={
            "exchanges": ("SH", "SZ"), "asset_types": ("stock", "etf"),
            "as_of_date": target,
        }),
        {MarketTable.INSTRUMENTS: raw_frame},
        (CoverageClaim(MarketTable.INSTRUMENTS, False, instrument_ids=("688825.SH",)),),
        raw_metadata,
    ))
    raw_stored = warehouse.read_observation_table(raw.observation_id, MarketTable.INSTRUMENTS)
    predecessor_frame = history_module.normalize_table(
        MarketTable.INSTRUMENTS,
        _master(),
        provider="trusted-predecessor",
        observed_at="2026-07-17T00:00:00+00:00",
        require_observation_id=False,
    )
    predecessor_id = "snap-trusted"
    observed_at = datetime(2026, 7, 18, 0, 4, tzinfo=timezone.utc)
    expected = history_module.normalize_table(
        MarketTable.INSTRUMENTS,
        pd.concat((raw_stored, predecessor_frame), ignore_index=True),
        provider=history_module.DAILY_CARRY_UNIVERSE_PROVIDER,
        observed_at=observed_at.isoformat(),
        require_observation_id=False,
    )
    canonical = warehouse.record_observation(ObservationPayload(
        history_module.DAILY_CARRY_UNIVERSE_PROVIDER,
        observed_at,
        ProviderRequest(ProviderCapability.CANONICAL_RECONCILIATION, parameters={
            "target_date": target,
            "predecessor_snapshot_id": predecessor_id,
            "official_observation_ids": (raw.observation_id,),
            "reason": "official_universe_component_unavailable",
            "pipeline": history_module.DAILY_CARRY_PIPELINE_VERSION,
        }),
        {MarketTable.INSTRUMENTS: expected},
        (CoverageClaim(
            MarketTable.INSTRUMENTS, True,
            instrument_ids=tuple(expected["instrument_id"]),
        ),),
        {
            "kind": history_module.DAILY_CARRY_UNIVERSE_KIND,
            "pipeline": history_module.DAILY_CARRY_PIPELINE_VERSION,
            "target_date": target,
            "predecessor_snapshot_id": predecessor_id,
            "input_observation_ids": (raw.observation_id,),
            "degraded_detail": {
                "unavailable_components": unavailable,
                "pending_onboarding": {},
            },
        },
    ))
    scope = UniverseScope(
        CURRENT_SH_SZ_STOCK_ETF_UNIVERSE,
        END,
        START - timedelta(days=1),
        START - timedelta(days=1),
        instrument_ids=tuple(predecessor_frame["instrument_id"]),
    )
    predecessor = SimpleNamespace(
        snapshot_id=predecessor_id,
        plan=SimpleNamespace(readiness=ReadinessProfile.SIMULATION, universe_scope=scope),
        quality=SimpleNamespace(ready=True),
        component_selections=(object(),),
    )
    facade = SimpleNamespace(
        load_observation=warehouse.load_observation,
        read_observation_table=warehouse.read_observation_table,
        current_snapshot_id=lambda: predecessor_id,
        load_snapshot=lambda snapshot_id: predecessor,
        query_loaded_snapshot_table=lambda *_args, **_kwargs: predecessor_frame.copy(),
    )
    return facade, canonical, warehouse.read_observation_table(
        canonical.observation_id, MarketTable.INSTRUMENTS,
    )


def test_explicit_history_accepts_verified_daily_partial_carry_for_successful_new_listing(tmp_path):
    warehouse, canonical, frame = _daily_partial_carry_universe(tmp_path)

    history_module._validate_explicit_history_universe(
        warehouse,
        canonical,
        frame,
        HistoryBuildSpec(END, start_date=START, instrument_ids=("688825.SH",)),
        trusted_predecessor_snapshot_id=None,
    )


@pytest.mark.parametrize("mutation", (
    "provider", "pipeline", "input", "predecessor", "merged_row", "price_tick",
))
def test_explicit_history_rejects_tampered_daily_partial_carry_provenance(tmp_path, mutation):
    warehouse, canonical, frame = _daily_partial_carry_universe(tmp_path)
    if mutation == "provider":
        canonical = replace(canonical, provider="canonical-universe-carry-forward-daily-pipeline-v1")
    elif mutation == "pipeline":
        canonical = replace(canonical, source_metadata={
            **canonical.source_metadata, "pipeline": "daily-pipeline-v1",
        })
    elif mutation == "input":
        canonical = replace(canonical, source_metadata={
            **canonical.source_metadata, "input_observation_ids": ("obs-missing",),
        })
    elif mutation == "predecessor":
        canonical = replace(canonical, source_metadata={
            **canonical.source_metadata, "predecessor_snapshot_id": "snap-other",
        })
    elif mutation == "merged_row":
        frame = frame.copy()
        frame.loc[frame["instrument_id"].eq("688825.SH"), "name"] = "forged raw row"
    else:
        frame = frame.copy()
        frame.loc[frame["instrument_id"].eq("688825.SH"), "price_tick"] += 1e-12

    with pytest.raises(ValueError):
        history_module._validate_explicit_history_universe(
            warehouse,
            canonical,
            frame,
            HistoryBuildSpec(END, start_date=START, instrument_ids=("688825.SH",)),
            trusted_predecessor_snapshot_id=None,
        )


def test_explicit_history_rejects_new_listing_only_present_in_carried_predecessor(tmp_path):
    warehouse, canonical, frame = _daily_partial_carry_universe(tmp_path)

    with pytest.raises(ValueError, match="successful official universe components"):
        history_module._validate_explicit_history_universe(
            warehouse,
            canonical,
            frame,
            HistoryBuildSpec(END, start_date=START, instrument_ids=("600000.SH",)),
            trusted_predecessor_snapshot_id=None,
        )


def test_explicit_history_rejects_daily_partial_carry_with_noncontiguous_predecessor(tmp_path):
    warehouse, canonical, frame = _daily_partial_carry_universe(tmp_path)
    predecessor = warehouse.load_snapshot("snap-trusted")
    stale_scope = replace(
        predecessor.plan.universe_scope,
        history_start=START - timedelta(days=2),
        history_end=START - timedelta(days=2),
    )
    warehouse.load_snapshot = lambda _snapshot_id: SimpleNamespace(
        snapshot_id="snap-trusted",
        plan=SimpleNamespace(
            readiness=ReadinessProfile.SIMULATION, universe_scope=stale_scope,
        ),
        quality=SimpleNamespace(ready=True),
        component_selections=(object(),),
    )

    with pytest.raises(ValueError, match="trusted simulation snapshot"):
        history_module._validate_explicit_history_universe(
            warehouse,
            canonical,
            frame,
            HistoryBuildSpec(END, start_date=START, instrument_ids=("688825.SH",)),
            trusted_predecessor_snapshot_id=None,
        )


def test_explicit_history_rejects_daily_partial_raw_backend_or_pending_overlap(tmp_path):
    warehouse, canonical, frame = _daily_partial_carry_universe(tmp_path)
    raw_id = canonical.request.parameters["official_observation_ids"][0]
    original_load = warehouse.load_observation

    def wrong_backend(observation_id):
        manifest = original_load(observation_id)
        if observation_id == raw_id:
            return replace(manifest, source_metadata={
                **manifest.source_metadata, "backend_group": "forged-backend",
            })
        return manifest

    warehouse.load_observation = wrong_backend
    with pytest.raises(ValueError, match="fixed official request"):
        history_module._validate_explicit_history_universe(
            warehouse,
            canonical,
            frame,
            HistoryBuildSpec(END, start_date=START, instrument_ids=("688825.SH",)),
            trusted_predecessor_snapshot_id=None,
        )
    raw = original_load(raw_id)
    invalid_pending = {"688825.SH": {}}
    warehouse.load_observation = lambda observation_id: (
        replace(raw, source_metadata={
            **raw.source_metadata, "pending_onboarding": invalid_pending,
        }) if observation_id == raw_id else original_load(observation_id)
    )
    canonical = replace(canonical, source_metadata={
        **canonical.source_metadata,
        "degraded_detail": {
            **canonical.source_metadata["degraded_detail"],
            "pending_onboarding": invalid_pending,
        },
    })
    with pytest.raises(ObservationError, match="Pending onboarding id was admitted"):
        history_module._validate_explicit_history_universe(
            warehouse,
            canonical,
            frame,
            HistoryBuildSpec(END, start_date=START, instrument_ids=("688825.SH",)),
            trusted_predecessor_snapshot_id=None,
        )


@pytest.mark.parametrize("mutation", ("effective_count", "lineage"))
def test_explicit_history_propagates_exchange_component_closure_failures(tmp_path, mutation):
    warehouse, canonical, frame = _daily_partial_carry_universe(tmp_path)
    raw_id = canonical.request.parameters["official_observation_ids"][0]
    original_load = warehouse.load_observation
    original_read = warehouse.read_observation_table
    if mutation == "effective_count":
        raw = original_load(raw_id)
        metadata = deepcopy(dict(raw.source_metadata))
        metadata["component_closure"]["sh-stock-star"]["endpoint_membership"][
            "sse-star-stock-list"
        ]["effective_row_count"] = 2
        warehouse.load_observation = lambda observation_id: (
            replace(raw, source_metadata=metadata)
            if observation_id == raw_id else original_load(observation_id)
        )
        match = "effective count mismatch"
    else:
        def forged_lineage(observation_id, table):
            result = original_read(observation_id, table)
            if observation_id == raw_id and table is MarketTable.INSTRUMENTS:
                result = result.copy()
                result.loc[:, "field_lineage"] = json.dumps({"endpoint": "forged"})
            return result

        warehouse.read_observation_table = forged_lineage
        match = "no authoritative successful endpoint"

    with pytest.raises(ObservationError, match=match):
        history_module._validate_explicit_history_universe(
            warehouse,
            canonical,
            frame,
            HistoryBuildSpec(END, start_date=START, instrument_ids=("688825.SH",)),
            trusted_predecessor_snapshot_id=None,
        )

def test_history_source_capture_reuses_verified_date_prefix(tmp_path):
    class RangeProvider:
        name = "range-source"
        backend_group = "range-source"
        capabilities = frozenset({ProviderCapability.DAILY_BARS_RAW})

        def __init__(self):
            self.calls = []

        def observe(self, request):
            self.calls.append(request)
            rows = []
            current = request.start_date
            while current <= request.end_date:
                rows.append({
                    "instrument_id": "600000.SH",
                    "session_date": current.isoformat(),
                    "price_mode": "raw",
                    "open": 10.0,
                    "high": 10.2,
                    "low": 9.8,
                    "close": 10.0,
                    "volume": 1_000_000,
                })
                current += timedelta(days=1)
            return ObservationPayload(
                self.name,
                datetime(2026, 7, 18, tzinfo=timezone.utc),
                request,
                {MarketTable.DAILY_BARS: pd.DataFrame(rows)},
                (CoverageClaim(
                    MarketTable.DAILY_BARS,
                    True,
                    request.start_date,
                    request.end_date,
                    request.instrument_ids,
                ),),
                {"backend_group": self.backend_group},
            )

    provider = RangeProvider()
    registry = ProviderRegistry()
    registry.register(provider)
    warehouse = MarketDataWarehouse(tmp_path / "market")
    builder = HistoryDatabaseBuilder(
        warehouse,
        tmp_path / "reports",
        registry=registry,
        source_pair=("range-source", "unused-source"),
    )
    parameters = {"batch_size": 1, "count": 10000}
    prefix_request = ProviderRequest(
        ProviderCapability.DAILY_BARS_RAW,
        START,
        END,
        ("600000.SH",),
        parameters,
    )
    prefix, reused = builder._capture_exact(
        "range-source", prefix_request, refresh=False,
    )
    assert reused is False

    extended_end = END + timedelta(days=1)
    extended_request = ProviderRequest(
        ProviderCapability.DAILY_BARS_RAW,
        START,
        extended_end,
        ("600000.SH",),
        parameters,
    )
    combined, reused = builder._capture_exact(
        "range-source", extended_request, refresh=False,
    )

    assert reused is False
    assert [(item.start_date, item.end_date) for item in provider.calls] == [
        (START, END),
        (extended_end, extended_end),
    ]
    bars = warehouse.read_observation_table(
        combined.observation_id, MarketTable.DAILY_BARS,
    )
    assert tuple(bars["session_date"].astype(str)) == (
        START.isoformat(), END.isoformat(), extended_end.isoformat(),
    )
    assert combined.source_metadata["range_composition"] == {
        "prefix_observation_id": prefix.observation_id,
        "suffix_observation_id": combined.source_metadata["range_composition"][
            "suffix_observation_id"
        ],
        "prefix_end": END.isoformat(),
        "suffix_start": extended_end.isoformat(),
    }


def test_history_source_capture_never_reuses_an_incomplete_prefix(tmp_path):
    class PartialThenCompleteProvider:
        name = "range-source"
        backend_group = "range-source"
        capabilities = frozenset({ProviderCapability.DAILY_BARS_RAW})

        def __init__(self):
            self.calls = []

        def observe(self, request):
            self.calls.append(request)
            rows = []
            current = request.start_date
            while current <= request.end_date:
                rows.append({
                    "instrument_id": "600000.SH",
                    "session_date": current.isoformat(),
                    "price_mode": "raw",
                    "open": 10.0,
                    "high": 10.2,
                    "low": 9.8,
                    "close": 10.0,
                    "volume": 1_000_000,
                })
                current += timedelta(days=1)
            return ObservationPayload(
                self.name,
                datetime(2026, 7, 18, tzinfo=timezone.utc),
                request,
                {MarketTable.DAILY_BARS: pd.DataFrame(rows)},
                (CoverageClaim(
                    MarketTable.DAILY_BARS,
                    len(self.calls) > 1,
                    request.start_date,
                    request.end_date,
                    request.instrument_ids,
                ),),
                {"backend_group": self.backend_group},
            )

    provider = PartialThenCompleteProvider()
    registry = ProviderRegistry()
    registry.register(provider)
    warehouse = MarketDataWarehouse(tmp_path / "market")
    builder = HistoryDatabaseBuilder(
        warehouse,
        tmp_path / "reports",
        registry=registry,
        source_pair=("range-source", "unused-source"),
    )
    parameters = {"batch_size": 1, "count": 10000}
    builder._capture_exact(
        "range-source",
        ProviderRequest(
            ProviderCapability.DAILY_BARS_RAW,
            START,
            END,
            ("600000.SH",),
            parameters,
        ),
        refresh=False,
    )
    extended_end = END + timedelta(days=1)
    builder._capture_exact(
        "range-source",
        ProviderRequest(
            ProviderCapability.DAILY_BARS_RAW,
            START,
            extended_end,
            ("600000.SH",),
            parameters,
        ),
        refresh=False,
    )

    assert [(item.start_date, item.end_date) for item in provider.calls] == [
        (START, END),
        (START, extended_end),
    ]


def test_history_source_capture_retries_incomplete_exact_suffix(tmp_path):
    class IncompleteSuffixProvider:
        name = "range-source"
        backend_group = "range-source"
        capabilities = frozenset({ProviderCapability.DAILY_BARS_RAW})

        def __init__(self):
            self.calls = []

        def observe(self, request):
            self.calls.append(request)
            rows = []
            current = request.start_date
            while current <= request.end_date:
                rows.append({
                    "instrument_id": "600000.SH",
                    "session_date": current.isoformat(),
                    "price_mode": "raw",
                    "open": 10.0,
                    "high": 10.2,
                    "low": 9.8,
                    "close": 10.0,
                    "volume": 1_000_000,
                })
                current += timedelta(days=1)
            complete = len(self.calls) != 2
            return ObservationPayload(
                self.name,
                datetime(2026, 7, 18, tzinfo=timezone.utc),
                request,
                {MarketTable.DAILY_BARS: pd.DataFrame(rows)},
                (CoverageClaim(
                    MarketTable.DAILY_BARS,
                    complete,
                    request.start_date,
                    request.end_date,
                    request.instrument_ids,
                ),),
                {"backend_group": self.backend_group},
            )

    provider = IncompleteSuffixProvider()
    registry = ProviderRegistry()
    registry.register(provider)
    warehouse = MarketDataWarehouse(tmp_path / "market")
    builder = HistoryDatabaseBuilder(
        warehouse,
        tmp_path / "reports",
        registry=registry,
        source_pair=("range-source", "unused-source"),
    )
    parameters = {"batch_size": 1, "count": 10000}
    prefix_request = ProviderRequest(
        ProviderCapability.DAILY_BARS_RAW,
        START,
        END,
        ("600000.SH",),
        parameters,
    )
    builder._capture_exact("range-source", prefix_request, refresh=False)
    extended_end = END + timedelta(days=1)
    extended_request = ProviderRequest(
        ProviderCapability.DAILY_BARS_RAW,
        START,
        extended_end,
        ("600000.SH",),
        parameters,
    )
    first, _ = builder._capture_exact(
        "range-source", extended_request, refresh=False,
    )
    assert not next(
        claim for claim in first.coverage if claim.table is MarketTable.DAILY_BARS
    ).complete

    recovered, reused = builder._capture_exact(
        "range-source", extended_request, refresh=False,
    )

    assert reused is False
    assert [(item.start_date, item.end_date) for item in provider.calls] == [
        (START, END),
        (extended_end, extended_end),
        (extended_end, extended_end),
    ]
    assert next(
        claim for claim in recovered.coverage if claim.table is MarketTable.DAILY_BARS
    ).complete


def test_history_source_capture_reuses_exact_universe_when_provider_later_fails(tmp_path):
    class OneShotUniverseProvider:
        name = "universe-source"
        capabilities = frozenset({ProviderCapability.INSTRUMENTS})

        def __init__(self):
            self.calls = 0

        def observe(self, request):
            self.calls += 1
            if self.calls > 1:
                raise RuntimeError("universe endpoint unavailable")
            frame = _master()
            ids = tuple(sorted(map(str, frame["instrument_id"])))
            return ObservationPayload(
                self.name,
                datetime(2026, 7, 18, tzinfo=timezone.utc),
                request,
                {MarketTable.INSTRUMENTS: frame},
                (CoverageClaim(
                    MarketTable.INSTRUMENTS,
                    True,
                    instrument_ids=ids,
                ),),
            )

    provider = OneShotUniverseProvider()
    registry = ProviderRegistry()
    registry.register(provider)
    builder = HistoryDatabaseBuilder(
        MarketDataWarehouse(tmp_path / "market"),
        tmp_path / "reports",
        registry=registry,
        source_pair=("unused-a", "unused-b"),
    )
    request = ProviderRequest(
        ProviderCapability.INSTRUMENTS,
        parameters={"as_of_date": END.isoformat()},
    )

    first, first_reused = builder._capture_exact(
        provider.name, request, refresh=False,
    )
    second, second_reused = builder._capture_exact(
        provider.name, request, refresh=False,
    )

    assert first_reused is False
    assert second_reused is True
    assert second.observation_id == first.observation_id
    assert provider.calls == 1


def test_history_builder_publishes_only_exact_two_source_scope_and_resumes(tmp_path):
    registry = ProviderRegistry()
    registry.register(_BaoProvider())
    registry.register(_TickProvider())
    warehouse = MarketDataWarehouse(tmp_path / "market")
    builder = HistoryDatabaseBuilder(
        warehouse,
        tmp_path / "reports",
        registry=registry,
    )
    spec = HistoryBuildSpec(
        END,
        start_date=START,
        instrument_ids=("510050.SH", "600000.SH"),
        exchanges=("SH",),
        batch_size=2,
        publish=True,
    )

    first = builder.build(spec)

    assert first.status == "ready_scoped"
    assert first.snapshot_ready and first.published
    assert first.included_instruments == 1
    assert first.excluded_instruments == 1
    market = CanonicalMarketData.open(
        tmp_path / "market",
        required_readiness=ReadinessProfile.RESEARCH_PRICE,
    )
    assert {item.instrument_id for item in market.instruments()} == {"600000.SH"}
    bars = market.bars(
        ("600000.SH",), START, END, price_mode=PriceMode.RAW, as_of=END,
    )
    assert bars["volume"].tolist() == [1_000_000, 1_000_000]

    repeated = builder.build(spec)

    assert repeated.snapshot_id == first.snapshot_id
    assert repeated.report == first.report


def test_history_builder_can_use_exact_exchange_master_for_new_listing(tmp_path):
    registry = ProviderRegistry()
    registry.register(_BaoProvider())
    registry.register(_AllTickProvider())
    warehouse = MarketDataWarehouse(tmp_path / "market")
    universe = _record_official_master(warehouse, _official_master())
    builder = HistoryDatabaseBuilder(
        warehouse, tmp_path / "reports", registry=registry,
    )

    result = builder.build(
        HistoryBuildSpec(
            END,
            start_date=START,
            instrument_ids=("688825.SH",),
            exchanges=("SH",),
        ),
        universe_observation_id=universe.observation_id,
    )

    assert result.status == "complete"
    assert result.included_instruments == 1
    assert result.universe_observation_id == universe.observation_id
    market = CanonicalMarketData.open(
        tmp_path / "market",
        result.snapshot_id,
        required_readiness=ReadinessProfile.RESEARCH_PRICE,
    )
    assert market.instrument("688825.SH").name == "New STAR"


def test_history_builder_rejects_partial_or_out_of_window_exchange_override(tmp_path):
    registry = ProviderRegistry()
    registry.register(_BaoProvider())
    registry.register(_AllTickProvider())
    warehouse = MarketDataWarehouse(tmp_path / "market")
    builder = HistoryDatabaseBuilder(
        warehouse, tmp_path / "reports", registry=registry,
    )
    spec = HistoryBuildSpec(
        END,
        start_date=START,
        instrument_ids=("688825.SH",),
        exchanges=("SH",),
    )

    partial = _official_master().loc[
        lambda frame: frame["instrument_id"].eq("688825.SH")
    ].copy()
    partial_observation = _record_official_master(warehouse, partial)
    with pytest.raises(ValueError, match="row counts"):
        builder.build(
            spec,
            universe_observation_id=partial_observation.observation_id,
        )

    stale_observation = _record_official_master(
        warehouse,
        _official_master(new_listed_date=START.replace(day=START.day - 1)),
    )
    with pytest.raises(ValueError, match="complete new listings"):
        builder.build(
            spec,
            universe_observation_id=stale_observation.observation_id,
        )


def test_history_builder_default_lock_identity_remains_backward_compatible(
    tmp_path, monkeypatch,
):
    registry = ProviderRegistry()
    registry.register(_BaoProvider())
    registry.register(_AllTickProvider())
    builder = HistoryDatabaseBuilder(
        MarketDataWarehouse(tmp_path / "market"),
        tmp_path / "reports",
        registry=registry,
    )
    spec = HistoryBuildSpec(
        END,
        start_date=START,
        instrument_ids=("600000.SH",),
        exchanges=("SH",),
    )
    captured: list[Path] = []

    @contextmanager
    def capture_lock(path):
        captured.append(path)
        yield

    sentinel = object()
    monkeypatch.setattr(history_module, "_exclusive_build_lock", capture_lock)
    monkeypatch.setattr(
        builder,
        "_build_locked",
        lambda received, **kwargs: sentinel,
    )

    assert builder.build(spec) is sentinel
    expected = stable_digest({
        "schema_version": history_module.HISTORY_BUILD_SCHEMA_VERSION,
        "spec": spec,
        "universe_provider": builder.universe_provider,
        "source_pair": builder.source_pair,
        "adjudicator_provider": builder.adjudicator_provider,
        "policy_version": builder.policy.version,
    })[:24]
    assert captured[0].name == f"{expected}.lock"


def test_history_shards_assemble_one_disjoint_published_cohort(tmp_path):
    registry = ProviderRegistry()
    registry.register(_BaoProvider())
    registry.register(_AllTickProvider())
    warehouse = MarketDataWarehouse(tmp_path / "market")
    builder = HistoryDatabaseBuilder(
        warehouse,
        tmp_path / "reports",
        registry=registry,
    )
    common = {
        "start_date": START,
        "instrument_ids": ("510050.SH", "600000.SH"),
        "exchanges": ("SH",),
        "batch_size": 1,
        "shard_count": 2,
    }

    first = builder.build(HistoryBuildSpec(END, shard_index=0, **common))
    second = builder.build(HistoryBuildSpec(END, shard_index=1, **common))
    assembled = builder.assemble(
        HistoryBuildSpec(END, shard_index=0, **common),
        publish=True,
    )

    assert first.cohort_id == second.cohort_id == assembled.cohort_id
    assert first.status == second.status == "ready_scoped"
    assert assembled.status == "complete"
    assert assembled.included_instruments == 2
    assert assembled.snapshot_ready and assembled.published
    market = CanonicalMarketData.open(
        tmp_path / "market",
        required_readiness=ReadinessProfile.RESEARCH_PRICE,
    )
    assert {item.instrument_id for item in market.instruments()} == {
        "510050.SH", "600000.SH",
    }
    checkpoint_payloads = [
        json.loads(item.checkpoint.read_text(encoding="utf-8"))
        for item in (first, second)
    ]
    partition_ids = tuple(
        next(iter(payload["batches"].values()))["canonical_observation_id"]
        for payload in checkpoint_payloads
    )
    composed, policy_version = compose_history_snapshot(
        warehouse,
        partition_ids,
        "direct composition test",
    )
    assert composed.quality.ready
    assert composed.quality.row_counts[MarketTable.INSTRUMENTS.value] == 2
    assert policy_version == "a-share-daily-research_price-v4"


def test_history_builder_captures_the_two_independent_sources_concurrently(tmp_path):
    barrier = Barrier(2, timeout=5)
    reached: list[str] = []

    class BarrierBao(_BaoProvider):
        def observe(self, request: ProviderRequest) -> ObservationPayload:
            if request.capability is ProviderCapability.DAILY_BARS_RAW:
                reached.append(self.name)
                barrier.wait()
            return super().observe(request)

    class BarrierTick(_AllTickProvider):
        def observe(self, request: ProviderRequest) -> ObservationPayload:
            reached.append(self.name)
            barrier.wait()
            return super().observe(request)

    registry = ProviderRegistry()
    registry.register(BarrierBao())
    registry.register(BarrierTick())
    builder = HistoryDatabaseBuilder(
        MarketDataWarehouse(tmp_path / "market"),
        tmp_path / "reports",
        registry=registry,
    )

    result = builder.build(HistoryBuildSpec(
        END,
        start_date=START,
        instrument_ids=("600000.SH",),
        exchanges=("SH",),
        batch_size=1,
    ))

    assert result.status == "complete"
    assert set(reached) == {"baostock", "tickflow"}


def test_history_builder_calls_third_source_only_for_conflicts_and_uses_two_to_one_cluster(
    tmp_path,
):
    calls: list[tuple[str, ...]] = []

    class ConflictTick(_AllTickProvider):
        def observe(self, request: ProviderRequest) -> ObservationPayload:
            payload = super().observe(request)
            frame = payload.tables[MarketTable.DAILY_BARS].copy()
            conflicted = (
                frame["instrument_id"].eq("600000.SH")
                & frame["session_date"].eq(END.isoformat())
            )
            frame.loc[conflicted, "close"] = 12.5
            frame.loc[conflicted, "high"] = 12.5
            return ObservationPayload(
                payload.provider, payload.observed_at, payload.request,
                {MarketTable.DAILY_BARS: frame}, payload.coverage,
                payload.source_metadata,
            )

    class ThirdProvider(_AllTickProvider):
        name = "xtquant"
        backend_group = "xtquant"

        def observe(self, request: ProviderRequest) -> ObservationPayload:
            calls.append(request.instrument_ids)
            payload = _BaoProvider().observe(request)
            return ObservationPayload(
                self.name, payload.observed_at, request, payload.tables,
                payload.coverage, {"backend_group": self.backend_group},
            )

    registry = ProviderRegistry()
    registry.register(_BaoProvider())
    registry.register(ConflictTick())
    registry.register(ThirdProvider())
    warehouse = MarketDataWarehouse(tmp_path / "market")
    result = HistoryDatabaseBuilder(
        warehouse,
        tmp_path / "reports",
        registry=registry,
        adjudicator_provider="xtquant",
    ).build(HistoryBuildSpec(
        END,
        start_date=START,
        instrument_ids=("510050.SH", "600000.SH"),
        exchanges=("SH",),
        batch_size=2,
        publish=True,
    ))

    assert result.status == "complete"
    assert calls == [("600000.SH",)]
    market = CanonicalMarketData.open(
        tmp_path / "market", required_readiness=ReadinessProfile.RESEARCH_PRICE,
    )
    bars = market.bars(
        ("510050.SH", "600000.SH"), START, END, price_mode=PriceMode.RAW, as_of=END,
    )
    assert bars.loc[
        bars["instrument_id"].eq("600000.SH")
        & bars["session_date"].eq(END.isoformat()),
        "close",
    ].item() == 11.0
    assert set(bars["instrument_id"]) == {"510050.SH", "600000.SH"}
    payloads = [
        (instrument_id, json.loads(payload))
        for instrument_id, payload in zip(
            bars["instrument_id"], bars["source_payload"], strict=True,
        )
    ]
    baseline = [payload for instrument_id, payload in payloads if instrument_id == "510050.SH"]
    adjudicated = [payload for instrument_id, payload in payloads if instrument_id == "600000.SH"]
    assert {payload["kind"] for payload in baseline} == {"two-source-reconciled-row"}
    assert all(len(payload["source_observation_ids"]) == 2 for payload in baseline)
    assert {payload["kind"] for payload in adjudicated} == {"three-source-adjudicated-row"}
    assert all(len(payload["source_observation_ids"]) == 3 for payload in adjudicated)
    end_payload = next(
        json.loads(row.source_payload)
        for row in bars.itertuples(index=False)
        if row.instrument_id == "600000.SH" and row.session_date == END.isoformat()
    )
    assert end_payload["consensus"]["field_selected_provider"]["close"] == "baostock"
    assert end_payload["consensus"]["field_sources"]["close"] == ["baostock", "xtquant"]
    manifest = warehouse.load_observation(
        warehouse.load_snapshot(result.snapshot_id).plan.selections[-1].observation_id
    )
    assert len(manifest.source_metadata["partition_quality"]["source_observation_ids"]) == 3


def test_third_source_tolerance_cluster_never_synthesizes_a_critical_value(tmp_path):
    class NearTick(_AllTickProvider):
        def observe(self, request: ProviderRequest) -> ObservationPayload:
            payload = super().observe(request)
            frame = payload.tables[MarketTable.DAILY_BARS].copy()
            frame.loc[frame["session_date"].eq(END.isoformat()), "close"] = 12.5
            return ObservationPayload(
                payload.provider, payload.observed_at, payload.request,
                {MarketTable.DAILY_BARS: frame}, payload.coverage,
                payload.source_metadata,
            )

    class NearThird(_AllTickProvider):
        name = "xtquant"
        backend_group = "xtquant"

        def observe(self, request: ProviderRequest) -> ObservationPayload:
            payload = super().observe(request)
            frame = payload.tables[MarketTable.DAILY_BARS].copy()
            frame.loc[frame["session_date"].eq(END.isoformat()), "close"] = 11.0002
            return ObservationPayload(
                self.name, payload.observed_at, request,
                {MarketTable.DAILY_BARS: frame}, payload.coverage,
                {"backend_group": self.backend_group},
            )

    registry = ProviderRegistry()
    registry.register(_BaoProvider())
    registry.register(NearTick())
    registry.register(NearThird())
    result = HistoryDatabaseBuilder(
        MarketDataWarehouse(tmp_path / "market"),
        tmp_path / "reports",
        registry=registry,
        adjudicator_provider="xtquant",
    ).build(HistoryBuildSpec(
        END,
        start_date=START,
        instrument_ids=("600000.SH",),
        exchanges=("SH",),
    ))

    assert result.status == "incomplete"
    assert "excluded_instruments:1" in result.blockers


def test_third_source_uses_market_resolution_without_averaging(tmp_path):
    class MissingTick(_AllTickProvider):
        def observe(self, request: ProviderRequest) -> ObservationPayload:
            payload = super().observe(request)
            frame = payload.tables[MarketTable.DAILY_BARS].iloc[0:0].copy()
            return ObservationPayload(
                payload.provider, payload.observed_at, payload.request,
                {MarketTable.DAILY_BARS: frame},
                (CoverageClaim(
                    MarketTable.DAILY_BARS, False, START, END,
                    request.instrument_ids,
                ),),
                payload.source_metadata,
            )

    class RoundedThird(_AllTickProvider):
        name = "xtquant"
        backend_group = "xtquant"

        def observe(self, request: ProviderRequest) -> ObservationPayload:
            payload = _BaoProvider().observe(request)
            frame = payload.tables[MarketTable.DAILY_BARS].copy()
            for field in ("open", "high", "low", "close"):
                frame[field] = frame[field].astype("float32")
            frame["volume"] = (frame["volume"] / 100).round() * 100
            return ObservationPayload(
                self.name, payload.observed_at, request,
                {MarketTable.DAILY_BARS: frame}, payload.coverage,
                {"backend_group": self.backend_group},
            )

    class OddLotBao(_BaoProvider):
        def observe(self, request: ProviderRequest) -> ObservationPayload:
            payload = super().observe(request)
            if request.capability is ProviderCapability.INSTRUMENTS:
                return payload
            frame = payload.tables[MarketTable.DAILY_BARS].copy()
            frame["volume"] = frame["volume"] + 7
            return ObservationPayload(
                payload.provider, payload.observed_at, payload.request,
                {MarketTable.DAILY_BARS: frame}, payload.coverage,
                payload.source_metadata,
            )

    registry = ProviderRegistry()
    registry.register(OddLotBao())
    registry.register(MissingTick())
    registry.register(RoundedThird())
    warehouse = MarketDataWarehouse(tmp_path / "market")
    result = HistoryDatabaseBuilder(
        warehouse,
        tmp_path / "reports",
        registry=registry,
        adjudicator_provider="xtquant",
    ).build(HistoryBuildSpec(
        END,
        start_date=START,
        instrument_ids=("510050.SH",),
        exchanges=("SH",),
        batch_size=1,
        publish=True,
    ))

    assert result.status == "complete"
    bars = CanonicalMarketData.open(
        tmp_path / "market", required_readiness=ReadinessProfile.RESEARCH_PRICE,
    ).bars(("510050.SH",), START, END, price_mode=PriceMode.RAW, as_of=END)
    assert bars["volume"].tolist() == [1_000_000, 1_000_000]


def test_history_builder_does_not_call_third_source_when_baseline_sources_agree(tmp_path):
    class UnexpectedThird(_AllTickProvider):
        name = "xtquant"
        backend_group = "xtquant"

        def observe(self, request: ProviderRequest) -> ObservationPayload:
            raise AssertionError("third source must remain lazy when baseline sources agree")

    registry = ProviderRegistry()
    registry.register(_BaoProvider())
    registry.register(_AllTickProvider())
    registry.register(UnexpectedThird())
    result = HistoryDatabaseBuilder(
        MarketDataWarehouse(tmp_path / "market"),
        tmp_path / "reports",
        registry=registry,
        adjudicator_provider="xtquant",
    ).build(HistoryBuildSpec(
        END,
        start_date=START,
        instrument_ids=("600000.SH",),
        exchanges=("SH",),
    ))

    assert result.status == "complete"


def test_identical_history_build_is_locked_until_checkpoint_owner_finishes(tmp_path):
    started = Event()
    release = Event()
    guard = Lock()
    calls = 0

    class BlockingTick(_AllTickProvider):
        def observe(self, request: ProviderRequest) -> ObservationPayload:
            nonlocal calls
            with guard:
                calls += 1
                first = calls == 1
            if first:
                started.set()
                assert release.wait(timeout=10)
            return super().observe(request)

    registry = ProviderRegistry()
    registry.register(_BaoProvider())
    registry.register(BlockingTick())
    warehouse = MarketDataWarehouse(tmp_path / "market")
    first_builder = HistoryDatabaseBuilder(
        warehouse, tmp_path / "reports", registry=registry,
    )
    second_builder = HistoryDatabaseBuilder(
        warehouse, tmp_path / "reports", registry=registry,
    )
    spec = HistoryBuildSpec(
        END,
        start_date=START,
        instrument_ids=("510050.SH", "600000.SH"),
        exchanges=("SH",),
        batch_size=1,
    )

    with ThreadPoolExecutor(max_workers=1) as executor:
        first = executor.submit(first_builder.build, spec)
        assert started.wait(timeout=10)
        try:
            with pytest.raises(RuntimeError, match="already running"):
                second_builder.build(spec)
        finally:
            release.set()
        result = first.result(timeout=20)

    checkpoint = json.loads(result.checkpoint.read_text(encoding="utf-8"))
    assert result.status == "complete"
    assert len(checkpoint["batches"]) == 2


def test_history_build_lock_rejects_a_second_process(tmp_path):
    lock_path = tmp_path / "same-build.lock"
    script = """
from pathlib import Path
import sys
from fundlab.marketdata.history import _exclusive_build_lock
try:
    with _exclusive_build_lock(Path(sys.argv[1])):
        pass
except RuntimeError:
    raise SystemExit(0)
raise SystemExit(1)
"""

    with _exclusive_build_lock(lock_path):
        attempted = subprocess.run(
            [sys.executable, "-c", script, str(lock_path)],
            cwd=Path(__file__).resolve().parents[2],
            check=False,
        )

    assert attempted.returncode == 0


def test_history_builder_aligns_explicit_suspension_with_flat_zero_turnover_bar(tmp_path):
    class SuspendedBao(_BaoProvider):
        def observe(self, request: ProviderRequest) -> ObservationPayload:
            payload = super().observe(request)
            if request.capability is ProviderCapability.INSTRUMENTS:
                return payload
            frame = payload.tables[MarketTable.DAILY_BARS].copy()
            first = frame["session_date"].eq(START.isoformat())
            frame.loc[first, ["open", "high", "low", "close"]] = 10.0
            frame.loc[first, "volume"] = 0
            frame.loc[first, "suspended"] = True
            return ObservationPayload(
                payload.provider, payload.observed_at, payload.request,
                {MarketTable.DAILY_BARS: frame}, payload.coverage,
                payload.source_metadata,
            )

    class FlatTick(_AllTickProvider):
        def observe(self, request: ProviderRequest) -> ObservationPayload:
            payload = super().observe(request)
            frame = payload.tables[MarketTable.DAILY_BARS].copy()
            first = frame["session_date"].eq(START.isoformat())
            frame.loc[first, ["open", "high", "low", "close"]] = 10.0
            frame.loc[first, "volume"] = 0
            frame.loc[first, "suspended"] = None
            return ObservationPayload(
                payload.provider, payload.observed_at, payload.request,
                {MarketTable.DAILY_BARS: frame}, payload.coverage,
                payload.source_metadata,
            )

    registry = ProviderRegistry()
    registry.register(SuspendedBao())
    registry.register(FlatTick())
    warehouse = MarketDataWarehouse(tmp_path / "market")
    builder = HistoryDatabaseBuilder(
        warehouse, tmp_path / "reports", registry=registry,
    )
    result = builder.build(HistoryBuildSpec(
        END,
        start_date=START,
        instrument_ids=("600000.SH",),
        exchanges=("SH",),
        batch_size=1,
        publish=True,
    ))

    assert result.status == "complete"
    market = CanonicalMarketData.open(
        tmp_path / "market",
        required_readiness=ReadinessProfile.RESEARCH_PRICE,
    )
    bars = market.bars(
        ("600000.SH",), START, END, price_mode=PriceMode.RAW, as_of=END,
    )
    assert bars["session_date"].tolist() == [END.isoformat()]


def test_no_trade_check_accepts_explicit_flat_zero_turnover_placeholder():
    frame = _bars("baostock", ("600000.SH",)).iloc[:1].copy()
    frame = frame.astype({"volume": "float64", "amount": "float64"})
    frame.loc[:, ["open", "high", "low", "close"]] = 10.0
    frame.loc[:, "volume"] = 0.0
    frame.loc[:, "amount"] = float("nan")
    frame.loc[:, "suspended"] = True

    assert history_module._active_no_trade_instruments(
        frame,
        applicable={"600000.SH"},
        start_date=START,
        end_date=END,
    ) == set()


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("high", 10.1),
        ("high", 10.0 + 1e-13),
        ("volume", 1.0),
        ("volume", 1e-10),
        ("amount", 1.0),
        ("amount", 1e-10),
        ("suspended", False),
    ),
)
def test_no_trade_check_rejects_active_or_ambiguous_rows(field, value):
    frame = _bars("baostock", ("600000.SH",)).iloc[:1].copy()
    frame = frame.astype({"volume": "float64", "amount": "float64"})
    frame.loc[:, ["open", "high", "low", "close"]] = 10.0
    frame.loc[:, "volume"] = 0.0
    frame.loc[:, "amount"] = float("nan")
    frame.loc[:, "suspended"] = True
    frame.loc[:, field] = value

    assert history_module._active_no_trade_instruments(
        frame,
        applicable={"600000.SH"},
        start_date=START,
        end_date=END,
    ) == {"600000.SH"}


def test_no_trade_partition_reports_active_evidence_per_instrument():
    target = ("510050.SH", "600000.SH")
    predecessor = SimpleNamespace(plan=SimpleNamespace(universe_scope=UniverseScope(
        CURRENT_SH_SZ_STOCK_ETF_UNIVERSE,
        START - timedelta(days=1),
        date(2026, 1, 1),
        START - timedelta(days=1),
        survivorship_bias=True,
        instrument_ids=target,
    )))
    universe = SimpleNamespace(
        provider="exchange-public",
        request=ProviderRequest(ProviderCapability.INSTRUMENTS, parameters={
            "as_of_date": END.isoformat(),
        }),
        source_metadata={"as_of_date": END.isoformat()},
        observed_at=datetime(2026, 7, 15, tzinfo=timezone.utc),
    )
    calendar = SimpleNamespace(
        source_metadata={"calendar_quality": {
            "validated": True,
            "start_date": START.isoformat(),
            "end_date": END.isoformat(),
        }},
        observed_at=datetime(2026, 7, 15, tzinfo=timezone.utc),
    )
    manifests = {
        f"obs-{provider}": SimpleNamespace(
            observation_id=f"obs-{provider}",
            provider=provider,
            request=ProviderRequest(
                ProviderCapability.DAILY_BARS_RAW, START, END, target,
            ),
            source_metadata={"backend_group": provider},
            observed_at=datetime(2026, 7, 15, tzinfo=timezone.utc),
        )
        for provider in ("tickflow", "xtquant", "baostock")
    }
    empty = _bars("baostock", target).iloc[0:0].copy()
    active = _bars("baostock", target).loc[
        lambda frame: frame["instrument_id"].eq("510050.SH")
    ].copy()

    class Warehouse:
        def load_snapshot(self, snapshot_id):
            assert snapshot_id == "snap-predecessor"
            return predecessor

        def load_observation(self, observation_id):
            if observation_id == "obs-universe":
                return universe
            if observation_id == "obs-calendar":
                return calendar
            return manifests[observation_id]

        def read_observation_table(self, observation_id, table):
            assert table in {MarketTable.INSTRUMENTS, MarketTable.DAILY_BARS}
            if observation_id == "obs-universe":
                return _master()
            if observation_id == "obs-baostock":
                return active
            return empty

    with pytest.raises(history_module.NoTradeSourceActiveError) as caught:
        history_module.record_no_trade_research_partition(
            Warehouse(),
            predecessor_snapshot_id="snap-predecessor",
            universe_observation_id="obs-universe",
            calendar_observation_id="obs-calendar",
            source_observation_ids=tuple(manifests),
            start_date=START,
            end_date=END,
            instrument_ids=target,
        )

    assert caught.value.active_observation_ids == {
        "510050.SH": ("obs-baostock",),
    }
    assert "510050.SH" in str(caught.value)


def test_no_trade_pending_onboarding_membership_is_exactly_bound_to_official_view():
    observed_at = datetime(2026, 7, 15, tzinfo=timezone.utc)
    pending = {
        "510050.SH": {
            "listed_date": "2026-07-15",
            "reason": "official_new_instrument_master_future_listed_date",
            "target_date": END.isoformat(),
        },
    }
    raw_frame = history_module.normalize_table(
        MarketTable.INSTRUMENTS,
        _master(),
        provider="exchange-public",
        observed_at=observed_at.isoformat(),
        require_observation_id=False,
    )
    wrapper_frame = history_module.normalize_table(
        MarketTable.INSTRUMENTS,
        raw_frame.loc[raw_frame["instrument_id"].eq("600000.SH")],
        provider=history_module.DAILY_PENDING_UNIVERSE_PROVIDER,
        observed_at=observed_at.isoformat(),
        require_observation_id=False,
    )
    raw = SimpleNamespace(
        observation_id="obs-official",
        provider="exchange-public",
        request=ProviderRequest(ProviderCapability.INSTRUMENTS, parameters={
            "as_of_date": END.isoformat(),
        }),
        source_metadata={"as_of_date": END.isoformat()},
    )
    wrapper = SimpleNamespace(
        provider=history_module.DAILY_PENDING_UNIVERSE_PROVIDER,
        observed_at=observed_at,
        request=ProviderRequest(
            ProviderCapability.CANONICAL_RECONCILIATION,
            parameters={
                "target_date": END.isoformat(),
                "official_observation_id": raw.observation_id,
                "pending_onboarding": pending,
                "pipeline": history_module.DAILY_CARRY_PIPELINE_VERSION,
            },
        ),
        coverage=(CoverageClaim(
            MarketTable.INSTRUMENTS,
            True,
            instrument_ids=("600000.SH",),
        ),),
        source_metadata={
            "kind": history_module.DAILY_PENDING_UNIVERSE_KIND,
            "official_observation_id": raw.observation_id,
            "pending_onboarding": pending,
        },
    )
    warehouse = SimpleNamespace(
        load_observation=lambda observation_id: raw,
        read_observation_table=lambda observation_id, table: raw_frame,
    )

    history_module._validate_no_trade_current_membership(
        warehouse,
        wrapper,
        wrapper_frame,
        predecessor_snapshot_id="snap-predecessor",
        start_date=END,
        end_date=END,
        instrument_ids=("600000.SH",),
    )

    tampered = wrapper_frame.copy()
    tampered.loc[:, "price_tick"] += 1e-12
    with pytest.raises(ValueError, match="exact official-minus-pending view"):
        history_module._validate_no_trade_current_membership(
            warehouse,
            wrapper,
            tampered,
            predecessor_snapshot_id="snap-predecessor",
            start_date=END,
            end_date=END,
            instrument_ids=("600000.SH",),
        )


def test_no_trade_partition_reuse_requires_the_exact_validated_boundary(tmp_path):
    warehouse = MarketDataWarehouse(tmp_path / "market")
    target = ("600000.SH",)
    universe_id = "obs-universe-boundary"
    calendar_id = "obs-calendar-boundary"
    predecessor_id = "snap-predecessor-boundary"
    manifest = warehouse.record_observation(ObservationPayload(
        history_module.NO_TRADE_RESEARCH_PROVIDER,
        datetime(2026, 7, 15, 1, 0, tzinfo=timezone.utc),
        ProviderRequest(
            ProviderCapability.CANONICAL_RECONCILIATION,
            START,
            END,
            target,
            {
                "kind": "independent-no-trade-consensus",
                "input_observation_ids": (universe_id, calendar_id),
            },
        ),
        {
            MarketTable.INSTRUMENTS: _master().loc[
                _master()["instrument_id"].eq(target[0])
            ].reset_index(drop=True),
            MarketTable.DAILY_BARS: empty_table(
                MarketTable.DAILY_BARS, include_lineage=True,
            ),
        },
        (
            CoverageClaim(MarketTable.INSTRUMENTS, True, instrument_ids=target),
            CoverageClaim(MarketTable.DAILY_BARS, True, START, END, target),
        ),
        {
            "kind": "field_level_reconciliation",
            "reconciliation_ready": True,
            "partition_quality": {
                "validated": True,
                "validator_version": history_module.NO_TRADE_RESEARCH_VALIDATOR_VERSION,
                "readiness": ReadinessProfile.RESEARCH_PRICE.value,
                "instrument_ids": target,
                "start_date": START,
                "end_date": END,
                "universe_definition": CURRENT_SH_SZ_STOCK_ETF_UNIVERSE,
                "universe_as_of": END,
                "row_count": 0,
                "predecessor_snapshot_id": predecessor_id,
                "input_observation_ids": (calendar_id, universe_id),
            },
        },
    ))

    found = history_module.find_no_trade_research_partition(
        warehouse,
        predecessor_snapshot_id=predecessor_id,
        universe_observation_id=universe_id,
        calendar_observation_id=calendar_id,
        start_date=START,
        end_date=END,
        instrument_ids=target,
    )

    assert found is not None and found.observation_id == manifest.observation_id
    assert history_module.find_no_trade_research_partition(
        warehouse,
        predecessor_snapshot_id=predecessor_id,
        universe_observation_id=universe_id,
        calendar_observation_id="obs-different-calendar",
        start_date=START,
        end_date=END,
        instrument_ids=target,
    ) is None


def test_current_research_projection_uses_exact_exchange_membership_and_disjoint_supplement(
    tmp_path,
):
    registry = ProviderRegistry()
    registry.register(_BaoProvider())
    registry.register(_AllTickProvider())
    warehouse = MarketDataWarehouse(tmp_path / "market")
    builder = HistoryDatabaseBuilder(
        warehouse, tmp_path / "reports", registry=registry,
    )
    stock = builder.build(HistoryBuildSpec(
        END,
        start_date=START,
        instrument_ids=("600000.SH",),
        exchanges=("SH",),
    ))
    etf = builder.build(HistoryBuildSpec(
        END,
        start_date=START,
        instrument_ids=("510050.SH",),
        exchanges=("SH",),
    ))
    assert stock.snapshot_id and etf.snapshot_id

    official = _master().copy()
    official["listed_date"] = None
    official["sell_delay_sessions"] = None
    universe = warehouse.record_observation(ObservationPayload(
        "exchange-public",
        datetime(2026, 7, 18, 0, 3, tzinfo=timezone.utc),
        ProviderRequest(
            ProviderCapability.INSTRUMENTS,
            parameters={"as_of_date": END},
        ),
        {MarketTable.INSTRUMENTS: official},
        (CoverageClaim(
            MarketTable.INSTRUMENTS,
            True,
            instrument_ids=("510050.SH", "600000.SH"),
        ),),
        {"as_of_date": END, "backend_group": "exchange-public"},
    ))

    result = derive_current_research_snapshot(
        warehouse,
        tmp_path / "reports",
        source_snapshot_id=stock.snapshot_id,
        supplement_snapshot_ids=(etf.snapshot_id,),
        universe_observation_id=universe.observation_id,
        universe_as_of=END,
        start_date=START,
        end_date=END,
    )

    assert result.status == "complete"
    assert result.target_instruments == result.included_instruments == 2
    assert result.source_snapshot_ids == (stock.snapshot_id, etf.snapshot_id)
    assert not result.missing_instrument_ids
    snapshot = warehouse.load_snapshot(result.snapshot_id)
    assert snapshot.plan.universe_scope is not None
    assert snapshot.plan.universe_scope.instrument_ids == ("510050.SH", "600000.SH")
