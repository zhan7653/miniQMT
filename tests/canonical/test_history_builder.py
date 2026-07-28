from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import date, datetime, timezone
import json
from pathlib import Path
import subprocess
import sys
from threading import Barrier, Event, Lock

import pandas as pd
import pytest

import fundlab.marketdata.history as history_module
from fundlab.common.canonical import stable_digest
from fundlab.marketdata import (
    CanonicalMarketData,
    CoverageClaim,
    HistoryBuildSpec,
    HistoryDatabaseBuilder,
    MarketDataWarehouse,
    MarketTable,
    ObservationPayload,
    PriceMode,
    ProviderCapability,
    ProviderRegistry,
    ProviderRequest,
    ReadinessProfile,
    compose_history_snapshot,
    derive_current_research_snapshot,
)
from fundlab.marketdata.history import _exclusive_build_lock


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
        ("volume", 1.0),
        ("amount", 1.0),
        ("suspended", False),
    ),
)
def test_no_trade_check_rejects_active_or_ambiguous_rows(field, value):
    frame = _bars("baostock", ("600000.SH",)).iloc[:1].copy()
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
