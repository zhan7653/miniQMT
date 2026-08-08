from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from threading import Event, Lock, enumerate as enumerate_threads
import json
import time

import pandas as pd

from fundlab.marketdata import MarketDataWarehouse, MarketTable
from fundlab.web.snapshot_cache import InstrumentNameResolver
from tests.canonical.fixtures import ready_market


@dataclass(frozen=True)
class _Scope:
    history_start: date
    history_end: date
    instrument_ids: tuple[str, ...]


@dataclass(frozen=True)
class _Plan:
    universe_scope: _Scope


@dataclass(frozen=True)
class _Manifest:
    snapshot_id: str
    plan: _Plan


class _FakeWarehouse:
    def __init__(self) -> None:
        self.snapshot_id = "snap-a"
        self.names = {"510300.SH": "沪深300ETF"}
        self.load_started = Event()
        self.allow_load = Event()
        self.allow_load.set()
        self.query_started = Event()
        self.allow_query = Event()
        self.allow_query.set()
        self._lock = Lock()
        self.load_calls = 0
        self.query_calls = 0

    def load_current_snapshot(self) -> _Manifest:
        with self._lock:
            self.load_calls += 1
        self.load_started.set()
        assert self.allow_load.wait(2), "test did not release snapshot load"
        return _Manifest(
            self.snapshot_id,
            _Plan(_Scope(date(2024, 1, 1), date(2026, 8, 6), tuple(self.names))),
        )

    def query_loaded_instrument_names(
        self, manifest, *, instrument_ids=(), **kwargs,
    ) -> pd.DataFrame:
        del manifest, kwargs
        with self._lock:
            self.query_calls += 1
        self.query_started.set()
        assert self.allow_query.wait(2), "test did not release name query"
        return pd.DataFrame([
            {"instrument_id": instrument_id, "name": self.names[instrument_id]}
            for instrument_id in instrument_ids
            if instrument_id in self.names
        ], columns=("instrument_id", "name"))


def _write_pointer(root: Path, snapshot_id: str, digest: str) -> None:
    root.mkdir(parents=True, exist_ok=True)
    (root / "current.json").write_text(
        json.dumps({"snapshot_id": snapshot_id, "manifest_sha256": digest}),
        encoding="utf-8",
    )


def _wait_until(predicate, *, timeout: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError("condition was not reached")


def test_cold_concurrent_name_reads_return_immediately_and_refresh_once(tmp_path):
    _write_pointer(tmp_path, "snap-a", "a" * 64)
    warehouse = _FakeWarehouse()
    warehouse.allow_load.clear()
    resolver = InstrumentNameResolver(
        tmp_path, warehouse=warehouse, refresh_delay_seconds=0,
    )
    try:
        started = time.perf_counter()
        with ThreadPoolExecutor(max_workers=20) as pool:
            results = list(pool.map(
                lambda _: resolver.lookup(("510300.SH",)), range(20),
            ))
        elapsed = time.perf_counter() - started

        assert results == [{}] * 20
        assert resolver.pending(("510300.SH",)) is True
        assert resolver.lookup_state(("510300.SH",)) == ({}, True)
        assert elapsed < 0.5
        assert warehouse.load_started.wait(1)
        assert warehouse.load_calls == 1
        workers = [
            thread for thread in enumerate_threads()
            if thread.name == "fundlab-web-snapshot"
        ]
        assert workers and all(thread.daemon for thread in workers)

        warehouse.allow_load.set()
        _wait_until(
            lambda: resolver.lookup(("510300.SH",)).get("510300.SH")
            == "沪深300ETF",
        )
        assert resolver.pending(("510300.SH",)) is False
        assert resolver.lookup_state(("510300.SH",)) == (
            {"510300.SH": "沪深300ETF"}, False,
        )
        assert warehouse.query_calls == 1
    finally:
        warehouse.allow_load.set()
        resolver.close()


def test_snapshot_rollover_serves_last_good_name_until_new_generation_wins(tmp_path):
    _write_pointer(tmp_path, "snap-a", "a" * 64)
    warehouse = _FakeWarehouse()
    resolver = InstrumentNameResolver(
        tmp_path, warehouse=warehouse, refresh_delay_seconds=0,
    )
    try:
        resolver.prewarm(("510300.SH",))
        _wait_until(
            lambda: resolver.lookup(("510300.SH",)).get("510300.SH")
            == "沪深300ETF",
        )

        warehouse.snapshot_id = "snap-b"
        warehouse.names["510300.SH"] = "沪深300ETF（新）"
        warehouse.allow_query.clear()
        warehouse.query_started.clear()
        _write_pointer(tmp_path, "snap-b", "b" * 64)

        assert resolver.lookup(("510300.SH",)) == {"510300.SH": "沪深300ETF"}
        assert warehouse.query_started.wait(1)
        assert resolver.lookup(("510300.SH",)) == {"510300.SH": "沪深300ETF"}

        warehouse.allow_query.set()
        _wait_until(
            lambda: resolver.lookup(("510300.SH",)).get("510300.SH")
            == "沪深300ETF（新）",
        )
        assert resolver.market_summary()["snapshot_id"] == "snap-b"
    finally:
        warehouse.allow_query.set()
        resolver.close()


def test_refresh_failure_never_removes_last_good_name(tmp_path):
    _write_pointer(tmp_path, "snap-a", "a" * 64)
    warehouse = _FakeWarehouse()
    resolver = InstrumentNameResolver(
        tmp_path, warehouse=warehouse, refresh_delay_seconds=0,
    )
    try:
        resolver.prewarm(("510300.SH",))
        _wait_until(lambda: bool(resolver.lookup(("510300.SH",))))

        warehouse.snapshot_id = "snap-b"
        warehouse.allow_load.clear()
        _write_pointer(tmp_path, "snap-b", "b" * 64)
        assert resolver.lookup(("510300.SH",)) == {"510300.SH": "沪深300ETF"}
        warehouse.allow_load.set()

        # The fake returns a mismatching manifest after the observed generation
        # changes again; the old last-good label must remain available.
        warehouse.snapshot_id = "snap-c"
        _wait_until(lambda: warehouse.load_calls >= 2)
        assert resolver.lookup(("510300.SH",)) == {"510300.SH": "沪深300ETF"}
    finally:
        warehouse.allow_load.set()
        resolver.close()


def test_specialized_instrument_name_query_matches_canonical_component_view(tmp_path):
    ready_market(tmp_path)
    warehouse = MarketDataWarehouse(tmp_path)
    snapshot = warehouse.load_current_snapshot()

    full = warehouse.query_loaded_snapshot_table(
        snapshot,
        MarketTable.INSTRUMENTS,
        instrument_ids=("600000.SH",),
    )[["instrument_id", "name"]].reset_index(drop=True)
    names = warehouse.query_loaded_instrument_names(
        snapshot,
        instrument_ids=("600000.SH",),
    )

    pd.testing.assert_frame_equal(names, full)
