from __future__ import annotations

from datetime import date, datetime
import json
from time import monotonic

import pandas as pd
import pyarrow as pa
import pytest

from fundlab.data.pipeline import FullMarketHistoryRunner, HistoryRunSpec
from fundlab.data.platform import (
    BatchRecord, BatchStatus, CollectionPartitionRecord, CollectionPhase, CollectionRunRecord,
    CollectionRunStatus, DataCatalog, ManifestIdentity, PartitionIdentity, PartitionStatus, PriceMode,
    ProviderCapability, ProviderHealth, PreflightResult, ProviderResult, SymbolResult, TrustState,
    VersionRecord, VersionStatus, stable_fingerprint,
)
from fundlab.data.storage import VersionedParquetStore


class FixtureProvider:
    name = "xtquant"

    def __init__(self, *, transient_download_failures: int = 0, adjusted_missing_last: bool = False,
                 clock=lambda: 0.0):
        self.transient_download_failures = transient_download_failures
        self.adjusted_missing_last = adjusted_missing_last
        self.download_calls = 0
        self.fetch_calls = []
        self.external_calls = []
        self.clock = clock

    def preflight(self):
        return PreflightResult(self.name, ProviderHealth.AVAILABLE, datetime.now().astimezone())

    def get_instruments(self):
        return [
            {"symbol": "510300.SH", "exchange": "SH", "product_type": "ETF", "is_active": 1,
             "listed_date": "2024-01-02", "delisted_date": None, "source": self.name},
            {"symbol": "160001.SZ", "exchange": "SZ", "product_type": "LOF", "is_active": 0,
             "listed_date": "2024-01-02", "delisted_date": "2024-01-05", "source": self.name},
        ]

    def download_daily_bar(self, symbols, start_date, end_date):
        self.download_calls += 1
        self.external_calls.append(("download", self.clock()))
        if self.download_calls <= self.transient_download_failures:
            raise TimeoutError("fixture timeout")

    def fetch(self, request):
        self.fetch_calls.append(request)
        self.external_calls.append((f"fetch:{request.capability.value}", self.clock()))
        if request.capability is ProviderCapability.TRADING_CALENDAR:
            frame = pd.DataFrame({"date": ["2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"]})
        else:
            days = ["2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"]
            if request.capability is ProviderCapability.DAILY_BARS_ADJUSTED and self.adjusted_missing_last:
                days = days[:-1]
            symbol = request.symbols[0]
            frame = pd.DataFrame({
                "date": days, "symbol": symbol, "open": [10.0] * len(days),
                "high": [11.0] * len(days), "low": [9.0] * len(days), "close": [10.5] * len(days),
                "volume": [100.0] * len(days), "amount": [1000.0] * len(days),
                "suspended": [False] * len(days),
                "price_mode": [
                    "raw" if request.capability is ProviderCapability.DAILY_BARS_RAW else "adjusted"
                ] * len(days),
            })
        result = ProviderResult(
            self.name, request.capability,
            tuple(SymbolResult(symbol, len(frame)) for symbol in request.symbols), datetime.now().astimezone(),
        )
        return frame, result


def config():
    return {
        "partition_years": 1,
        "retry": {"max_attempts": 3, "backoff_seconds": [0, 0, 0]},
        "throttle": {
            "initial": {"workers": 1, "request_interval_seconds": 2, "cooldown_every_symbols": 20, "cooldown_seconds": 60},
            "maximum": {"workers": 4, "requests_per_second": 2, "cooldown_every_symbols": 100, "cooldown_seconds": 30},
        },
        "coverage": {"min_symbol_trading_day_ratio": 0.95},
    }


def runner(tmp_path, provider, *, sleeper=lambda _: None, clock=monotonic):
    catalog = DataCatalog(tmp_path / "v2" / "catalog.sqlite3"); catalog.initialize()
    store = VersionedParquetStore(tmp_path / "v2", catalog)
    legacy = tmp_path / "legacy"; legacy.mkdir(); (legacy / "sentinel.bin").write_bytes(b"legacy-v1")
    return FullMarketHistoryRunner(
        provider=provider, catalog=catalog, store=store, report_root=tmp_path / "reports",
        history_config=config(), config_hash="fixture-config", legacy_paths=(legacy,),
        sleeper=sleeper, clock=clock,
    ), catalog


class FakeTime:
    def __init__(self):
        self.now = 0.0
        self.sleeps = []

    def clock(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


def seed_latest(catalog: DataCatalog) -> str:
    fingerprint = stable_fingerprint({"fixture": "latest"})
    batch_id = "batch-fixture"
    catalog.create_batch(BatchRecord(
        batch_id, "xtquant", date(2024, 1, 2), date(2024, 1, 5), ("510300.SH",),
        "fixture", "fixture", fingerprint, BatchStatus.PENDING, 0, None,
    ))
    catalog.transition_batch(batch_id, BatchStatus.RUNNING)
    catalog.transition_batch(batch_id, BatchStatus.COMPLETE, row_count=4)
    version_id = "version-fixture"
    catalog.create_version(VersionRecord(
        version_id, batch_id, "manifest", "content", VersionStatus.BUILDING, None, None, None,
    ))
    catalog.complete_version(version_id)
    return version_id


def seed_v2_partitions(history, catalog):
    run_id = "history-old-v2-provenance"
    catalog.create_collection_run(CollectionRunRecord(
        run_id, "xtquant", CollectionPhase.CANARY, date(2024, 1, 5), "old-config",
        "initial", CollectionRunStatus.PENDING,
    ))
    catalog.transition_collection_run(run_id, CollectionRunStatus.RUNNING)
    artifacts = {}
    for symbol in ("160001.SZ", "510300.SH"):
        for mode in (PriceMode.RAW, PriceMode.ADJUSTED):
            identity = PartitionIdentity(
                "xtquant", symbol, mode, date(2024, 1, 2), date(2024, 1, 5),
                "xtquant:daily:1d:raw-front:v2:per-request-throttled:provider-suspension-required",
            )
            frame = pd.DataFrame({
                "date": ["2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"],
                "symbol": [symbol] * 4, "open": [10.0] * 4, "high": [11.0] * 4,
                "low": [9.0] * 4, "close": [10.5] * 4, "volume": [100.0] * 4,
                "amount": [1000.0] * 4, "suspended": [False] * 4,
                "price_mode": [mode.value] * 4,
            })
            artifact = history.store.write_partition(
                identity, pa.Table.from_pandas(frame, preserve_index=False),
            )
            catalog.create_partition(CollectionPartitionRecord(
                identity.fingerprint, run_id, identity, PartitionStatus.PENDING,
            ))
            catalog.transition_partition(identity.fingerprint, PartitionStatus.RUNNING)
            catalog.transition_partition(
                identity.fingerprint, PartitionStatus.COMPLETE, row_count=artifact.row_count,
                checksum=artifact.checksum, storage_path=artifact.path,
            )
            artifacts[(symbol, mode)] = artifact
    return artifacts


def test_canary_retries_three_attempts_pauses_and_preserves_latest_complete(tmp_path):
    provider = FixtureProvider(transient_download_failures=2)
    history, catalog = runner(tmp_path, provider)
    latest = seed_latest(catalog)
    result = history.run(HistoryRunSpec(
        CollectionPhase.CANARY, date(2024, 1, 5), sample_limit=20, minimum_free_bytes=0,
    ))
    assert result.status == "paused"
    assert result.latest_complete_before == result.latest_complete_after == latest
    assert result.completed_partitions == 4
    attempts = [catalog.list_attempts(item.partition_id) for item in catalog.list_partitions(run_id=result.run_id)]
    assert sorted(len(items) for items in attempts) == [1, 1, 1, 3]
    assert result.legacy_manifest_before == result.legacy_manifest_after
    payload = json.loads((tmp_path / "reports" / f"full-market-canary-{result.run_id}.json").read_text(encoding="utf-8"))
    assert payload["evidence"]["publication_decision"] == "forbidden_phase_1"
    assert payload["latest_complete_after"] == latest


def test_validated_partitions_are_reused_without_duplicate_work(tmp_path):
    provider = FixtureProvider()
    history, _ = runner(tmp_path, provider)
    spec = HistoryRunSpec(CollectionPhase.CANARY, date(2024, 1, 5), minimum_free_bytes=0)
    first = history.run(spec)
    first_downloads = provider.download_calls
    second = history.run(spec)
    assert first.status == second.status == "paused"
    assert second.reused_partitions == second.scheduled_partitions == 4
    assert provider.download_calls == first_downloads


def test_raw_adjusted_key_mismatch_quarantines_symbol_and_reports_missing_dates(tmp_path):
    history, _ = runner(tmp_path, FixtureProvider(adjusted_missing_last=True))
    result = history.run(HistoryRunSpec(
        CollectionPhase.CANARY, date(2024, 1, 5), minimum_free_bytes=0,
    ))
    assert result.status == "paused"
    assert all("raw_adjusted_key_mismatch" in result.quarantined_symbols[symbol]
               for symbol in result.selected_symbols)
    assert result.missing_dates["510300.SH"] == ()


def test_discovery_is_deterministic_and_does_not_create_partitions(tmp_path):
    history, catalog = runner(tmp_path, FixtureProvider())
    first = history.run(HistoryRunSpec(CollectionPhase.DISCOVER, date(2024, 1, 5), minimum_free_bytes=0))
    second = history.run(HistoryRunSpec(CollectionPhase.DISCOVER, date(2024, 1, 5), minimum_free_bytes=0))
    assert first.selected_symbols == second.selected_symbols == ("510300.SH", "160001.SZ")
    assert catalog.list_partitions() == ()


def test_every_download_and_raw_adjusted_fetch_has_its_own_throttle_gate(tmp_path):
    fake = FakeTime()
    provider = FixtureProvider(clock=fake.clock)
    history, catalog = runner(tmp_path, provider, sleeper=fake.sleep, clock=fake.clock)
    result = history.run(HistoryRunSpec(
        CollectionPhase.CANARY, date(2024, 1, 5), minimum_free_bytes=0,
    ))
    assert result.status == "paused"
    kinds = [kind for kind, _ in provider.external_calls]
    assert kinds == [
        "fetch:trading_calendar",
        "download", "fetch:daily_bars_raw", "fetch:daily_bars_adjusted",
        "download", "fetch:daily_bars_raw", "fetch:daily_bars_adjusted",
    ]
    timestamps = [timestamp for _, timestamp in provider.external_calls]
    assert timestamps == pytest.approx([0.0, 2.01, 4.02, 6.03, 8.04, 10.05, 12.06])
    assert all(later - earlier >= 2.0 for earlier, later in zip(timestamps, timestamps[1:]))
    request_events = [
        event for event in catalog.list_throttle_events(result.run_id)
        if event.reason.startswith("request_gate:")
    ]
    assert len(request_events) == len(provider.external_calls)
    assert sum("download_daily_bar" in event.reason for event in request_events) == 2
    assert sum("daily_bars_raw" in event.reason for event in request_events) == 2
    assert sum("daily_bars_adjusted" in event.reason for event in request_events) == 2


def test_v2_provenance_partitions_are_preserved_but_not_reused_for_v3(tmp_path):
    provider = FixtureProvider()
    history, catalog = runner(tmp_path, provider)
    old_artifacts = seed_v2_partitions(history, catalog)
    result = history.run(HistoryRunSpec(
        CollectionPhase.CANARY, date(2024, 1, 5), minimum_free_bytes=0,
    ))
    assert result.status == "paused"
    assert result.reused_partitions == 0
    assert result.scheduled_partitions == result.completed_partitions == 4
    assert provider.download_calls == 2
    partitions = catalog.list_partitions()
    old = [item for item in partitions if ":v2:" in item.identity.source_identity]
    new = [item for item in partitions if ":v3:" in item.identity.source_identity]
    assert len(old) == len(new) == 4
    assert {item.identity.source_identity for item in new} == {
        "xtquant:daily:1d:raw-front:v3:per-request-throttled:strict-interval-10ms-safety:provider-suspension-required"
    }
    for old_record in old:
        old_artifact = old_artifacts[(old_record.identity.symbol, old_record.identity.price_mode)]
        assert history.store.validate_partition(
            old_record.identity, expected_checksum=old_artifact.checksum,
            expected_row_count=old_artifact.row_count,
        ) == old_artifact
        matching = next(
            item for item in new
            if item.identity.symbol == old_record.identity.symbol
            and item.identity.price_mode is old_record.identity.price_mode
            and item.identity.start_date == old_record.identity.start_date
            and item.identity.end_date == old_record.identity.end_date
        )
        assert matching.identity.fingerprint != old_record.identity.fingerprint
        assert history.store.partition_path(matching.identity) != history.store.partition_path(old_record.identity)
