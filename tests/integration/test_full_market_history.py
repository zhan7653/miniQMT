from __future__ import annotations

from datetime import date, datetime
import json

import pandas as pd

from fundlab.data.pipeline import FullMarketHistoryRunner, HistoryRunSpec
from fundlab.data.platform import (
    BatchRecord, BatchStatus, CollectionPhase, DataCatalog, ManifestIdentity, ProviderCapability,
    ProviderHealth, PreflightResult, ProviderResult, SymbolResult, TrustState, VersionRecord,
    VersionStatus, stable_fingerprint,
)
from fundlab.data.storage import VersionedParquetStore


class FixtureProvider:
    name = "xtquant"

    def __init__(self, *, transient_download_failures: int = 0, adjusted_missing_last: bool = False):
        self.transient_download_failures = transient_download_failures
        self.adjusted_missing_last = adjusted_missing_last
        self.download_calls = 0
        self.fetch_calls = []

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
        if self.download_calls <= self.transient_download_failures:
            raise TimeoutError("fixture timeout")

    def fetch(self, request):
        self.fetch_calls.append(request)
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


def runner(tmp_path, provider):
    catalog = DataCatalog(tmp_path / "v2" / "catalog.sqlite3"); catalog.initialize()
    store = VersionedParquetStore(tmp_path / "v2", catalog)
    legacy = tmp_path / "legacy"; legacy.mkdir(); (legacy / "sentinel.bin").write_bytes(b"legacy-v1")
    return FullMarketHistoryRunner(
        provider=provider, catalog=catalog, store=store, report_root=tmp_path / "reports",
        history_config=config(), config_hash="fixture-config", legacy_paths=(legacy,),
        sleeper=lambda _: None,
    ), catalog


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
