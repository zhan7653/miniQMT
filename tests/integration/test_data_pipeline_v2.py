from datetime import date, datetime, timedelta

import pandas as pd
import pyarrow as pa

from fundlab.data.pipeline import DailyUpdateRunner
from fundlab.data.platform import (BatchRecord, BatchStatus, DataCatalog, ManifestIdentity, PreflightResult,
                                   ProviderCapability, ProviderHealth, PriceMode, ProviderRequest, ProviderResult,
                                   SymbolResult, TrustState, UniverseSnapshot, VersionRecord, VersionStatus,
                                   stable_fingerprint)
from fundlab.data.portal import DataPortal
from fundlab.data.sources.provider_registry import ProviderRegistry
from fundlab.data.storage import VersionedParquetStore


class FakeProvider:
    name = "xtquant"
    capabilities = frozenset({ProviderCapability.TRADING_CALENDAR, ProviderCapability.DAILY_BARS_RAW,
                              ProviderCapability.DAILY_BARS_ADJUSTED})

    def __init__(self, *, revision=0, unavailable=False, bad_symbol=None):
        self.revision, self.unavailable, self.bad_symbol = revision, unavailable, bad_symbol
        self.calls = []

    def preflight(self):
        health = ProviderHealth.SERVICE_UNAVAILABLE if self.unavailable else ProviderHealth.AVAILABLE
        return PreflightResult(self.name, health, datetime.now().astimezone(), "offline" if self.unavailable else None)

    def fetch(self, request):
        self.calls.append(request.capability)
        days = pd.bdate_range(request.start_date, request.end_date).strftime("%Y-%m-%d")
        if request.capability is ProviderCapability.TRADING_CALENDAR:
            frame = pd.DataFrame({"date": days})
            statuses = (SymbolResult(request.symbols[0], len(frame)),)
        else:
            rows, statuses = [], []
            for symbol in request.symbols:
                if symbol == self.bad_symbol:
                    statuses.append(SymbolResult(symbol, 0, "symbol unavailable")); continue
                for offset, day in enumerate(days):
                    close = 10 + offset / 100 + self.revision / 1000
                    rows.append({"date": day, "symbol": symbol, "open": close, "high": close + .1,
                                 "low": close - .1, "close": close, "volume": 1000, "amount": 10000,
                                 "suspended": False})
                statuses.append(SymbolResult(symbol, len(days)))
            frame = pd.DataFrame(rows)
        return frame, ProviderResult(self.name, request.capability, tuple(statuses), datetime.now().astimezone())


def make_runner(tmp_path, provider):
    catalog = DataCatalog(tmp_path / "catalog.sqlite3"); catalog.initialize()
    registry = ProviderRegistry(); registry.register(provider)
    universe = UniverseSnapshot.create(version="u1", effective_date=date(2026, 1, 1),
        symbols=("510300.SH", "510500.SH"), benchmarks=("510300.SH",), configuration={"u": 1})
    return DailyUpdateRunner(registry=registry, provider_name="xtquant", catalog=catalog,
        store=VersionedParquetStore(tmp_path / "warehouse", catalog), universe=universe,
        report_root=tmp_path / "reports", config_hash="cfg"), catalog


def seed_complete_snapshot(runner, catalog, provider):
    start, end = date(2026, 5, 1), date(2026, 5, 5)
    calendar, _ = provider.fetch(ProviderRequest(("510300.SH",), start, end, ProviderCapability.TRADING_CALENDAR))
    raw, _ = provider.fetch(ProviderRequest(("510300.SH", "510500.SH"), start, end,
                                            ProviderCapability.DAILY_BARS_RAW))
    adjusted, _ = provider.fetch(ProviderRequest(("510300.SH", "510500.SH"), start, end,
                                                 ProviderCapability.DAILY_BARS_ADJUSTED))
    tables = {"calendar": calendar, "daily_bars_raw": raw, "daily_bars_adjusted": adjusted,
              "features": pd.DataFrame({"date": ["2026-05-05"], "symbol": ["510300.SH"], "ret_1d": [.01]}),
              "universe": pd.DataFrame({"symbol": ["510300.SH", "510500.SH"],
                                        "effective_date": ["2026-01-01", "2026-01-01"]}),
              "quarantine": pd.DataFrame({"symbol": ["BAD.SH"], "reason": ["legacy contamination"]}),
              "future_table": pd.DataFrame({"key": ["preserve-me"], "value": [7]})}
    batch_id, version_id = "seed-batch", "seed-version"
    catalog.create_batch(BatchRecord(batch_id, "xtquant", start, end, ("510300.SH", "510500.SH"), "u1",
        "cfg", "seed-request", BatchStatus.PENDING, 0, None))
    catalog.transition_batch(batch_id, BatchStatus.RUNNING)
    runner.store.begin_version(version_id)
    for name, frame in tables.items():
        runner.store.write_table(version_id, name, pa.Table.from_pandas(frame, preserve_index=False))
    counts = {name: len(frame) for name, frame in tables.items()}
    content = stable_fingerprint(counts)
    identity = ManifestIdentity("xtquant", batch_id, version_id, datetime.now().astimezone(), TrustState.TRUSTED,
                                content, 1, counts)
    manifest = runner.store.prepare_manifest(identity, counts)
    catalog.create_version(VersionRecord(version_id, batch_id, manifest.fingerprint, content,
                                          VersionStatus.BUILDING, None, None, None))
    runner.store.finalize_manifest(manifest)
    catalog.transition_batch(batch_id, BatchStatus.COMPLETE, row_count=sum(counts.values()))
    catalog.complete_version(version_id)
    return tables


def test_preflight_failure_does_not_fallback_or_publish(tmp_path):
    provider = FakeProvider(unavailable=True)
    runner, catalog = make_runner(tmp_path, provider)
    result = runner.run(date(2026, 5, 7))
    assert result.status == "failed" and provider.calls == []
    assert catalog.latest_complete() is None


def test_publish_idempotent_revision_and_symbol_isolation(tmp_path):
    provider = FakeProvider(bad_symbol="510500.SH")
    runner, catalog = make_runner(tmp_path, provider)
    first = runner.run(date(2026, 5, 7), start_date=date(2026, 5, 5))
    assert first.succeeded and first.excluded_symbols == ("510500.SH",)
    assert (tmp_path / "warehouse" / "published" / first.version_id / "daily_bars_raw").is_dir()
    assert (tmp_path / "warehouse" / "published" / first.version_id / "daily_bars_adjusted").is_dir()
    same = runner.run(date(2026, 5, 7), start_date=date(2026, 5, 5))
    assert same.version_id == first.version_id and same.reused
    provider.revision = 1
    revised = runner.run(date(2026, 5, 7), start_date=date(2026, 5, 5))
    assert revised.succeeded and revised.version_id != first.version_id
    assert revised.previous_version_id == first.version_id
    assert catalog.latest_complete().version_id == revised.version_id
    assert list((tmp_path / "reports").glob("*.json"))
    assert list((tmp_path / "reports").glob("*.md"))


def test_failure_keeps_previous_complete_visible(tmp_path):
    provider = FakeProvider()
    runner, catalog = make_runner(tmp_path, provider)
    first = runner.run(date(2026, 5, 7), start_date=date(2026, 5, 7))
    provider.unavailable = True
    failed = runner.run(date(2026, 5, 8), start_date=date(2026, 5, 8))
    assert failed.status == "failed"
    assert catalog.latest_complete().version_id == first.version_id


def test_crash_after_finalize_is_failed_and_retry_uses_new_immutable_attempt(tmp_path, monkeypatch):
    provider = FakeProvider()
    runner, catalog = make_runner(tmp_path, provider)
    original = runner.store.finalize_manifest

    def finalize_then_crash(manifest):
        original(manifest)
        raise RuntimeError("injected crash after atomic rename")

    monkeypatch.setattr(runner.store, "finalize_manifest", finalize_then_crash)
    failed = runner.run(date(2026, 5, 7), start_date=date(2026, 5, 7))
    assert failed.status == "failed" and catalog.latest_complete() is None
    monkeypatch.setattr(runner.store, "finalize_manifest", original)
    retried = runner.run(date(2026, 5, 7), start_date=date(2026, 5, 7))
    assert retried.succeeded and retried.batch_id != failed.batch_id
    assert retried.version_id != failed.version_id
    assert catalog.latest_complete().version_id == retried.version_id


def test_successor_is_full_snapshot_and_preserves_excluded_symbol_history(tmp_path):
    provider = FakeProvider()
    runner, _ = make_runner(tmp_path, provider)
    first = runner.run(date(2026, 5, 5), start_date=date(2026, 5, 1))
    assert first.succeeded
    provider.bad_symbol = "510500.SH"
    second = runner.run(date(2026, 5, 7), start_date=date(2026, 5, 6))
    assert second.succeeded and second.excluded_symbols == ("510500.SH",)

    portal = DataPortal.open_latest_complete(runner.store)
    bars = portal.get_daily_bar(["510300.SH", "510500.SH"], "2026-05-01", "2026-05-07",
                                price_mode=PriceMode.RAW).reset_index()
    eligible_dates = set(bars.loc[bars["symbol"] == "510300.SH", "date"])
    excluded_dates = set(bars.loc[bars["symbol"] == "510500.SH", "date"])
    assert {"2026-05-01", "2026-05-07"}.issubset(eligible_dates)
    assert "2026-05-01" in excluded_dates
    assert "2026-05-06" not in excluded_dates and "2026-05-07" not in excluded_dates


def test_backfill_flag_changes_missing_date_resolution(tmp_path):
    provider = FakeProvider()
    runner, _ = make_runner(tmp_path / "off", provider)
    assert runner.run(date(2026, 5, 5), start_date=date(2026, 5, 5)).succeeded
    target_only = runner.run(date(2026, 5, 7), backfill_missing=False)
    assert target_only.missing_dates == ("2026-05-07",)

    provider2 = FakeProvider()
    runner2, _ = make_runner(tmp_path / "on", provider2)
    assert runner2.run(date(2026, 5, 5), start_date=date(2026, 5, 5)).succeeded
    backfilled = runner2.run(date(2026, 5, 7), backfill_missing=True)
    assert backfilled.missing_dates == ("2026-05-06", "2026-05-07")


def test_successor_carries_every_predecessor_table_and_universe_is_readable(tmp_path):
    provider = FakeProvider()
    runner, catalog = make_runner(tmp_path, provider)
    seeded = seed_complete_snapshot(runner, catalog, provider)
    provider.bad_symbol = "510500.SH"
    result = runner.run(date(2026, 5, 7), start_date=date(2026, 5, 6))
    assert result.succeeded

    _, manifest = runner.store.resolve_complete(result.version_id)
    table_names = {item.path.split("/", 1)[0] for item in manifest.files}
    assert {"calendar", "universe", "daily_bars_raw", "daily_bars_adjusted", "features",
            "quarantine", "future_table"}.issubset(table_names)
    pd.testing.assert_frame_equal(runner.store.read_table(result.version_id, "quarantine").to_pandas(),
                                  seeded["quarantine"])
    pd.testing.assert_frame_equal(runner.store.read_table(result.version_id, "future_table").to_pandas(),
                                  seeded["future_table"])
    portal = DataPortal.open_latest_complete(runner.store)
    assert portal.get_universe("2026-05-07") == ["510300.SH", "510500.SH"]
