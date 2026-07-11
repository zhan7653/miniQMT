from datetime import date, datetime, timedelta

import pandas as pd

from fundlab.data.pipeline import DailyUpdateRunner
from fundlab.data.platform import (DataCatalog, PreflightResult, ProviderCapability, ProviderHealth,
                                   ProviderResult, SymbolResult, UniverseSnapshot)
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
