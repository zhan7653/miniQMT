from __future__ import annotations

from datetime import date, datetime
from hashlib import sha256
from pathlib import Path

import pandas as pd
import pytest

from fundlab.data.pipeline import DailyUpdateRunner
from fundlab.data.platform import (DataCatalog, PreflightResult, ProviderCapability, ProviderHealth,
                                   ProviderResult, SymbolResult, UniverseSnapshot)
from fundlab.data.portal import DataPortal
from fundlab.data.sources.provider_registry import ProviderRegistry
from fundlab.data.storage import VersionNotVisibleError, VersionedParquetStore


pytestmark = pytest.mark.system


class FaultProvider:
    name = "xtquant"
    capabilities = frozenset({ProviderCapability.TRADING_CALENDAR, ProviderCapability.DAILY_BARS_RAW,
                              ProviderCapability.DAILY_BARS_ADJUSTED})

    def __init__(self, *, unavailable=False, revision=0):
        self.unavailable, self.revision, self.calls = unavailable, revision, []

    def preflight(self):
        health = ProviderHealth.SERVICE_UNAVAILABLE if self.unavailable else ProviderHealth.AVAILABLE
        return PreflightResult(self.name, health, datetime.now().astimezone(), "injected outage" if self.unavailable else None)

    def fetch(self, request):
        self.calls.append(request.capability)
        days = pd.bdate_range(request.start_date, request.end_date).strftime("%Y-%m-%d")
        if request.capability is ProviderCapability.TRADING_CALENDAR:
            frame = pd.DataFrame({"date": days})
        else:
            frame = pd.DataFrame([{"date": day, "symbol": symbol, "open": 10.0, "high": 10.1,
                "low": 9.9, "close": 10.0 + self.revision / 1000, "volume": 1000,
                "amount": 10000, "suspended": False} for symbol in request.symbols for day in days])
        statuses = tuple(SymbolResult(symbol, len(days)) for symbol in request.symbols)
        return frame, ProviderResult(self.name, request.capability, statuses, datetime.now().astimezone())


def runner(tmp_path, provider):
    catalog = DataCatalog(tmp_path / "catalog.sqlite3"); catalog.initialize()
    registry = ProviderRegistry(); registry.register(provider)
    universe = UniverseSnapshot.create(version="u1", effective_date=date(2026, 1, 1),
        symbols=("510300.SH", "510500.SH"), benchmarks=("510300.SH",), configuration={"fixed": True})
    return DailyUpdateRunner(registry=registry, provider_name="xtquant", catalog=catalog,
        store=VersionedParquetStore(tmp_path / "warehouse", catalog), universe=universe,
        report_root=tmp_path / "reports", config_hash="fixed"), catalog


def test_outage_has_no_fallback_and_no_incomplete_visibility(tmp_path):
    provider = FaultProvider(unavailable=True)
    subject, catalog = runner(tmp_path, provider)
    result = subject.run(date(2026, 5, 7))
    assert result.status == "failed" and provider.calls == []
    assert catalog.latest_complete() is None
    with pytest.raises(VersionNotVisibleError):
        DataPortal.open_latest_complete(subject.store)


def test_crash_retry_revision_and_complete_only_visibility(tmp_path, monkeypatch):
    provider = FaultProvider()
    subject, catalog = runner(tmp_path, provider)
    original = subject.store.finalize_manifest
    monkeypatch.setattr(subject.store, "finalize_manifest", lambda manifest: (_ for _ in ()).throw(OSError("crash boundary")))
    failed = subject.run(date(2026, 5, 7), start_date=date(2026, 5, 7))
    assert failed.status == "failed" and catalog.latest_complete() is None
    monkeypatch.setattr(subject.store, "finalize_manifest", original)
    first = subject.run(date(2026, 5, 7), start_date=date(2026, 5, 7))
    assert first.succeeded and first.version_id != failed.version_id
    repeated = subject.run(date(2026, 5, 7), start_date=date(2026, 5, 7))
    assert repeated.reused and repeated.version_id == first.version_id
    provider.revision = 1
    revised = subject.run(date(2026, 5, 7), start_date=date(2026, 5, 7))
    assert revised.succeeded and revised.previous_version_id == first.version_id


def test_legacy_artifacts_are_byte_immutable(tmp_path):
    legacy = tmp_path / "legacy"; legacy.mkdir()
    files = [legacy / "fundlab.db", legacy / "part.parquet"]
    for index, path in enumerate(files):
        path.write_bytes((b"legacy-read-only-" + bytes([index])) * 32)
    before = {path: sha256(path.read_bytes()).hexdigest() for path in files}
    subject, _ = runner(tmp_path / "isolated-v2", FaultProvider())
    assert subject.run(date(2026, 5, 7), start_date=date(2026, 5, 7)).succeeded
    assert {path: sha256(path.read_bytes()).hexdigest() for path in files} == before
