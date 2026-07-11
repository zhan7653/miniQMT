from datetime import date, datetime, timezone

import pyarrow as pa
import pytest

from fundlab.data.platform import (BatchRecord, BatchStatus, DataCatalog, ManifestIdentity, PriceMode, TrustState,
                                   VersionRecord, VersionStatus)
from fundlab.data.portal import AdjustedDataUnavailable, DataPortal, InvalidPriceMode, LegacyDataPortal
from fundlab.data.storage import VersionedParquetStore


def _published(tmp_path, *, adjusted=True):
    catalog = DataCatalog(tmp_path / "catalog.sqlite")
    catalog.initialize()
    catalog.create_batch(BatchRecord("b1", "mini_qmt", date(2026, 1, 1), date(2026, 1, 3), ("A",),
                                     "u1", "cfg", "request", BatchStatus.PENDING, 0, None))
    store = VersionedParquetStore(tmp_path / "v2", catalog)
    store.begin_version("v1")
    raw = pa.table({"date": ["2026-01-02", "2026-01-03"], "symbol": ["A", "A"],
                    "open": [10.0, 11.0], "close": [10.5, 11.5], "extra": [1, 2]})
    store.write_table("v1", "daily_bars_raw", raw)
    if adjusted:
        store.write_table("v1", "daily_bars_adjusted", raw.set_column(3, "close", pa.array([105.0, 115.0])))
    store.write_table("v1", "calendar", pa.table({"date": ["2026-01-02", "2026-01-03"],
                                                    "is_trading_day": [True, True]}))
    store.write_table("v1", "universe", pa.table({"symbol": ["A"], "effective_date": ["2026-01-01"]}))
    identity = ManifestIdentity("mini_qmt", "b1", "v1", datetime.now(timezone.utc), TrustState.TRUSTED,
                                "content-1", 1, {})
    manifest = store.prepare_manifest(identity, {})
    catalog.create_version(VersionRecord("v1", "b1", manifest.fingerprint, "content-1",
                                         VersionStatus.BUILDING, None, None, None))
    store.finalize_manifest(manifest)
    catalog.complete_version("v1")
    return store


def test_portal_pins_complete_version_requires_explicit_mode_and_projects(tmp_path):
    portal = DataPortal.open_latest_complete(_published(tmp_path))
    assert portal.data_version == "v1"
    with pytest.raises(TypeError):
        portal.get_daily_bar(["A"], "2026-01-02", "2026-01-03")
    with pytest.raises(InvalidPriceMode):
        portal.get_daily_bar(["A"], "2026-01-02", "2026-01-03", price_mode="raw")
    bars = portal.get_daily_bar(["A"], "2026-01-02", "2026-01-02", fields=["close"],
                                price_mode=PriceMode.RAW)
    assert list(bars.columns) == ["close"]
    assert bars.iloc[0, 0] == 10.5
    assert portal.get_open_price_for_execution("A", "2026-01-02") == 10.0


def test_adjusted_missing_never_falls_back_to_raw(tmp_path):
    portal = DataPortal.open_latest_complete(_published(tmp_path, adjusted=False))
    with pytest.raises(AdjustedDataUnavailable):
        portal.get_daily_bar(["A"], "2026-01-02", "2026-01-03", price_mode=PriceMode.ADJUSTED)


def test_snapshot_caches_identical_reads_and_returns_defensive_copy(tmp_path, monkeypatch):
    store = _published(tmp_path)
    portal = DataPortal.open_latest_complete(store)
    calls = 0
    original = store.read_table
    def counted(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)
    monkeypatch.setattr(store, "read_table", counted)
    first = portal.get_daily_bar(["A"], "2026-01-02", "2026-01-03", price_mode=PriceMode.RAW)
    first.iloc[0, 0] = 999
    second = portal.get_daily_bar(["A"], "2026-01-02", "2026-01-03", price_mode=PriceMode.RAW)
    assert calls == 1
    assert second.iloc[0]["open"] == 10.0


def test_legacy_facade_rejects_mutation_methods_and_attributes():
    class Old:
        def get_price(self): return 1
        def update_data(self): return None
    legacy = LegacyDataPortal(Old())
    assert legacy.get_price() == 1
    with pytest.raises(AttributeError):
        legacy.update_data
    with pytest.raises(AttributeError):
        legacy.new_attribute = 1


def test_legacy_constructor_compatibility_is_calendar_only():
    class SQLite:
        def read_frame(self, query, params):
            import pandas as pd
            return pd.DataFrame({"is_trading_day": [1]})
    portal = DataPortal(sqlite_store=SQLite(), parquet_store=object())
    assert portal.data_version is None
    assert portal.is_trading_day("2026-01-02") is True
    with pytest.raises(RuntimeError, match="calendar queries only"):
        portal.snapshot()
