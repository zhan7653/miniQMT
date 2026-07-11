from datetime import date, datetime, timezone
from hashlib import sha256

import pyarrow as pa
import pytest

from fundlab.data.platform import (BatchRecord, BatchStatus, DataCatalog, ManifestIdentity, TrustState,
                                   VersionRecord, VersionStatus)
from fundlab.data.storage import (CorruptVersionError, ImmutableVersionError, VersionedParquetStore,
                                  VersionNotVisibleError)


def _catalog(tmp_path):
    catalog = DataCatalog(tmp_path / "catalog.sqlite")
    catalog.initialize()
    catalog.create_batch(BatchRecord("b1", "mini_qmt", date(2026, 1, 1), date(2026, 1, 2), ("A",),
                                     "u1", "cfg", "request", BatchStatus.PENDING, 0, None))
    return catalog


def _stage(store, version="v1"):
    store.begin_version(version)
    table = pa.table({"date": ["2026-01-02"], "symbol": ["A"], "close": [10.0]})
    store.write_table(version, "daily_bars_raw", table)
    identity = ManifestIdentity("mini_qmt", "b1", version, datetime.now(timezone.utc), TrustState.TRUSTED,
                                "content-1", 1, {})
    return identity


def test_building_and_failed_versions_are_invisible_without_scanning(tmp_path):
    catalog = _catalog(tmp_path)
    store = VersionedParquetStore(tmp_path / "v2", catalog)
    identity = _stage(store)
    manifest = store.prepare_manifest(identity, {"daily_bars_raw": 1})
    catalog.create_version(VersionRecord("v1", "b1", manifest.fingerprint, "content-1",
                                         VersionStatus.BUILDING, None, None, None))
    store.finalize_manifest(manifest)
    with pytest.raises(VersionNotVisibleError):
        store.resolve_complete("v1")
    catalog.fail_version("v1", "quality failed")
    with pytest.raises(VersionNotVisibleError):
        store.resolve_complete("v1")


def test_complete_version_checksum_corruption_is_typed(tmp_path):
    catalog = _catalog(tmp_path)
    store = VersionedParquetStore(tmp_path / "v2", catalog)
    identity = _stage(store)
    manifest = store.prepare_manifest(identity, {"daily_bars_raw": 1})
    catalog.create_version(VersionRecord("v1", "b1", manifest.fingerprint, "content-1",
                                         VersionStatus.BUILDING, None, None, None))
    store.finalize_manifest(manifest)
    catalog.complete_version("v1")
    assert store.resolve_complete()[0] == "v1"
    parquet = next(store.published_path("v1").rglob("*.parquet"))
    parquet.write_bytes(parquet.read_bytes() + b"corrupt")
    with pytest.raises(CorruptVersionError, match="checksum"):
        store.resolve_complete("v1")


def test_published_and_raw_paths_are_immutable(tmp_path):
    catalog = _catalog(tmp_path)
    store = VersionedParquetStore(tmp_path / "v2", catalog)
    identity = _stage(store)
    manifest = store.prepare_manifest(identity, {"daily_bars_raw": 1})
    store.finalize_manifest(manifest)
    with pytest.raises(ImmutableVersionError):
        store.begin_version("v1")
    table = pa.table({"x": [1]})
    store.write_raw("b1", "bars", table)
    with pytest.raises(ImmutableVersionError):
        store.write_raw("b1", "bars", table)


def test_explicit_superseded_pin_remains_readable_but_latest_discovers_successor(tmp_path):
    catalog = _catalog(tmp_path)
    store = VersionedParquetStore(tmp_path / "v2", catalog)
    identity = _stage(store)
    manifest = store.prepare_manifest(identity, {"daily_bars_raw": 1})
    catalog.create_version(VersionRecord("v1", "b1", manifest.fingerprint, "content-1",
                                         VersionStatus.BUILDING, None, None, None))
    store.finalize_manifest(manifest)
    catalog.complete_version("v1")

    catalog.create_batch(BatchRecord("b2", "mini_qmt", date(2026, 1, 3), date(2026, 1, 3), ("A",),
                                     "u1", "cfg", "request-2", BatchStatus.PENDING, 0, None))
    store.begin_version("v2")
    store.write_table("v2", "daily_bars_raw",
                      pa.table({"date": ["2026-01-03"], "symbol": ["A"], "close": [11.0]}))
    identity2 = ManifestIdentity("mini_qmt", "b2", "v2", datetime.now(timezone.utc), TrustState.TRUSTED,
                                 "content-2", 1, {})
    manifest2 = store.prepare_manifest(identity2, {"daily_bars_raw": 1})
    catalog.create_version(VersionRecord("v2", "b2", manifest2.fingerprint, "content-2",
                                         VersionStatus.BUILDING, "v1", None, None))
    store.finalize_manifest(manifest2)
    catalog.complete_version("v2")

    assert store.resolve_complete()[0] == "v2"
    assert store.resolve_complete("v1")[0] == "v1"
    assert store.read_table("v1", "daily_bars_raw").column("close").to_pylist() == [10.0]

    parquet = next(store.published_path("v1").rglob("*.parquet"))
    parquet.write_bytes(parquet.read_bytes() + b"corrupt")
    with pytest.raises(CorruptVersionError):
        store.resolve_complete("v1")
