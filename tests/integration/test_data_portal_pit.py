from datetime import date

import pandas as pd
import pyarrow as pa

from fundlab.common.dates import audit_now
from fundlab.data.platform import (
    BatchRecord, BatchStatus, DataCatalog, ManifestIdentity, TrustState,
    VersionRecord, VersionStatus,
)
from fundlab.data.portal import DataPortal
from fundlab.data.storage import VersionedParquetStore
from scripts.create_fake_data import create_fake_v2_portal


def test_legacy_pit_datasets_are_absent_from_normal_v2_portal(tmp_path):
    portal = create_fake_v2_portal(tmp_path / "v2")
    for method in ("get_index_valuation", "get_nav", "get_premium_discount", "get_dividends"):
        assert not hasattr(portal, method)


def _published_portal(tmp_path, tables):
    catalog = DataCatalog(tmp_path / "catalog.sqlite3"); catalog.initialize()
    store = VersionedParquetStore(tmp_path, catalog)
    batch = BatchRecord(
        "b1", "fixture", date(2020, 1, 1), date(2024, 1, 1), ("A.SH",),
        "fixture", "fixture", "request", BatchStatus.PENDING, 0, None,
    )
    catalog.create_batch(batch); catalog.transition_batch("b1", BatchStatus.RUNNING)
    store.begin_version("v1")
    for name, frame in tables.items():
        store.write_table("v1", name, pa.Table.from_pandas(frame, preserve_index=False))
    counts = {name: len(frame) for name, frame in tables.items()}
    identity = ManifestIdentity("fixture", "b1", "v1", audit_now(), TrustState.TRUSTED, "content", 1, counts)
    manifest = store.prepare_manifest(identity, counts)
    catalog.create_version(VersionRecord(
        "v1", "b1", manifest.fingerprint, "content", VersionStatus.BUILDING, None, None, None,
    ))
    store.finalize_manifest(manifest)
    catalog.transition_batch("b1", BatchStatus.COMPLETE, row_count=sum(counts.values()))
    catalog.complete_version("v1")
    return DataPortal(store, "v1")


def test_full_market_pit_uses_listing_delisting_intervals_and_trust_state(tmp_path):
    portal = _published_portal(tmp_path / "pit", {
        "fund_master": pd.DataFrame([
            {"symbol": "OLD.SH", "listed_date": "2020-01-01", "delisted_date": "2022-12-31", "trust_state": "trusted"},
            {"symbol": "NEW.SH", "listed_date": "2023-01-01", "delisted_date": None, "trust_state": "trusted"},
            {"symbol": "BAD.SH", "listed_date": "2020-01-01", "delisted_date": None, "trust_state": "quarantined"},
        ]),
        "universe": pd.DataFrame({"symbol": ["NEW.SH"], "effective_date": ["2024-01-01"]}),
    })
    assert portal.get_universe("2021-01-01") == ["OLD.SH"]
    assert portal.get_universe("2024-01-01") == ["NEW.SH"]


def test_existing_version_without_fund_master_keeps_universe_snapshot_fallback(tmp_path):
    portal = _published_portal(tmp_path / "fallback", {
        "universe": pd.DataFrame({
            "symbol": ["A.SH", "A.SH", "B.SH"],
            "effective_date": ["2020-01-01", "2021-01-01", "2021-01-01"],
        }),
    })
    assert portal.get_universe("2020-06-01") == ["A.SH"]
    assert portal.get_universe("2021-06-01") == ["A.SH", "B.SH"]
