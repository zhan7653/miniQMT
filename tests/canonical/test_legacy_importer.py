from __future__ import annotations

from datetime import date
from hashlib import sha256
import json
from pathlib import Path
import sqlite3

import pandas as pd

from fundlab.common.canonical import stable_digest
from fundlab.marketdata import LegacyV2Importer, MarketDataWarehouse, MarketTable


def test_legacy_audit_is_read_only_and_imports_only_an_incomplete_observation(tmp_path):
    legacy = tmp_path / "legacy"
    report_root = tmp_path / "reports"
    legacy.mkdir()
    report_root.mkdir()
    catalog = legacy / "catalog.sqlite3"
    connection = sqlite3.connect(catalog)
    connection.execute("""CREATE TABLE collection_partitions(
        partition_id TEXT,symbol TEXT,price_mode TEXT,start_date TEXT,end_date TEXT,status TEXT,
        row_count INTEGER,checksum TEXT,storage_path TEXT
    )""")
    connection.execute("""CREATE TABLE collection_runs(
        run_id TEXT,provider TEXT,phase TEXT,target_date TEXT,config_hash TEXT,status TEXT
    )""")
    connection.execute("CREATE TABLE throttle_events(run_id TEXT,reason TEXT)")
    rows = []
    for mode in ("raw", "adjusted"):
        partition_id = f"partition-{mode}"
        directory = legacy / "raw" / "partitions" / partition_id
        directory.mkdir(parents=True)
        frame = pd.DataFrame([{
            "date": "2026-07-13", "symbol": "510300.SH", "open": 4.0, "high": 4.1,
            "low": 3.9, "close": 4.0, "volume": 1000, "amount": 4000.0, "pre_close": 3.9,
            "suspended": False, "price_mode": mode, "limit_up": None, "limit_down": None,
            "source": "xtquant", "updated_at": "2026-07-13T16:00:00",
        }])
        parquet = directory / "part-0.parquet"
        frame.to_parquet(parquet, index=False)
        checksum = _hash(parquet)
        (directory / "partition.json").write_text(json.dumps({
            "identity": {"fingerprint": partition_id}, "checksum": checksum, "row_count": 1,
        }), encoding="utf-8")
        rows.append((partition_id, "510300.SH", mode, "2026-07-13", "2026-07-13",
                     "complete", 1, checksum, str(directory)))
    connection.executemany("INSERT INTO collection_partitions VALUES (?,?,?,?,?,?,?,?,?)", rows)
    connection.commit()
    connection.close()
    master = [{
        "symbol": "510300.SH", "exchange": "SH", "product_type": "ETF",
        "name": "fixture ETF", "listed_date": "2012-05-28", "delisted_date": None,
        "lot_size": 100, "price_tick": 0.001, "management_type": "passive_index",
    }]
    calendar = ["2026-07-13"]
    collect_evidence = {
        "partition_fingerprints": [item[0] for item in rows],
        "run_id": "legacy-run", "provider": "xtquant", "target_date": "2026-07-13",
        "config_hash": "config", "selected_symbols": ["510300.SH"], "discovered_count": 1,
        "quarantined_symbols": {}, "missing_dates": {}, "instrument_coverage": 0.9,
        "expected_day_coverage": 0.97, "coverage_gates_passed": False,
        "legacy_manifest_before": {}, "legacy_manifest_after": {},
        "fund_master": master, "calendar": calendar,
    }
    evidence_hash = stable_digest(collect_evidence)
    connection = sqlite3.connect(catalog)
    connection.execute(
        "INSERT INTO collection_runs VALUES (?,?,?,?,?,?)",
        ("legacy-run", "xtquant", "collect", "2026-07-13", "config", "complete"),
    )
    connection.execute(
        "INSERT INTO throttle_events VALUES (?,?)",
        ("legacy-run", f"collect_evidence_sha256:{evidence_hash}"),
    )
    connection.commit()
    connection.close()
    report = {
        "run_id": "legacy-run", "status": "complete", "target_date": "2026-07-13",
        "coverage_gates_passed": False,
        "discovered_count": 1, "selected_symbols": ["510300.SH"],
        "instrument_coverage": 0.9, "expected_day_coverage": 0.97,
        "quarantined_symbols": {}, "missing_dates": {},
        "legacy_manifest_before": {}, "legacy_manifest_after": {},
        "evidence": {
            "collect_evidence": collect_evidence,
            "collect_evidence_sha256": evidence_hash,
            "calendar": calendar,
            "fund_master": master,
        },
    }
    (report_root / "full-market-collect-fixture.json").write_text(
        json.dumps(report), encoding="utf-8",
    )
    protected = tmp_path / "fundlab.db"
    protected.write_bytes(b"protected")

    importer = LegacyV2Importer(
        legacy_root=legacy, legacy_report_root=report_root, protected_v1_db=protected,
    )
    audit = importer.audit()
    assert audit.source_import_ready and not audit.canonical_ready and audit.protected_unchanged
    assert audit.raw_adjusted_pairs == 1 and audit.complete_rows == 2
    assert audit.unbound_complete_partitions == 0
    assert "legacy_coverage_gates_failed" in audit.blockers
    assert protected.read_bytes() == b"protected"

    warehouse = MarketDataWarehouse(tmp_path / "canonical")
    observed = importer.import_source_observation(
        warehouse, audit=audit, allow_incomplete_source=True,
    )
    assert {item.table for item in observed.files} == {
        MarketTable.INSTRUMENTS, MarketTable.CALENDAR, MarketTable.DAILY_BARS,
    }
    assert all(not claim.complete for claim in observed.coverage)
    bars = warehouse.read_observation_table(observed.observation_id, MarketTable.DAILY_BARS)
    assert set(bars["price_mode"]) == {"raw", "adjusted"}
    assert set(bars["source_provider"]) == {"xtquant-legacy-import"}


def _hash(path: Path) -> str:
    return sha256(path.read_bytes()).hexdigest()
