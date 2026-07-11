from __future__ import annotations

import argparse
import json
from pathlib import Path

from fundlab.data.migration import LegacyV1Migrator
from fundlab.data.migration.v1_migrator import LegacyV1Reader
from fundlab.data.platform import DataCatalog, load_universe_snapshot
from fundlab.data.sources.xtquant_source import XtQuantSource
from fundlab.data.storage.versioned_parquet_store import VersionedParquetStore


def main() -> int:
    parser = argparse.ArgumentParser(description="Bootstrap Data Platform v2 from read-only v1 artifacts")
    parser.add_argument("--sqlite", default="data/warehouse/sqlite/fundlab.db")
    parser.add_argument("--parquet", default="data/warehouse/parquet/fund_daily_bar")
    parser.add_argument("--v2-root", default="data/warehouse/v2")
    parser.add_argument("--report-root", default="data/reports/data_v2/migration")
    parser.add_argument("--universe", default="config/universe.yaml")
    parser.add_argument("--expected-hashes", help="JSON map of canonical path to SHA-256")
    args = parser.parse_args()
    universe = load_universe_snapshot(args.universe)
    root = Path(args.v2_root)
    catalog = DataCatalog(root / "catalog.sqlite")
    catalog.initialize()
    store = VersionedParquetStore(root, catalog)
    expected = json.loads(Path(args.expected_hashes).read_text(encoding="utf-8")) if args.expected_hashes else None
    try:
        result = LegacyV1Migrator(reader=LegacyV1Reader(args.sqlite, args.parquet), provider=XtQuantSource(),
                                  catalog=catalog, store=store, report_root=args.report_root).migrate(
            active_symbols=universe.symbols, universe_version=universe.version,
            config_hash=universe.config_hash, expected_hashes=expected,
        )
        payload, exit_code = result.to_dict(), 0 if result.status in {"complete", "already_complete"} else 1
    except Exception as exc:
        payload, exit_code = {"status": "failed", "error": str(exc), "version_id": None, "batch_id": None}, 1
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
