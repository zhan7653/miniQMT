from __future__ import annotations

import argparse
from datetime import date
import json
from pathlib import Path
import sys

from fundlab.common.config import get_path, load_config, resolve_config_path, validate_platform_config
from fundlab.data.migration import LegacyV1Migrator
from fundlab.data.migration.v1_migrator import LegacyV1Reader
from fundlab.data.platform import DataCatalog, load_universe_snapshot
from fundlab.data.sources.xtquant_source import XtQuantSource
from fundlab.data.storage.versioned_parquet_store import VersionedParquetStore


EXIT_SUCCESS = 0
EXIT_MIGRATION_FAILED = 1
EXIT_CONFIGURATION_ERROR = 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Bootstrap Data Platform v2 from read-only v1 artifacts")
    parser.add_argument("--config", required=True, help="Data Platform configuration YAML")
    parser.add_argument("--target-date", required=True, help="Inclusive migration target in YYYY-MM-DD form")
    parser.add_argument("--json-output", required=True, help="Machine-readable migration evidence path")
    parser.add_argument("--markdown-output", required=True, help="Operator-readable migration evidence path")
    parser.add_argument("--sqlite", help="Compatible override for the read-only legacy SQLite path")
    parser.add_argument("--parquet", help="Compatible override for the legacy fund_daily_bar root")
    parser.add_argument("--v2-root", help="Compatible override for the v2 warehouse root")
    parser.add_argument("--report-root", help="Compatible override for migration rollback reports")
    parser.add_argument("--universe", help="Compatible override for the reviewed universe YAML")
    parser.add_argument("--expected-hashes", help="JSON map of canonical path to SHA-256")
    return parser


def _write_reports(payload: dict, json_output: Path, markdown_output: Path) -> None:
    json_output.parent.mkdir(parents=True, exist_ok=True)
    markdown_output.parent.mkdir(parents=True, exist_ok=True)
    json_output.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    reconciliation = payload.get("reconciliation") or {}
    hashes = payload.get("legacy", {}).get("hashes", {})
    quarantine = payload.get("quarantined") or []
    lines = [
        "# Data Platform v2 migration evidence", "", f"- Result: `{payload['status']}`",
        f"- Target date: `{payload.get('target_date')}`", f"- Version: `{payload.get('version_id')}`",
        f"- Batch: `{payload.get('batch_id')}`", f"- Legacy backtest runs: `{payload.get('legacy', {}).get('backtest_run_count')}`",
        f"- Quarantined rows: `{len(quarantine)}`", "", "## Five legacy v1 hashes", "",
    ]
    lines.extend(f"- `{path}`: `{digest}`" for path, digest in sorted(hashes.items()))
    lines.extend(["", "## Reconciliation", ""])
    lines.extend(f"- {key}: `{value}`" for key, value in sorted(reconciliation.items()))
    if payload.get("error"):
        lines.extend(["", "## Failure", "", str(payload["error"])])
    markdown_output.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    json_output, markdown_output = Path(args.json_output).resolve(), Path(args.markdown_output).resolve()
    payload: dict = {"status": "failed", "target_date": args.target_date, "version_id": None, "batch_id": None,
                     "reconciliation": None, "quarantined": [], "legacy": {"hashes": {}, "backtest_run_count": None}}
    try:
        target_date = date.fromisoformat(args.target_date)
        config = load_config(args.config)
        validate_platform_config(config)
        sqlite_path = Path(args.sqlite).resolve() if args.sqlite else get_path(config, "sqlite_db")
        parquet_path = Path(args.parquet).resolve() if args.parquet else get_path(config, "parquet_root") / "fund_daily_bar"
        catalog_path = get_path(config, "v2_catalog")
        v2_root = Path(args.v2_root).resolve() if args.v2_root else catalog_path.parent
        report_root = Path(args.report_root).resolve() if args.report_root else get_path(config, "v2_report_root") / "migration"
        universe_path = (Path(args.universe).resolve() if args.universe else
                         resolve_config_path(config, config["reviewed_universe"]["config_path"]))
        universe = load_universe_snapshot(universe_path)
        reader = LegacyV1Reader(sqlite_path, parquet_path)
        payload["legacy"] = {"hashes": reader.hashes(), "backtest_run_count": reader.backtest_run_count()}
        catalog = DataCatalog(catalog_path if not args.v2_root else v2_root / "catalog.sqlite")
        catalog.initialize()
        expected = json.loads(Path(args.expected_hashes).read_text(encoding="utf-8")) if args.expected_hashes else None
        result = LegacyV1Migrator(reader=reader, provider=XtQuantSource(), catalog=catalog,
                                  store=VersionedParquetStore(v2_root, catalog), report_root=report_root).migrate(
            active_symbols=universe.symbols, universe_version=universe.version, config_hash=universe.config_hash,
            expected_hashes=expected, target_date=target_date,
            universe_effective_date=universe.effective_date,
        )
        payload.update(result.to_dict())
        payload["target_date"] = target_date.isoformat()
        exit_code = EXIT_SUCCESS if result.status in {"complete", "already_complete"} else EXIT_MIGRATION_FAILED
    except (FileNotFoundError, KeyError, ValueError, json.JSONDecodeError) as exc:
        payload["error"], exit_code = str(exc), EXIT_CONFIGURATION_ERROR
    except Exception as exc:
        payload["error"], exit_code = str(exc), EXIT_MIGRATION_FAILED
    _write_reports(payload, json_output, markdown_output)
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
