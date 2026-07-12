from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path
import sys

from fundlab.common.config import get_path, load_config, validate_platform_config
from fundlab.data.pipeline import FullMarketHistoryRunner, HistoryRunSpec
from fundlab.data.platform import CollectionPhase, DataCatalog, stable_fingerprint
from fundlab.data.sources import XtQuantSource
from fundlab.data.storage import VersionedParquetStore


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Resumable full-market fund history bootstrap")
    parser.add_argument("command", nargs="?", choices=("preflight", "discover", "canary", "collect", "publish"))
    parser.add_argument("--phase", choices=("preflight", "discover", "canary", "collect", "publish"))
    parser.add_argument("--config")
    parser.add_argument("--target-date", type=date.fromisoformat)
    parser.add_argument("--sample-limit", type=int, default=20)
    parser.add_argument("--approved-speed-level", default="initial")
    parser.add_argument("--no-publish", action="store_true")
    parser.add_argument("--minimum-free-gb", type=float, default=5.0)
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--json-output", type=Path)
    parser.add_argument("--markdown-output", type=Path)
    return parser


def _write_requested_outputs(result, args) -> None:
    if args.json_output:
        args.json_output.parent.mkdir(parents=True, exist_ok=True)
        args.json_output.write_text(result.to_json() + "\n", encoding="utf-8", newline="\n")
    if args.markdown_output:
        args.markdown_output.parent.mkdir(parents=True, exist_ok=True)
        args.markdown_output.write_text(result.to_markdown(), encoding="utf-8", newline="\n")


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    phase_name = args.phase or args.command
    if not phase_name:
        print("configuration_error: one of command or --phase is required", file=sys.stderr)
        return 2
    try:
        config = load_config(args.config)
        validate_platform_config(config)
        catalog = DataCatalog(get_path(config, "v2_catalog")); catalog.initialize()
        store = VersionedParquetStore(get_path(config, "v2_published_root").parent, catalog)
        provider = XtQuantSource(config.get("xtquant", {}))
        runner = FullMarketHistoryRunner(
            provider=provider, catalog=catalog, store=store,
            report_root=get_path(config, "v2_report_root"),
            history_config=config["full_market_history"], config_hash=stable_fingerprint(config),
            legacy_paths=(get_path(config, "sqlite_db"), get_path(config, "parquet_root")),
        )
        minimum_free_bytes = max(0, int(args.minimum_free_gb * 1024**3))
        if phase_name == "preflight":
            result = runner.preflight(minimum_free_bytes=minimum_free_bytes)
        else:
            phase = CollectionPhase(phase_name)
            publish = phase is CollectionPhase.PUBLISH and not args.no_publish
            result = runner.run(HistoryRunSpec(
                phase, args.target_date, args.sample_limit, publish,
                args.approved_speed_level, minimum_free_bytes,
            ))
    except Exception as exc:
        print(f"configuration_error: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2
    _write_requested_outputs(result, args)
    print(result.to_json() + "\n" if args.json else result.to_markdown(), end="")
    return 0 if result.succeeded else 1


if __name__ == "__main__":
    raise SystemExit(main())
