from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path
import sys

from fundlab.common.config import get_path, load_config, resolve_config_path, validate_platform_config
from fundlab.data.pipeline import DailyUpdateRunner
from fundlab.data.platform import DataCatalog, load_universe_snapshot, stable_fingerprint
from fundlab.data.sources import XtQuantSource
from fundlab.data.sources.provider_registry import build_v1_registry
from fundlab.data.storage import VersionedParquetStore


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Publish a deterministic Data Platform v2 daily version")
    parser.add_argument("--config")
    parser.add_argument("--target-date", required=True, type=date.fromisoformat)
    parser.add_argument("--start-date", type=date.fromisoformat, help="Explicit missing-date backfill start")
    parser.add_argument("--backfill-missing", action="store_true",
                        help="Resolve and update all missing dates through the target date")
    parser.add_argument("--json", action="store_true", help="Print the machine-readable result")
    parser.add_argument("--json-output", type=Path, help="Write the machine-readable result to this path")
    parser.add_argument("--markdown-output", type=Path, help="Write the Markdown result to this path")
    return parser


def write_requested_outputs(result, *, json_output: Path | None, markdown_output: Path | None) -> None:
    if json_output:
        json_output.parent.mkdir(parents=True, exist_ok=True)
        json_output.write_text(result.to_json() + "\n", encoding="utf-8", newline="\n")
    if markdown_output:
        markdown_output.parent.mkdir(parents=True, exist_ok=True)
        markdown_output.write_text(result.to_markdown(), encoding="utf-8", newline="\n")


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        config = load_config(args.config)
        validate_platform_config(config)
        universe = load_universe_snapshot(resolve_config_path(config, config["reviewed_universe"]["config_path"]))
        catalog = DataCatalog(get_path(config, "v2_catalog")); catalog.initialize()
        root = get_path(config, "v2_published_root").parent
        runner = DailyUpdateRunner(registry=build_v1_registry(lambda: XtQuantSource(config.get("xtquant", {}))),
            provider_name=config["providers"]["enabled"][0], catalog=catalog,
            store=VersionedParquetStore(root, catalog), universe=universe,
            report_root=get_path(config, "v2_report_root"), config_hash=stable_fingerprint(config))
        result = runner.run(args.target_date, start_date=args.start_date,
                            backfill_missing=args.backfill_missing)
    except Exception as exc:
        print(f"configuration_error: {exc}", file=sys.stderr)
        return 2
    write_requested_outputs(result, json_output=args.json_output, markdown_output=args.markdown_output)
    print(result.to_json() + "\n" if args.json else result.to_markdown(), end="")
    return 0 if result.succeeded else 1


if __name__ == "__main__":
    raise SystemExit(main())
