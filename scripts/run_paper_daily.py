from __future__ import annotations

import argparse
from dataclasses import asdict
from datetime import date
import json
from pathlib import Path
import sys
from typing import Any, Mapping, Sequence

import yaml

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from fundlab.common.config import get_path, load_config, load_paper_trading_config, resolve_config_path
from fundlab.data.platform import DataCatalog
from fundlab.data.portal import DataPortal
from fundlab.data.storage import VersionedParquetStore
from fundlab.paper import (
    DailyPaperRunner,
    PaperLedgerRepository,
    StrategyRegistry,
    build_paper_report,
)


DEFAULT_CONFIG = REPOSITORY_ROOT / "config" / "paper_trading.yaml"


class JsonArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        print(json.dumps({"ok": False, "command": "parse",
                          "error": {"type": "ArgumentError", "message": message}},
                         ensure_ascii=False, sort_keys=True))
        self.exit(2)


def _raw(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as stream:
        value = yaml.safe_load(stream) or {}
    if not isinstance(value, dict):
        raise ValueError("paper trading config must be a mapping")
    return value


def _registry(raw: Mapping[str, Any]) -> StrategyRegistry:
    registry = StrategyRegistry()
    universes = raw.get("universes", {})
    if not isinstance(universes, Mapping):
        raise ValueError("universes must be a version-to-symbols mapping")
    for version, symbols in universes.items():
        if not isinstance(symbols, (list, tuple)):
            raise ValueError(f"universe {version} must be a symbol list")
        registry.register_universe(str(version), symbols)
    for item in raw.get("strategy_configs", []):
        registry.register_config(str(item["strategy_id"]), str(item["version"]), item.get("parameters", {}))
    return registry


def _portal(platform_config_path: Path) -> DataPortal:
    platform = load_config(platform_config_path)
    catalog = DataCatalog(get_path(platform, "v2_catalog"))
    store = VersionedParquetStore(get_path(platform, "v2_published_root").parent, catalog)
    return DataPortal.open_latest_complete(store)


def _write_reports(repository: PaperLedgerRepository, portal: DataPortal, report_root: Path,
                   risk_free_rate: float) -> list[dict[str, str]]:
    written: list[dict[str, str]] = []
    for account in repository.list_accounts():
        snapshots = repository.account_snapshots(account.account_id)
        benchmark = {
            str(row["trade_date"]): price
            for row in snapshots
            if (price := portal.get_close_price_for_valuation(
                account.bindings.benchmark_symbol, str(row["trade_date"])
            )) is not None
        }
        bundle = build_paper_report(
            repository, account.account_id, benchmark_closes=benchmark,
            risk_free_rate=risk_free_rate, rolling_windows=(5, 20, 60),
        )
        account_root = report_root / account.account_id
        account_root.mkdir(parents=True, exist_ok=True)
        files = {
            "json": account_root / "latest.json",
            "csv": account_root / "latest.csv",
            "markdown": account_root / "latest.md",
        }
        files["json"].write_text(bundle.to_json() + "\n", encoding="utf-8")
        files["csv"].write_text(bundle.to_csv(), encoding="utf-8", newline="")
        files["markdown"].write_text(bundle.to_markdown(), encoding="utf-8")
        written.append({"account_id": account.account_id,
                        **{kind: str(path) for kind, path in files.items()}})
    return written


def build_parser() -> argparse.ArgumentParser:
    parser = JsonArgumentParser(description="Advance FundLab paper accounts from pinned complete v2 data.")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--database", type=Path)
    dates = parser.add_mutually_exclusive_group(required=True)
    dates.add_argument("--date", help="Run one trading date (YYYY-MM-DD).")
    dates.add_argument("--target-date", help="Backfill through this trading date (YYYY-MM-DD).")
    parser.add_argument("--start-date", help="Required with --target-date.")
    parser.add_argument("--no-reports", action="store_true",
                        help="Skip canonical JSON/CSV/Markdown report generation.")
    return parser


def execute(args: argparse.Namespace) -> tuple[Mapping[str, Any], int]:
    if args.target_date and not args.start_date:
        raise ValueError("--start-date is required with --target-date")
    config_path = args.config.expanduser().resolve()
    config = load_paper_trading_config(config_path)
    raw = _raw(config_path)
    platform_path = resolve_config_path({"_config_dir": config_path.parent}, raw.get("data_platform_config", "base.yaml"))
    database = args.database.expanduser().resolve() if args.database else config.paths.database
    portal = _portal(platform_path)
    with PaperLedgerRepository(database) as repository:
        runner = DailyPaperRunner(repository, lambda *_: portal, _registry(raw))
        if args.date:
            results = (runner.run_date(args.date),)
            mode = "daily"
        else:
            results = runner.backfill(args.start_date, args.target_date)
            mode = "backfill"
        reports = [] if args.no_reports else _write_reports(
            repository, portal, config.paths.report_root, config.risk_free_rate,
        )
    payload = {"ok": all(result.status == "complete" for result in results), "command": mode,
               "database": str(database), "results": [asdict(result) for result in results],
               "reports": reports}
    return payload, 0 if payload["ok"] else 3


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        payload, code = execute(args)
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        return code
    except Exception as exc:
        print(json.dumps({"ok": False, "command": "daily" if args.date else "backfill",
                          "error": {"type": type(exc).__name__, "message": str(exc)}},
                         ensure_ascii=False, sort_keys=True))
        return 1


if __name__ == "__main__":
    sys.exit(main())
