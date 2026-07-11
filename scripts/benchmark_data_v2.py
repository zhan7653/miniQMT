from __future__ import annotations

import argparse
from datetime import date, datetime
import json
from pathlib import Path
import tempfile
from time import perf_counter

import pandas as pd

from fundlab.backtest import BacktestEngine
from fundlab.data.pipeline import DailyUpdateRunner
from fundlab.data.platform import (DataCatalog, PreflightResult, PriceMode, ProviderCapability,
                                   ProviderHealth, ProviderResult, SymbolResult, UniverseSnapshot)
from fundlab.data.portal import DataPortal
from fundlab.data.sources.provider_registry import ProviderRegistry
from fundlab.data.storage import VersionedParquetStore
from fundlab.strategies import EqualWeightStrategy
from tests.performance.data_v2_fixture import SYMBOLS, market_frames, publish_fixture


THRESHOLDS = {"backtest_seconds": 10.0, "snapshot_cold_seconds": 1.0,
              "snapshot_repeat_seconds": 0.1, "feature_publish_seconds": 60.0}


class LocalProvider:
    name = "xtquant"
    capabilities = frozenset({ProviderCapability.TRADING_CALENDAR, ProviderCapability.DAILY_BARS_RAW,
                              ProviderCapability.DAILY_BARS_ADJUSTED})

    def __init__(self, raw, adjusted, calendar):
        self.raw, self.adjusted, self.calendar = raw, adjusted, calendar

    def preflight(self):
        return PreflightResult(self.name, ProviderHealth.AVAILABLE, datetime.now().astimezone(), "fixed local fixture")

    def fetch(self, request):
        source = self.calendar if request.capability is ProviderCapability.TRADING_CALENDAR else (
            self.raw if request.capability is ProviderCapability.DAILY_BARS_RAW else self.adjusted)
        frame = source[(source.date >= request.start_date.isoformat()) & (source.date <= request.end_date.isoformat())].copy()
        if "symbol" in frame:
            frame = frame[frame.symbol.isin(request.symbols)]
        statuses = tuple(SymbolResult(symbol, len(frame) if "symbol" not in frame else int((frame.symbol == symbol).sum()))
                         for symbol in request.symbols)
        return frame, ProviderResult(self.name, request.capability, statuses, datetime.now().astimezone())


def run_benchmarks(root: Path) -> dict[str, float]:
    fixture_root = root / "read"
    store, version_id, days = publish_fixture(fixture_root)
    portal = DataPortal.open_version(store, version_id)

    started = perf_counter()
    BacktestEngine(portal, EqualWeightStrategy(list(SYMBOLS[:3])), days[0], days[-1]).run()
    backtest = perf_counter() - started

    query_date = days[-1]
    fresh = DataPortal.open_version(store, version_id)
    started = perf_counter()
    frame = fresh.get_daily_bar(SYMBOLS, query_date, query_date, fields=["close"], price_mode=PriceMode.RAW)
    cold = perf_counter() - started
    started = perf_counter()
    repeated = fresh.get_daily_bar(SYMBOLS, query_date, query_date, fields=["close"], price_mode=PriceMode.RAW)
    repeat = perf_counter() - started
    assert len(frame) == len(repeated) == 30

    _, raw, adjusted, calendar, _, _ = market_frames(SYMBOLS, periods=260)
    publish_root = root / "publish"
    catalog = DataCatalog(publish_root / "catalog.sqlite3"); catalog.initialize()
    registry = ProviderRegistry(); registry.register(LocalProvider(raw, adjusted, calendar))
    universe = UniverseSnapshot.create(version="benchmark-u1", effective_date=date.fromisoformat(calendar.date.iloc[0]),
        symbols=SYMBOLS, benchmarks=(SYMBOLS[0],), configuration={"fixed_local": True})
    runner = DailyUpdateRunner(registry=registry, provider_name="xtquant", catalog=catalog,
        store=VersionedParquetStore(publish_root / "warehouse", catalog), universe=universe,
        report_root=publish_root / "reports", config_hash="benchmark", warmup_days=120)
    started = perf_counter()
    result = runner.run(date.fromisoformat(calendar.date.iloc[-1]), start_date=date.fromisoformat(calendar.date.iloc[-1]))
    feature_publish = perf_counter() - started
    if not result.succeeded:
        raise RuntimeError(result.error or "local feature publication failed")
    return {"backtest_seconds": backtest, "snapshot_cold_seconds": cold,
            "snapshot_repeat_seconds": repeat, "feature_publish_seconds": feature_publish}


def main(argv=None):
    parser = argparse.ArgumentParser(description="Run deterministic local Data Platform v2 performance evidence")
    parser.add_argument("--config", required=True, help="Accepted for operator command compatibility; benchmark never writes configured data roots")
    parser.add_argument("--assert-thresholds", action="store_true")
    args = parser.parse_args(argv)
    # Windows scanners and SQLite can briefly retain a handle after the final
    # read. Cleanup is best-effort because the fixture is already isolated.
    with tempfile.TemporaryDirectory(prefix="fundlab-data-v2-benchmark-", ignore_cleanup_errors=True) as directory:
        result = run_benchmarks(Path(directory))
    payload = {"fixture": "fixed-local-isolated", "provider_time_included": False,
               "thresholds": THRESHOLDS, "measurements": result}
    print(json.dumps(payload, indent=2, sort_keys=True))
    failures = {key: value for key, value in result.items() if value > THRESHOLDS[key]}
    return 1 if args.assert_thresholds and failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
