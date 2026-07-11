from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa

from fundlab.data.platform import (
    BatchRecord, BatchStatus, DataCatalog, ManifestIdentity, TrustState,
    VersionRecord, VersionStatus, stable_fingerprint,
)
from fundlab.data.storage import VersionedParquetStore


SYMBOLS = tuple(f"{510000 + index:06d}.SH" for index in range(30))


def market_frames(symbols: tuple[str, ...] = SYMBOLS, periods: int = 756):
    days = pd.bdate_range(end="2026-05-07", periods=periods).strftime("%Y-%m-%d")
    rows = []
    for symbol_index, symbol in enumerate(symbols):
        for day_index, day in enumerate(days):
            close = 10.0 + symbol_index * 0.1 + day_index * 0.002
            rows.append({
                "date": day, "symbol": symbol, "open": close - 0.01,
                "high": close + 0.05, "low": close - 0.05, "close": close,
                "volume": 100_000 + day_index, "amount": close * (100_000 + day_index),
                "suspended": False,
            })
    raw = pd.DataFrame(rows)
    adjusted = raw.copy()
    adjusted[["open", "high", "low", "close"]] *= 0.99
    calendar = pd.DataFrame({"date": days, "is_trading_day": True})
    universe = pd.DataFrame({"symbol": symbols, "effective_date": days[0]})
    features = raw.loc[raw["date"] == days[-1], ["date", "symbol"]].copy()
    features["ret_20d"] = 0.01
    return days, raw, adjusted, calendar, universe, features


def publish_fixture(root: Path, symbols: tuple[str, ...] = SYMBOLS):
    days, raw, adjusted, calendar, universe, features = market_frames(symbols)
    catalog = DataCatalog(root / "catalog.sqlite3")
    catalog.initialize()
    store = VersionedParquetStore(root / "warehouse", catalog)
    batch_id, version_id = "benchmark-batch", "benchmark-version"
    request = stable_fingerprint({"fixture": "data-v2", "symbols": symbols})
    catalog.create_batch(BatchRecord(batch_id, "fixture", date.fromisoformat(days[0]),
        date.fromisoformat(days[-1]), symbols, "benchmark-u1", "benchmark-config", request,
        BatchStatus.PENDING, 0, None))
    catalog.transition_batch(batch_id, BatchStatus.RUNNING)
    tables = {
        "calendar": calendar, "universe": universe, "daily_bars_raw": raw,
        "daily_bars_adjusted": adjusted, "features": features,
    }
    store.begin_version(version_id)
    for name, frame in tables.items():
        store.write_table(version_id, name, pa.Table.from_pandas(frame, preserve_index=False))
    counts = {name: len(frame) for name, frame in tables.items()}
    content = stable_fingerprint(counts)
    identity = ManifestIdentity("fixture", batch_id, version_id, datetime.now().astimezone(),
                                TrustState.TRUSTED, content, 1, counts)
    manifest = store.prepare_manifest(identity, counts)
    catalog.create_version(VersionRecord(version_id, batch_id, manifest.fingerprint, content,
                                          VersionStatus.BUILDING, None, None, None))
    store.finalize_manifest(manifest)
    catalog.transition_batch(batch_id, BatchStatus.COMPLETE, row_count=sum(counts.values()))
    catalog.complete_version(version_id)
    return store, version_id, days
