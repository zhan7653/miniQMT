from __future__ import annotations

from datetime import datetime
from pathlib import Path
import sqlite3

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from fundlab.data.migration import LegacyV1Migrator
from fundlab.data.migration.v1_migrator import LegacyV1Reader
from fundlab.data.platform import (
    DataCatalog, PreflightResult, ProviderCapability, ProviderHealth, ProviderResult,
    SymbolResult,
)
from fundlab.data.storage import VersionNotVisibleError, VersionedParquetStore


ACTIVE = ("510300.SH", "510500.SH", "518880.SH")


class FixtureProvider:
    name = "xtquant"

    def __init__(self, raw: pd.DataFrame, adjusted: pd.DataFrame):
        self.raw, self.adjusted = raw, adjusted
        self.calls = []

    def preflight(self):
        return PreflightResult(self.name, ProviderHealth.AVAILABLE, datetime.now().astimezone())

    def fetch(self, request):
        self.calls.append(request.capability)
        frame = self.raw if request.capability is ProviderCapability.DAILY_BARS_RAW else self.adjusted
        statuses = tuple(SymbolResult(symbol, int((frame.symbol == symbol).sum())) for symbol in request.symbols)
        return frame.copy(), ProviderResult(self.name, request.capability, statuses, datetime.now().astimezone())


class FailingFinalizeStore(VersionedParquetStore):
    def finalize_manifest(self, manifest):
        raise OSError("injected publication failure")


def _bars(close_multiplier=1.0):
    rows = []
    for symbol_index, symbol in enumerate(ACTIVE):
        for day_index, day in enumerate(("2026-01-02", "2026-01-05")):
            close = (10 + symbol_index + day_index) * close_multiplier
            rows.append({"date": day, "symbol": symbol, "open": close, "high": close + .1,
                         "low": close - .1, "close": close, "volume": 1000, "amount": close * 1000,
                         "pre_close": close - .05, "suspended": False, "source": "xtquant"})
    return pd.DataFrame(rows)


def _legacy_fixture(tmp_path: Path):
    sqlite_path = tmp_path / "v1" / "fundlab.db"
    sqlite_path.parent.mkdir()
    with sqlite3.connect(sqlite_path) as connection:
        connection.execute("CREATE TABLE backtest_run(run_id TEXT PRIMARY KEY)")
        connection.executemany("INSERT INTO backtest_run VALUES (?)", [(f"run-{i}",) for i in range(52)])
    root = tmp_path / "v1" / "fund_daily_bar"
    legacy = _bars()
    inactive = legacy.iloc[[0]].copy()
    inactive["symbol"] = "159001.SZ"
    legacy = pd.concat([legacy, inactive], ignore_index=True)
    legacy["adj_factor"] = 1.0
    for year in (2023, 2024, 2025, 2026):
        directory = root / f"year={year}"
        directory.mkdir(parents=True)
        frame = legacy if year == 2026 else legacy.iloc[0:0]
        pq.write_table(pa.Table.from_pandas(frame, preserve_index=False), directory / "part-000.parquet")
    return LegacyV1Reader(sqlite_path, root)


def _migrator(tmp_path, reader, provider, store_type=VersionedParquetStore):
    root = tmp_path / "v2"
    catalog = DataCatalog(root / "catalog.sqlite")
    catalog.initialize()
    store = store_type(root, catalog)
    return LegacyV1Migrator(reader=reader, provider=provider, catalog=catalog, store=store,
                            report_root=tmp_path / "reports"), catalog, store


def test_bootstrap_quarantines_exact_rows_repairs_both_price_modes_and_is_idempotent(tmp_path):
    reader = _legacy_fixture(tmp_path)
    hashes, sizes = reader.hashes(), reader.sizes()
    provider = FixtureProvider(_bars(), _bars(.99))
    migrator, catalog, store = _migrator(tmp_path, reader, provider)

    result = migrator.migrate(active_symbols=ACTIVE, universe_version="fixture", config_hash="hash",
                              expected_hashes=hashes)

    assert result.status == "complete"
    assert {(row.symbol, row.date) for row in result.quarantined} == {
        (symbol, "2026-01-02") for symbol in ACTIVE
    }
    assert result.reconciliation.quarantined_rows == 3
    assert result.reconciliation.active_symbol_counts == {symbol: 2 for symbol in ACTIVE}
    assert provider.calls == [ProviderCapability.DAILY_BARS_RAW, ProviderCapability.DAILY_BARS_ADJUSTED]
    raw = store.read_table(result.version_id, "daily_bars_raw").to_pandas()
    adjusted = store.read_table(result.version_id, "daily_bars_adjusted").to_pandas()
    features = store.read_table(result.version_id, "features_daily").to_pandas()
    assert set(raw.loc[raw.symbol.isin(ACTIVE), "price_mode"]) == {"raw"}
    assert set(adjusted.price_mode) == {"adjusted"}
    assert not raw.loc[raw.symbol.isin(ACTIVE), "close"].equals(adjusted.close)
    assert "adj_factor" not in raw and "adj_factor" not in adjusted
    assert set(features.source_price_mode) == {"adjusted"}
    assert set(features.liquidity_price_mode) == {"raw"}
    assert reader.hashes() == hashes and reader.sizes() == sizes and reader.backtest_run_count() == 52
    assert Path(result.rollback_manifest).is_file()

    repeated = migrator.migrate(active_symbols=ACTIVE, universe_version="fixture", config_hash="hash",
                                expected_hashes=hashes)
    assert repeated.status == "already_complete"
    assert repeated.version_id == result.version_id
    assert catalog.latest_complete().version_id == result.version_id


def test_failed_migration_is_visible_as_failed_but_never_published(tmp_path):
    reader = _legacy_fixture(tmp_path)
    provider = FixtureProvider(_bars(), _bars(.99))
    migrator, catalog, store = _migrator(tmp_path, reader, provider, FailingFinalizeStore)

    result = migrator.migrate(active_symbols=ACTIVE, universe_version="fixture", config_hash="hash",
                              expected_hashes=reader.hashes())

    assert result.status == "failed"
    assert "injected publication failure" in result.error
    assert catalog.latest_complete() is None
    with pytest.raises(VersionNotVisibleError):
        store.resolve_complete(result.version_id)
    with sqlite3.connect(catalog.path) as connection:
        assert connection.execute("SELECT status FROM published_versions WHERE version_id=?", (result.version_id,)).fetchone()[0] == "failed"
        assert connection.execute("SELECT status FROM ingestion_batches WHERE batch_id=?", (result.batch_id,)).fetchone()[0] == "failed"


def test_large_unexplained_adjustment_difference_pauses_before_any_catalog_write(tmp_path):
    reader = _legacy_fixture(tmp_path)
    migrator, catalog, _ = _migrator(tmp_path, reader, FixtureProvider(_bars(), _bars(.5)))
    with pytest.raises(Exception, match="large raw/adjusted"):
        migrator.migrate(active_symbols=ACTIVE, universe_version="fixture", config_hash="hash")
    with sqlite3.connect(catalog.path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM ingestion_batches").fetchone()[0] == 0
