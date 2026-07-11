from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
import sqlite3

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml

from fundlab.data.migration import LegacyV1Migrator
from fundlab.data.migration.v1_migrator import LegacyV1Reader
from fundlab.data.platform import (
    BatchRecord, BatchStatus, DataCatalog, ManifestIdentity, PreflightResult, PriceMode,
    ProviderCapability, ProviderHealth, ProviderResult, SymbolResult, TrustState,
    VersionRecord, VersionStatus,
)
from fundlab.data.portal import DataPortal
from fundlab.data.storage import VersionNotVisibleError, VersionedParquetStore
from scripts import migrate_data_v1_to_v2 as migration_cli


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
    features = store.read_table(result.version_id, "features").to_pandas()
    assert set(raw.loc[raw.symbol.isin(ACTIVE), "price_mode"]) == {"raw"}
    assert set(adjusted.price_mode) == {"adjusted"}
    assert not raw.loc[raw.symbol.isin(ACTIVE), "close"].equals(adjusted.close)
    assert "adj_factor" not in raw and "adj_factor" not in adjusted
    assert set(features.source_price_mode) == {"adjusted"}
    assert set(features.liquidity_price_mode) == {"raw"}
    assert reader.hashes() == hashes and reader.sizes() == sizes and reader.backtest_run_count() == 52
    assert Path(result.rollback_manifest).is_file()
    rollback = __import__("json").loads(Path(result.rollback_manifest).read_text(encoding="utf-8"))
    assert set(rollback["raw_inputs"]) == {"legacy_trusted", "repaired_raw", "repaired_adjusted", "quarantine"}
    assert all(Path(path).is_file() for path in rollback["raw_inputs"].values())

    repeated = migrator.migrate(active_symbols=ACTIVE, universe_version="fixture", config_hash="hash",
                                expected_hashes=hashes)
    assert repeated.status == "already_complete"
    assert repeated.version_id == result.version_id
    assert catalog.latest_complete().version_id == result.version_id


def test_same_count_changed_provider_values_create_a_semantic_revision(tmp_path):
    reader = _legacy_fixture(tmp_path)
    first_migrator, catalog, store = _migrator(tmp_path, reader, FixtureProvider(_bars(), _bars(.99)))
    first = first_migrator.migrate(active_symbols=ACTIVE, universe_version="fixture", config_hash="hash")
    second_migrator = LegacyV1Migrator(
        reader=reader, provider=FixtureProvider(_bars(1.01), _bars(1.00)), catalog=catalog, store=store,
        report_root=tmp_path / "reports",
    )

    second = second_migrator.migrate(active_symbols=ACTIVE, universe_version="fixture", config_hash="hash")

    assert second.status == "complete" and second.version_id != first.version_id
    assert catalog.latest_complete().version_id == second.version_id
    with sqlite3.connect(catalog.path) as connection:
        rows = connection.execute(
            "SELECT version_id, content_fingerprint, status FROM published_versions ORDER BY created_at"
        ).fetchall()
    assert len({row[1] for row in rows}) == 2
    assert {row[2] for row in rows} == {"superseded", "complete"}


def test_same_bootstrap_rerun_after_later_successor_keeps_latest_pointer_unchanged(tmp_path):
    reader = _legacy_fixture(tmp_path)
    provider = FixtureProvider(_bars(), _bars(.99))
    migrator, catalog, store = _migrator(tmp_path, reader, provider)
    bootstrap = migrator.migrate(active_symbols=ACTIVE, universe_version="fixture", config_hash="hash")
    successor_id, successor_batch = "daily-successor-v1", "daily-successor-b1"
    catalog.create_batch(BatchRecord(successor_batch, "xtquant", date(2026, 1, 5), date(2026, 1, 5), ACTIVE,
                                     "fixture", "hash", "successor-request", BatchStatus.PENDING, 0, None))
    store.begin_version(successor_id)
    for table_name in ("daily_bars_raw", "daily_bars_adjusted", "calendar", "universe", "features", "quarantine"):
        store.write_table(successor_id, table_name, store.read_table(bootstrap.version_id, table_name))
    identity = ManifestIdentity("xtquant", successor_batch, successor_id, datetime.now().astimezone(),
                                TrustState.TRUSTED, "successor-content", 1, {})
    manifest = store.prepare_manifest(identity, {})
    catalog.create_version(VersionRecord(successor_id, successor_batch, manifest.fingerprint, "successor-content",
                                         VersionStatus.BUILDING, bootstrap.version_id, None, None))
    store.finalize_manifest(manifest)
    catalog.complete_version(successor_id)

    repeated = migrator.migrate(active_symbols=ACTIVE, universe_version="fixture", config_hash="hash")

    assert repeated.status == "already_complete" and repeated.version_id == bootstrap.version_id
    assert catalog.latest_complete().version_id == successor_id


def test_migrated_complete_version_is_fully_readable_through_frozen_data_portal(tmp_path):
    reader = _legacy_fixture(tmp_path)
    hashes, sizes = reader.hashes(), reader.sizes()
    migrator, _, store = _migrator(tmp_path, reader, FixtureProvider(_bars(), _bars(.99)))
    result = migrator.migrate(
        active_symbols=ACTIVE, universe_version="fixture", config_hash="hash",
        expected_hashes=hashes, target_date=date(2026, 1, 5),
        universe_effective_date=date(2026, 1, 2),
    )

    portal = DataPortal.open_version(store, result.version_id)
    assert portal.data_version == result.version_id
    assert portal.get_trading_days("2026-01-02", "2026-01-05") == ["2026-01-02", "2026-01-05"]
    assert portal.is_trading_day("2026-01-05") is True
    assert portal.get_universe("2026-01-05") == sorted(ACTIVE)
    raw = portal.get_daily_bar(ACTIVE, "2026-01-02", "2026-01-05", fields=["close"], price_mode=PriceMode.RAW)
    adjusted = portal.get_daily_bar(ACTIVE, "2026-01-02", "2026-01-05", fields=["close"], price_mode=PriceMode.ADJUSTED)
    assert len(raw) == len(adjusted) == 6
    assert not raw["close"].equals(adjusted["close"])
    features = portal.get_features(ACTIVE, "2026-01-05", fields=["ret_1d", "price_mode", "provider"])
    assert list(features.index) == sorted(ACTIVE)
    assert set(features["price_mode"]) == {"adjusted"}
    assert set(features["provider"]) == {"xtquant"}
    assert reader.hashes() == hashes and reader.sizes() == sizes and reader.backtest_run_count() == 52


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


def test_confirmed_cli_invocation_is_cwd_independent_and_writes_json_and_markdown(tmp_path, monkeypatch):
    reader = _legacy_fixture(tmp_path)
    config_dir = tmp_path / "configuration"
    config_dir.mkdir()
    universe_path = config_dir / "universe.yaml"
    universe_path.write_text(yaml.safe_dump({
        "version": "fixture", "effective_date": "2026-01-05", "symbols": list(ACTIVE),
        "benchmarks": [ACTIVE[0]],
    }), encoding="utf-8")
    config_path = config_dir / "base.yaml"
    config_path.write_text(yaml.safe_dump({
        "paths": {
            "sqlite_db": str(reader.sqlite_path), "parquet_root": str(reader.parquet_root.parent),
            "v2_catalog": "../runtime/v2/catalog.sqlite", "v2_raw_root": "../runtime/v2/raw",
            "v2_staging_root": "../runtime/v2/staging", "v2_published_root": "../runtime/v2/published",
            "v2_report_root": "../runtime/reports",
        },
        "platform": {"timezone": "Asia/Hong_Kong"},
        "providers": {"enabled": ["xtquant"], "fallback": None},
        "reviewed_universe": {"config_path": "universe.yaml", "benchmark_symbols": [ACTIVE[0]],
                              "feature_lookback_days": 120},
    }), encoding="utf-8")
    monkeypatch.setattr(migration_cli, "XtQuantSource", lambda: FixtureProvider(_bars(), _bars(.99)))
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)
    json_output, markdown_output = tmp_path / "evidence" / "migration.json", tmp_path / "evidence" / "migration.md"

    exit_code = migration_cli.main([
        "--config", str(config_path), "--target-date", "2026-01-05",
        "--json-output", str(json_output), "--markdown-output", str(markdown_output),
    ])

    assert exit_code == migration_cli.EXIT_SUCCESS
    payload = __import__("json").loads(json_output.read_text(encoding="utf-8"))
    assert payload["status"] == "complete" and payload["target_date"] == "2026-01-05"
    assert payload["legacy"]["backtest_run_count"] == 52 and len(payload["legacy"]["hashes"]) == 5
    assert len(payload["quarantined"]) == 3
    assert payload["reconciliation"]["quarantined_rows"] == 3
    report = markdown_output.read_text(encoding="utf-8")
    assert "Five legacy v1 hashes" in report and "Legacy backtest runs: `52`" in report


def test_cli_configuration_error_has_deterministic_exit_and_reports(tmp_path):
    json_output, markdown_output = tmp_path / "failure.json", tmp_path / "failure.md"
    exit_code = migration_cli.main([
        "--config", str(tmp_path / "missing.yaml"), "--target-date", "2026-01-05",
        "--json-output", str(json_output), "--markdown-output", str(markdown_output),
    ])
    assert exit_code == migration_cli.EXIT_CONFIGURATION_ERROR
    assert '"status": "failed"' in json_output.read_text(encoding="utf-8")
    assert "## Failure" in markdown_output.read_text(encoding="utf-8")
