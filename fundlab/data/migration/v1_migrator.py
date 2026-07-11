from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime
from hashlib import sha256
import json
from pathlib import Path
import sqlite3
from typing import Mapping, Sequence
from uuid import uuid4

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds

from fundlab.common.dates import audit_now
from fundlab.data.platform import (
    BatchRecord, BatchStatus, DataCatalog, ManifestIdentity, ProviderCapability,
    ProviderRequest, TrustState, VersionRecord, VersionStatus, stable_fingerprint,
)
from fundlab.data.processors.quality_checker import DailyBarQualityChecker
from fundlab.data.storage.versioned_parquet_store import VersionedParquetStore


KNOWN_CONTAMINATION = frozenset({
    ("510300.SH", "2026-01-02"), ("510500.SH", "2026-01-02"),
    ("518880.SH", "2026-01-02"),
})


class MigrationError(RuntimeError):
    pass


@dataclass(frozen=True)
class ContaminationRecord:
    symbol: str
    date: str
    reason: str = "known_fake_legacy_row"


@dataclass(frozen=True)
class ReconciliationResult:
    legacy_rows: int
    quarantined_rows: int
    retained_legacy_rows: int
    repaired_raw_rows: int
    repaired_adjusted_rows: int
    published_raw_rows: int
    published_adjusted_rows: int
    feature_rows: int
    active_symbol_counts: Mapping[str, int]


@dataclass(frozen=True)
class RollbackManifest:
    captured_at: str
    source_hashes: Mapping[str, str]
    source_sizes: Mapping[str, int]
    legacy_backtest_run_count: int
    version_id: str
    previous_version_id: str | None

    def write(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(asdict(self), ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")


@dataclass(frozen=True)
class MigrationResult:
    status: str
    version_id: str | None
    batch_id: str | None
    reconciliation: ReconciliationResult | None
    quarantined: tuple[ContaminationRecord, ...]
    rollback_manifest: str | None
    error: str | None = None

    def to_dict(self) -> dict:
        return asdict(self)


class LegacyV1Reader:
    """Read-only access to the five canonical v1 warehouse artifacts."""

    def __init__(self, sqlite_path: str | Path, parquet_root: str | Path) -> None:
        self.sqlite_path = Path(sqlite_path).resolve()
        self.parquet_root = Path(parquet_root).resolve()

    @property
    def source_files(self) -> tuple[Path, ...]:
        return (self.sqlite_path, *tuple(sorted(self.parquet_root.glob("year=*/part-000.parquet"))))

    def hashes(self) -> dict[str, str]:
        return {str(path): self._hash(path) for path in self.source_files}

    def sizes(self) -> dict[str, int]:
        return {str(path): path.stat().st_size for path in self.source_files}

    def backtest_run_count(self) -> int:
        with self._connect() as connection:
            return int(connection.execute("SELECT COUNT(*) FROM backtest_run").fetchone()[0])

    def read_bars(self) -> pd.DataFrame:
        if len(self.source_files) != 5:
            raise MigrationError(f"Expected five canonical v1 artifacts, found {len(self.source_files)}")
        table = ds.dataset(self.parquet_root, format="parquet", partitioning="hive").to_table()
        frame = table.to_pandas()
        frame["date"] = frame["date"].astype(str)
        frame["symbol"] = frame["symbol"].astype(str)
        return frame.sort_values(["date", "symbol"]).reset_index(drop=True)

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(f"file:{self.sqlite_path.as_posix()}?mode=ro", uri=True)

    @staticmethod
    def _hash(path: Path) -> str:
        digest = sha256()
        with path.open("rb") as stream:
            while chunk := stream.read(1024 * 1024):
                digest.update(chunk)
        return digest.hexdigest()


class LegacyV1Migrator:
    def __init__(self, *, reader: LegacyV1Reader, provider, catalog: DataCatalog,
                 store: VersionedParquetStore, report_root: str | Path,
                 quality_checker: DailyBarQualityChecker | None = None,
                 max_relative_difference: float = 0.25) -> None:
        self.reader, self.provider, self.catalog, self.store = reader, provider, catalog, store
        self.report_root = Path(report_root)
        self.quality_checker = quality_checker or DailyBarQualityChecker()
        self.max_relative_difference = max_relative_difference

    def migrate(self, *, active_symbols: Sequence[str], universe_version: str,
                config_hash: str, expected_hashes: Mapping[str, str] | None = None,
                expected_backtest_runs: int = 52, target_date: date | None = None) -> MigrationResult:
        symbols = tuple(sorted(set(active_symbols)))
        if not symbols:
            raise ValueError("active_symbols cannot be empty")
        before_hashes, before_sizes = self.reader.hashes(), self.reader.sizes()
        if expected_hashes is not None and dict(expected_hashes) != before_hashes:
            raise MigrationError("Legacy source hash mismatch; migration paused")
        run_count = self.reader.backtest_run_count()
        if run_count != expected_backtest_runs:
            raise MigrationError(f"Expected {expected_backtest_runs} legacy backtest runs, found {run_count}")

        legacy = self.reader.read_bars()
        if target_date is not None:
            legacy = legacy.loc[legacy["date"] <= target_date.isoformat()].copy()
        if legacy.empty:
            raise MigrationError("No legacy rows exist on or before target_date")
        contaminated = legacy.apply(lambda row: (row["symbol"], row["date"]) in KNOWN_CONTAMINATION, axis=1)
        quarantined_frame = legacy.loc[contaminated].copy()
        found = set(zip(quarantined_frame["symbol"], quarantined_frame["date"]))
        if found != KNOWN_CONTAMINATION:
            raise MigrationError("Known contamination quarantine did not reconcile exactly")
        trusted_legacy = legacy.loc[~contaminated].copy()
        if "source" not in trusted_legacy or set(trusted_legacy["source"].dropna()) != {"xtquant"}:
            raise MigrationError("Only legacy source=xtquant rows are eligible for migration")
        legacy_quality = self.quality_checker.check(trusted_legacy)
        if not legacy_quality.publication_eligible or legacy_quality.blocked_symbols:
            raise MigrationError("Trusted legacy rows failed validation")
        start_date = date.fromisoformat(legacy["date"].min())
        end_date = target_date or date.fromisoformat(legacy["date"].max())
        raw = self._fetch(symbols, start_date, end_date, ProviderCapability.DAILY_BARS_RAW)
        adjusted = self._fetch(symbols, start_date, end_date, ProviderCapability.DAILY_BARS_ADJUSTED)
        self._validate_repair(raw, adjusted, symbols)

        retained = trusted_legacy.loc[~trusted_legacy["symbol"].isin(symbols)].copy()
        raw = self._standardize_bars(raw, "raw")
        adjusted = self._standardize_bars(adjusted, "adjusted")
        retained = self._standardize_bars(retained, "raw")
        published_raw = pd.concat([retained, raw], ignore_index=True).drop_duplicates(["date", "symbol"], keep="last")
        published_raw = published_raw.sort_values(["date", "symbol"]).reset_index(drop=True)
        features = self._features(adjusted, raw)
        reconciliation = ReconciliationResult(
            legacy_rows=len(legacy), quarantined_rows=len(quarantined_frame), retained_legacy_rows=len(retained),
            repaired_raw_rows=len(raw), repaired_adjusted_rows=len(adjusted), published_raw_rows=len(published_raw),
            published_adjusted_rows=len(adjusted), feature_rows=len(features),
            active_symbol_counts={symbol: int((raw["symbol"] == symbol).sum()) for symbol in symbols},
        )
        content = stable_fingerprint({"source_hashes": before_hashes, "symbols": symbols,
                                      "raw": len(published_raw), "adjusted": len(adjusted), "features": len(features)})
        latest = self.catalog.latest_complete()
        if latest and latest.content_fingerprint == content:
            return MigrationResult("already_complete", latest.version_id, latest.batch_id, reconciliation,
                                   self._quarantine_records(found), str(self.report_root / latest.version_id / "rollback.json"))

        batch_id, version_id = f"migration-{uuid4().hex}", f"v2-bootstrap-{uuid4().hex}"
        request_fp = stable_fingerprint({"content": content, "attempt": batch_id})
        self.catalog.create_batch(BatchRecord(batch_id, self.provider.name, start_date, end_date, symbols,
                                              universe_version, config_hash, request_fp, BatchStatus.PENDING, 0, None))
        self.catalog.transition_batch(batch_id, BatchStatus.RUNNING)
        self.catalog.create_version(VersionRecord(version_id, batch_id, "pending", content, VersionStatus.BUILDING,
                                                  latest.version_id if latest else None, None, None))
        try:
            self.store.begin_version(version_id)
            tables = {"daily_bars_raw": published_raw, "daily_bars_adjusted": adjusted,
                      "features_daily": features, "quarantine": quarantined_frame}
            for name, frame in tables.items():
                self.store.write_table(version_id, name, pa.Table.from_pandas(frame, preserve_index=False))
            row_counts = {name: len(frame) for name, frame in tables.items()}
            identity = ManifestIdentity(self.provider.name, batch_id, version_id, audit_now(), TrustState.TRUSTED,
                                        content, 1, row_counts)
            manifest = self.store.prepare_manifest(identity, row_counts)
            # Catalog stores the final manifest fingerprint before visibility changes.
            with sqlite3.connect(self.catalog.path) as connection:
                connection.execute("UPDATE published_versions SET manifest_fingerprint=? WHERE version_id=?",
                                   (manifest.fingerprint, version_id))
            self.store.finalize_manifest(manifest)
            self.catalog.transition_batch(batch_id, BatchStatus.COMPLETE, row_count=sum(row_counts.values()))
            self.catalog.complete_version(version_id)
            rollback = RollbackManifest(audit_now().isoformat(), before_hashes, before_sizes, run_count,
                                        version_id, latest.version_id if latest else None)
            rollback_path = self.report_root / version_id / "rollback.json"
            rollback.write(rollback_path)
            self._verify_legacy(before_hashes, before_sizes, run_count)
            return MigrationResult("complete", version_id, batch_id, reconciliation,
                                   self._quarantine_records(found), str(rollback_path))
        except Exception as exc:
            self.store.discard_staging(version_id)
            try:
                self.catalog.fail_version(version_id, str(exc))
                self.catalog.transition_batch(batch_id, BatchStatus.FAILED, error=str(exc))
            except Exception:
                pass
            self._verify_legacy(before_hashes, before_sizes, run_count)
            return MigrationResult("failed", version_id, batch_id, reconciliation,
                                   self._quarantine_records(found), None, str(exc))

    def _fetch(self, symbols, start, end, capability):
        preflight = self.provider.preflight()
        if not preflight.available:
            raise MigrationError(f"MiniQMT unavailable: {preflight.detail or preflight.health.value}")
        frame, result = self.provider.fetch(ProviderRequest(symbols, start, end, capability))
        errors = [item for item in result.symbols if item.error]
        if errors or frame.empty:
            raise MigrationError(f"MiniQMT {capability.value} repair incomplete: {errors}")
        return frame

    def _validate_repair(self, raw, adjusted, symbols):
        for label, frame in (("raw", raw), ("adjusted", adjusted)):
            missing = set(symbols) - set(frame.get("symbol", []))
            report = self.quality_checker.check(frame)
            if missing or not report.publication_eligible or report.blocked_symbols:
                raise MigrationError(f"{label} repair failed quality checks; missing={sorted(missing)}")
        raw_keys, adjusted_keys = set(zip(raw["date"], raw["symbol"])), set(zip(adjusted["date"], adjusted["symbol"]))
        if raw_keys != adjusted_keys:
            raise MigrationError("Raw and front-adjusted repair keys differ; fallback is forbidden")
        merged = raw[["date", "symbol", "close"]].merge(adjusted[["date", "symbol", "close"]], on=["date", "symbol"], suffixes=("_raw", "_adj"))
        relative = ((merged.close_adj - merged.close_raw).abs() / merged.close_raw.abs().replace(0, pd.NA)).dropna()
        if not relative.empty and float(relative.max()) > self.max_relative_difference:
            raise MigrationError("Unexplained large raw/adjusted reconciliation difference; migration paused")

    @staticmethod
    def _standardize_bars(frame, price_mode):
        data = frame.copy()
        data["date"], data["symbol"] = data["date"].astype(str), data["symbol"].astype(str)
        data["price_mode"] = price_mode
        data["trust_state"] = TrustState.TRUSTED.value
        if price_mode == "raw" and "source" not in data:
            data["source"] = "xtquant"
        return data.drop(columns=["adj_factor"], errors="ignore")

    @staticmethod
    def _features(adjusted, raw):
        bars = adjusted[["date", "symbol", "close"]].merge(raw[["date", "symbol", "amount"]], on=["date", "symbol"], validate="one_to_one")
        outputs = []
        for _, group in bars.groupby("symbol"):
            group = group.sort_values("date").copy()
            close, amount = group.close.astype(float), group.amount.astype(float)
            returns = close.pct_change()
            for days in (1, 5, 20, 60, 120): group[f"ret_{days}d"] = close.pct_change(days)
            for days in (20, 60):
                group[f"volatility_{days}d"] = returns.rolling(days, min_periods=days).std() * (252 ** .5)
                group[f"amount_avg_{days}d"] = amount.rolling(days, min_periods=days).mean()
            group["max_drawdown_60d"] = (close / close.rolling(60, min_periods=60).max() - 1).rolling(60, min_periods=60).min()
            outputs.append(group)
        result = pd.concat(outputs, ignore_index=True) if outputs else pd.DataFrame()
        if not result.empty:
            result["price_mode"], result["source_price_mode"], result["liquidity_price_mode"] = "adjusted", "adjusted", "raw"
            result["provider"], result["quality_disposition"] = "xtquant", "pass"
        return result

    def _verify_legacy(self, hashes, sizes, runs):
        if self.reader.hashes() != hashes or self.reader.sizes() != sizes or self.reader.backtest_run_count() != runs:
            raise MigrationError("Legacy v1 changed during migration")

    @staticmethod
    def _quarantine_records(keys):
        return tuple(ContaminationRecord(symbol, day) for symbol, day in sorted(keys))
