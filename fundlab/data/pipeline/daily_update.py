from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
import json
from pathlib import Path
import sqlite3
from typing import Any, Callable

import pandas as pd
import pyarrow as pa

from fundlab.common.dates import audit_now
from fundlab.data.platform import (
    BatchRecord, BatchStatus, DataCatalog, ManifestIdentity, ProviderCapability,
    ProviderRequest, QualityDisposition, TrustState, VersionRecord, VersionStatus,
    stable_fingerprint,
)
from fundlab.data.processors import DailyBarQualityChecker
from fundlab.data.sources.provider_registry import ProviderRegistry
from fundlab.data.storage import VersionedParquetStore


@dataclass(frozen=True)
class UpdateResult:
    status: str
    target_date: str
    provider: str
    batch_id: str | None = None
    version_id: str | None = None
    previous_version_id: str | None = None
    reused: bool = False
    missing_dates: tuple[str, ...] = ()
    excluded_symbols: tuple[str, ...] = ()
    row_counts: dict[str, int] | None = None
    error: str | None = None
    started_at: str | None = None
    finished_at: str | None = None

    @property
    def succeeded(self) -> bool:
        return self.status == "complete"

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["missing_dates"] = list(self.missing_dates)
        value["excluded_symbols"] = list(self.excluded_symbols)
        value["row_counts"] = self.row_counts or {}
        return value

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, sort_keys=True, indent=2)

    def to_markdown(self) -> str:
        rows = [
            "# Data Platform v2 daily update", "",
            f"- Status: `{self.status}`", f"- Target date: `{self.target_date}`",
            f"- Provider: `{self.provider}`", f"- Batch: `{self.batch_id or '-'}`",
            f"- Version: `{self.version_id or '-'}`", f"- Previous version: `{self.previous_version_id or '-'}`",
            f"- Reused: `{str(self.reused).lower()}`", f"- Missing dates: `{', '.join(self.missing_dates) or '-'}`",
            f"- Excluded symbols: `{', '.join(self.excluded_symbols) or '-'}`", "", "## Row counts", "",
        ]
        rows.extend(f"- `{name}`: {count}" for name, count in sorted((self.row_counts or {}).items()))
        if self.error:
            rows.extend(["", "## Error", "", self.error])
        return "\n".join(rows) + "\n"


class DailyUpdateRunner:
    """Deterministic, single-provider daily publication orchestrator."""

    def __init__(self, *, registry: ProviderRegistry, provider_name: str, catalog: DataCatalog,
                 store: VersionedParquetStore, universe, report_root: str | Path,
                 quality_checker: DailyBarQualityChecker | None = None,
                 feature_builder: Callable[[pd.DataFrame, pd.DataFrame, tuple[str, ...], str, str], pd.DataFrame] | None = None,
                 config_hash: str = "", warmup_days: int = 120) -> None:
        self.registry, self.provider_name, self.catalog, self.store = registry, provider_name, catalog, store
        self.universe, self.report_root = universe, Path(report_root)
        self.quality_checker = quality_checker or DailyBarQualityChecker()
        self.feature_builder = feature_builder or self._build_features
        self.config_hash, self.warmup_days = config_hash, warmup_days

    def run(self, target_date: date, *, start_date: date | None = None) -> UpdateResult:
        started = audit_now()
        batch_id = version_id = None
        previous = self.catalog.latest_complete()
        try:
            provider = self.registry.resolve(self.provider_name)
            required = (ProviderCapability.TRADING_CALENDAR, ProviderCapability.DAILY_BARS_RAW,
                        ProviderCapability.DAILY_BARS_ADJUSTED)
            for capability in required:
                self.registry.resolve(self.provider_name, capability)
            preflight = provider.preflight()
            if not preflight.available:
                raise RuntimeError(f"Provider preflight failed: {preflight.health.value}: {preflight.detail or ''}".rstrip())

            scope_start = start_date or self._next_missing_start(previous, target_date)
            if scope_start > target_date:
                result = UpdateResult("complete", target_date.isoformat(), self.provider_name,
                                      version_id=previous.version_id if previous else None,
                                      previous_version_id=previous.previous_version_id if previous else None,
                                      reused=True, started_at=started.isoformat(), finished_at=audit_now().isoformat())
                return self._report(result)
            warmup_start = scope_start - timedelta(days=max(self.warmup_days * 2, 180))
            symbols = tuple(sorted(set(self.universe.symbols) | set(self.universe.benchmarks)))
            calendar, _ = provider.fetch(ProviderRequest((self.universe.benchmarks[0],), warmup_start, target_date,
                                                         ProviderCapability.TRADING_CALENDAR))
            all_days = tuple(sorted(str(value)[:10] for value in calendar.get("date", pd.Series(dtype=str)).dropna().unique()))
            days = tuple(value for value in all_days if scope_start.isoformat() <= value <= target_date.isoformat())
            if not days:
                raise RuntimeError("Trading calendar returned no target dates")
            raw, raw_result = provider.fetch(ProviderRequest(symbols, warmup_start, target_date,
                                                             ProviderCapability.DAILY_BARS_RAW))
            adjusted, adjusted_result = provider.fetch(ProviderRequest(symbols, warmup_start, target_date,
                                                                       ProviderCapability.DAILY_BARS_ADJUSTED))
            system_errors = [item.error for result in (raw_result, adjusted_result) for item in result.symbols
                             if item.error and item.symbol == "*"]
            if system_errors:
                raise RuntimeError("Provider system error: " + "; ".join(system_errors))
            provider_bad = {item.symbol for result in (raw_result, adjusted_result) for item in result.symbols if item.error}
            report = self.quality_checker.check(raw, trading_days=all_days, requested_symbols=symbols,
                                                suspended=self._suspensions(raw))
            if report.disposition is QualityDisposition.BLOCK_BATCH:
                raise RuntimeError("Batch quality gate failed")
            excluded = tuple(sorted(provider_bad | set(report.blocked_symbols)))
            eligible = tuple(symbol for symbol in symbols if symbol not in excluded)
            if not eligible:
                raise RuntimeError("No symbols remain after quality gating")
            raw = raw[raw["symbol"].isin(eligible)].copy()
            adjusted = adjusted[adjusted["symbol"].isin(eligible)].copy()
            features = self.feature_builder(raw, adjusted, eligible, scope_start.isoformat(), target_date.isoformat())
            tables = {"calendar": calendar, "daily_bars_raw": raw, "daily_bars_adjusted": adjusted,
                      "features": features}
            content = stable_fingerprint({name: self._frame_fingerprint(frame) for name, frame in tables.items()})
            existing = self._version_by_content(content)
            if existing:
                result = UpdateResult("complete", target_date.isoformat(), self.provider_name,
                                      batch_id=existing.batch_id, version_id=existing.version_id,
                                      previous_version_id=existing.previous_version_id, reused=True,
                                      missing_dates=days, excluded_symbols=excluded,
                                      row_counts={k: len(v) for k, v in tables.items()}, started_at=started.isoformat(),
                                      finished_at=audit_now().isoformat())
                return self._report(result)

            base_request = stable_fingerprint({"provider": self.provider_name, "start": scope_start,
                                               "target": target_date, "symbols": symbols,
                                               "universe": self.universe.version, "config": self.config_hash})
            attempt = self._failed_attempts(content, previous.version_id if previous else None)
            request_fingerprint = stable_fingerprint({"request": base_request, "content": content, "attempt": attempt})
            batch_id = "batch-" + request_fingerprint[:16]
            version_id = "version-" + stable_fingerprint({"content": content, "previous": previous.version_id if previous else None,
                                                           "attempt": attempt})[:16]
            batch = self.catalog.create_batch(BatchRecord(batch_id, self.provider_name, scope_start, target_date,
                symbols, self.universe.version, self.config_hash, request_fingerprint, BatchStatus.PENDING, 0, None))
            if batch.status is BatchStatus.PENDING:
                self.catalog.transition_batch(batch_id, BatchStatus.RUNNING)
            recovered = self._recover_finalized(version_id, batch_id, content, previous.version_id if previous else None)
            if recovered:
                result = UpdateResult("complete", target_date.isoformat(), self.provider_name, batch_id, version_id,
                                      previous.version_id if previous else None, False, days, excluded,
                                      {k: len(v) for k, v in tables.items()}, started_at=started.isoformat(),
                                      finished_at=audit_now().isoformat())
                return self._report(result)
            self.store.begin_version(version_id)
            for name, frame in tables.items():
                self.store.write_table(version_id, name, pa.Table.from_pandas(frame.reset_index(drop=True), preserve_index=False))
            row_counts = {name: len(frame) for name, frame in tables.items()}
            identity = ManifestIdentity(self.provider_name, batch_id, version_id, audit_now(), TrustState.TRUSTED,
                                        content, 1, row_counts)
            manifest = self.store.prepare_manifest(identity, row_counts)
            self.catalog.create_version(VersionRecord(version_id, batch_id, manifest.fingerprint, content,
                                                       VersionStatus.BUILDING, previous.version_id if previous else None,
                                                       None, None))
            for name, frame in tables.items():
                try:
                    self.store.write_raw(batch_id, name, pa.Table.from_pandas(frame.reset_index(drop=True), preserve_index=False))
                except Exception as exc:
                    if "already exists" not in str(exc):
                        raise
            self.store.finalize_manifest(manifest)
            self.catalog.transition_batch(batch_id, BatchStatus.COMPLETE, row_count=sum(row_counts.values()))
            self.catalog.complete_version(version_id)
            result = UpdateResult("complete", target_date.isoformat(), self.provider_name, batch_id, version_id,
                                  previous.version_id if previous else None, False, days, excluded, row_counts,
                                  started_at=started.isoformat(), finished_at=audit_now().isoformat())
            return self._report(result)
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
            self._mark_failed(batch_id, version_id, error)
            result = UpdateResult("failed", target_date.isoformat(), self.provider_name, batch_id, version_id,
                                  previous.version_id if previous else None, error=error,
                                  started_at=started.isoformat(), finished_at=audit_now().isoformat())
            return self._report(result)

    def _next_missing_start(self, previous, target: date) -> date:
        if previous is None:
            return target
        try:
            frame = self.store.read_table(previous.version_id, "calendar").to_pandas()
            latest = max(date.fromisoformat(str(value)) for value in frame["date"])
            return latest + timedelta(days=1)
        except Exception:
            return target

    def _version_by_content(self, content: str):
        if not self.catalog.path.exists():
            return None
        with sqlite3.connect(f"file:{self.catalog.path.as_posix()}?mode=ro", uri=True) as connection:
            connection.row_factory = sqlite3.Row
            row = connection.execute("SELECT * FROM published_versions WHERE content_fingerprint=? AND status=?",
                                     (content, VersionStatus.COMPLETE.value)).fetchone()
        return self.catalog._version(row) if row else None

    def _failed_attempts(self, content: str, previous_version_id: str | None) -> int:
        if not self.catalog.path.exists():
            return 0
        with sqlite3.connect(f"file:{self.catalog.path.as_posix()}?mode=ro", uri=True) as connection:
            return int(connection.execute(
                "SELECT COUNT(*) FROM published_versions WHERE content_fingerprint=? AND previous_version_id IS ? AND status=?",
                (content, previous_version_id, VersionStatus.FAILED.value),
            ).fetchone()[0])

    def _recover_finalized(self, version_id: str, batch_id: str, content: str,
                           previous_version_id: str | None) -> bool:
        """Finish the catalog transaction after a crash following the atomic directory rename."""
        published = self.store.published_path(version_id)
        if not published.is_dir() or not self.catalog.path.exists():
            return False
        from fundlab.data.storage.manifest import PublishedManifest
        manifest = PublishedManifest.read(published)
        if manifest.identity.content_fingerprint != content or manifest.identity.batch_id != batch_id:
            raise RuntimeError(f"Conflicting finalized storage exists for {version_id}")
        with sqlite3.connect(f"file:{self.catalog.path.as_posix()}?mode=ro", uri=True) as connection:
            connection.row_factory = sqlite3.Row
            row = connection.execute("SELECT * FROM published_versions WHERE version_id=?", (version_id,)).fetchone()
        if row is None:
            self.catalog.create_version(VersionRecord(version_id, batch_id, manifest.fingerprint, content,
                VersionStatus.BUILDING, previous_version_id, None, None))
        elif VersionStatus(row["status"]) is not VersionStatus.BUILDING:
            return VersionStatus(row["status"]) is VersionStatus.COMPLETE
        try:
            self.catalog.transition_batch(batch_id, BatchStatus.COMPLETE,
                                          row_count=sum(manifest.identity.row_counts.values()))
        except Exception:
            pass
        self.catalog.complete_version(version_id)
        return True

    def _mark_failed(self, batch_id: str | None, version_id: str | None, error: str) -> None:
        if version_id:
            try: self.catalog.fail_version(version_id, error)
            except Exception: pass
            self.store.discard_staging(version_id)
        if batch_id:
            try: self.catalog.transition_batch(batch_id, BatchStatus.FAILED, error=error)
            except Exception: pass

    def _report(self, result: UpdateResult) -> UpdateResult:
        self.report_root.mkdir(parents=True, exist_ok=True)
        stem = f"update-{result.target_date}-{result.batch_id or 'preflight'}"
        (self.report_root / f"{stem}.json").write_text(result.to_json() + "\n", encoding="utf-8", newline="\n")
        (self.report_root / f"{stem}.md").write_text(result.to_markdown(), encoding="utf-8", newline="\n")
        return result

    @staticmethod
    def _suspensions(frame: pd.DataFrame) -> tuple[tuple[str, str], ...]:
        if "suspended" not in frame:
            return ()
        return tuple((str(row.symbol), str(row.date)) for row in frame[frame["suspended"]].itertuples())

    @staticmethod
    def _frame_fingerprint(frame: pd.DataFrame) -> str:
        normalized = frame.copy()
        normalized = normalized.reindex(sorted(normalized.columns), axis=1)
        if {"date", "symbol"}.issubset(normalized.columns):
            normalized = normalized.sort_values(["date", "symbol"])
        elif "date" in normalized.columns:
            normalized = normalized.sort_values(["date"])
        return stable_fingerprint(json.loads(normalized.to_json(orient="records", date_format="iso")))

    @staticmethod
    def _build_features(raw: pd.DataFrame, adjusted: pd.DataFrame, symbols: tuple[str, ...],
                        start: str, end: str) -> pd.DataFrame:
        bars = adjusted[["date", "symbol", "close"]].merge(raw[["date", "symbol", "amount"]], on=["date", "symbol"])
        output = []
        for symbol, group in bars[bars["symbol"].isin(symbols)].groupby("symbol"):
            item = group.sort_values("date").copy()
            item["ret_1d"] = item["close"].pct_change()
            item["ret_20d"] = item["close"].pct_change(20)
            item["volatility_20d"] = item["close"].pct_change().rolling(20, min_periods=20).std() * (252 ** .5)
            item["amount_avg_20d"] = item["amount"].rolling(20, min_periods=20).mean()
            output.append(item[(item["date"] >= start) & (item["date"] <= end)])
        return pd.concat(output, ignore_index=True) if output else pd.DataFrame(columns=["date", "symbol", "ret_1d"])
