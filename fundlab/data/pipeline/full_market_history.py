from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date, datetime, time, timedelta
from hashlib import sha256
import json
from pathlib import Path
import shutil
from time import monotonic, sleep
from typing import Any, Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo

import pandas as pd
import pyarrow as pa

from fundlab.common.dates import audit_now
from fundlab.data.platform import (
    AttemptStatus,
    CollectionPartitionRecord,
    CollectionPhase,
    CollectionRunRecord,
    CollectionRunStatus,
    DataCatalog,
    PartitionIdentity,
    PartitionStatus,
    PriceMode,
    ProviderCapability,
    ProviderRequest,
    ThrottleEventRecord,
    stable_fingerprint,
)
from fundlab.data.sources.base import ProviderUnavailableError
from fundlab.data.storage.versioned_parquet_store import CorruptPartitionError, VersionedParquetStore

from .throttle import AdaptiveThrottle, SpeedProfile, build_speed_profiles


REQUIRED_BAR_COLUMNS = frozenset(
    {"date", "symbol", "open", "high", "low", "close", "volume", "amount", "suspended", "price_mode"}
)
HISTORY_PARTITION_NORMALIZATION_IDENTITY = (
    "daily:1d:raw-front:v2:per-request-throttled:provider-suspension-required"
)


@dataclass(frozen=True)
class HistoryRunSpec:
    phase: CollectionPhase
    target_date: date | None = None
    sample_limit: int = 20
    publish: bool = False
    approved_speed_level: str = "initial"
    minimum_free_bytes: int = 5 * 1024**3

    def __post_init__(self) -> None:
        if self.sample_limit < 1 or self.sample_limit > 20:
            raise ValueError("Canary sample_limit must be between 1 and 20")
        if self.phase is CollectionPhase.CANARY and self.publish:
            raise ValueError("Phase 1 canary publication is forbidden")


@dataclass(frozen=True)
class FullMarketHistoryResult:
    status: str
    phase: str
    provider: str
    run_id: str | None = None
    target_date: str | None = None
    selected_symbols: tuple[str, ...] = ()
    unrepresented_categories: tuple[str, ...] = ()
    discovered_count: int = 0
    scheduled_partitions: int = 0
    completed_partitions: int = 0
    reused_partitions: int = 0
    quarantined_symbols: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    missing_dates: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    latest_complete_before: str | None = None
    latest_complete_after: str | None = None
    legacy_manifest_before: Mapping[str, str] = field(default_factory=dict)
    legacy_manifest_after: Mapping[str, str] = field(default_factory=dict)
    report_json: str | None = None
    report_markdown: str | None = None
    error: str | None = None

    @property
    def succeeded(self) -> bool:
        return self.status in {"complete", "paused"}

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["selected_symbols"] = list(self.selected_symbols)
        value["unrepresented_categories"] = list(self.unrepresented_categories)
        value["quarantined_symbols"] = {
            key: list(reasons) for key, reasons in sorted(self.quarantined_symbols.items())
        }
        value["missing_dates"] = {
            key: list(days) for key, days in sorted(self.missing_dates.items())
        }
        value["legacy_manifest_before"] = dict(sorted(self.legacy_manifest_before.items()))
        value["legacy_manifest_after"] = dict(sorted(self.legacy_manifest_after.items()))
        return value

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, sort_keys=True, indent=2)

    def to_markdown(self) -> str:
        rows = [
            "# Full-market fund history",
            "",
            f"- Status: `{self.status}`",
            f"- Phase: `{self.phase}`",
            f"- Provider: `{self.provider}`",
            f"- Run: `{self.run_id or '-'}`",
            f"- Target date: `{self.target_date or '-'}`",
            f"- Discovered instruments: {self.discovered_count}",
            f"- Selected instruments: {len(self.selected_symbols)}",
            f"- Scheduled partitions: {self.scheduled_partitions}",
            f"- Completed partitions: {self.completed_partitions}",
            f"- Reused partitions: {self.reused_partitions}",
            f"- Latest complete before: `{self.latest_complete_before or '-'}`",
            f"- Latest complete after: `{self.latest_complete_after or '-'}`",
            "",
            "## Selected symbols",
            "",
        ]
        rows.extend(f"- `{symbol}`" for symbol in self.selected_symbols)
        rows.extend(["", "## Quarantine", ""])
        if self.quarantined_symbols:
            rows.extend(
                f"- `{symbol}`: {'; '.join(reasons)}"
                for symbol, reasons in sorted(self.quarantined_symbols.items())
            )
        else:
            rows.append("- None")
        if self.unrepresented_categories:
            rows.extend(["", "## Unrepresented canary categories", ""])
            rows.extend(f"- `{item}`" for item in self.unrepresented_categories)
        if self.error:
            rows.extend(["", "## Error", "", self.error])
        return "\n".join(rows) + "\n"


class FullMarketHistoryRunner:
    """Resumable phase-1 collector. Publication remains deliberately unavailable."""

    def __init__(
        self,
        *,
        provider: Any,
        catalog: DataCatalog,
        store: VersionedParquetStore,
        report_root: str | Path,
        history_config: Mapping[str, Any],
        config_hash: str,
        legacy_paths: Sequence[str | Path] = (),
        sleeper=sleep,
        clock=monotonic,
    ) -> None:
        self.provider = provider
        self.catalog = catalog
        self.store = store
        self.report_root = Path(report_root)
        self.config = dict(history_config)
        self.config_hash = config_hash
        self.legacy_paths = tuple(Path(path) for path in legacy_paths)
        self._sleep = sleeper
        self._clock = clock

    def preflight(self, *, minimum_free_bytes: int = 5 * 1024**3) -> FullMarketHistoryResult:
        before = self.catalog.latest_complete()
        legacy = self._legacy_manifest()
        try:
            self._guard_disk(minimum_free_bytes)
            result = self.provider.preflight()
            if not result.available:
                raise RuntimeError(
                    f"Provider preflight failed: {result.health.value}: {result.detail or ''}".rstrip()
                )
            return FullMarketHistoryResult(
                "complete", "preflight", self.provider.name,
                latest_complete_before=before.version_id if before else None,
                latest_complete_after=before.version_id if before else None,
                legacy_manifest_before=legacy,
                legacy_manifest_after=legacy,
            )
        except Exception as exc:
            return FullMarketHistoryResult(
                "failed", "preflight", self.provider.name,
                latest_complete_before=before.version_id if before else None,
                latest_complete_after=before.version_id if before else None,
                legacy_manifest_before=legacy,
                legacy_manifest_after=self._legacy_manifest(),
                error=f"{type(exc).__name__}: {exc}",
            )

    def run(self, spec: HistoryRunSpec) -> FullMarketHistoryResult:
        if spec.phase is CollectionPhase.PUBLISH:
            return self._blocked_publication(spec)
        if spec.phase not in {CollectionPhase.DISCOVER, CollectionPhase.CANARY, CollectionPhase.COLLECT}:
            raise ValueError(f"Unsupported historical phase: {spec.phase.value}")
        if spec.phase is CollectionPhase.COLLECT:
            return FullMarketHistoryResult(
                "blocked", spec.phase.value, self.provider.name,
                error="Full-market collection requires the mandatory post-canary user approval and TASK-6",
            )

        latest_before = self.catalog.latest_complete()
        legacy_before = self._legacy_manifest()
        run_id: str | None = None
        try:
            self._guard_disk(spec.minimum_free_bytes)
            preflight = self.provider.preflight()
            if not preflight.available:
                raise RuntimeError(
                    f"Provider preflight failed: {preflight.health.value}: {preflight.detail or ''}".rstrip()
                )
            target = spec.target_date or self._latest_complete_trading_date()
            master = self._discover()
            if not master:
                raise RuntimeError("Fund discovery returned no supported instruments")
            selected, gaps = self.select_canary(master, spec.sample_limit)
            if spec.phase is CollectionPhase.DISCOVER:
                return self._write_report(FullMarketHistoryResult(
                    "complete", spec.phase.value, self.provider.name, target_date=target.isoformat(),
                    selected_symbols=tuple(row["symbol"] for row in selected),
                    unrepresented_categories=gaps, discovered_count=len(master),
                    latest_complete_before=latest_before.version_id if latest_before else None,
                    latest_complete_after=latest_before.version_id if latest_before else None,
                    legacy_manifest_before=legacy_before, legacy_manifest_after=self._legacy_manifest(),
                ), extra={"fund_master": master})

            run_id = "history-" + stable_fingerprint({
                "provider": self.provider.name,
                "phase": spec.phase.value,
                "target": target,
                "symbols": [row["symbol"] for row in selected],
                "config": self.config_hash,
            })[:20]
            run = self.catalog.create_collection_run(CollectionRunRecord(
                run_id, self.provider.name, spec.phase, target, self.config_hash,
                spec.approved_speed_level, CollectionRunStatus.PENDING,
            ))
            if run.status in {CollectionRunStatus.PENDING, CollectionRunStatus.PAUSED}:
                self.catalog.transition_collection_run(run_id, CollectionRunStatus.RUNNING)
            elif run.status is CollectionRunStatus.COMPLETE:
                raise RuntimeError("A completed phase-1 run cannot be reopened")

            event_counter = len(self.catalog.list_throttle_events(run_id))

            def persist_event(profile: SpeedProfile, reason: str) -> None:
                nonlocal event_counter
                event_counter += 1
                event_id = "throttle-" + stable_fingerprint({
                    "run": run_id, "sequence": event_counter, "profile": profile.name, "reason": reason,
                })[:24]
                self.catalog.record_throttle_event(ThrottleEventRecord(
                    event_id, run_id, profile.name, profile.workers,
                    profile.request_interval_seconds, profile.cooldown_every_symbols,
                    profile.cooldown_seconds, reason, audit_now(),
                ))

            profiles = build_speed_profiles(self.config["throttle"])
            approved = spec.approved_speed_level
            if approved == "maximum" and profiles[-1].name != "maximum":
                approved = profiles[-1].name
            throttle = AdaptiveThrottle(
                profiles, approved_max_level=approved, event_callback=persist_event,
                sleeper=self._sleep, clock=self._clock,
            )
            calendar = self._calendar_for(selected, target, throttle)
            quarantine: dict[str, list[str]] = {}
            identities = self._schedule(run_id, selected, target, quarantine)
            downloaded_scopes: set[tuple[str, date, date]] = set()
            reused = completed = 0
            for symbol in sorted({item.symbol for item in identities}):
                for identity in [item for item in identities if item.symbol == symbol]:
                    outcome = self._collect_partition(
                        run_id, identity, throttle, downloaded_scopes=downloaded_scopes,
                    )
                    reused += int(outcome == "reused")
                    completed += int(outcome in {"reused", "complete"})
                    if outcome.startswith("quarantined:"):
                        quarantine.setdefault(symbol, []).append(outcome.split(":", 1)[1])
                    if throttle.paused:
                        raise RuntimeError("Provider service failure paused collection at the initial profile")
                throttle.complete_symbol()

            missing = self._validate_symbols(selected, identities, calendar, quarantine)
            latest_after = self.catalog.latest_complete()
            legacy_after = self._legacy_manifest()
            if legacy_after != legacy_before:
                raise RuntimeError("Legacy v1 SHA-256 manifest changed during phase 1")
            before_id = latest_before.version_id if latest_before else None
            after_id = latest_after.version_id if latest_after else None
            if after_id != before_id:
                raise RuntimeError("Phase 1 changed latest_complete")
            self.catalog.transition_collection_run(run_id, CollectionRunStatus.PAUSED)
            result = FullMarketHistoryResult(
                "paused", spec.phase.value, self.provider.name, run_id, target.isoformat(),
                tuple(row["symbol"] for row in selected), gaps, len(master), len(identities), completed,
                reused, {key: tuple(sorted(set(value))) for key, value in quarantine.items()},
                missing, before_id, after_id, legacy_before, legacy_after,
            )
            return self._write_report(result, extra={
                "fund_master": master,
                "partitions": [self._partition_dict(item) for item in self.catalog.list_partitions(run_id=run_id)],
                "attempts": {
                    item.fingerprint: [asdict(attempt) for attempt in self.catalog.list_attempts(item.fingerprint)]
                    for item in identities
                },
                "throttle_events": [asdict(item) for item in self.catalog.list_throttle_events(run_id)],
                "publication_decision": "forbidden_phase_1",
            })
        except Exception as exc:
            if run_id is not None:
                try:
                    run = self.catalog.get_collection_run(run_id)
                    if run.status is CollectionRunStatus.RUNNING:
                        self.catalog.transition_collection_run(run_id, CollectionRunStatus.FAILED, error=str(exc))
                except Exception:
                    pass
            latest_after = self.catalog.latest_complete()
            result = FullMarketHistoryResult(
                "failed", spec.phase.value, self.provider.name, run_id,
                target_date=spec.target_date.isoformat() if spec.target_date else None,
                latest_complete_before=latest_before.version_id if latest_before else None,
                latest_complete_after=latest_after.version_id if latest_after else None,
                legacy_manifest_before=legacy_before, legacy_manifest_after=self._legacy_manifest(),
                error=f"{type(exc).__name__}: {exc}",
            )
            return self._write_report(result)

    @staticmethod
    def select_canary(master: Sequence[Mapping[str, Any]], limit: int) -> tuple[list[dict[str, Any]], tuple[str, ...]]:
        rows = [dict(row) for row in sorted(master, key=lambda item: str(item["symbol"]))]
        dimensions = (
            ("exchange", ("SH", "SZ")),
            ("product_type", ("ETF", "LOF", "MONEY_ETF")),
            ("is_active", (1, 0)),
        )
        chosen: list[dict[str, Any]] = []
        seen: set[str] = set()
        gaps: list[str] = []
        for column, values in dimensions:
            for value in values:
                candidates = [row for row in rows if row.get(column) == value]
                if not candidates:
                    gaps.append(f"{column}={value}")
                    continue
                candidate = candidates[0]
                if candidate["symbol"] not in seen and len(chosen) < limit:
                    chosen.append(candidate); seen.add(candidate["symbol"])
        dated = [row for row in rows if row.get("listed_date")]
        for candidate in (
            min(dated, key=lambda row: (row["listed_date"], row["symbol"])) if dated else None,
            max(dated, key=lambda row: (row["listed_date"], row["symbol"])) if dated else None,
        ):
            if candidate and candidate["symbol"] not in seen and len(chosen) < limit:
                chosen.append(candidate); seen.add(candidate["symbol"])
        for candidate in rows:
            if len(chosen) >= limit:
                break
            if candidate["symbol"] not in seen:
                chosen.append(candidate); seen.add(candidate["symbol"])
        return chosen, tuple(sorted(gaps))

    def _discover(self) -> list[dict[str, Any]]:
        rows = self.provider.get_instruments()
        supported = {"ETF", "LOF", "MONEY_ETF"}
        return sorted(
            [dict(row) for row in rows if row.get("product_type") in supported],
            key=lambda row: str(row["symbol"]),
        )

    def _latest_complete_trading_date(self) -> date:
        now = datetime.now(ZoneInfo("Asia/Hong_Kong"))
        end = now.date() if now.time() >= time(18, 0) else now.date() - timedelta(days=1)
        start = end - timedelta(days=45)
        frame, _ = self.provider.fetch(ProviderRequest(
            (str(getattr(self.provider, "config", {}).get("calendar_probe_symbol", "510300.SH")),),
            start, end, ProviderCapability.TRADING_CALENDAR,
        ))
        values = sorted(date.fromisoformat(str(value)[:10]) for value in frame.get("date", []))
        if not values:
            raise RuntimeError("Trading calendar returned no complete trading date")
        return values[-1]

    def _calendar_for(
        self, selected: Sequence[Mapping[str, Any]], target: date, throttle: AdaptiveThrottle,
    ) -> tuple[str, ...]:
        starts = [date.fromisoformat(str(row["listed_date"])[:10]) for row in selected if row.get("listed_date")]
        if not starts:
            return ()
        throttle.before_request("trading_calendar_fetch")
        started = self._clock()
        frame, result = self.provider.fetch(ProviderRequest(
            (str(selected[0]["symbol"]),), min(starts), target, ProviderCapability.TRADING_CALENDAR,
        ))
        errors = [item.error for item in result.symbols if item.error]
        if errors:
            throttle.observe_failure("calendar_error", system_failure=True)
            raise RuntimeError("Trading calendar failed: " + "; ".join(errors))
        throttle.observe_success(latency_seconds=self._clock() - started)
        return tuple(sorted({str(value)[:10] for value in frame.get("date", []) if value is not None}))

    def _schedule(
        self, run_id: str, selected: Sequence[Mapping[str, Any]], target: date,
        quarantine: dict[str, list[str]],
    ) -> list[PartitionIdentity]:
        identities: list[PartitionIdentity] = []
        years = max(1, int(self.config.get("partition_years", 1)))
        source_identity = f"{self.provider.name}:{HISTORY_PARTITION_NORMALIZATION_IDENTITY}"
        for row in selected:
            symbol = str(row["symbol"])
            if not row.get("listed_date"):
                quarantine.setdefault(symbol, []).append("missing_listing_date")
                continue
            start = date.fromisoformat(str(row["listed_date"])[:10])
            end = min(target, date.fromisoformat(str(row["delisted_date"])[:10])) if row.get("delisted_date") else target
            if start > end:
                quarantine.setdefault(symbol, []).append("invalid_validity_interval")
                continue
            cursor = start
            while cursor <= end:
                block_end_year = cursor.year + years - 1
                partition_end = min(end, date(block_end_year, 12, 31))
                for mode in (PriceMode.RAW, PriceMode.ADJUSTED):
                    identity = PartitionIdentity(
                        self.provider.name, symbol, mode, cursor, partition_end, source_identity,
                    )
                    self.catalog.create_partition(CollectionPartitionRecord(
                        identity.fingerprint, run_id, identity, PartitionStatus.PENDING,
                    ))
                    identities.append(identity)
                cursor = partition_end + timedelta(days=1)
        return identities

    def _collect_partition(
        self, run_id: str, identity: PartitionIdentity, throttle: AdaptiveThrottle,
        *, downloaded_scopes: set[tuple[str, date, date]],
    ) -> str:
        record = self.catalog.get_partition(identity.fingerprint)
        if record.status is PartitionStatus.COMPLETE:
            try:
                self.store.validate_partition(
                    identity, expected_checksum=record.checksum, expected_row_count=record.row_count,
                )
                return "reused"
            except CorruptPartitionError as exc:
                raise RuntimeError(f"Validated resume rejected corrupt partition: {exc}") from exc
        if record.status is PartitionStatus.QUARANTINED:
            return f"quarantined:{record.error or 'previously_quarantined'}"
        for stale in self.catalog.list_attempts(identity.fingerprint):
            if stale.status is AttemptStatus.RUNNING:
                self.catalog.finish_attempt(stale.attempt_id, AttemptStatus.FAILED, error="interrupted_before_resume")
        attempts = len(self.catalog.list_attempts(identity.fingerprint))
        max_attempts = int(self.config["retry"]["max_attempts"])
        backoff = tuple(float(value) for value in self.config["retry"].get("backoff_seconds", ()))
        if attempts >= max_attempts:
            if record.status is PartitionStatus.RUNNING:
                self.catalog.transition_partition(identity.fingerprint, PartitionStatus.QUARANTINED,
                                                  error="retry_limit_exhausted")
            elif record.status is PartitionStatus.FAILED:
                self.catalog.transition_partition(identity.fingerprint, PartitionStatus.QUARANTINED,
                                                  error="retry_limit_exhausted")
            return "quarantined:retry_limit_exhausted"
        if record.status in {PartitionStatus.PENDING, PartitionStatus.FAILED}:
            self.catalog.transition_partition(identity.fingerprint, PartitionStatus.RUNNING)

        while attempts < max_attempts:
            attempt_number = attempts + 1
            attempt_id = "attempt-" + stable_fingerprint({
                "partition": identity.fingerprint, "number": attempt_number,
            })[:24]
            self.catalog.start_attempt(identity.fingerprint, attempt_id)
            try:
                started = self._clock()
                download_scope = (identity.symbol, identity.start_date, identity.end_date)
                if download_scope not in downloaded_scopes:
                    throttle.before_request(
                        f"download_daily_bar:{identity.symbol}:{identity.start_date}:{identity.end_date}"
                    )
                    self.provider.download_daily_bar(
                        [identity.symbol], identity.start_date.isoformat(), identity.end_date.isoformat(),
                    )
                    downloaded_scopes.add(download_scope)
                capability = (
                    ProviderCapability.DAILY_BARS_RAW
                    if identity.price_mode is PriceMode.RAW
                    else ProviderCapability.DAILY_BARS_ADJUSTED
                )
                throttle.before_request(
                    f"fetch:{capability.value}:{identity.symbol}:{identity.start_date}:{identity.end_date}"
                )
                frame, result = self.provider.fetch(ProviderRequest(
                    (identity.symbol,), identity.start_date, identity.end_date, capability,
                ))
                errors = [item.error for item in result.symbols if item.symbol == identity.symbol and item.error]
                if errors:
                    raise PartitionQualityError("; ".join(errors))
                normalized = self._validate_partition_frame(frame, identity)
                artifact = self.store.write_partition(
                    identity, pa.Table.from_pandas(normalized, preserve_index=False),
                )
                self.catalog.finish_attempt(attempt_id, AttemptStatus.SUCCEEDED)
                self.catalog.transition_partition(
                    identity.fingerprint, PartitionStatus.COMPLETE, row_count=artifact.row_count,
                    checksum=artifact.checksum, storage_path=artifact.path,
                )
                throttle.observe_success(latency_seconds=self._clock() - started)
                return "complete"
            except PartitionQualityError as exc:
                self.catalog.finish_attempt(attempt_id, AttemptStatus.FAILED, error=str(exc))
                throttle.observe_failure("partition_quality", system_failure=False)
                self.catalog.transition_partition(
                    identity.fingerprint, PartitionStatus.QUARANTINED, error=str(exc),
                )
                return f"quarantined:{exc}"
            except Exception as exc:
                self.catalog.finish_attempt(attempt_id, AttemptStatus.FAILED, error=f"{type(exc).__name__}: {exc}")
                attempts += 1
                service_failure = isinstance(exc, ProviderUnavailableError)
                throttle.observe_failure(type(exc).__name__, system_failure=service_failure)
                self.catalog.transition_partition(
                    identity.fingerprint, PartitionStatus.FAILED, error=f"{type(exc).__name__}: {exc}",
                )
                if service_failure:
                    raise
                if attempts >= max_attempts:
                    self.catalog.transition_partition(
                        identity.fingerprint, PartitionStatus.QUARANTINED,
                        error=f"retry_limit_exhausted: {type(exc).__name__}: {exc}",
                    )
                    return f"quarantined:retry_limit_exhausted:{type(exc).__name__}"
                self._sleep(backoff[min(attempts - 1, len(backoff) - 1)] if backoff else 0)
                self.catalog.transition_partition(identity.fingerprint, PartitionStatus.RUNNING)
        return "quarantined:retry_limit_exhausted"

    @staticmethod
    def _validate_partition_frame(frame: pd.DataFrame, identity: PartitionIdentity) -> pd.DataFrame:
        missing = REQUIRED_BAR_COLUMNS - set(frame.columns)
        if missing:
            raise PartitionQualityError("missing_columns:" + ",".join(sorted(missing)))
        data = frame.copy()
        data["date"] = data["date"].astype(str).str[:10]
        data = data[data["symbol"].astype(str) == identity.symbol]
        data = data[(data["date"] >= identity.start_date.isoformat()) & (data["date"] <= identity.end_date.isoformat())]
        if data.empty:
            raise PartitionQualityError("no_data")
        expected_mode = identity.price_mode.value
        if set(data["price_mode"].astype(str)) != {expected_mode}:
            raise PartitionQualityError(f"price_mode_mismatch:expected={expected_mode}")
        if data.duplicated(["date", "symbol"]).any():
            raise PartitionQualityError("duplicate_date_symbol")
        invalid_ohlc = (
            (data["high"] < data[["open", "close", "low"]].max(axis=1))
            | (data["low"] > data[["open", "close", "high"]].min(axis=1))
        )
        if invalid_ohlc.any():
            raise PartitionQualityError("invalid_ohlc")
        if (pd.to_numeric(data["volume"], errors="coerce") < 0).any() or (
            pd.to_numeric(data["amount"], errors="coerce") < 0
        ).any():
            raise PartitionQualityError("negative_volume_or_amount")
        return data.sort_values(["date", "symbol"]).reset_index(drop=True)

    def _validate_symbols(
        self,
        selected: Sequence[Mapping[str, Any]],
        identities: Sequence[PartitionIdentity],
        calendar: Sequence[str],
        quarantine: dict[str, list[str]],
    ) -> dict[str, tuple[str, ...]]:
        threshold = float(self.config["coverage"]["min_symbol_trading_day_ratio"])
        missing_by_symbol: dict[str, tuple[str, ...]] = {}
        for row in selected:
            symbol = str(row["symbol"])
            symbol_ids = [item for item in identities if item.symbol == symbol]
            frames: dict[PriceMode, list[pd.DataFrame]] = {PriceMode.RAW: [], PriceMode.ADJUSTED: []}
            for identity in symbol_ids:
                record = self.catalog.get_partition(identity.fingerprint)
                if record.status is PartitionStatus.COMPLETE:
                    frames[identity.price_mode].append(self.store.read_partition(identity).to_pandas())
            if not frames[PriceMode.RAW] or not frames[PriceMode.ADJUSTED]:
                quarantine.setdefault(symbol, []).append("raw_adjusted_partition_incomplete")
                continue
            raw = pd.concat(frames[PriceMode.RAW], ignore_index=True)
            adjusted = pd.concat(frames[PriceMode.ADJUSTED], ignore_index=True)
            raw_keys = set(zip(raw["date"].astype(str), raw["symbol"].astype(str)))
            adjusted_keys = set(zip(adjusted["date"].astype(str), adjusted["symbol"].astype(str)))
            if raw_keys != adjusted_keys:
                quarantine.setdefault(symbol, []).append("raw_adjusted_key_mismatch")
            start = str(row["listed_date"])[:10]
            end = min(str(row.get("delisted_date") or "9999-12-31")[:10], max(calendar, default="0001-01-01"))
            expected = tuple(day for day in calendar if start <= day <= end)
            actual = {day for day, key_symbol in raw_keys if key_symbol == symbol}
            missing = tuple(day for day in expected if day not in actual)
            missing_by_symbol[symbol] = missing
            coverage = len(actual & set(expected)) / len(expected) if expected else 0.0
            if coverage < threshold:
                quarantine.setdefault(symbol, []).append(f"trading_day_coverage:{coverage:.6f}")
        return dict(sorted(missing_by_symbol.items()))

    def _guard_disk(self, minimum_free_bytes: int) -> None:
        self.store.initialize()
        self.report_root.mkdir(parents=True, exist_ok=True)
        free = shutil.disk_usage(self.store.root).free
        if free < minimum_free_bytes:
            raise RuntimeError(f"Insufficient disk space: free={free}, required={minimum_free_bytes}")

    def _legacy_manifest(self) -> dict[str, str]:
        output: dict[str, str] = {}
        for root in self.legacy_paths:
            if root.is_file():
                output[str(root.resolve())] = self._hash_file(root)
            elif root.is_dir():
                for path in sorted(item for item in root.rglob("*") if item.is_file()):
                    output[str(path.resolve())] = self._hash_file(path)
        return output

    @staticmethod
    def _hash_file(path: Path) -> str:
        digest = sha256()
        with path.open("rb") as stream:
            for block in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(block)
        return digest.hexdigest()

    def _write_report(
        self, result: FullMarketHistoryResult, *, extra: Mapping[str, Any] | None = None,
    ) -> FullMarketHistoryResult:
        self.report_root.mkdir(parents=True, exist_ok=True)
        stem = f"full-market-{result.phase}-{result.run_id or result.target_date or 'preflight'}"
        json_path = self.report_root / f"{stem}.json"
        markdown_path = self.report_root / f"{stem}.md"
        payload = result.to_dict()
        payload["evidence"] = self._json_safe(extra or {})
        payload["report_json"] = str(json_path)
        payload["report_markdown"] = str(markdown_path)
        json_path.write_text(
            json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
            encoding="utf-8", newline="\n",
        )
        markdown_path.write_text(result.to_markdown(), encoding="utf-8", newline="\n")
        return FullMarketHistoryResult(**{
            **asdict(result), "report_json": str(json_path), "report_markdown": str(markdown_path),
        })

    def _blocked_publication(self, spec: HistoryRunSpec) -> FullMarketHistoryResult:
        return self._write_report(FullMarketHistoryResult(
            "blocked", spec.phase.value, self.provider.name,
            target_date=spec.target_date.isoformat() if spec.target_date else None,
            error="Publication is unavailable in phase 1; latest_complete was not modified",
        ))

    @staticmethod
    def _partition_dict(record: CollectionPartitionRecord) -> dict[str, Any]:
        value = asdict(record)
        value["identity"] = asdict(record.identity)
        return value

    @classmethod
    def _json_safe(cls, value: Any) -> Any:
        if isinstance(value, Mapping):
            return {str(key): cls._json_safe(item) for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))}
        if isinstance(value, (list, tuple)):
            return [cls._json_safe(item) for item in value]
        if isinstance(value, (date, datetime)):
            return value.isoformat()
        if hasattr(value, "value"):
            return value.value
        return value


class PartitionQualityError(RuntimeError):
    pass
