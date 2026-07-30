from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
import shutil
import tempfile
from time import sleep
from typing import Any, Iterable, Mapping

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from fundlab.common.canonical import canonical_json, stable_digest, to_primitive
from fundlab.marketdata.contracts import (
    CoverageClaim,
    IntegrityError,
    MarketTable,
    ObservationError,
    ObservationManifest,
    ObservationPayload,
    ProviderCapability,
    ProviderRequest,
    QualityReport,
    ReadinessProfile,
    SnapshotManifest,
    SnapshotComponentSelection,
    SnapshotNotReadyError,
    SnapshotPlan,
    SnapshotState,
    SourceConflictError,
    SourceSlice,
    StoredFile,
    UniverseScope,
)
from fundlab.marketdata.schema import (
    TABLE_DATE_COLUMNS,
    TABLE_INSTRUMENT_COLUMNS,
    TABLE_KEYS,
    normalize_table,
    validate_snapshot_tables,
)


WAREHOUSE_SCHEMA_VERSION = 6


@contextmanager
def _exclusive_pointer_lock(path: Path):
    """Hold one OS-level byte lock while checking/replacing current.json."""

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+b") as handle:
        handle.seek(0, os.SEEK_END)
        if handle.tell() == 0:
            handle.write(b"\0")
            handle.flush()
            os.fsync(handle.fileno())
        handle.seek(0)
        if os.name == "nt":
            import msvcrt

            msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
            try:
                yield
            finally:
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


class MarketDataWarehouse:
    """Immutable source observations and version-pinned canonical snapshots."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root).resolve()
        self.observation_root = self.root / "observations"
        self.snapshot_root = self.root / "snapshots"

    def initialize(self) -> None:
        self.observation_root.mkdir(parents=True, exist_ok=True)
        self.snapshot_root.mkdir(parents=True, exist_ok=True)

    def record_observation(self, payload: ObservationPayload) -> ObservationManifest:
        self.initialize()
        temporary = Path(tempfile.mkdtemp(prefix=".observation-", dir=self.observation_root))
        try:
            files: list[StoredFile] = []
            observed_at = payload.observed_at.astimezone(timezone.utc).isoformat()
            for table in sorted(payload.tables, key=lambda item: item.value):
                frame = normalize_table(
                    table,
                    payload.tables[table],
                    provider=payload.provider,
                    observed_at=observed_at,
                    require_observation_id=False,
                )
                path = temporary / f"{table.value}.parquet"
                frame.to_parquet(path, index=False, engine="pyarrow", compression="zstd")
                files.append(StoredFile(table, path.name, _file_hash(path), len(frame)))

            return self._commit_observation(
                temporary,
                provider=payload.provider,
                observed_at=observed_at,
                request=payload.request,
                coverage=payload.coverage,
                files=tuple(files),
                source_metadata=payload.source_metadata,
            )
        finally:
            if temporary.exists():
                shutil.rmtree(temporary)

    def record_streamed_observation(
        self,
        *,
        provider: str,
        observed_at: datetime,
        request: ProviderRequest,
        table_batches: Mapping[MarketTable, Iterable[pd.DataFrame]],
        coverage: tuple[CoverageClaim, ...],
        source_metadata: Mapping[str, Any] | None = None,
    ) -> ObservationManifest:
        """Compact many read-only source batches without holding the whole observation in memory."""
        if not provider.strip() or observed_at.tzinfo is None or not table_batches:
            raise ValueError("Streamed observation requires provider, aware observed_at, and table batches")
        if not set(table_batches) <= {claim.table for claim in coverage}:
            raise ValueError("Every streamed table requires an explicit coverage claim")
        self.initialize()
        temporary = Path(tempfile.mkdtemp(prefix=".observation-", dir=self.observation_root))
        observed_text = observed_at.astimezone(timezone.utc).isoformat()
        try:
            files: list[StoredFile] = []
            for table in sorted(table_batches, key=lambda item: item.value):
                path = temporary / f"{table.value}.parquet"
                writer: pq.ParquetWriter | None = None
                row_count = 0
                try:
                    for batch in table_batches[table]:
                        normalized = normalize_table(
                            table, batch, provider=provider, observed_at=observed_text,
                            require_observation_id=False,
                        )
                        arrow = pa.Table.from_pandas(normalized, preserve_index=False)
                        if writer is None:
                            writer = pq.ParquetWriter(path, arrow.schema, compression="zstd")
                        elif arrow.schema != writer.schema:
                            arrow = arrow.cast(writer.schema)
                        writer.write_table(arrow)
                        row_count += len(normalized)
                    if writer is None:
                        from fundlab.marketdata.schema import empty_table

                        empty = normalize_table(
                            table, empty_table(table), provider=provider, observed_at=observed_text,
                            require_observation_id=False,
                        )
                        arrow = pa.Table.from_pandas(empty, preserve_index=False)
                        writer = pq.ParquetWriter(path, arrow.schema, compression="zstd")
                        writer.write_table(arrow)
                finally:
                    if writer is not None:
                        writer.close()
                files.append(StoredFile(table, path.name, _file_hash(path), row_count))
            return self._commit_observation(
                temporary,
                provider=provider,
                observed_at=observed_text,
                request=request,
                coverage=coverage,
                files=tuple(files),
                source_metadata=source_metadata or {},
            )
        finally:
            if temporary.exists():
                shutil.rmtree(temporary)

    def _commit_observation(
        self,
        temporary: Path,
        *,
        provider: str,
        observed_at: str,
        request: ProviderRequest,
        coverage: tuple[CoverageClaim, ...],
        files: tuple[StoredFile, ...],
        source_metadata: Mapping[str, Any],
    ) -> ObservationManifest:
        identity = {
            "provider": provider,
            "observed_at": observed_at,
            "request": request,
            "coverage": coverage,
            "files": files,
            "source_metadata": source_metadata,
            "schema_version": WAREHOUSE_SCHEMA_VERSION,
        }
        observation_id = f"obs-{stable_digest(identity)[:24]}"
        manifest = ObservationManifest(
            observation_id,
            provider,
            datetime.fromisoformat(observed_at),
            request,
            coverage,
            files,
            dict(source_metadata),
            WAREHOUSE_SCHEMA_VERSION,
        )
        _write_manifest(temporary / "manifest.json", manifest)
        target = self.observation_path(observation_id)
        if target.exists():
            existing = self.load_observation(observation_id)
            if _manifest_identity(existing) != _manifest_identity(manifest):
                raise IntegrityError(f"Observation id collision: {observation_id}")
            return existing
        for attempt in range(4):
            try:
                os.replace(temporary, target)
                break
            except PermissionError:
                # Windows scanners and indexers can briefly hold a newly written
                # directory.  Preserve the atomic rename contract, but tolerate
                # that transient lock instead of discarding a valid observation.
                if target.exists():
                    existing = self.load_observation(observation_id)
                    if _manifest_identity(existing) != _manifest_identity(manifest):
                        raise IntegrityError(f"Observation id collision: {observation_id}")
                    return existing
                if attempt == 3:
                    raise
                sleep(0.05 * (2 ** attempt))
        return self.load_observation(observation_id)

    def observation_path(self, observation_id: str) -> Path:
        _validate_id(observation_id, "obs-")
        return self.observation_root / observation_id

    def load_observation(self, observation_id: str) -> ObservationManifest:
        directory = self.observation_path(observation_id)
        payload = _read_json(directory / "manifest.json")
        manifest = _observation_manifest(payload)
        if manifest.observation_id != observation_id:
            raise IntegrityError(f"Observation manifest identity mismatch: {observation_id}")
        self._verify_files(directory, manifest.files)
        expected = f"obs-{stable_digest(_observation_identity(manifest))[:24]}"
        if expected != observation_id:
            raise IntegrityError(f"Observation content identity mismatch: {observation_id}")
        return manifest

    def observations(self, *, provider: str | None = None) -> tuple[ObservationManifest, ...]:
        """List verified immutable observations without relying on a mutable catalog."""

        if not self.observation_root.is_dir():
            return ()
        found = []
        for directory in sorted(self.observation_root.glob("obs-*")):
            if not directory.is_dir():
                continue
            if provider is not None:
                candidate = _observation_manifest(_read_json(directory / "manifest.json"))
                if candidate.provider != provider:
                    continue
            manifest = self.load_observation(directory.name)
            found.append(manifest)
        return tuple(sorted(found, key=lambda item: (item.observed_at, item.observation_id)))

    def matching_observations(
        self,
        *,
        provider: str,
        request: ProviderRequest,
    ) -> tuple[ObservationManifest, ...]:
        """Find exact request matches, verifying Parquet only for matching manifests."""

        if not self.observation_root.is_dir():
            return ()
        found = []
        for manifest_path in sorted(self.observation_root.glob("obs-*/manifest.json")):
            candidate = _observation_manifest(_read_json(manifest_path))
            if candidate.provider == provider and candidate.request == request:
                found.append(self.load_observation(candidate.observation_id))
        return tuple(sorted(found, key=lambda item: (item.observed_at, item.observation_id)))

    def read_observation_table(self, observation_id: str, table: MarketTable) -> pd.DataFrame:
        manifest = self.load_observation(observation_id)
        item = next((item for item in manifest.files if item.table is table), None)
        if item is None:
            raise ObservationError(f"Observation {observation_id} has no {table.value} table")
        return pd.read_parquet(self.observation_path(observation_id) / item.path)

    def build_snapshot(self, plan: SnapshotPlan) -> SnapshotManifest:
        if plan.readiness is ReadinessProfile.SIMULATION:
            raise SnapshotNotReadyError(
                "Non-componentized simulation snapshot construction is retired; "
                "validate an EOD partition and use the componentized incremental publisher"
            )
        self.initialize()
        tables: dict[MarketTable, pd.DataFrame] = {}
        coverage_errors: list[str] = []
        selected_manifests: dict[str, ObservationManifest] = {}
        for table in MarketTable:
            selections = tuple(item for item in plan.selections if item.table is table)
            if not selections:
                continue
            pieces: list[pd.DataFrame] = []
            for ordinal, selection in enumerate(selections):
                manifest = self.load_observation(selection.observation_id)
                selected_manifests[manifest.observation_id] = manifest
                frame = self.read_observation_table(selection.observation_id, table)
                frame = _apply_slice(frame, selection)
                frame["source_observation_id"] = selection.observation_id
                frame["_selection_priority"] = selection.priority
                frame["_selection_ordinal"] = ordinal
                pieces.append(frame)
                if plan.require_complete_coverage:
                    coverage_errors.extend(_coverage_errors(manifest, selection, frame))
            combined = pd.concat(pieces, ignore_index=True) if pieces else pd.DataFrame()
            tables[table] = _resolve_explicit_conflicts(table, combined)

        unreconciled = sorted(
            manifest.observation_id
            for manifest in selected_manifests.values()
            if manifest.source_metadata.get("kind") != "field_level_reconciliation"
        )
        coverage_errors.extend(
            f"trust:unreconciled_observation:{observation_id}"
            for observation_id in unreconciled
        )
        not_ready = sorted(
            manifest.observation_id
            for manifest in selected_manifests.values()
            if manifest.source_metadata.get("kind") == "field_level_reconciliation"
            and not _reconciliation_ready(manifest.source_metadata)
        )
        coverage_errors.extend(
            f"trust:reconciliation_not_ready:{observation_id}"
            for observation_id in not_ready
        )

        quality = validate_snapshot_tables(
            tables,
            coverage_errors=tuple(coverage_errors),
            profile=plan.readiness,
            universe_scope=plan.universe_scope,
        )
        referenced_files: list[StoredFile] = []
        seen_references: set[tuple[str, MarketTable]] = set()
        for selection in plan.selections:
            reference_key = (selection.observation_id, selection.table)
            if reference_key in seen_references:
                continue
            observed = self.load_observation(selection.observation_id)
            source_file = next((item for item in observed.files if item.table is selection.table), None)
            if source_file is None:
                raise ObservationError(
                    f"Observation {selection.observation_id} has no {selection.table.value} table"
                )
            referenced_files.append(StoredFile(
                selection.table,
                f"{selection.observation_id}/{source_file.path}",
                source_file.sha256,
                source_file.row_count,
            ))
            seen_references.add(reference_key)
        files = tuple(sorted(referenced_files, key=lambda item: (item.table.value, item.path)))
        return self._commit_snapshot(plan, quality, files)

    def build_partitioned_snapshot(self, plan: SnapshotPlan) -> SnapshotManifest:
        """Build a research snapshot from disjoint, independently validated partitions.

        A full A-share history is too large to concatenate in memory merely to repeat
        validation that already ran before each immutable reconciled partition was
        recorded.  This path verifies every file hash, trust marker, coverage claim,
        instrument scope and partition-quality declaration, while loading only the
        small instrument master pieces.  It deliberately supports research readiness
        only. Simulation snapshots must be published through the componentized
        incremental path, which owns their scoped validation and manifest assembly.
        """

        if plan.readiness is ReadinessProfile.SIMULATION:
            raise ValueError(
                "Partitioned simulation snapshots are retired; "
                "use the componentized incremental publisher"
            )
        if plan.readiness is not ReadinessProfile.RESEARCH_PRICE:
            raise ValueError("Unsupported partitioned snapshot readiness")
        self.initialize()
        errors: list[str] = []
        warnings: list[str] = []
        manifests: dict[str, ObservationManifest] = {}
        referenced_files: list[StoredFile] = []
        seen_references: set[tuple[str, MarketTable]] = set()
        scopes: dict[MarketTable, set[str]] = {
            MarketTable.INSTRUMENTS: set(),
            MarketTable.DAILY_BARS: set(),
        }
        instrument_frames: list[pd.DataFrame] = []
        row_counts = {MarketTable.INSTRUMENTS.value: 0, MarketTable.DAILY_BARS.value: 0}

        selected_tables = {selection.table for selection in plan.selections}
        required = {MarketTable.INSTRUMENTS, MarketTable.DAILY_BARS}
        errors.extend(
            f"missing_table:{table.value}"
            for table in sorted(required - selected_tables, key=lambda item: item.value)
        )
        unsupported = selected_tables - required
        errors.extend(
            f"partitioned_snapshot_unsupported_table:{table.value}"
            for table in sorted(unsupported, key=lambda item: item.value)
        )

        for selection in plan.selections:
            if selection.table not in required:
                continue
            manifest = manifests.get(selection.observation_id)
            if manifest is None:
                manifest = self.load_observation(selection.observation_id)
                manifests[manifest.observation_id] = manifest
            if manifest.source_metadata.get("kind") != "field_level_reconciliation":
                errors.append(f"trust:unreconciled_observation:{manifest.observation_id}")
            if not _reconciliation_ready(manifest.source_metadata):
                errors.append(f"trust:reconciliation_not_ready:{manifest.observation_id}")
            partition_quality = manifest.source_metadata.get("partition_quality")
            if not isinstance(partition_quality, Mapping) or not partition_quality.get("validated"):
                errors.append(f"trust:partition_not_validated:{manifest.observation_id}")
                partition_quality = {}
            if partition_quality.get("readiness") != ReadinessProfile.RESEARCH_PRICE.value:
                errors.append(f"trust:partition_readiness_mismatch:{manifest.observation_id}")

            declared_ids = tuple(sorted(map(str, partition_quality.get("instrument_ids", ()))))
            selected_ids = tuple(sorted(selection.instrument_ids or declared_ids))
            if not selected_ids or not set(selected_ids) <= set(declared_ids):
                errors.append(f"partition_scope_mismatch:{selection.table.value}:{manifest.observation_id}")
            overlap = scopes[selection.table].intersection(selected_ids)
            if overlap:
                errors.append(
                    f"partition_scope_overlap:{selection.table.value}:{','.join(sorted(overlap)[:10])}"
                )
            scopes[selection.table].update(selected_ids)

            expected_start = partition_quality.get("start_date")
            expected_end = partition_quality.get("end_date")
            if selection.table is MarketTable.DAILY_BARS and (
                selection.start_date is None
                or selection.start_date.isoformat() != expected_start
                or selection.end_date is None
                or selection.end_date.isoformat() != expected_end
            ):
                errors.append(f"partition_date_scope_mismatch:{manifest.observation_id}")

            stored = next((item for item in manifest.files if item.table is selection.table), None)
            if stored is None:
                errors.append(f"missing_partition_file:{selection.table.value}:{manifest.observation_id}")
                continue
            reference_key = (manifest.observation_id, selection.table)
            if reference_key not in seen_references:
                referenced_files.append(StoredFile(
                    selection.table,
                    f"{manifest.observation_id}/{stored.path}",
                    stored.sha256,
                    stored.row_count,
                ))
                seen_references.add(reference_key)
            filters: list[tuple[str, str, Any]] = []
            if selected_ids:
                filters.append(("instrument_id", "in", list(selected_ids)))
            if selection.table is MarketTable.DAILY_BARS and selection.start_date is not None:
                filters.extend((
                    ("session_date", ">=", selection.start_date.isoformat()),
                    ("session_date", "<=", selection.end_date.isoformat()),
                ))
            source_path = self.observation_path(manifest.observation_id) / stored.path
            logical_rows = pq.read_table(
                source_path,
                columns=["instrument_id"],
                filters=filters or None,
            ).num_rows
            row_counts[selection.table.value] += logical_rows

            empty = pd.DataFrame(columns=(
                "instrument_id", "session_date", "price_mode",
            ))
            errors.extend(_coverage_errors(manifest, selection, empty))
            if selection.table is MarketTable.INSTRUMENTS:
                frame = self.read_observation_table(manifest.observation_id, selection.table)
                frame = _apply_slice(frame, selection)
                frame["source_observation_id"] = manifest.observation_id
                instrument_frames.append(frame)

        if scopes[MarketTable.INSTRUMENTS] != scopes[MarketTable.DAILY_BARS]:
            errors.append("partition_instrument_bar_scope_mismatch")
        if (
            plan.universe_scope is not None
            and scopes[MarketTable.INSTRUMENTS] != set(plan.universe_scope.instrument_ids)
        ):
            errors.append("snapshot_universe_instrument_mismatch")
        instruments = (
            pd.concat(instrument_frames, ignore_index=True)
            if instrument_frames else pd.DataFrame()
        )
        if not instruments.empty:
            try:
                instruments = normalize_table(
                    MarketTable.INSTRUMENTS, instruments, require_observation_id=True,
                )
                if instruments.duplicated(["instrument_id"]).any():
                    errors.append("duplicate_key:instruments")
                if instruments["listed_date"].isna().any():
                    errors.append("missing_instrument_listed_date")
                if (instruments["buy_lot"] <= 0).any() or (instruments["price_tick"] <= 0).any():
                    errors.append("invalid_instrument_trading_rule")
                if (instruments["sell_delay_sessions"].dropna() < 0).any():
                    errors.append("invalid_sell_delay")
                if not set(instruments["asset_type"].dropna()) <= {"stock", "etf"}:
                    errors.append("unsupported_asset_type")
            except ObservationError as exc:
                errors.append(f"schema:instruments:{exc}")
        else:
            errors.append("empty_instruments")
        if row_counts[MarketTable.DAILY_BARS.value] <= 0:
            errors.append("missing_raw_bars")

        quality = QualityReport(
            SnapshotState.READY if not errors else SnapshotState.INCOMPLETE,
            tuple(sorted(set(errors))),
            tuple(sorted(set(warnings))),
            row_counts,
        )
        files = tuple(sorted(referenced_files, key=lambda item: (item.table.value, item.path)))
        return self._commit_snapshot(plan, quality, files)

    def build_component_snapshot(
        self,
        *,
        plan: SnapshotPlan,
        quality: QualityReport,
        component_selections: tuple[SnapshotComponentSelection, ...],
    ) -> SnapshotManifest:
        """Commit a snapshot that composes immutable internal components.

        This is the post-Issue-8 assembly path.  Source observations remain
        immutable evidence, but query identity and incremental reuse are pinned by
        component content rather than by a whole upstream snapshot identity.
        """

        if not component_selections:
            raise ValueError("Component snapshot requires component selections")
        if not quality.ready:
            raise SnapshotNotReadyError(
                "Component snapshot must be validated before it can be committed"
            )
        self.initialize()
        candidate = self._commit_snapshot(
            plan,
            quality,
            (),
            tuple(component_selections),
        )
        self._verify_snapshot_components(candidate)
        return candidate

    def _commit_snapshot(
        self,
        plan: SnapshotPlan,
        quality: QualityReport,
        files: tuple[StoredFile, ...],
        component_selections: tuple[SnapshotComponentSelection, ...] = (),
    ) -> SnapshotManifest:
        temporary = Path(tempfile.mkdtemp(prefix=".snapshot-", dir=self.snapshot_root))
        try:
            identity = {
                "plan": plan,
                "quality": quality,
                "files": files,
                "schema_version": WAREHOUSE_SCHEMA_VERSION,
                "component_selections": component_selections,
            }
            snapshot_id = f"snap-{stable_digest(identity)[:24]}"
            manifest = SnapshotManifest(
                snapshot_id,
                datetime.now(timezone.utc),
                plan,
                quality,
                files,
                WAREHOUSE_SCHEMA_VERSION,
                component_selections,
            )
            _write_manifest(temporary / "manifest.json", manifest)
            target = self.snapshot_path(snapshot_id)
            if target.exists():
                existing = self.load_snapshot(snapshot_id, require_ready=False)
                if _snapshot_identity(existing) != _snapshot_identity(manifest):
                    raise IntegrityError(f"Snapshot id collision: {snapshot_id}")
                return existing
            os.replace(temporary, target)
            return self.load_snapshot(snapshot_id, require_ready=False)
        finally:
            if temporary.exists():
                shutil.rmtree(temporary)

    def snapshot_path(self, snapshot_id: str) -> Path:
        _validate_id(snapshot_id, "snap-")
        return self.snapshot_root / snapshot_id

    def load_snapshot(self, snapshot_id: str, *, require_ready: bool = True) -> SnapshotManifest:
        directory = self.snapshot_path(snapshot_id)
        payload = _read_json(directory / "manifest.json")
        manifest = _snapshot_manifest(payload)
        if manifest.snapshot_id != snapshot_id:
            raise IntegrityError(f"Snapshot manifest identity mismatch: {snapshot_id}")
        self._verify_snapshot_references(manifest)
        self._verify_snapshot_components(manifest)
        expected = f"snap-{stable_digest(_snapshot_identity(manifest))[:24]}"
        if expected != snapshot_id:
            raise IntegrityError(f"Snapshot content identity mismatch: {snapshot_id}")
        if require_ready and not manifest.quality.ready:
            raise SnapshotNotReadyError(
                f"Snapshot {snapshot_id} is incomplete: {', '.join(manifest.quality.errors)}"
            )
        trust_errors = self._snapshot_trust_errors(manifest)
        if require_ready and trust_errors:
            raise SnapshotNotReadyError(
                f"Snapshot {snapshot_id} lacks current reconciliation trust evidence: "
                f"{', '.join(trust_errors)}"
            )
        return manifest

    def read_snapshot_table(self, snapshot_id: str, table: MarketTable) -> pd.DataFrame:
        return self.query_snapshot_table(snapshot_id, table)

    def query_snapshot_table(
        self,
        snapshot_id: str,
        table: MarketTable,
        *,
        instrument_ids: Iterable[str] = (),
        start_date: date | None = None,
        end_date: date | None = None,
        price_mode: str | None = None,
    ) -> pd.DataFrame:
        manifest = self.load_snapshot(snapshot_id)
        return self.query_loaded_snapshot_table(
            manifest,
            table,
            instrument_ids=instrument_ids,
            start_date=start_date,
            end_date=end_date,
            price_mode=price_mode,
        )

    def query_loaded_snapshot_table(
        self,
        manifest: SnapshotManifest,
        table: MarketTable,
        *,
        instrument_ids: Iterable[str] = (),
        start_date: date | None = None,
        end_date: date | None = None,
        price_mode: str | None = None,
    ) -> pd.DataFrame:
        """Query a snapshot manifest already verified by ``load_snapshot``.

        Long-running builders call ``load_snapshot`` once at their trust boundary and
        keep that immutable binding for the remainder of the run.  Re-hashing every
        referenced Parquet file for every small partition would add quadratic I/O
        without adding a stronger guarantee inside that one operation.
        """

        if manifest.component_selections:
            from fundlab.marketdata.incremental import compose_component_snapshot_table

            return compose_component_snapshot_table(
                self,
                manifest,
                table,
                instrument_ids=instrument_ids,
                start_date=start_date,
                end_date=end_date,
                price_mode=price_mode,
            )

        snapshot_id = manifest.snapshot_id
        expected = f"snap-{stable_digest(_snapshot_identity(manifest))[:24]}"
        if expected != snapshot_id:
            raise IntegrityError(f"Loaded snapshot content identity mismatch: {snapshot_id}")
        if not manifest.quality.ready:
            raise SnapshotNotReadyError(
                f"Snapshot {snapshot_id} is incomplete: {', '.join(manifest.quality.errors)}"
            )
        requested_instruments = tuple(sorted(set(instrument_ids)))
        if (start_date is None) != (end_date is None):
            raise ValueError("Snapshot query dates must be supplied together")
        if start_date is not None and start_date > end_date:
            raise ValueError("Snapshot query start_date must not exceed end_date")
        selections = tuple(item for item in manifest.plan.selections if item.table is table)
        if not selections:
            raise IntegrityError(f"Snapshot {snapshot_id} has no {table.value} selection")
        pieces: list[pd.DataFrame] = []
        for ordinal, selection in enumerate(selections):
            effective_instruments = requested_instruments
            if selection.instrument_ids:
                if effective_instruments:
                    effective_instruments = tuple(sorted(set(effective_instruments) & set(selection.instrument_ids)))
                    if not effective_instruments:
                        continue
                else:
                    effective_instruments = selection.instrument_ids
            effective_start, effective_end = start_date, end_date
            if selection.start_date is not None:
                effective_start = selection.start_date if effective_start is None else max(effective_start, selection.start_date)
                effective_end = selection.end_date if effective_end is None else min(effective_end, selection.end_date)
                if effective_start > effective_end:
                    continue
            observed = self.load_observation(selection.observation_id)
            item = next((item for item in observed.files if item.table is table), None)
            if item is None:
                raise IntegrityError(f"Selected observation has no table: {selection.observation_id}/{table.value}")
            filters: list[tuple[str, str, Any]] = []
            instrument_column = TABLE_INSTRUMENT_COLUMNS[table]
            date_column = TABLE_DATE_COLUMNS[table]
            if effective_instruments and instrument_column is not None:
                filters.append((instrument_column, "in", list(effective_instruments)))
            if effective_start is not None and date_column is not None:
                filters.extend((
                    (date_column, ">=", effective_start.isoformat()),
                    (date_column, "<=", effective_end.isoformat()),
                ))
            if table is MarketTable.DAILY_BARS and price_mode is not None:
                filters.append(("price_mode", "==", price_mode))
            path = self.observation_path(selection.observation_id) / item.path
            frame = pd.read_parquet(path, filters=filters or None)
            frame = _apply_slice(frame, selection)
            frame["source_observation_id"] = selection.observation_id
            frame["_selection_priority"] = selection.priority
            frame["_selection_ordinal"] = ordinal
            pieces.append(frame)
        if not pieces:
            # Preserve the canonical table shape even when a valid filter has no rows.
            source = self.read_observation_table(selections[0].observation_id, table).iloc[0:0]
            source["source_observation_id"] = pd.Series(dtype="string")
            return normalize_table(table, source, require_observation_id=True)
        return normalize_table(
            table,
            _resolve_explicit_conflicts(table, pd.concat(pieces, ignore_index=True)),
            require_observation_id=True,
        )

    def publish(self, snapshot_id: str) -> None:
        manifest = self._publishable_snapshot(snapshot_id)
        if manifest.plan.readiness is ReadinessProfile.SIMULATION:
            raise SnapshotNotReadyError(
                "Simulation publication requires predecessor compare-and-swap; "
                "use publish_if_current"
            )
        with _exclusive_pointer_lock(self.root / ".current.lock"):
            self._replace_current_pointer(manifest)

    def publish_if_current(
        self,
        expected_snapshot_id: str,
        successor_snapshot_id: str,
    ) -> None:
        """Atomically compare and replace the current snapshot across processes."""

        manifest = self._publishable_snapshot(successor_snapshot_id)
        with _exclusive_pointer_lock(self.root / ".current.lock"):
            try:
                current = self.current_snapshot_id()
            except (FileNotFoundError, IntegrityError):
                current = None
            if current != expected_snapshot_id:
                raise SnapshotNotReadyError(
                    "Stale EOD predecessor: "
                    f"expected current={expected_snapshot_id}, actual={current}"
                )
            self._replace_current_pointer(manifest)

    def _publishable_snapshot(self, snapshot_id: str) -> SnapshotManifest:
        manifest = self.load_snapshot(snapshot_id)
        if manifest.plan.readiness is ReadinessProfile.LEGACY_UNKNOWN:
            raise SnapshotNotReadyError(
                f"Legacy snapshot {snapshot_id} has no v2 readiness declaration and cannot be published"
            )
        if not manifest.quality.ready:
            raise SnapshotNotReadyError(
                f"Snapshot is not ready for publication: {snapshot_id}"
            )
        if (
            manifest.plan.readiness is ReadinessProfile.SIMULATION
            and not manifest.component_selections
        ):
            raise SnapshotNotReadyError(
                "Non-componentized simulation publication is retired; "
                "use publish_if_current with a componentized successor"
            )
        return manifest

    def _replace_current_pointer(self, manifest: SnapshotManifest) -> None:
        pointer = {
            "snapshot_id": manifest.snapshot_id,
            "manifest_sha256": _file_hash(
                self.snapshot_path(manifest.snapshot_id) / "manifest.json"
            ),
        }
        self.root.mkdir(parents=True, exist_ok=True)
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=".current.", suffix=".tmp", dir=self.root,
        )
        temporary = Path(temporary_name)
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
                handle.write(canonical_json(pointer))
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self.root / "current.json")
        finally:
            temporary.unlink(missing_ok=True)

    def current_snapshot_id(self) -> str:
        pointer = _read_json(self.root / "current.json")
        snapshot_id = str(pointer.get("snapshot_id", ""))
        manifest = self.load_snapshot(snapshot_id)
        actual = _file_hash(self.snapshot_path(snapshot_id) / "manifest.json")
        if pointer.get("manifest_sha256") != actual:
            raise IntegrityError("Current snapshot pointer checksum mismatch")
        return manifest.snapshot_id

    @staticmethod
    def _verify_files(directory: Path, files: Iterable[StoredFile]) -> None:
        root = directory.resolve()
        for item in files:
            path = (directory / item.path).resolve()
            if root not in path.parents or not path.is_file():
                raise IntegrityError(f"Manifest file is missing or outside its directory: {item.path}")
            if _file_hash(path) != item.sha256:
                raise IntegrityError(f"Manifest file checksum mismatch: {item.path}")
            if pq.read_metadata(path).num_rows != item.row_count:
                raise IntegrityError(f"Manifest row count mismatch: {item.path}")

    def _verify_snapshot_references(self, manifest: SnapshotManifest) -> None:
        for item in manifest.files:
            observation_id, separator, relative = item.path.partition("/")
            if not separator:
                raise IntegrityError(f"Snapshot reference is invalid: {item.path}")
            observed = self.load_observation(observation_id)
            source = next((source for source in observed.files
                           if source.table is item.table and source.path == relative), None)
            if source is None or source.sha256 != item.sha256 or source.row_count != item.row_count:
                raise IntegrityError(f"Snapshot reference does not match its observation: {item.path}")

    def _verify_snapshot_components(self, manifest: SnapshotManifest) -> None:
        if not manifest.component_selections:
            return
        from fundlab.marketdata.components import ComponentStore

        store = ComponentStore(self.root / "components", self)
        seen: set[str] = set()
        manifests = []
        for selection in manifest.component_selections:
            if selection.component_id in seen:
                raise IntegrityError(
                    f"Snapshot selects a component more than once: {selection.component_id}"
                )
            manifests.append(store.load(selection.component_id, verify_payload=False))
            seen.add(selection.component_id)
        for component in manifests:
            missing = set(component.dependency_ids) - seen
            if missing:
                raise IntegrityError(
                    "Snapshot omits direct component dependencies: "
                    f"{component.component_id}/{sorted(missing)}"
                )

    def _snapshot_trust_errors(self, manifest: SnapshotManifest) -> tuple[str, ...]:
        if manifest.component_selections:
            # Component manifests pin reconciled source projections or materialized
            # adjudications at creation.  Re-opening every predecessor Parquet blob
            # here would defeat exact incremental publication; payloads are verified
            # when their rows are actually queried.
            return ()
        errors = []
        for observation_id in sorted({item.observation_id for item in manifest.plan.selections}):
            observed = self.load_observation(observation_id)
            if observed.source_metadata.get("kind") != "field_level_reconciliation":
                errors.append(f"unreconciled:{observation_id}")
            elif not _reconciliation_ready(observed.source_metadata):
                errors.append(f"reconciliation_not_ready:{observation_id}")
        return tuple(errors)


def _apply_slice(frame: pd.DataFrame, selection: SourceSlice) -> pd.DataFrame:
    result = frame.copy()
    instrument_column = TABLE_INSTRUMENT_COLUMNS[selection.table]
    if selection.instrument_ids:
        if instrument_column is None:
            raise ObservationError(f"{selection.table.value} cannot be sliced by instrument")
        result = result[result[instrument_column].isin(selection.instrument_ids)]
    date_column = TABLE_DATE_COLUMNS[selection.table]
    if selection.start_date is not None:
        if date_column is None:
            raise ObservationError(f"{selection.table.value} cannot be sliced by date")
        start, end = selection.start_date.isoformat(), selection.end_date.isoformat()
        result = result[result[date_column].between(start, end)]
    return result.reset_index(drop=True)


def _resolve_explicit_conflicts(table: MarketTable, frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame.drop(columns=["_selection_priority", "_selection_ordinal"], errors="ignore")
    keys = list(TABLE_KEYS[table])
    if not frame.duplicated(keys).any():
        return frame.drop(
            columns=["_selection_priority", "_selection_ordinal"], errors="ignore",
        ).reset_index(drop=True)
    kept: list[int] = []
    for key, group in frame.groupby(keys, sort=False, dropna=False):
        maximum = group["_selection_priority"].max()
        winners = group[group["_selection_priority"] == maximum]
        if len(winners) != 1:
            printable = key if isinstance(key, tuple) else (key,)
            observations = sorted(set(winners["source_observation_id"].astype(str)))
            raise SourceConflictError(
                f"Explicit precedence is required for {table.value} key {printable}: {observations}"
            )
        kept.append(int(winners.index[0]))
    return (
        frame.loc[kept]
        .drop(columns=["_selection_priority", "_selection_ordinal"])
        .reset_index(drop=True)
    )


def _coverage_errors(
    manifest: ObservationManifest, selection: SourceSlice, selected_frame: pd.DataFrame,
) -> list[str]:
    claims = [item for item in manifest.coverage if item.table is selection.table]
    prefix = f"coverage:{selection.table.value}:{selection.observation_id}"
    if not claims:
        return [f"{prefix}:missing"]
    usable = [
        claim for claim in claims
        if claim.complete and _claim_contains(claim, selection, selected_frame)
    ]
    return [] if usable else [f"{prefix}:incomplete_or_out_of_scope"]


def _claim_contains(
    claim: CoverageClaim, selection: SourceSlice, selected_frame: pd.DataFrame,
) -> bool:
    required_start, required_end = selection.start_date, selection.end_date
    date_column = TABLE_DATE_COLUMNS[selection.table]
    if date_column is not None and not selected_frame.empty:
        observed_start = date.fromisoformat(str(selected_frame[date_column].min())[:10])
        observed_end = date.fromisoformat(str(selected_frame[date_column].max())[:10])
        required_start = observed_start if required_start is None else min(required_start, observed_start)
        required_end = observed_end if required_end is None else max(required_end, observed_end)
    if required_start is not None:
        if claim.start_date is None or claim.start_date > required_start or claim.end_date < required_end:
            return False
    required_instruments = set(selection.instrument_ids)
    instrument_column = TABLE_INSTRUMENT_COLUMNS[selection.table]
    if instrument_column is not None and not selected_frame.empty:
        required_instruments.update(map(str, selected_frame[instrument_column].dropna().unique()))
    if required_instruments and claim.instrument_ids:
        if not required_instruments <= set(claim.instrument_ids):
            return False
    return True


def _write_manifest(path: Path, value: Any) -> None:
    path.write_text(canonical_json(value), encoding="utf-8", newline="\n")


def _read_json(path: Path) -> Mapping[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise IntegrityError(f"Cannot read manifest: {path}") from exc
    if not isinstance(payload, dict):
        raise IntegrityError(f"Manifest root must be an object: {path}")
    return payload


def _file_hash(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _validate_id(value: str, prefix: str) -> None:
    if not value.startswith(prefix) or not value[len(prefix):].isalnum():
        raise ValueError(f"Invalid immutable identity: {value!r}")


def _date_range(start: date, end: date):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def _request(payload: Mapping[str, Any]) -> ProviderRequest:
    return ProviderRequest(
        ProviderCapability(payload["capability"]),
        _date(payload.get("start_date")),
        _date(payload.get("end_date")),
        tuple(payload.get("instrument_ids", ())),
        payload.get("parameters", {}),
    )


def _coverage(payload: Mapping[str, Any]) -> CoverageClaim:
    return CoverageClaim(
        MarketTable(payload["table"]),
        bool(payload["complete"]),
        _date(payload.get("start_date")),
        _date(payload.get("end_date")),
        tuple(payload.get("instrument_ids", ())),
        payload.get("detail"),
    )


def _stored_file(payload: Mapping[str, Any]) -> StoredFile:
    return StoredFile(
        MarketTable(payload["table"]), str(payload["path"]), str(payload["sha256"]), int(payload["row_count"]),
    )


def _observation_manifest(payload: Mapping[str, Any]) -> ObservationManifest:
    return ObservationManifest(
        str(payload["observation_id"]),
        str(payload["provider"]),
        datetime.fromisoformat(str(payload["observed_at"])),
        _request(payload["request"]),
        tuple(_coverage(item) for item in payload.get("coverage", ())),
        tuple(_stored_file(item) for item in payload.get("files", ())),
        payload.get("source_metadata", {}),
        int(payload.get("schema_version", 1)),
    )


def _source_slice(payload: Mapping[str, Any]) -> SourceSlice:
    return SourceSlice(
        str(payload["observation_id"]),
        MarketTable(payload["table"]),
        str(payload["reason"]),
        tuple(payload.get("instrument_ids", ())),
        _date(payload.get("start_date")),
        _date(payload.get("end_date")),
        int(payload.get("priority", 0)),
    )


def _plan(payload: Mapping[str, Any]) -> SnapshotPlan:
    raw_scope = payload.get("universe_scope")
    universe_scope = None
    if isinstance(raw_scope, Mapping):
        universe_scope = UniverseScope(
            str(raw_scope["definition"]),
            date.fromisoformat(str(raw_scope["as_of_date"])),
            date.fromisoformat(str(raw_scope["history_start"])),
            date.fromisoformat(str(raw_scope["history_end"])),
            bool(raw_scope.get("survivorship_bias", False)),
            tuple(raw_scope.get("instrument_ids", ())),
        )
    return SnapshotPlan(
        tuple(_source_slice(item) for item in payload.get("selections", ())),
        str(payload["description"]),
        bool(payload.get("require_complete_coverage", True)),
        ReadinessProfile(payload.get("readiness", ReadinessProfile.LEGACY_UNKNOWN.value)),
        universe_scope,
    )


def _quality(payload: Mapping[str, Any]) -> QualityReport:
    return QualityReport(
        SnapshotState(payload["state"]),
        tuple(payload.get("errors", ())),
        tuple(payload.get("warnings", ())),
        payload.get("row_counts", {}),
    )


def _snapshot_manifest(payload: Mapping[str, Any]) -> SnapshotManifest:
    return SnapshotManifest(
        str(payload["snapshot_id"]),
        datetime.fromisoformat(str(payload["created_at"])),
        _plan(payload["plan"]),
        _quality(payload["quality"]),
        tuple(_stored_file(item) for item in payload.get("files", ())),
        int(payload.get("schema_version", 1)),
        tuple(
            SnapshotComponentSelection(
                str(item["component_id"]),
                int(item.get("priority", 0)),
                int(item.get("ordinal", 0)),
            )
            for item in payload.get("component_selections", ())
        ),
    )


def _date(value: Any) -> date | None:
    return None if value is None else date.fromisoformat(str(value))


def _observation_identity(manifest: ObservationManifest) -> Mapping[str, Any]:
    return {
        "provider": manifest.provider,
        "observed_at": manifest.observed_at.isoformat(),
        "request": manifest.request,
        "coverage": manifest.coverage,
        "files": manifest.files,
        "source_metadata": manifest.source_metadata,
        "schema_version": manifest.schema_version,
    }


def _manifest_identity(manifest: ObservationManifest) -> Any:
    return to_primitive(_observation_identity(manifest))


def _snapshot_identity(manifest: SnapshotManifest) -> Mapping[str, Any]:
    plan: Any = manifest.plan
    if manifest.schema_version < 2:
        # SnapshotPlan gained a readiness declaration in schema v2.  Reproduce the
        # original three-field identity so existing immutable v1 manifests still verify.
        plan = {
            "selections": manifest.plan.selections,
            "description": manifest.plan.description,
            "require_complete_coverage": manifest.plan.require_complete_coverage,
        }
    elif manifest.schema_version < 3:
        # UniverseScope was added in schema v3.  Keep immutable schema-v2 snapshot
        # identities byte-for-byte reproducible instead of hashing the new None field.
        plan = {
            "selections": manifest.plan.selections,
            "description": manifest.plan.description,
            "require_complete_coverage": manifest.plan.require_complete_coverage,
            "readiness": manifest.plan.readiness,
        }
    identity = {
        "plan": plan,
        "quality": manifest.quality,
        "files": manifest.files,
        "schema_version": manifest.schema_version,
    }
    if manifest.schema_version >= 6:
        identity["component_selections"] = manifest.component_selections
    return identity


def _reconciliation_ready(source_metadata: Mapping[str, Any]) -> bool:
    explicit = source_metadata.get("reconciliation_ready")
    if isinstance(explicit, bool):
        return explicit
    report = source_metadata.get("report")
    if not isinstance(report, Mapping):
        return False
    return not report.get("blockers") and not report.get("unresolved_conflicts")
