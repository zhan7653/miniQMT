"""Immutable, content-addressed building blocks for canonical market data.

This module is deliberately an internal storage layer.  It gives a future
publisher a way to reuse an exact fact/rule/adjudication slice without making
components a second public query API or another mutable warehouse.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from enum import StrEnum
from hashlib import sha256
import os
from pathlib import Path
import shutil
import tempfile
import time
from typing import Any, Iterable, TYPE_CHECKING

import pandas as pd

from fundlab.common.canonical import canonical_json, stable_digest, to_primitive
from fundlab.marketdata.contracts import IntegrityError, MarketTable
from fundlab.marketdata.schema import TABLE_DATE_COLUMNS, TABLE_INSTRUMENT_COLUMNS, TABLE_KEYS
from fundlab.marketdata.schema import BUSINESS_SCHEMAS, LINEAGE_SCHEMA

if TYPE_CHECKING:
    from fundlab.marketdata.warehouse import MarketDataWarehouse


class ComponentKind(StrEnum):
    MARKET_FACTS = "market_facts"
    TRADING_RULEBOOK = "trading_rulebook"
    FIELD_ADJUDICATIONS = "field_adjudications"
    SIMULATION_VIEW = "simulation_view"


@dataclass(frozen=True)
class ComponentScope:
    """The exact table, fields, instruments and inclusive date range of a component."""

    table: MarketTable
    instrument_ids: tuple[str, ...] = ()
    start_date: date | None = None
    end_date: date | None = None
    fields: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        table = MarketTable(self.table)
        if (self.start_date is None) != (self.end_date is None):
            raise ValueError("Component scope dates must be supplied together")
        if self.start_date is not None and self.start_date > self.end_date:
            raise ValueError("Component scope start_date must not exceed end_date")
        fields = tuple(sorted(set(map(str, self.fields))))
        if not fields or any(not field.strip() for field in fields):
            raise ValueError("Component scope requires at least one field")
        object.__setattr__(self, "table", table)
        object.__setattr__(self, "instrument_ids", tuple(sorted(set(map(str, self.instrument_ids)))))
        object.__setattr__(self, "fields", fields)

    def overlaps(self, other: "ComponentScope") -> bool:
        if self.table is not other.table:
            return False
        if set(self.instrument_ids) and set(other.instrument_ids) and not (
            set(self.instrument_ids) & set(other.instrument_ids)
        ):
            return False
        if self.start_date is None or other.start_date is None:
            return True
        return self.start_date <= other.end_date and other.start_date <= self.end_date

    def contains(self, other: "ComponentScope") -> bool:
        if self.table is not other.table:
            return False
        if self.instrument_ids:
            # An empty selector means the table-wide domain, not an empty set.
            if not other.instrument_ids or not set(other.instrument_ids) <= set(self.instrument_ids):
                return False
        if self.start_date is not None:
            if other.start_date is None:
                return False
            if self.start_date > other.start_date or self.end_date < other.end_date:
                return False
        return set(other.fields) <= set(self.fields)


@dataclass(frozen=True)
class SourceProjection:
    """A zero-copy view of one verified immutable observation blob.

    ``observation_id`` is only the locator.  It is intentionally excluded from
    :meth:`content_identity`, because observation identity includes observed_at;
    it is nevertheless included in :meth:`locator_identity`, which is bound into
    the component id so a component cannot be redirected to another observation.
    """

    observation_id: str
    table: MarketTable
    fields: tuple[str, ...]
    scope: ComponentScope
    file_sha256: str

    def __post_init__(self) -> None:
        if not self.observation_id.strip():
            raise ValueError("Source projection requires an observation_id")
        table = MarketTable(self.table)
        fields = tuple(sorted(set(map(str, self.fields))))
        if not fields or any(not field.strip() for field in fields):
            raise ValueError("Source projection requires fields")
        if self.scope.table is not table or tuple(self.scope.fields) != fields:
            raise ValueError("Source projection table and fields must exactly match its scope")
        digest = self.file_sha256.lower()
        if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
            raise ValueError("Source projection requires a SHA-256 file digest")
        object.__setattr__(self, "table", table)
        object.__setattr__(self, "fields", fields)
        object.__setattr__(self, "file_sha256", digest)

    def content_identity(self) -> dict[str, Any]:
        return {
            "table": self.table,
            "fields": self.fields,
            "scope": self.scope,
            "file_sha256": self.file_sha256,
        }

    def locator_identity(self) -> dict[str, Any]:
        """Full immutable locator; unlike content_identity this pins its source."""
        return {"observation_id": self.observation_id, **self.content_identity()}


@dataclass(frozen=True)
class ComponentRef:
    component_id: str
    priority: int = 0
    ordinal: int = 0

    def __post_init__(self) -> None:
        _validate_component_id(self.component_id)
        if self.ordinal < 0:
            raise ValueError("Component ref ordinal must not be negative")


@dataclass(frozen=True)
class ComponentManifest:
    component_id: str
    kind: ComponentKind
    scope: ComponentScope
    content_sha256: str
    dependency_ids: tuple[str, ...]
    builder_version: str
    projections: tuple[SourceProjection, ...] = ()
    data_file: str | None = None
    data_file_sha256: str | None = None
    schema_version: int = 1

    def __post_init__(self) -> None:
        _validate_component_id(self.component_id)
        kind = ComponentKind(self.kind)
        if not self.builder_version.strip():
            raise ValueError("Component builder_version cannot be empty")
        digest = self.content_sha256.lower()
        if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
            raise ValueError("Component requires a content SHA-256 digest")
        dependencies = tuple(sorted(set(map(str, self.dependency_ids))))
        for dependency_id in dependencies:
            _validate_component_id(dependency_id)
        if self.data_file is None and self.data_file_sha256 is not None:
            raise ValueError("A component data digest requires a data file")
        if self.data_file is not None and self.data_file_sha256 is None:
            raise ValueError("A component data file requires its digest")
        if self.data_file is not None and Path(self.data_file).name != self.data_file:
            raise ValueError("Component data file must be a simple relative name")
        if kind is ComponentKind.SIMULATION_VIEW and (self.projections or self.data_file):
            raise ValueError("Virtual simulation views cannot own data or source projections")
        if self.projections and self.data_file:
            raise ValueError("A component is either source-backed or materialized")
        object.__setattr__(self, "kind", kind)
        object.__setattr__(self, "dependency_ids", dependencies)
        object.__setattr__(self, "content_sha256", digest)


class ComponentStore:
    """Stores immutable component manifests below a caller-owned v2 root."""

    def __init__(self, root: str | Path, warehouse: "MarketDataWarehouse") -> None:
        self.root = Path(root).resolve()
        self.warehouse = warehouse
        self._source_read_cache: dict[tuple[Any, ...], pd.DataFrame] = {}
        # Publisher composition creates short-lived stores.  These immutable
        # verification caches therefore belong to the warehouse lifetime, while
        # query-result frames deliberately remain local to one store/query.
        verified = getattr(warehouse, "_verified_component_manifests", None)
        if verified is None:
            verified = {}
            setattr(warehouse, "_verified_component_manifests", verified)
        self._verified: dict[str, ComponentManifest] = verified
        source_paths = getattr(warehouse, "_verified_component_source_paths", None)
        if source_paths is None:
            source_paths = {}
            setattr(warehouse, "_verified_component_source_paths", source_paths)
        self._verified_source_paths: dict[tuple[str, MarketTable, str], Path] = source_paths
        opened = getattr(warehouse, "_opened_component_ids", None)
        if opened is None:
            opened = set()
            setattr(warehouse, "_opened_component_ids", opened)
        self.opened_component_ids: set[str] = opened
        events = getattr(warehouse, "_component_open_events", None)
        if events is None:
            events = []
            setattr(warehouse, "_component_open_events", events)
        self.open_events: list[str] = events

    def initialize(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)

    def component_path(self, component_id: str) -> Path:
        _validate_component_id(component_id)
        return self.root / component_id

    def source_projection(
        self,
        observation_id: str,
        scope: ComponentScope,
    ) -> SourceProjection:
        """Pin the exact source file, without copying its historical parquet blob."""
        observed = self.warehouse.load_observation(observation_id)
        file = next((item for item in observed.files if item.table is scope.table), None)
        if file is None:
            raise IntegrityError(f"Observation {observation_id} has no {scope.table.value} table")
        return SourceProjection(observation_id, scope.table, scope.fields, scope, file.sha256)

    def record_source_backed(
        self,
        kind: ComponentKind,
        scope: ComponentScope,
        projections: Iterable[SourceProjection],
        *,
        dependency_ids: Iterable[str] = (),
        builder_version: str,
    ) -> ComponentManifest:
        sources = tuple(sorted(projections, key=lambda item: canonical_json(item.locator_identity())))
        if not sources:
            raise ValueError("A source-backed component requires a source projection")
        if any(source.scope != scope for source in sources):
            raise ValueError("Source projections must exactly match the component scope")
        content = stable_digest({"projections": tuple(item.content_identity() for item in sources)})
        return self._record(
            kind=kind,
            scope=scope,
            content_sha256=content,
            dependency_ids=dependency_ids,
            builder_version=builder_version,
            projections=sources,
        )

    def record_source(
        self,
        kind: ComponentKind,
        scope: ComponentScope,
        projections: Iterable[SourceProjection],
        *,
        dependency_ids: Iterable[str] = (),
        builder_version: str,
    ) -> ComponentManifest:
        """Public-to-the-publisher spelling of :meth:`record_source_backed`."""
        return self.record_source_backed(
            kind, scope, projections, dependency_ids=dependency_ids,
            builder_version=builder_version,
        )

    def record_materialized(
        self,
        kind: ComponentKind,
        scope: ComponentScope,
        frame: pd.DataFrame,
        *,
        dependency_ids: Iterable[str] = (),
        builder_version: str,
    ) -> ComponentManifest:
        materialized = _canonical_frame(scope.table, frame)
        _validate_frame_scope(scope, materialized)
        content = _frame_digest(scope.table, materialized)
        identity = _component_identity(
            kind=kind, scope=scope, content_sha256=content,
            dependency_ids=dependency_ids, builder_version=builder_version,
        )
        component_id = _component_id(identity)
        target = self.component_path(component_id)
        self.initialize()
        if target.exists():
            existing = self.load(component_id)
            if _component_identity(
                kind=existing.kind, scope=existing.scope, content_sha256=existing.content_sha256,
                dependency_ids=existing.dependency_ids, builder_version=existing.builder_version,
            ) != identity:
                raise IntegrityError(f"Component id collision: {component_id}")
            return existing
        temporary = Path(tempfile.mkdtemp(prefix=".component-", dir=self.root))
        try:
            path = temporary / "data.parquet"
            materialized.to_parquet(path, index=False, engine="pyarrow", compression="zstd")
            digest = _file_hash(path)
            manifest = ComponentManifest(
                component_id, ComponentKind(kind), scope, content,
                tuple(dependency_ids), builder_version,
                data_file="data.parquet", data_file_sha256=digest,
            )
            _write_json(temporary / "manifest.json", manifest)
            self._commit(temporary, manifest)
            return self.load(component_id)
        finally:
            if temporary.exists():
                shutil.rmtree(temporary)

    def record_virtual_simulation_view(
        self,
        scope: ComponentScope,
        dependency_ids: Iterable[str],
        *,
        builder_version: str,
    ) -> ComponentManifest:
        dependencies = tuple(sorted(set(map(str, dependency_ids))))
        content = stable_digest({
            "scope": scope,
            "dependency_ids": dependencies,
            "builder_version": builder_version,
        })
        return self._record(
            kind=ComponentKind.SIMULATION_VIEW,
            scope=scope,
            content_sha256=content,
            dependency_ids=dependencies,
            builder_version=builder_version,
        )

    def record_view(
        self,
        scope: ComponentScope,
        dependency_ids: Iterable[str],
        *,
        builder_version: str,
    ) -> ComponentManifest:
        return self.record_virtual_simulation_view(
            scope, dependency_ids, builder_version=builder_version,
        )

    def load(
        self,
        component_id: str,
        *,
        verify_payload: bool = True,
    ) -> ComponentManifest:
        # Metadata-only loading lets the incremental publisher inspect dependency
        # scopes without opening accepted historical blobs.  Every data read calls
        # this method with payload verification enabled before opening its rows.
        if verify_payload and component_id in self._verified:
            return self._verified[component_id]
        directory = self.component_path(component_id)
        manifest = _manifest_from_payload(_read_json(directory / "manifest.json"))
        if manifest.component_id != component_id:
            raise IntegrityError(f"Component manifest identity mismatch: {component_id}")
        expected = _component_id(_manifest_identity(manifest, include_file_hash=False))
        if expected != component_id:
            raise IntegrityError(f"Component content identity mismatch: {component_id}")
        if manifest.projections and verify_payload:
            for projection in manifest.projections:
                self._verify_source_projection(projection, component_id=component_id)
            expected_content = stable_digest({
                "projections": tuple(item.content_identity() for item in manifest.projections),
            })
            if expected_content != manifest.content_sha256:
                raise IntegrityError(f"Source component content digest mismatch: {component_id}")
        if manifest.data_file is not None and verify_payload:
            path = directory / manifest.data_file
            self._note_open(component_id)
            if not path.is_file() or _file_hash(path) != manifest.data_file_sha256:
                raise IntegrityError(f"Materialized component file mismatch: {component_id}")
            if _frame_digest(manifest.scope.table, _canonical_frame(manifest.scope.table, pd.read_parquet(path))) != manifest.content_sha256:
                raise IntegrityError(f"Materialized component content mismatch: {component_id}")
        if manifest.kind is ComponentKind.SIMULATION_VIEW:
            expected_content = stable_digest({
                "scope": manifest.scope,
                "dependency_ids": manifest.dependency_ids,
                "builder_version": manifest.builder_version,
            })
            if expected_content != manifest.content_sha256:
                raise IntegrityError(f"Virtual component content mismatch: {component_id}")
        if verify_payload:
            self._verified[component_id] = manifest
        return manifest

    def read_frame(self, component_id: str) -> pd.DataFrame:
        manifest = self.load(component_id)
        if manifest.kind is ComponentKind.SIMULATION_VIEW:
            raise ValueError("Virtual simulation views have no materialized frame")
        if manifest.projections:
            frames = []
            for projection in manifest.projections:
                self._note_open(component_id)
                frame = pd.read_parquet(
                    self._verified_source_path(projection)
                )
                frames.append(_project_frame(frame, projection.scope))
            return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=manifest.scope.fields)
        assert manifest.data_file is not None
        self._note_open(component_id)
        return _project_frame(pd.read_parquet(self.component_path(component_id) / manifest.data_file), manifest.scope)

    def read(
        self,
        component_id: str,
        *,
        instrument_ids: Iterable[str] = (),
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> pd.DataFrame:
        """Read a component, intersecting (never widening) its declared scope."""
        if (start_date is None) != (end_date is None):
            raise ValueError("Component read dates must be supplied together")
        if start_date is not None and start_date > end_date:
            raise ValueError("Component read start_date must not exceed end_date")
        manifest = self.load(component_id)
        if manifest.kind is ComponentKind.SIMULATION_VIEW:
            raise ValueError("Virtual simulation views have no materialized frame")
        requested_ids = tuple(sorted(set(map(str, instrument_ids))))
        if manifest.projections:
            frames = [
                self._read_source_projection(
                    component_id, item, requested_ids, start_date, end_date,
                )
                for item in manifest.projections
            ]
        else:
            assert manifest.data_file is not None
            self._note_open(component_id)
            frames = [_read_projected(
                pd.read_parquet(self.component_path(component_id) / manifest.data_file),
                manifest.scope, requested_ids, start_date, end_date,
            )]
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=manifest.scope.fields)

    def _read_source_projection(
        self,
        component_id: str,
        projection: SourceProjection,
        instrument_ids: tuple[str, ...],
        start_date: date | None,
        end_date: date | None,
    ) -> pd.DataFrame:
        effective_ids, effective_start, effective_end = _effective_read_scope(
            projection, instrument_ids, start_date, end_date,
        )
        key = (
            projection.observation_id,
            projection.table.value,
            effective_ids,
            effective_start,
            effective_end,
        )
        frame = self._source_read_cache.get(key)
        if frame is None:
            path = self._verified_source_path(projection)
            filters: list[tuple[str, str, Any]] = []
            instrument_column = TABLE_INSTRUMENT_COLUMNS[projection.table]
            if effective_ids and instrument_column is not None:
                filters.append((instrument_column, "in", list(effective_ids)))
            date_column = TABLE_DATE_COLUMNS[projection.table]
            if (
                effective_start is not None
                and effective_end is not None
                and effective_start <= effective_end
                and date_column is not None
            ):
                filters.extend((
                    (date_column, ">=", effective_start.isoformat()),
                    (date_column, "<=", effective_end.isoformat()),
                ))
            self._note_open(component_id)
            frame = pd.read_parquet(path, filters=filters or None)
            self._source_read_cache[key] = frame
        result = frame.copy()
        schema = {**BUSINESS_SCHEMAS[projection.table], **LINEAGE_SCHEMA}
        for field in projection.scope.fields:
            if field in result:
                continue
            spec = schema.get(field)
            if spec is None or (not spec.nullable and spec.default is None):
                raise IntegrityError(
                    f"Source projection is missing required field: "
                    f"{projection.observation_id}/{projection.table.value}/{field}"
                )
            result[field] = spec.default if spec.default is not None else pd.NA
        return _read_projected(
            result, projection.scope, instrument_ids, start_date, end_date,
        )

    def _record(
        self,
        *,
        kind: ComponentKind,
        scope: ComponentScope,
        content_sha256: str,
        dependency_ids: Iterable[str],
        builder_version: str,
        projections: tuple[SourceProjection, ...] = (),
    ) -> ComponentManifest:
        identity = _component_identity(
            kind=kind, scope=scope, content_sha256=content_sha256,
            dependency_ids=dependency_ids, builder_version=builder_version,
            projections=projections,
        )
        manifest = ComponentManifest(
            _component_id(identity), ComponentKind(kind), scope, content_sha256,
            tuple(dependency_ids), builder_version, projections=projections,
        )
        self.initialize()
        target = self.component_path(manifest.component_id)
        if target.exists():
            existing = self.load(manifest.component_id)
            if _manifest_identity(existing, include_file_hash=False) != _manifest_identity(manifest, include_file_hash=False):
                raise IntegrityError(f"Component id collision: {manifest.component_id}")
            return existing
        temporary = Path(tempfile.mkdtemp(prefix=".component-", dir=self.root))
        try:
            _write_json(temporary / "manifest.json", manifest)
            self._commit(temporary, manifest)
            return self.load(manifest.component_id)
        finally:
            if temporary.exists():
                shutil.rmtree(temporary)

    def _commit(self, temporary: Path, manifest: ComponentManifest) -> None:
        target = self.component_path(manifest.component_id)
        for attempt in range(5):
            try:
                os.replace(temporary, target)
                return
            except (FileExistsError, PermissionError):
                if target.exists():
                    existing = self.load(manifest.component_id)
                    if (
                        _manifest_identity(existing, include_file_hash=False)
                        != _manifest_identity(manifest, include_file_hash=False)
                    ):
                        raise IntegrityError(
                            f"Component id collision: {manifest.component_id}"
                        )
                    return
                if attempt == 4:
                    raise
                # Windows can briefly hold a newly-created directory while its
                # manifest is scanned.  A bounded retry preserves the same atomic
                # rename contract and never overwrites an existing component.
                time.sleep(0.05 * (attempt + 1))

    def _verify_source_projection(
        self,
        projection: SourceProjection,
        *,
        component_id: str | None = None,
    ) -> None:
        key = (projection.observation_id, projection.table, projection.file_sha256)
        if key in self._verified_source_paths:
            return
        if component_id is not None:
            self._note_open(component_id)
        observed = self.warehouse.load_observation(projection.observation_id)
        stored = next((item for item in observed.files if item.table is projection.table), None)
        if stored is None or stored.sha256 != projection.file_sha256:
            raise IntegrityError(
                f"Source projection blob mismatch: {projection.observation_id}/{projection.table.value}"
            )
        path = self.warehouse.observation_path(projection.observation_id) / stored.path
        if not path.is_file():
            raise IntegrityError(
                f"Missing source projection blob: {projection.observation_id}/{projection.table.value}"
            )
        self._verified_source_paths[key] = path

    def _verified_source_path(self, projection: SourceProjection) -> Path:
        key = (projection.observation_id, projection.table, projection.file_sha256)
        path = self._verified_source_paths.get(key)
        if path is None:
            self._verify_source_projection(projection)
            path = self._verified_source_paths[key]
        return path

    def _note_open(self, component_id: str) -> None:
        self.opened_component_ids.add(component_id)
        self.open_events.append(component_id)


def _component_identity(
    *, kind: ComponentKind, scope: ComponentScope, content_sha256: str,
    dependency_ids: Iterable[str], builder_version: str,
    projections: Iterable[SourceProjection] = (),
) -> dict[str, Any]:
    return {
        "kind": ComponentKind(kind),
        "scope": scope,
        "content_sha256": content_sha256,
        "dependency_ids": tuple(sorted(set(map(str, dependency_ids)))),
        "builder_version": builder_version,
        "projections": tuple(
            item.locator_identity()
            for item in sorted(projections, key=lambda item: canonical_json(item.locator_identity()))
        ),
    }


def _component_id(identity: dict[str, Any]) -> str:
    return f"cmp-{stable_digest(identity)[:24]}"


def _manifest_identity(manifest: ComponentManifest, *, include_file_hash: bool = True) -> dict[str, Any]:
    identity = _component_identity(
        kind=manifest.kind, scope=manifest.scope, content_sha256=manifest.content_sha256,
        dependency_ids=manifest.dependency_ids, builder_version=manifest.builder_version,
        projections=manifest.projections,
    )
    if include_file_hash:
        identity.update({
            "data_file": manifest.data_file,
            "data_file_sha256": manifest.data_file_sha256,
            "schema_version": manifest.schema_version,
        })
    return identity


def _canonical_frame(table: MarketTable, frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy(deep=True)
    result = result.reindex(columns=sorted(map(str, result.columns)))
    if result.empty:
        return result.reset_index(drop=True)
    sort_columns = [column for column in TABLE_KEYS[table] if column in result.columns]
    sort_columns += [column for column in result.columns if column not in sort_columns]
    return result.sort_values(sort_columns, kind="mergesort", na_position="last").reset_index(drop=True)


def _frame_digest(table: MarketTable, frame: pd.DataFrame) -> str:
    rows = [
        {column: _primitive_cell(row[column], date_column=TABLE_DATE_COLUMNS[table] == column)
         for column in frame.columns}
        for _, row in frame.iterrows()
    ]
    return stable_digest({"table": table, "columns": tuple(frame.columns), "rows": rows})


def _primitive_cell(value: Any, *, date_column: bool) -> Any:
    if value is None or value is pd.NA or (not isinstance(value, (list, tuple, dict)) and pd.isna(value)):
        return None
    if date_column:
        return pd.Timestamp(value).date().isoformat()
    if hasattr(value, "item") and not isinstance(value, (str, bytes)):
        value = value.item()
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return to_primitive(value)


def _validate_frame_scope(scope: ComponentScope, frame: pd.DataFrame) -> None:
    missing = set(scope.fields) - set(frame.columns)
    if missing:
        raise ValueError(f"Component frame is missing scoped fields: {sorted(missing)}")
    instrument_column = TABLE_INSTRUMENT_COLUMNS[scope.table]
    if scope.instrument_ids and instrument_column is None:
        raise ValueError(f"{scope.table.value} does not have an instrument column")
    date_column = TABLE_DATE_COLUMNS[scope.table]
    if scope.start_date is not None and date_column is None:
        raise ValueError(f"{scope.table.value} does not have a date column")
    scoped = _project_frame(frame, scope)
    if len(scoped) != len(frame):
        raise ValueError("Materialized component frame contains rows outside its declared scope")


def _project_frame(frame: pd.DataFrame, scope: ComponentScope) -> pd.DataFrame:
    result = _filter_scope_rows(frame, scope)
    fields = list(scope.fields)
    missing = set(fields) - set(result.columns)
    if missing:
        raise IntegrityError(f"Component frame is missing scoped fields: {sorted(missing)}")
    return result.loc[:, fields].reset_index(drop=True)


def _read_projected(
    frame: pd.DataFrame,
    scope: ComponentScope,
    instrument_ids: tuple[str, ...],
    start_date: date | None,
    end_date: date | None,
) -> pd.DataFrame:
    result = _filter_scope_rows(frame, scope)
    instrument_column = TABLE_INSTRUMENT_COLUMNS[scope.table]
    if instrument_ids and instrument_column is not None:
        result = result.loc[result[instrument_column].astype(str).isin(instrument_ids)]
    date_column = TABLE_DATE_COLUMNS[scope.table]
    if start_date is not None and date_column is not None:
        values = pd.to_datetime(result[date_column], errors="coerce").dt.date
        result = result.loc[values.between(start_date, end_date)]
    fields = list(scope.fields)
    missing = set(fields) - set(result.columns)
    if missing:
        raise IntegrityError(f"Component frame is missing scoped fields: {sorted(missing)}")
    return result.loc[:, fields].reset_index(drop=True)


def _effective_read_scope(
    projection: SourceProjection,
    instrument_ids: tuple[str, ...],
    start_date: date | None,
    end_date: date | None,
) -> tuple[tuple[str, ...], date | None, date | None]:
    effective_ids = instrument_ids
    if projection.scope.instrument_ids:
        effective_ids = tuple(sorted(
            set(effective_ids) & set(projection.scope.instrument_ids)
            if effective_ids else set(projection.scope.instrument_ids)
        ))
    effective_start, effective_end = start_date, end_date
    if projection.scope.start_date is not None:
        effective_start = (
            projection.scope.start_date if effective_start is None
            else max(effective_start, projection.scope.start_date)
        )
        effective_end = (
            projection.scope.end_date if effective_end is None
            else min(effective_end, projection.scope.end_date)
        )
    return effective_ids, effective_start, effective_end




def _filter_scope_rows(frame: pd.DataFrame, scope: ComponentScope) -> pd.DataFrame:
    result = frame.copy()
    instrument_column = TABLE_INSTRUMENT_COLUMNS[scope.table]
    if scope.instrument_ids and instrument_column is not None:
        result = result.loc[result[instrument_column].astype(str).isin(scope.instrument_ids)]
    date_column = TABLE_DATE_COLUMNS[scope.table]
    if scope.start_date is not None and date_column is not None:
        values = pd.to_datetime(result[date_column], errors="coerce").dt.date
        result = result.loc[values.between(scope.start_date, scope.end_date)]
    return result


def _write_json(path: Path, value: Any) -> None:
    path.write_text(canonical_json(value), encoding="utf-8")


def _read_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise IntegrityError(f"Missing component manifest: {path}")
    import json

    return json.loads(path.read_text(encoding="utf-8"))


def _date(value: Any) -> date | None:
    return None if value is None else date.fromisoformat(str(value))


def _scope_from_payload(payload: dict[str, Any]) -> ComponentScope:
    return ComponentScope(
        MarketTable(payload["table"]), tuple(payload.get("instrument_ids", ())),
        _date(payload.get("start_date")), _date(payload.get("end_date")),
        tuple(payload.get("fields", ())),
    )


def _projection_from_payload(payload: dict[str, Any]) -> SourceProjection:
    return SourceProjection(
        str(payload["observation_id"]), MarketTable(payload["table"]),
        tuple(payload.get("fields", ())), _scope_from_payload(payload["scope"]),
        str(payload["file_sha256"]),
    )


def _manifest_from_payload(payload: dict[str, Any]) -> ComponentManifest:
    return ComponentManifest(
        str(payload["component_id"]), ComponentKind(payload["kind"]),
        _scope_from_payload(payload["scope"]), str(payload["content_sha256"]),
        tuple(payload.get("dependency_ids", ())), str(payload["builder_version"]),
        tuple(_projection_from_payload(item) for item in payload.get("projections", ())),
        payload.get("data_file"), payload.get("data_file_sha256"), int(payload.get("schema_version", 1)),
    )


def _file_hash(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _validate_component_id(component_id: str) -> None:
    if not component_id.startswith("cmp-") or len(component_id) != 28:
        raise ValueError(f"Invalid component id: {component_id}")
