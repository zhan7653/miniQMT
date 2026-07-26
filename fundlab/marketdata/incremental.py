"""Issue #8 componentized publication and exact incremental composition.

The public contract remains one immutable ``snapshot_id``.  Internally a snapshot
selects independently content-addressed market facts, effective-dated trading
rules, field adjudications, and disposable simulation views.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Mapping

import pandas as pd

from fundlab.common.canonical import canonical_json, stable_digest, to_primitive
from fundlab.marketdata.components import (
    ComponentKind,
    ComponentManifest,
    ComponentScope,
    ComponentStore,
)
from fundlab.marketdata.contracts import (
    IntegrityError,
    MarketTable,
    QualityReport,
    ReadinessProfile,
    SIMULATION_PARTITION_VALIDATOR_VERSION,
    SnapshotComponentSelection,
    SnapshotManifest,
    SnapshotNotReadyError,
    SnapshotPlan,
    SnapshotState,
    SourceSlice,
    UniverseScope,
)
from fundlab.marketdata.schema import (
    BUSINESS_SCHEMAS,
    LINEAGE_SCHEMA,
    TABLE_DATE_COLUMNS,
    TABLE_INSTRUMENT_COLUMNS,
    TABLE_KEYS,
    normalize_table,
)


COMPONENT_BUILDER_VERSION = "canonical-components-r1"
SIMULATION_VIEW_BUILDER_VERSION = "simulation-view-r1"

_DETAIL_LINEAGE_FIELDS = ("field_lineage", "source_payload")
_SOURCE_LINEAGE_FIELDS = tuple(LINEAGE_SCHEMA)
_INSTRUMENT_RULE_FIELDS = (
    "buy_lot", "quantity_step", "odd_lot_sell_all", "price_tick",
    "sell_delay_sessions", "price_limit_ratio",
)
_DAILY_RULE_FIELDS = (
    "trade_rule_id", "trade_rule_known_date", "buy_lot", "quantity_step",
    "odd_lot_sell_all", "price_tick", "sell_delay_sessions",
    "price_limit_state", "price_limit_ratio",
)
_DERIVED_VIEW_FIELDS = ("limit_up", "limit_down")


@dataclass(frozen=True)
class IncrementalBuildAudit:
    predecessor_snapshot_id: str
    snapshot_id: str
    reused_component_ids: tuple[str, ...]
    created_component_ids: tuple[str, ...]
    opened_prior_daily_components: tuple[str, ...]
    increment_start: date
    increment_end: date

    @property
    def history_daily_reads(self) -> int:
        return len(self.opened_prior_daily_components)


@dataclass(frozen=True)
class ShadowParity:
    reference_snapshot_id: str
    candidate_snapshot_id: str
    checked_slices: int
    mismatches: tuple[str, ...]
    provenance_differences: tuple[str, ...] = ()

    @property
    def equivalent(self) -> bool:
        return not self.mismatches


class IncrementalCanonicalPublisher:
    """Build component snapshots without reopening predecessor daily facts."""

    def __init__(self, warehouse) -> None:
        self.warehouse = warehouse
        self.components = ComponentStore(warehouse.root / "components", warehouse)

    def bootstrap(self, snapshot_id: str, *, description: str | None = None) -> SnapshotManifest:
        """Register an accepted legacy snapshot as zero-copy source projections."""

        source = self.warehouse.load_snapshot(snapshot_id)
        if source.component_selections:
            return source
        selections: list[SnapshotComponentSelection] = []
        for ordinal, selected in enumerate(source.plan.selections):
            selections.extend(self._source_components(selected, ordinal * 10))
        plan = SnapshotPlan(
            source.plan.selections,
            description or f"componentized shadow of {source.snapshot_id}",
            source.plan.require_complete_coverage,
            source.plan.readiness,
            source.plan.universe_scope,
        )
        return self.warehouse.build_component_snapshot(
            plan=plan,
            quality=source.quality,
            component_selections=tuple(selections),
        )

    def extend(
        self,
        *,
        predecessor_snapshot_id: str,
        calendar_observation_id: str,
        increment_observation_ids: tuple[str, ...],
        universe_scope: UniverseScope,
        description: str,
    ) -> tuple[SnapshotManifest, IncrementalBuildAudit]:
        predecessor = self.warehouse.load_snapshot(predecessor_snapshot_id)
        if not predecessor.component_selections:
            raise SnapshotNotReadyError(
                "Increment requires a componentized predecessor; the one-time "
                "migration bootstrap is not a routine update fallback"
            )
        previous_scope = predecessor.plan.universe_scope
        if (
            predecessor.plan.readiness is not ReadinessProfile.SIMULATION
            or previous_scope is None
        ):
            raise SnapshotNotReadyError("Increment predecessor is not simulation_ready")
        previous_ids = set(previous_scope.instrument_ids)
        target_ids = set(universe_scope.instrument_ids)
        if (
            universe_scope.definition != previous_scope.definition
            or universe_scope.history_start != previous_scope.history_start
            or universe_scope.history_end <= previous_scope.history_end
            or previous_ids - target_ids
        ):
            raise SnapshotNotReadyError(
                "Increment requires the complete predecessor universe and a later end date"
            )
        increment_start = previous_scope.history_end + timedelta(days=1)
        increment_ids = tuple(sorted(set(increment_observation_ids)))
        manifests = self._validate_increment_partitions(
            increment_ids,
            calendar_observation_id=calendar_observation_id,
            universe_scope=universe_scope,
            increment_start=increment_start,
            added_instrument_ids=tuple(sorted(target_ids - previous_ids)),
        )

        opened_before = len(getattr(self.warehouse, "_component_open_events", ()))
        refs = list(predecessor.component_selections)
        existing_ids = {item.component_id for item in refs}
        prior_daily_ids = {
            item.component_id
            for item in refs
            if self.components.load(
                item.component_id, verify_payload=False,
            ).scope.table is MarketTable.DAILY_BARS
        }
        created: list[str] = []
        ordinal = max((item.ordinal for item in refs), default=-1) + 1
        added_ids = tuple(sorted(target_ids - previous_ids))

        calendar_frame = self._read_observation_slice(
            calendar_observation_id,
            MarketTable.CALENDAR,
            start_date=increment_start,
            end_date=universe_scope.history_end,
        )
        additions = self._materialized_components(
            MarketTable.CALENDAR,
            calendar_frame,
            instrument_ids=(),
            start_date=increment_start,
            end_date=universe_scope.history_end,
            priority=ordinal + 100,
            ordinal=ordinal,
        )
        refs.extend(additions)
        created.extend(item.component_id for item in additions if item.component_id not in existing_ids)
        ordinal += 10

        incoming_instrument_frames = []
        for manifest in manifests:
            declared = tuple(sorted(map(
                str, manifest.source_metadata["partition_quality"]["instrument_ids"],
            )))
            frame = self._read_observation_slice(
                manifest.observation_id,
                MarketTable.INSTRUMENTS,
                instrument_ids=declared,
            )
            if not frame.empty:
                incoming_instrument_frames.append(frame)
        if incoming_instrument_frames:
            incoming_instruments = normalize_table(
                MarketTable.INSTRUMENTS,
                pd.concat(incoming_instrument_frames, ignore_index=True),
                require_observation_id=True,
            )
            additions = self._instrument_update_components(
                predecessor,
                incoming_instruments,
                priority=ordinal + 100,
                ordinal=ordinal,
            )
            refs.extend(additions)
            created.extend(
                item.component_id for item in additions
                if item.component_id not in existing_ids
            )
            ordinal += len(additions) + 1

        increment_frames: dict[MarketTable, list[pd.DataFrame]] = {
            table: [] for table in (
                MarketTable.DAILY_BARS,
                MarketTable.CORPORATE_ACTIONS,
                MarketTable.ADJUSTMENT_FACTORS,
            )
        }
        for manifest in manifests:
            declared = tuple(sorted(map(
                str, manifest.source_metadata["partition_quality"]["instrument_ids"],
            )))
            for table in increment_frames:
                frame = self._read_observation_slice(
                    manifest.observation_id,
                    table,
                    instrument_ids=declared,
                    start_date=increment_start,
                    end_date=universe_scope.history_end,
                )
                if not frame.empty:
                    increment_frames[table].append(frame)

        for table, pieces in increment_frames.items():
            if not pieces:
                continue
            frame = pd.concat(pieces, ignore_index=True)
            ids = tuple(sorted(set(map(
                str, frame[TABLE_INSTRUMENT_COLUMNS[table]],
            ))))
            additions = self._materialized_components(
                table,
                frame,
                instrument_ids=ids,
                start_date=None if table is MarketTable.INSTRUMENTS else increment_start,
                end_date=None if table is MarketTable.INSTRUMENTS else universe_scope.history_end,
                priority=ordinal + 100,
                ordinal=ordinal,
            )
            refs.extend(additions)
            created.extend(item.component_id for item in additions if item.component_id not in existing_ids)
            ordinal += 10

        plan_selections = list(predecessor.plan.selections)
        plan_selections.append(SourceSlice(
            calendar_observation_id,
            MarketTable.CALENDAR,
            f"componentized calendar increment {increment_start}..{universe_scope.history_end}",
            start_date=increment_start,
            end_date=universe_scope.history_end,
            priority=100,
        ))
        for manifest in manifests:
            declared = tuple(sorted(map(
                str, manifest.source_metadata["partition_quality"]["instrument_ids"],
            )))
            for table in (
                MarketTable.INSTRUMENTS,
                MarketTable.DAILY_BARS,
                MarketTable.CORPORATE_ACTIONS,
                MarketTable.ADJUSTMENT_FACTORS,
            ):
                selected_ids = (
                    tuple(item for item in declared if item in added_ids)
                    if table is MarketTable.INSTRUMENTS else declared
                )
                if not selected_ids:
                    continue
                plan_selections.append(SourceSlice(
                    manifest.observation_id,
                    table,
                    "content-addressed simulation increment",
                    selected_ids,
                    None if table is MarketTable.INSTRUMENTS else increment_start,
                    None if table is MarketTable.INSTRUMENTS else universe_scope.history_end,
                    100,
                ))

        quality = self._increment_quality(
            predecessor,
            calendar_frame=calendar_frame,
            increment_frames=increment_frames,
            added_count=len(added_ids),
        )
        snapshot = self.warehouse.build_component_snapshot(
            plan=SnapshotPlan(
                tuple(plan_selections),
                description,
                readiness=ReadinessProfile.SIMULATION,
                universe_scope=universe_scope,
            ),
            quality=quality,
            component_selections=tuple(refs),
        )
        opened_events = tuple(
            getattr(self.warehouse, "_component_open_events", ())
        )[opened_before:]
        audit = IncrementalBuildAudit(
            predecessor.snapshot_id,
            snapshot.snapshot_id,
            tuple(sorted(existing_ids)),
            tuple(sorted(set(created))),
            tuple(item for item in opened_events if item in prior_daily_ids),
            increment_start,
            universe_scope.history_end,
        )
        return snapshot, audit

    def compare(
        self,
        reference_snapshot_id: str,
        candidate_snapshot_id: str,
    ) -> ShadowParity:
        """Compare public canonical rows in bounded slices, never whole-history RAM."""

        reference = self.warehouse.load_snapshot(reference_snapshot_id)
        candidate = self.warehouse.load_snapshot(candidate_snapshot_id)
        if reference.plan.universe_scope != candidate.plan.universe_scope:
            return ShadowParity(
                reference_snapshot_id, candidate_snapshot_id, 0,
                ("universe_scope",), (),
            )
        mismatches: list[str] = []
        provenance_differences: list[str] = []
        checked = 0
        for table in MarketTable:
            if table is MarketTable.DAILY_BARS:
                continue
            left = _fast_query_legacy(self.warehouse, reference, table)
            right = self.warehouse.query_loaded_snapshot_table(candidate, table)
            checked += 1
            left_full, left_simulation = _comparison_identities(table, left)
            right_full, right_simulation = _comparison_identities(table, right)
            if left_simulation != right_simulation:
                mismatches.append(table.value)
            elif left_full != right_full:
                provenance_differences.append(table.value)

        daily_scopes = tuple(sorted({
            (
                item.instrument_ids or (
                    () if reference.plan.universe_scope is None
                    else reference.plan.universe_scope.instrument_ids
                ),
                item.start_date or (
                    None if reference.plan.universe_scope is None
                    else reference.plan.universe_scope.history_start
                ),
                item.end_date or (
                    None if reference.plan.universe_scope is None
                    else reference.plan.universe_scope.history_end
                ),
            )
            for item in reference.plan.selections
            if item.table is MarketTable.DAILY_BARS
        }, key=lambda item: (item[1] or date.min, item[2] or date.max, item[0])))
        for instrument_ids, start, end in daily_scopes:
            if start is None or end is None or not instrument_ids:
                mismatches.append("daily_bars:unknown_scope")
                continue
            left = _fast_query_legacy(
                self.warehouse,
                reference,
                MarketTable.DAILY_BARS,
                instrument_ids=instrument_ids,
                start_date=start,
                end_date=end,
            )
            right = self.warehouse.query_loaded_snapshot_table(
                candidate,
                MarketTable.DAILY_BARS,
                instrument_ids=instrument_ids,
                start_date=start,
                end_date=end,
            )
            checked += 1
            left_full, left_simulation = _comparison_identities(MarketTable.DAILY_BARS, left)
            right_full, right_simulation = _comparison_identities(MarketTable.DAILY_BARS, right)
            label = (
                f"daily_bars:{instrument_ids[0]}..{instrument_ids[-1]}:"
                f"{start}..{end}"
            )
            if left_simulation != right_simulation:
                mismatches.append(label)
            elif left_full != right_full:
                provenance_differences.append(label)
        return ShadowParity(
            reference_snapshot_id,
            candidate_snapshot_id,
            checked,
            tuple(mismatches),
            tuple(provenance_differences),
        )

    def apply_scoped_update(
        self,
        *,
        predecessor_snapshot_id: str,
        component_ids: tuple[str, ...],
        declared_impacts: tuple[ComponentScope, ...],
        description: str,
    ) -> SnapshotManifest:
        """Overlay an explicitly adjudicated correction with fail-closed scope.

        The caller must provide the complete instrument/date/field dependency
        closure.  This method never guesses a broader scope and never falls back to
        a full rebuild.
        """

        predecessor = self.warehouse.load_snapshot(predecessor_snapshot_id)
        if not predecessor.component_selections:
            raise SnapshotNotReadyError(
                "Scoped updates require a componentized predecessor"
            )
        if not component_ids or not declared_impacts:
            raise SnapshotNotReadyError(
                "Scoped update has no exact component or declared impact"
            )
        for impact in declared_impacts:
            instrument_column = TABLE_INSTRUMENT_COLUMNS[impact.table]
            date_column = TABLE_DATE_COLUMNS[impact.table]
            if instrument_column is not None and not impact.instrument_ids:
                raise SnapshotNotReadyError(
                    f"Unknown instrument impact scope: {impact.table.value}"
                )
            if date_column is not None and impact.start_date is None:
                raise SnapshotNotReadyError(
                    f"Unknown date impact scope: {impact.table.value}"
                )
        manifests = tuple(
            self.components.load(item, verify_payload=False)
            for item in tuple(sorted(set(component_ids)))
        )
        for manifest in manifests:
            if not any(impact.contains(manifest.scope) for impact in declared_impacts):
                raise SnapshotNotReadyError(
                    "Component lies outside the declared dependency closure: "
                    f"{manifest.component_id}"
                )
        existing = {item.component_id for item in predecessor.component_selections}
        if existing.intersection(item.component_id for item in manifests):
            raise SnapshotNotReadyError("Scoped update must introduce new component content")
        priority = max(
            (item.priority for item in predecessor.component_selections), default=0,
        ) + 100
        ordinal = max(
            (item.ordinal for item in predecessor.component_selections), default=-1,
        ) + 1
        refs = list(predecessor.component_selections)
        for manifest in manifests:
            refs.append(SnapshotComponentSelection(
                manifest.component_id, priority, ordinal,
            ))
            ordinal += 1
        for impact in declared_impacts:
            if impact.table is not MarketTable.DAILY_BARS:
                continue
            direct_dependencies = tuple(sorted({
                *(item.component_id for item in manifests if item.scope.overlaps(impact)),
                *(
                    selected.component_id
                    for selected in predecessor.component_selections
                    if (
                        (candidate := self.components.load(
                            selected.component_id, verify_payload=False,
                        )).kind is not ComponentKind.SIMULATION_VIEW
                        and candidate.scope.overlaps(impact)
                    )
                ),
            }))
            view_scope = ComponentScope(
                MarketTable.DAILY_BARS,
                impact.instrument_ids,
                impact.start_date,
                impact.end_date,
                tuple((*TABLE_KEYS[MarketTable.DAILY_BARS], *_DERIVED_VIEW_FIELDS)),
            )
            view = self.components.record_view(
                view_scope,
                direct_dependencies,
                builder_version=SIMULATION_VIEW_BUILDER_VERSION,
            )
            refs.append(SnapshotComponentSelection(view.component_id, priority, ordinal))
            ordinal += 1
        return self.warehouse.build_component_snapshot(
            plan=SnapshotPlan(
                predecessor.plan.selections,
                description,
                predecessor.plan.require_complete_coverage,
                predecessor.plan.readiness,
                predecessor.plan.universe_scope,
            ),
            quality=predecessor.quality,
            component_selections=tuple(refs),
        )

    def publish_if_equivalent(
        self,
        *,
        expected_current_snapshot_id: str,
        candidate_snapshot_id: str,
    ) -> ShadowParity:
        parity = self.compare(expected_current_snapshot_id, candidate_snapshot_id)
        if not parity.equivalent:
            raise SnapshotNotReadyError(
                "Shadow component snapshot differs from the published baseline: "
                + ", ".join(parity.mismatches[:10])
            )
        self.warehouse.publish_if_current(
            expected_current_snapshot_id,
            candidate_snapshot_id,
        )
        return parity

    def write_shadow_report(
        self,
        report_root: str | Path,
        *,
        audit: IncrementalBuildAudit,
        parity: ShadowParity,
        published: bool,
    ) -> Path:
        payload = {
            "decision_source": "https://github.com/zhan7653/miniQMT/issues/8",
            "decision_revision": 1,
            "kind": "componentized_incremental_shadow_validation",
            "audit": audit,
            "parity": parity,
            "provenance_difference_explanation": {
                "calendar": (
                    "The predecessor calendar provenance is intentionally retained; "
                    "the retired path rewrote prior dates with the latest full-calendar "
                    "observation even when is_open was unchanged."
                ),
            },
            "published": published,
        }
        root = Path(report_root)
        root.mkdir(parents=True, exist_ok=True)
        digest = stable_digest(payload)[:16]
        path = root / f"incremental-shadow-{parity.candidate_snapshot_id}-{digest}.json"
        primitive = to_primitive(payload)
        if path.exists():
            import json

            if json.loads(path.read_text(encoding="utf-8")) != primitive:
                raise IntegrityError(f"Immutable shadow report collision: {path}")
        else:
            path.write_text(canonical_json(payload), encoding="utf-8", newline="\n")
        return path

    def _source_components(
        self,
        selection: SourceSlice,
        ordinal: int,
    ) -> tuple[SnapshotComponentSelection, ...]:
        groups = _field_groups(selection.table)
        manifests: list[ComponentManifest] = []
        for kind, fields in groups:
            scope = ComponentScope(
                selection.table,
                selection.instrument_ids,
                selection.start_date,
                selection.end_date,
                fields,
            )
            projection = self.components.source_projection(selection.observation_id, scope)
            manifests.append(self.components.record_source(
                kind,
                scope,
                (projection,),
                builder_version=COMPONENT_BUILDER_VERSION,
            ))
        refs = [
            SnapshotComponentSelection(
                manifest.component_id,
                selection.priority,
                ordinal + offset,
            )
            for offset, manifest in enumerate(manifests)
        ]
        if selection.table is MarketTable.DAILY_BARS:
            scope = ComponentScope(
                MarketTable.DAILY_BARS,
                selection.instrument_ids,
                selection.start_date,
                selection.end_date,
                tuple((*TABLE_KEYS[MarketTable.DAILY_BARS], *_DERIVED_VIEW_FIELDS)),
            )
            view = self.components.record_view(
                scope,
                tuple(item.component_id for item in manifests),
                builder_version=SIMULATION_VIEW_BUILDER_VERSION,
            )
            refs.append(SnapshotComponentSelection(
                view.component_id, selection.priority, ordinal + len(manifests),
            ))
        return tuple(refs)

    def _instrument_update_components(
        self,
        predecessor: SnapshotManifest,
        incoming: pd.DataFrame,
        *,
        priority: int,
        ordinal: int,
    ) -> tuple[SnapshotComponentSelection, ...]:
        """Persist only changed instrument fields, including newly listed rows."""

        previous = self.warehouse.query_loaded_snapshot_table(
            predecessor,
            MarketTable.INSTRUMENTS,
        )
        keys = list(TABLE_KEYS[MarketTable.INSTRUMENTS])
        previous = previous.set_index(keys)
        current = incoming.set_index(keys)
        refs: list[SnapshotComponentSelection] = []
        next_ordinal = ordinal
        for kind, fields in _field_groups(MarketTable.INSTRUMENTS):
            payload_fields = [item for item in fields if item not in keys]
            grouped: dict[tuple[str, ...], list[str]] = {}
            for instrument_id, row in current.iterrows():
                instrument_key = str(instrument_id)
                if instrument_id not in previous.index:
                    changed = tuple(payload_fields)
                else:
                    prior = previous.loc[instrument_id]
                    changed = tuple(
                        field for field in payload_fields
                        if _cell_identity(row[field]) != _cell_identity(prior[field])
                    )
                if changed:
                    grouped.setdefault(changed, []).append(instrument_key)
            for changed_fields, instrument_ids in sorted(grouped.items()):
                selected = incoming.loc[
                    incoming["instrument_id"].isin(instrument_ids),
                    [*keys, *changed_fields],
                ].copy()
                scope = ComponentScope(
                    MarketTable.INSTRUMENTS,
                    tuple(sorted(instrument_ids)),
                    fields=tuple((*keys, *changed_fields)),
                )
                manifest = self.components.record_materialized(
                    kind,
                    scope,
                    selected,
                    builder_version=COMPONENT_BUILDER_VERSION,
                )
                refs.append(SnapshotComponentSelection(
                    manifest.component_id, priority, next_ordinal,
                ))
                next_ordinal += 1
        return tuple(refs)

    def _materialized_components(
        self,
        table: MarketTable,
        frame: pd.DataFrame,
        *,
        instrument_ids: tuple[str, ...],
        start_date: date | None,
        end_date: date | None,
        priority: int,
        ordinal: int,
    ) -> tuple[SnapshotComponentSelection, ...]:
        manifests: list[ComponentManifest] = []
        for kind, fields in _field_groups(table):
            selected = frame.loc[:, list(fields)].copy()
            if kind is ComponentKind.TRADING_RULEBOOK and table is MarketTable.DAILY_BARS:
                selected["effective_from"] = selected["session_date"]
                selected["effective_to"] = selected["session_date"]
                fields = tuple((*fields, "effective_from", "effective_to"))
            scope = ComponentScope(table, instrument_ids, start_date, end_date, fields)
            manifests.append(self.components.record_materialized(
                kind,
                scope,
                selected,
                builder_version=COMPONENT_BUILDER_VERSION,
            ))
        refs = [
            SnapshotComponentSelection(manifest.component_id, priority, ordinal + offset)
            for offset, manifest in enumerate(manifests)
        ]
        if table is MarketTable.DAILY_BARS:
            view_scope = ComponentScope(
                table,
                instrument_ids,
                start_date,
                end_date,
                tuple((*TABLE_KEYS[table], *_DERIVED_VIEW_FIELDS)),
            )
            view = self.components.record_view(
                view_scope,
                tuple(item.component_id for item in manifests),
                builder_version=SIMULATION_VIEW_BUILDER_VERSION,
            )
            refs.append(SnapshotComponentSelection(
                view.component_id, priority, ordinal + len(manifests),
            ))
        return tuple(refs)

    def _validate_increment_partitions(
        self,
        observation_ids: tuple[str, ...],
        *,
        calendar_observation_id: str,
        universe_scope: UniverseScope,
        increment_start: date,
        added_instrument_ids: tuple[str, ...],
    ) -> tuple[Any, ...]:
        if not observation_ids:
            raise ValueError("Increment requires validated partitions")
        manifests = tuple(self.warehouse.load_observation(item) for item in observation_ids)
        declared_union: set[str] = set()
        required_tables = {
            MarketTable.INSTRUMENTS,
            MarketTable.DAILY_BARS,
            MarketTable.CORPORATE_ACTIONS,
            MarketTable.ADJUSTMENT_FACTORS,
        }
        for manifest in manifests:
            quality = manifest.source_metadata.get("partition_quality")
            if (
                manifest.source_metadata.get("kind") != "field_level_reconciliation"
                or not isinstance(quality, Mapping)
                or not quality.get("validated")
                or quality.get("validator_version") != SIMULATION_PARTITION_VALIDATOR_VERSION
                or quality.get("readiness") != ReadinessProfile.SIMULATION.value
                or quality.get("calendar_observation_id") != calendar_observation_id
                or str(quality.get("start_date"))[:10] != increment_start.isoformat()
                or str(quality.get("end_date"))[:10] != universe_scope.history_end.isoformat()
                or str(quality.get("universe_as_of"))[:10] != universe_scope.as_of_date.isoformat()
                or {item.table for item in manifest.files} != required_tables
            ):
                raise SnapshotNotReadyError(
                    f"Invalid component increment partition: {manifest.observation_id}"
                )
            declared = set(map(str, quality.get("instrument_ids", ())))
            if not declared or declared_union.intersection(declared):
                raise SnapshotNotReadyError("Increment partitions are empty or overlap")
            declared_union.update(declared)
        if declared_union != set(universe_scope.instrument_ids):
            raise SnapshotNotReadyError("Increment partitions do not cover the exact universe")
        if added_instrument_ids:
            found: dict[str, date] = {}
            for manifest in manifests:
                frame = self._read_observation_slice(
                    manifest.observation_id,
                    MarketTable.INSTRUMENTS,
                    instrument_ids=added_instrument_ids,
                )
                for row in frame.to_dict("records"):
                    instrument_id = str(row["instrument_id"])
                    try:
                        listed = date.fromisoformat(str(row["listed_date"])[:10])
                    except (TypeError, ValueError) as exc:
                        raise SnapshotNotReadyError(
                            f"Added instrument has no valid listing date: {instrument_id}"
                        ) from exc
                    if instrument_id in found:
                        raise SnapshotNotReadyError(
                            f"Added instrument metadata overlaps: {instrument_id}"
                        )
                    found[instrument_id] = listed
            if set(found) != set(added_instrument_ids):
                raise SnapshotNotReadyError("Increment lacks added instrument metadata")
            invalid = {
                instrument_id: listed for instrument_id, listed in found.items()
                if not increment_start <= listed <= universe_scope.history_end
            }
            if invalid:
                instrument_id, listed = sorted(invalid.items())[0]
                raise SnapshotNotReadyError(
                    "Incremental simulation cannot backfill an older instrument: "
                    f"{instrument_id}/{listed.isoformat()}"
                )
        return manifests

    def _read_observation_slice(
        self,
        observation_id: str,
        table: MarketTable,
        *,
        instrument_ids: tuple[str, ...] = (),
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> pd.DataFrame:
        if (start_date is None) != (end_date is None):
            raise ValueError("Slice dates must be supplied together")
        manifest = self.warehouse.load_observation(observation_id)
        stored = next((item for item in manifest.files if item.table is table), None)
        if stored is None:
            raise IntegrityError(f"Observation has no table: {observation_id}/{table.value}")
        filters: list[tuple[str, str, Any]] = []
        instrument_column = TABLE_INSTRUMENT_COLUMNS[table]
        if instrument_ids and instrument_column:
            filters.append((instrument_column, "in", list(instrument_ids)))
        date_column = TABLE_DATE_COLUMNS[table]
        if start_date is not None and date_column:
            filters.extend((
                (date_column, ">=", start_date.isoformat()),
                (date_column, "<=", end_date.isoformat()),
            ))
        frame = pd.read_parquet(
            self.warehouse.observation_path(observation_id) / stored.path,
            filters=filters or None,
        )
        frame["source_observation_id"] = observation_id
        return frame.reset_index(drop=True)

    @staticmethod
    def _increment_quality(
        predecessor: SnapshotManifest,
        *,
        calendar_frame: pd.DataFrame,
        increment_frames: Mapping[MarketTable, list[pd.DataFrame]],
        added_count: int,
    ) -> QualityReport:
        counts = dict(predecessor.quality.row_counts)
        counts[MarketTable.CALENDAR.value] = int(counts[MarketTable.CALENDAR.value]) + len(calendar_frame)
        counts[MarketTable.INSTRUMENTS.value] = int(counts[MarketTable.INSTRUMENTS.value]) + added_count
        for table in (
            MarketTable.DAILY_BARS,
            MarketTable.CORPORATE_ACTIONS,
            MarketTable.ADJUSTMENT_FACTORS,
        ):
            counts[table.value] = int(counts[table.value]) + sum(
                len(item) for item in increment_frames[table]
            )
        return QualityReport(
            SnapshotState.READY,
            (),
            tuple(predecessor.quality.warnings),
            counts,
        )

def compose_component_snapshot_table(
    warehouse,
    snapshot: SnapshotManifest,
    table: MarketTable,
    *,
    instrument_ids: Iterable[str] = (),
    start_date: date | None = None,
    end_date: date | None = None,
    price_mode: str | None = None,
) -> pd.DataFrame:
    if (start_date is None) != (end_date is None):
        raise ValueError("Snapshot query dates must be supplied together")
    requested_ids = tuple(sorted(set(map(str, instrument_ids))))
    store = ComponentStore(warehouse.root / "components", warehouse)
    grouped: dict[ComponentKind, list[tuple[SnapshotComponentSelection, ComponentManifest]]] = {
        kind: [] for kind in ComponentKind
    }
    cache = getattr(warehouse, "_component_manifest_cache", None)
    if cache is None:
        cache = {}
        setattr(warehouse, "_component_manifest_cache", cache)
    selected_by_id: dict[str, tuple[SnapshotComponentSelection, ComponentManifest]] = {}
    for selected in snapshot.component_selections:
        manifest = cache.get(selected.component_id)
        if manifest is None:
            manifest = store.load(selected.component_id, verify_payload=False)
            cache[selected.component_id] = manifest
        selected_by_id[selected.component_id] = (selected, manifest)
        if manifest.scope.table is not table:
            continue
        if requested_ids and manifest.scope.instrument_ids and not (
            set(requested_ids) & set(manifest.scope.instrument_ids)
        ):
            continue
        if start_date is not None and manifest.scope.start_date is not None and (
            manifest.scope.end_date < start_date or manifest.scope.start_date > end_date
        ):
            continue
        grouped[manifest.kind].append((selected, manifest))
    facts = _overlay_kind(
        store, grouped[ComponentKind.MARKET_FACTS], table,
        requested_ids, start_date, end_date,
    )
    if facts.empty:
        # Use the canonical schema even for a correctly filtered empty result.
        return normalize_table(
            table,
            pd.DataFrame(columns=(*BUSINESS_SCHEMAS[table], *_SOURCE_LINEAGE_FIELDS)),
            require_observation_id=True,
        )
    rules = _overlay_kind(
        store, grouped[ComponentKind.TRADING_RULEBOOK], table,
        requested_ids, start_date, end_date,
    )
    adjudications = _overlay_kind(
        store, grouped[ComponentKind.FIELD_ADJUDICATIONS], table,
        requested_ids, start_date, end_date,
    )
    keys = list(TABLE_KEYS[table])
    result = facts
    for extra in (rules, adjudications):
        if not extra.empty:
            payload_columns = [item for item in extra.columns if item not in keys]
            result = result.merge(
                extra[keys + payload_columns],
                on=keys,
                how="left",
                validate="one_to_one",
            )
    if table is MarketTable.DAILY_BARS:
        result = _replay_simulation_views(
            result,
            grouped[ComponentKind.SIMULATION_VIEW],
            selected_by_id,
        )
        if price_mode is not None:
            result = result.loc[result["price_mode"].eq(price_mode)]
    return normalize_table(table, result, require_observation_id=True)


def _fast_query_legacy(
    warehouse,
    snapshot: SnapshotManifest,
    table: MarketTable,
    *,
    instrument_ids: tuple[str, ...] = (),
    start_date: date | None = None,
    end_date: date | None = None,
) -> pd.DataFrame:
    """Read a verified immutable legacy snapshot without re-hashing sibling files."""

    from fundlab.marketdata.warehouse import _resolve_explicit_conflicts

    pieces = []
    requested_ids = set(instrument_ids)
    selections = tuple(item for item in snapshot.plan.selections if item.table is table)
    for ordinal, selection in enumerate(selections):
        effective_ids = tuple(selection.instrument_ids)
        if requested_ids:
            effective_ids = tuple(sorted(
                requested_ids & set(effective_ids)
                if effective_ids else requested_ids
            ))
            if not effective_ids and TABLE_INSTRUMENT_COLUMNS[table] is not None:
                continue
        effective_start, effective_end = start_date, end_date
        if selection.start_date is not None:
            effective_start = (
                selection.start_date if effective_start is None
                else max(effective_start, selection.start_date)
            )
            effective_end = (
                selection.end_date if effective_end is None
                else min(effective_end, selection.end_date)
            )
            if effective_start > effective_end:
                continue
        filters: list[tuple[str, str, Any]] = []
        instrument_column = TABLE_INSTRUMENT_COLUMNS[table]
        if effective_ids and instrument_column is not None:
            filters.append((instrument_column, "in", list(effective_ids)))
        date_column = TABLE_DATE_COLUMNS[table]
        if effective_start is not None and date_column is not None:
            filters.extend((
                (date_column, ">=", effective_start.isoformat()),
                (date_column, "<=", effective_end.isoformat()),
            ))
        path = (
            warehouse.observation_path(selection.observation_id)
            / f"{table.value}.parquet"
        )
        frame = pd.read_parquet(path, filters=filters or None)
        frame["source_observation_id"] = selection.observation_id
        frame["_selection_priority"] = selection.priority
        frame["_selection_ordinal"] = ordinal
        pieces.append(frame)
    if not pieces:
        return normalize_table(
            table,
            pd.DataFrame(columns=(*BUSINESS_SCHEMAS[table], *_SOURCE_LINEAGE_FIELDS)),
            require_observation_id=True,
        )
    return normalize_table(
        table,
        _resolve_explicit_conflicts(table, pd.concat(pieces, ignore_index=True)),
        require_observation_id=True,
    )


def _overlay_kind(
    store: ComponentStore,
    selected: list[tuple[SnapshotComponentSelection, ComponentManifest]],
    table: MarketTable,
    instrument_ids: tuple[str, ...],
    start_date: date | None,
    end_date: date | None,
) -> pd.DataFrame:
    if not selected:
        return pd.DataFrame()
    keys = list(TABLE_KEYS[table])
    result = pd.DataFrame()
    for ref, manifest in sorted(selected, key=lambda item: (item[0].priority, item[0].ordinal)):
        frame = store.read(
            manifest.component_id,
            instrument_ids=instrument_ids,
            start_date=start_date,
            end_date=end_date,
        )
        if frame.empty:
            continue
        if manifest.projections and "source_observation_id" in frame:
            # Legacy snapshot queries identify the selected canonical observation,
            # not an upstream evidence id embedded in its stored rows.
            frame["source_observation_id"] = manifest.projections[0].observation_id
        if frame.duplicated(keys).any():
            raise IntegrityError(
                f"Component has duplicate canonical keys: {manifest.component_id}"
            )
        current = frame.set_index(keys)
        if result.empty:
            result = current.copy()
            continue
        union = result.index.union(current.index)
        result = result.reindex(union)
        for column in current.columns:
            if column in result.columns:
                result.loc[current.index, column] = current[column].to_numpy()
            else:
                result[column] = pd.NA
                result.loc[current.index, column] = current[column].to_numpy()
    return result.reset_index() if not result.empty else pd.DataFrame()


def _replay_simulation_views(
    frame: pd.DataFrame,
    views: list[tuple[SnapshotComponentSelection, ComponentManifest]],
    selected_by_id: Mapping[
        str, tuple[SnapshotComponentSelection, ComponentManifest]
    ],
) -> pd.DataFrame:
    """Replay every selected view from its pinned direct dependencies and version."""

    if not views:
        raise IntegrityError("Daily component rows have no selected simulation view")
    result = frame.copy()
    keys = list(TABLE_KEYS[MarketTable.DAILY_BARS])
    covered: set[tuple[Any, ...]] = set()
    for ref, view in sorted(views, key=lambda item: (item[0].priority, item[0].ordinal)):
        dependencies: dict[str, tuple[SnapshotComponentSelection, ComponentManifest]] = {}
        for dependency_id in view.dependency_ids:
            selected = selected_by_id.get(dependency_id)
            if selected is None:
                raise IntegrityError(
                    f"Simulation view dependency is not selected: "
                    f"{view.component_id}/{dependency_id}"
                )
            dependency_ref, dependency = selected
            if (
                dependency.kind is ComponentKind.SIMULATION_VIEW
                or dependency.scope.table is not MarketTable.DAILY_BARS
                or not dependency.scope.overlaps(view.scope)
                or (dependency_ref.priority, dependency_ref.ordinal)
                >= (ref.priority, ref.ordinal)
            ):
                raise IntegrityError(
                    f"Simulation view has an invalid direct dependency: "
                    f"{view.component_id}/{dependency_id}"
                )
            dependencies[dependency_id] = selected
        expected = {
            component_id
            for component_id, (candidate_ref, candidate) in selected_by_id.items()
            if (
                candidate.kind is not ComponentKind.SIMULATION_VIEW
                and candidate.scope.table is MarketTable.DAILY_BARS
                and candidate.scope.overlaps(view.scope)
                and (candidate_ref.priority, candidate_ref.ordinal)
                < (ref.priority, ref.ordinal)
            )
        }
        if set(dependencies) != expected:
            raise IntegrityError(
                f"Simulation view direct dependency closure mismatch: {view.component_id}"
            )
        mask = _scope_mask(result, view.scope)
        if not mask.any():
            continue
        rebuilt = _materialize_simulation_view_versioned(
            result.loc[mask].copy(), view.builder_version,
        )
        for field in _DERIVED_VIEW_FIELDS:
            result.loc[mask, field] = rebuilt[field].to_numpy()
        covered.update(
            tuple(row)
            for row in result.loc[mask, keys].itertuples(index=False, name=None)
        )
    expected_keys = {
        tuple(row) for row in result[keys].itertuples(index=False, name=None)
    }
    if covered != expected_keys:
        raise IntegrityError("Daily component rows are not covered by a pinned simulation view")
    return result.drop(columns=["effective_from", "effective_to"], errors="ignore")


def _scope_mask(frame: pd.DataFrame, scope: ComponentScope) -> pd.Series:
    mask = pd.Series(True, index=frame.index)
    if scope.instrument_ids:
        mask &= frame["instrument_id"].astype(str).isin(scope.instrument_ids)
    if scope.start_date is not None:
        sessions = pd.to_datetime(frame["session_date"], errors="coerce").dt.date
        mask &= sessions.between(scope.start_date, scope.end_date)
    return mask


def _field_groups(table: MarketTable) -> tuple[tuple[ComponentKind, tuple[str, ...]], ...]:
    keys = tuple(TABLE_KEYS[table])
    business = tuple(BUSINESS_SCHEMAS[table])
    if table is MarketTable.INSTRUMENTS:
        rule_fields = _INSTRUMENT_RULE_FIELDS
    elif table is MarketTable.DAILY_BARS:
        rule_fields = _DAILY_RULE_FIELDS
    else:
        rule_fields = ()
    facts = tuple(dict.fromkeys((
        *keys,
        *(item for item in business if item not in {
            *rule_fields, *_DETAIL_LINEAGE_FIELDS, *_DERIVED_VIEW_FIELDS,
        }),
        *_SOURCE_LINEAGE_FIELDS,
    )))
    adjudications = tuple(dict.fromkeys((*keys, *_DETAIL_LINEAGE_FIELDS)))
    groups = [(ComponentKind.MARKET_FACTS, facts)]
    if rule_fields:
        groups.append((
            ComponentKind.TRADING_RULEBOOK,
            tuple(dict.fromkeys((*keys, *rule_fields))),
        ))
    groups.append((ComponentKind.FIELD_ADJUDICATIONS, adjudications))
    return tuple(groups)


def _materialize_simulation_view(frame: pd.DataFrame) -> pd.DataFrame:
    from fundlab.marketdata.trade_rules import _round_price_series_to_tick

    result = frame.copy()
    result["limit_up"] = pd.Series(pd.NA, index=result.index, dtype="Float64")
    result["limit_down"] = pd.Series(pd.NA, index=result.index, dtype="Float64")
    bounded = (
        result["price_limit_state"].astype("string").eq("bounded")
        & ~result["suspended"].fillna(False).astype(bool)
    )
    if result.loc[bounded, ["previous_close", "price_tick"]].isna().any().any():
        raise SnapshotNotReadyError("Bounded component rule lacks price/tick inputs")
    symmetric = bounded & result["price_limit_ratio"].notna()
    for ratio, group in result.loc[symmetric].groupby("price_limit_ratio", sort=False):
        movement = Decimal(str(ratio))
        result.loc[group.index, "limit_up"] = _round_price_series_to_tick(
            group["previous_close"],
            group["price_tick"],
            Decimal("1") + movement,
        )
        result.loc[group.index, "limit_down"] = _round_price_series_to_tick(
            group["previous_close"],
            group["price_tick"],
            Decimal("1") - movement,
        )
    asymmetric = bounded & result["price_limit_ratio"].isna()
    supported = {
        "cn-stock-legacy-ipo-first-session-44up-36down-v1": (
            Decimal("1.44"), Decimal("0.64"),
        ),
    }
    for rule_id, group in result.loc[asymmetric].groupby("trade_rule_id", sort=False):
        multipliers = supported.get(str(rule_id))
        if multipliers is None:
            raise SnapshotNotReadyError(
                f"Bounded component rule has no reconstructable multipliers: {rule_id}"
            )
        upper, lower = multipliers
        result.loc[group.index, "limit_up"] = _round_price_series_to_tick(
            group["previous_close"], group["price_tick"], upper,
        )
        result.loc[group.index, "limit_down"] = _round_price_series_to_tick(
            group["previous_close"], group["price_tick"], lower,
        )
    return result.drop(columns=["effective_from", "effective_to"], errors="ignore")


def _materialize_simulation_view_versioned(
    frame: pd.DataFrame,
    builder_version: str,
) -> pd.DataFrame:
    materializers = {
        "simulation-view-r1": _materialize_simulation_view,
    }
    materializer = materializers.get(builder_version)
    if materializer is None:
        raise IntegrityError(
            f"Unsupported pinned simulation view builder: {builder_version}"
        )
    return materializer(frame)


def _frame_identity(table: MarketTable, frame: pd.DataFrame) -> str:
    return _comparison_identities(table, frame)[0]


def _simulation_identity(table: MarketTable, frame: pd.DataFrame) -> str:
    """Identity of values observable by simulation, excluding audit-only provenance."""

    return _comparison_identities(table, frame)[1]


def _comparison_identities(table: MarketTable, frame: pd.DataFrame) -> tuple[str, str]:
    normalized = normalize_table(table, frame, require_observation_id=True)
    full_payload = normalized.astype("string").fillna("<NA>")
    full_rows = pd.util.hash_pandas_object(full_payload, index=False).astype("uint64")
    full_identity = stable_digest({
        "table": table,
        "columns": tuple(normalized.columns),
        "rows": tuple(map(int, full_rows)),
    })
    audit_only = {
        "field_lineage", "source_payload", "source_provider",
        "source_observation_id", "observed_at",
    }
    columns = tuple(item for item in normalized.columns if item not in audit_only)
    payload = normalized.loc[:, columns].astype("string").fillna("<NA>")
    row_hash = pd.util.hash_pandas_object(payload, index=False).astype("uint64")
    simulation_identity = stable_digest({
        "table": table,
        "columns": columns,
        "rows": tuple(map(int, row_hash)),
    })
    return full_identity, simulation_identity


def _cell_identity(value: Any) -> str:
    if value is None or value is pd.NA:
        return "<NA>"
    try:
        if pd.isna(value):
            return "<NA>"
    except (TypeError, ValueError):
        pass
    return str(value)
