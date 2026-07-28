from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date
from itertools import combinations
import json
import os
from pathlib import Path
from threading import Lock
from types import MappingProxyType
from typing import Any, Iterator, Mapping
from uuid import uuid4

import pandas as pd

from fundlab.common.canonical import canonical_json, stable_digest, to_primitive
from fundlab.marketdata.contracts import (
    CURRENT_SH_SZ_STOCK_ETF_UNIVERSE,
    CoverageClaim,
    MarketTable,
    ObservationManifest,
    ObservationPayload,
    PriceMode,
    ProviderCapability,
    ProviderRequest,
    ReadinessProfile,
    SnapshotManifest,
    SnapshotPlan,
    SourceSlice,
    UniverseScope,
)
from fundlab.marketdata.providers import ProviderRegistry
from fundlab.marketdata.reconciliation import (
    ReconciliationPolicy,
    default_reconciliation_policy,
)
from fundlab.marketdata.schema import BUSINESS_SCHEMAS
from fundlab.marketdata.sources import default_provider_registry
from fundlab.marketdata.trade_rules import materialize_order_quantity_rules
from fundlab.marketdata.warehouse import MarketDataWarehouse


DEFAULT_HISTORY_START = date(2010, 1, 1)
DEFAULT_SOURCE_PAIR = ("tickflow", "baostock")
HISTORY_BUILD_SCHEMA_VERSION = 6
_HELD_BUILD_LOCKS: set[Path] = set()
_HELD_BUILD_LOCKS_GUARD = Lock()


@dataclass(frozen=True)
class HistoryBuildSpec:
    end_date: date
    start_date: date = DEFAULT_HISTORY_START
    instrument_ids: tuple[str, ...] = ()
    exchanges: tuple[str, ...] = ("SH", "SZ")
    asset_types: tuple[str, ...] = ("stock", "etf")
    universe_as_of: date | None = None
    batch_size: int = 50
    max_instruments: int | None = None
    shard_count: int = 1
    shard_index: int = 0
    publish: bool = False
    refresh: bool = False

    def __post_init__(self) -> None:
        if self.start_date > self.end_date:
            raise ValueError("History start_date must not exceed end_date")
        universe_as_of = self.end_date if self.universe_as_of is None else self.universe_as_of
        if self.end_date > universe_as_of:
            raise ValueError("History end_date must not exceed universe_as_of")
        if self.batch_size < 1 or self.batch_size > 100:
            raise ValueError("History batch_size must be between 1 and 100")
        if self.max_instruments is not None and self.max_instruments < 1:
            raise ValueError("max_instruments must be positive")
        if self.shard_count < 1 or self.shard_count > 16:
            raise ValueError("shard_count must be between 1 and 16")
        if self.shard_index < 0 or self.shard_index >= self.shard_count:
            raise ValueError("shard_index must be within shard_count")
        if self.publish and self.shard_count > 1:
            raise ValueError("Individual history shards cannot publish; assemble the cohort first")
        object.__setattr__(self, "instrument_ids", tuple(sorted(set(self.instrument_ids))))
        object.__setattr__(
            self, "exchanges", tuple(sorted({str(item).upper() for item in self.exchanges})),
        )
        object.__setattr__(
            self, "asset_types", tuple(sorted({str(item).lower() for item in self.asset_types})),
        )
        object.__setattr__(self, "universe_as_of", universe_as_of)


@dataclass(frozen=True)
class HistoryBatchResult:
    batch_id: str
    requested_instruments: tuple[str, ...]
    included_instruments: tuple[str, ...]
    excluded: Mapping[str, tuple[str, ...]]
    source_observation_ids: Mapping[str, str]
    canonical_observation_id: str | None
    reused: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "excluded", MappingProxyType({
            str(key): tuple(value) for key, value in self.excluded.items()
        }))
        object.__setattr__(
            self, "source_observation_ids", MappingProxyType(dict(self.source_observation_ids)),
        )


@dataclass(frozen=True)
class HistoryBuildResult:
    build_id: str
    cohort_id: str
    status: str
    start_date: date
    end_date: date
    universe_observation_id: str
    eligible_instruments: int
    included_instruments: int
    excluded_instruments: int
    source_observations: int
    canonical_partitions: int
    blockers: tuple[str, ...]
    snapshot_id: str | None
    snapshot_ready: bool
    published: bool
    checkpoint: Path
    report: Path

    def to_dict(self) -> dict[str, Any]:
        return to_primitive(self)


@dataclass(frozen=True)
class CurrentResearchResult:
    status: str
    source_snapshot_id: str
    source_snapshot_ids: tuple[str, ...]
    snapshot_id: str
    target_instruments: int
    included_instruments: int
    missing_instrument_ids: tuple[str, ...]
    excluded_source_instrument_ids: tuple[str, ...]
    published: bool
    report: Path

    def to_dict(self) -> dict[str, Any]:
        return to_primitive(self)


def record_no_trade_research_partition(
    warehouse: MarketDataWarehouse,
    *,
    predecessor_snapshot_id: str,
    universe_observation_id: str,
    calendar_observation_id: str,
    source_observation_ids: tuple[str, ...],
    start_date: date,
    end_date: date,
    instrument_ids: tuple[str, ...],
) -> ObservationManifest:
    """Record a research partition whose exact interval has no active price rows.

    Absence is publishable only when the immutable predecessor proves the lifecycle,
    the current official universe still contains the instruments, and at least three
    independent history backends all return no rows for the interval.  The empty
    research table is an explicit fact; simulation status later materializes one
    suspended row per open session without inventing OHLC.
    """

    target = tuple(sorted(set(map(str, instrument_ids))))
    if not target or start_date > end_date:
        raise ValueError("No-trade research evidence requires instruments and a valid interval")
    predecessor = warehouse.load_snapshot(predecessor_snapshot_id)
    predecessor_scope = predecessor.plan.universe_scope
    if (
        predecessor_scope is None
        or date.fromordinal(
            predecessor_scope.history_end.toordinal() + 1
        ) != start_date
        or not set(target) <= set(predecessor_scope.instrument_ids)
    ):
        raise ValueError("No-trade evidence requires the contiguous trusted predecessor")
    universe = warehouse.load_observation(universe_observation_id)
    universe_frame = warehouse.read_observation_table(
        universe_observation_id, MarketTable.INSTRUMENTS,
    )
    if (
        universe.provider != "exchange-public"
        or str(universe.source_metadata.get("as_of_date"))[:10] != end_date.isoformat()
        or not set(target) <= set(map(str, universe_frame["instrument_id"]))
    ):
        raise ValueError("No-trade evidence requires current official membership")
    calendar = warehouse.load_observation(calendar_observation_id)
    calendar_quality = calendar.source_metadata.get("calendar_quality")
    if (
        not isinstance(calendar_quality, Mapping)
        or not calendar_quality.get("validated")
        or str(calendar_quality.get("start_date"))[:10] > start_date.isoformat()
        or str(calendar_quality.get("end_date"))[:10] < end_date.isoformat()
    ):
        raise ValueError("No-trade evidence requires a validated calendar")

    evidence: dict[str, dict[str, str]] = {item: {} for item in target}
    manifests = tuple(
        warehouse.load_observation(item)
        for item in sorted(set(source_observation_ids))
    )
    for manifest in manifests:
        if (
            manifest.request.capability is not ProviderCapability.DAILY_BARS_RAW
            or manifest.request.start_date is None
            or manifest.request.end_date is None
            or manifest.request.start_date > start_date
            or manifest.request.end_date < end_date
        ):
            raise ValueError(f"Invalid no-trade source scope: {manifest.observation_id}")
        applicable = set(target).intersection(manifest.request.instrument_ids)
        if not applicable:
            continue
        frame = warehouse.read_observation_table(
            manifest.observation_id, MarketTable.DAILY_BARS,
        )
        active = set(map(str, frame.loc[
            frame["instrument_id"].isin(applicable)
            & frame["session_date"].between(
                start_date.isoformat(), end_date.isoformat(),
            ),
            "instrument_id",
        ]))
        if active:
            raise ValueError(
                f"No-trade source contains active rows: {manifest.observation_id}/{sorted(active)}"
            )
        backend = str(manifest.source_metadata.get("backend_group", manifest.provider))
        for instrument_id in applicable:
            previous = evidence[instrument_id].get(backend)
            if previous is not None and previous != manifest.observation_id:
                raise ValueError(
                    f"Duplicate no-trade backend evidence: {instrument_id}/{backend}"
                )
            evidence[instrument_id][backend] = manifest.observation_id
    insufficient = {
        instrument_id: tuple(sorted(backends))
        for instrument_id, backends in evidence.items()
        if len(backends) < 3
    }
    if insufficient:
        raise ValueError(f"No-trade evidence lacks three independent backends: {insufficient}")

    instruments = warehouse.query_loaded_snapshot_table(
        predecessor, MarketTable.INSTRUMENTS, instrument_ids=target,
    )
    if set(map(str, instruments["instrument_id"])) != set(target):
        raise ValueError("Predecessor instrument metadata is incomplete")
    instruments = materialize_order_quantity_rules(instruments)
    prior_bars = warehouse.query_loaded_snapshot_table(
        predecessor,
        MarketTable.DAILY_BARS,
        instrument_ids=target,
        start_date=predecessor_scope.history_start,
        end_date=predecessor_scope.history_end,
        price_mode="raw",
    )
    last_close = {}
    for instrument_id in target:
        rows = prior_bars.loc[
            prior_bars["instrument_id"].eq(instrument_id)
            & prior_bars["close"].notna()
        ].sort_values("session_date", kind="stable")
        if rows.empty:
            raise ValueError(f"Predecessor has no last traded close: {instrument_id}")
        last_close[instrument_id] = {
            "session_date": str(rows.iloc[-1]["session_date"]),
            "close": float(rows.iloc[-1]["close"]),
            "source_observation_id": str(rows.iloc[-1]["source_observation_id"]),
        }
    empty_bars = prior_bars.iloc[0:0].copy()
    observed_at = max((universe.observed_at, calendar.observed_at, *(m.observed_at for m in manifests)))
    input_ids = tuple(sorted({
        universe_observation_id,
        calendar_observation_id,
        *source_observation_ids,
        *(item["source_observation_id"] for item in last_close.values()),
    }))
    quality = {
        "validated": True,
        "validator_version": "no-trade-research-r2-v1",
        "readiness": ReadinessProfile.RESEARCH_PRICE.value,
        "instrument_ids": target,
        "start_date": start_date,
        "end_date": end_date,
        "universe_definition": CURRENT_SH_SZ_STOCK_ETF_UNIVERSE,
        "universe_as_of": end_date,
        "row_count": 0,
        "independent_empty_backends": {
            key: dict(sorted(value.items())) for key, value in sorted(evidence.items())
        },
            "predecessor_last_traded_close": last_close,
            "predecessor_snapshot_id": predecessor_snapshot_id,
        "input_observation_ids": input_ids,
    }
    return warehouse.record_observation(ObservationPayload(
        "canonical-no-trade-reconciler-r2-v1",
        observed_at,
        ProviderRequest(
            ProviderCapability.CANONICAL_RECONCILIATION,
            start_date,
            end_date,
            target,
            {
                "kind": "independent-no-trade-consensus",
                "input_observation_ids": input_ids,
            },
        ),
        {
            MarketTable.INSTRUMENTS: instruments,
            MarketTable.DAILY_BARS: empty_bars,
        },
        (
            CoverageClaim(
                MarketTable.INSTRUMENTS, True, instrument_ids=target,
                detail="Trusted predecessor lifecycle plus current official membership",
            ),
            CoverageClaim(
                MarketTable.DAILY_BARS, True, start_date, end_date, target,
                "Three-or-more independent backends confirm no active price rows",
            ),
        ),
        {
            "kind": "field_level_reconciliation",
            "reconciliation_ready": True,
            "policy": to_primitive(
                default_reconciliation_policy(ReadinessProfile.RESEARCH_PRICE)
            ),
            "partition_quality": quality,
            "report": {"blockers": (), "unresolved_conflicts": ()},
        },
    ))


class HistoryDatabaseBuilder:
    """Resumable source capture and scoped research-price database construction.

    The immutable observation warehouse is the durable resume boundary.  A small
    mutable checkpoint only indexes completed batches; losing it never loses source
    evidence because exact observations can still be rediscovered and verified.
    """

    def __init__(
        self,
        warehouse: MarketDataWarehouse,
        report_root: str | Path,
        *,
        registry: ProviderRegistry | None = None,
        universe_provider: str = "baostock",
        source_pair: tuple[str, str] = DEFAULT_SOURCE_PAIR,
        adjudicator_provider: str | None = None,
        policy: ReconciliationPolicy | None = None,
    ) -> None:
        self.warehouse = warehouse
        self.report_root = Path(report_root).resolve()
        self.registry = registry or default_provider_registry()
        self.universe_provider = universe_provider
        self.source_pair = tuple(source_pair)
        if len(self.source_pair) != 2 or self.source_pair[0] == self.source_pair[1]:
            raise ValueError("History build requires two distinct named source providers")
        if adjudicator_provider in self.source_pair:
            raise ValueError("History adjudicator must be distinct from both baseline providers")
        self.adjudicator_provider = adjudicator_provider
        self.policy = policy or default_reconciliation_policy(ReadinessProfile.RESEARCH_PRICE)

    def build(
        self,
        spec: HistoryBuildSpec,
        *,
        universe_observation_id: str | None = None,
    ) -> HistoryBuildResult:
        """Build one exact price partition.

        ``universe_observation_id`` is an explicit, already-recorded master
        override for narrowly scoped work such as onboarding an exchange-
        announced new listing before the default historical master catches up.
        It does not change the default historical build path.
        """
        self.warehouse.initialize()
        lock_identity: dict[str, Any] = {
            "schema_version": HISTORY_BUILD_SCHEMA_VERSION,
            "spec": spec,
            "universe_provider": self.universe_provider,
            "source_pair": self.source_pair,
            "adjudicator_provider": self.adjudicator_provider,
            "policy_version": self.policy.version,
        }
        # Preserve the exact pre-override lock identity for the default path.
        # This keeps old and upgraded processes mutually exclusive while they
        # share the same build/checkpoint identity during a rolling upgrade.
        if universe_observation_id is not None:
            lock_identity["universe_observation_id"] = universe_observation_id
        lock_id = stable_digest(lock_identity)[:24]
        lock_path = self.warehouse.root / "builds" / ".locks" / f"{lock_id}.lock"
        with _exclusive_build_lock(lock_path):
            return self._build_locked(
                spec,
                universe_observation_id=universe_observation_id,
            )

    def _build_locked(
        self,
        spec: HistoryBuildSpec,
        *,
        universe_observation_id: str | None,
    ) -> HistoryBuildResult:
        self.warehouse.initialize()
        self.report_root.mkdir(parents=True, exist_ok=True)
        if universe_observation_id is None:
            universe_request = ProviderRequest(
                ProviderCapability.INSTRUMENTS,
                parameters={
                    "exchanges": tuple(
                        item for item in spec.exchanges if item in {"SH", "SZ"}
                    ),
                    "asset_types": spec.asset_types,
                    "include_delisted": False,
                },
            )
            universe, _ = self._capture_exact(
                self.universe_provider, universe_request, refresh=spec.refresh,
            )
            universe_frame = self.warehouse.read_observation_table(
                universe.observation_id, MarketTable.INSTRUMENTS,
            )
        else:
            universe = self.warehouse.load_observation(universe_observation_id)
            universe_frame = self.warehouse.read_observation_table(
                universe.observation_id, MarketTable.INSTRUMENTS,
            )
            _validate_explicit_history_universe(universe, universe_frame, spec)
        eligible, not_in_master = _select_universe(universe_frame, spec)
        cohort_id = _cohort_id(
            spec,
            universe.observation_id,
            self.source_pair,
            self.policy.version,
            self.adjudicator_provider,
        )
        build_id = "history-" + stable_digest({
            "schema_version": HISTORY_BUILD_SCHEMA_VERSION,
            "spec": spec,
            "universe_observation_id": universe.observation_id,
            "source_pair": self.source_pair,
            "adjudicator_provider": self.adjudicator_provider,
            "policy_version": self.policy.version,
        })[:24]
        checkpoint_path = self.warehouse.root / "builds" / build_id / "checkpoint.json"
        checkpoint = _read_checkpoint(checkpoint_path, build_id)
        stored_batches = checkpoint.setdefault("batches", {})
        if not isinstance(stored_batches, dict):
            raise ValueError(f"History checkpoint batches are invalid: {checkpoint_path}")

        results: list[HistoryBatchResult] = []
        batches = tuple(_chunks(tuple(eligible["instrument_id"]), spec.batch_size))
        selected_batches = tuple(
            batch
            for ordinal, batch in enumerate(batches)
            if ordinal % spec.shard_count == spec.shard_index
        )
        for instrument_batch in selected_batches:
            batch_id = "batch-" + stable_digest(instrument_batch)[:16]
            restored = self._restore_batch(stored_batches.get(batch_id))
            if restored is not None and not spec.refresh:
                results.append(restored)
                continue
            batch_master = eligible.loc[
                eligible["instrument_id"].isin(instrument_batch)
            ].reset_index(drop=True)
            outcome = self._build_batch(
                spec,
                cohort_id,
                build_id,
                batch_id,
                instrument_batch,
                batch_master,
                universe,
            )
            results.append(outcome)
            stored_batches[batch_id] = to_primitive(outcome)
            checkpoint.update({
                "schema_version": HISTORY_BUILD_SCHEMA_VERSION,
                "build_id": build_id,
                "cohort_id": cohort_id,
                "spec": to_primitive(spec),
                "universe_observation_id": universe.observation_id,
                "source_pair": self.source_pair,
                "policy_version": self.policy.version,
            })
            _write_atomic(checkpoint_path, checkpoint)

        canonical = [item for item in results if item.canonical_observation_id]
        snapshot: SnapshotManifest | None = None
        published = False
        if canonical:
            snapshot_instrument_ids = tuple(sorted({
                instrument_id
                for item in canonical
                for instrument_id in item.included_instruments
            }))
            selections: list[SourceSlice] = []
            for item in canonical:
                assert item.canonical_observation_id is not None
                reason = f"validated history partition {item.batch_id} policy {self.policy.version}"
                selections.extend((
                    SourceSlice(
                        item.canonical_observation_id,
                        MarketTable.INSTRUMENTS,
                        reason,
                        item.included_instruments,
                    ),
                    SourceSlice(
                        item.canonical_observation_id,
                        MarketTable.DAILY_BARS,
                        reason,
                        item.included_instruments,
                        spec.start_date,
                        spec.end_date,
                    ),
                ))
            snapshot = self.warehouse.build_partitioned_snapshot(SnapshotPlan(
                tuple(selections),
                (
                    f"Issue #7 scoped research history {spec.start_date.isoformat()}"
                    f"..{spec.end_date.isoformat()} build={build_id}"
                ),
                readiness=ReadinessProfile.RESEARCH_PRICE,
                universe_scope=_universe_scope(spec, snapshot_instrument_ids),
            ))
            if spec.publish and snapshot.quality.ready:
                self.warehouse.publish(snapshot.snapshot_id)
                published = True

        included_ids = {
            instrument_id
            for result in results
            for instrument_id in result.included_instruments
        }
        excluded = {
            instrument_id: reasons
            for result in results
            for instrument_id, reasons in result.excluded.items()
        }
        excluded.update({item: ("not_in_historical_master",) for item in not_in_master})
        blockers: list[str] = []
        unsupported_exchanges = sorted(set(spec.exchanges) - {"SH", "SZ"})
        blockers.extend(f"historical_universe_missing:{item}" for item in unsupported_exchanges)
        if spec.shard_count > 1:
            blockers.append(f"shard_scope:{spec.shard_index + 1}/{spec.shard_count}")
        if excluded:
            blockers.append(f"excluded_instruments:{len(excluded)}")
        if snapshot is None:
            blockers.append("no_reconciled_partition")
        elif not snapshot.quality.ready:
            blockers.extend(f"snapshot:{item}" for item in snapshot.quality.errors)

        source_ids = {
            observation_id
            for result in results
            for observation_id in result.source_observation_ids.values()
        }
        status = (
            "complete" if not blockers and snapshot is not None and snapshot.quality.ready
            else "ready_scoped" if snapshot is not None and snapshot.quality.ready
            else "incomplete"
        )
        report_payload = {
            "decision_source": "https://github.com/zhan7653/miniQMT/issues/7",
            "schema_version": HISTORY_BUILD_SCHEMA_VERSION,
            "build_id": build_id,
            "cohort_id": cohort_id,
            "status": status,
            "spec": to_primitive(spec),
            "policy": to_primitive(self.policy),
            "universe_observation_id": universe.observation_id,
            "eligible_instruments": len(eligible),
            "eligible_instrument_ids": tuple(sorted(map(str, eligible["instrument_id"]))),
            "eligible_universe_hash": stable_digest(tuple(sorted(map(str, eligible["instrument_id"])))),
            "included_instrument_ids": tuple(sorted(included_ids)),
            "excluded": dict(sorted(excluded.items())),
            "source_observation_ids": tuple(sorted(source_ids)),
            "canonical_observation_ids": tuple(
                item.canonical_observation_id for item in canonical
            ),
            "blockers": tuple(sorted(set(blockers))),
            "snapshot_id": None if snapshot is None else snapshot.snapshot_id,
            "snapshot_quality": None if snapshot is None else to_primitive(snapshot.quality),
            "published": published,
            "checkpoint": checkpoint_path,
            "source_identity_note": (
                f"Explicit source pair={self.source_pair}; TickFlow remains recorded as "
                "tickflow-unverified and the comparison backend keeps its own identity. "
                "Scope expansion is blocked wherever two-source rows do not agree."
            ),
        }
        report_hash = stable_digest(report_payload)[:16]
        report_path = self.report_root / f"history-build-{build_id}-{report_hash}.json"
        if report_path.exists():
            existing = json.loads(report_path.read_text(encoding="utf-8"))
            if existing != to_primitive(report_payload):
                raise ValueError(f"Immutable history report collision: {report_path}")
        else:
            report_path.write_text(
                canonical_json(report_payload), encoding="utf-8", newline="\n",
            )
        return HistoryBuildResult(
            build_id,
            cohort_id,
            status,
            spec.start_date,
            spec.end_date,
            universe.observation_id,
            len(eligible),
            len(included_ids),
            len(excluded),
            len(source_ids),
            len(canonical),
            tuple(sorted(set(blockers))),
            None if snapshot is None else snapshot.snapshot_id,
            bool(snapshot and snapshot.quality.ready),
            published,
            checkpoint_path,
            report_path,
        )

    def assemble(
        self,
        spec: HistoryBuildSpec,
        *,
        publish: bool = False,
    ) -> HistoryBuildResult:
        """Assemble all completed disjoint partitions in one shard cohort."""

        self.warehouse.initialize()
        self.report_root.mkdir(parents=True, exist_ok=True)
        universe_request = ProviderRequest(
            ProviderCapability.INSTRUMENTS,
            parameters={
                "exchanges": tuple(item for item in spec.exchanges if item in {"SH", "SZ"}),
                "asset_types": spec.asset_types,
                "include_delisted": False,
            },
        )
        universe, _ = self._capture_exact(
            self.universe_provider, universe_request, refresh=False,
        )
        universe_frame = self.warehouse.read_observation_table(
            universe.observation_id, MarketTable.INSTRUMENTS,
        )
        eligible, not_in_master = _select_universe(universe_frame, spec)
        cohort_id = _cohort_id(
            spec,
            universe.observation_id,
            self.source_pair,
            self.policy.version,
            self.adjudicator_provider,
        )
        canonical_provider = f"canonical-reconciler-{self.policy.version}"
        by_scope: dict[tuple[str, ...], ObservationManifest] = {}
        for manifest in self.warehouse.observations(provider=canonical_provider):
            quality = manifest.source_metadata.get("partition_quality")
            if not isinstance(quality, Mapping) or quality.get("cohort_id") != cohort_id:
                continue
            instrument_ids = tuple(sorted(map(str, quality.get("instrument_ids", ()))))
            if not instrument_ids or not quality.get("validated"):
                continue
            previous = by_scope.get(instrument_ids)
            if previous is None or (
                manifest.observed_at,
                manifest.observation_id,
            ) > (
                previous.observed_at,
                previous.observation_id,
            ):
                by_scope[instrument_ids] = manifest

        selected: list[tuple[tuple[str, ...], ObservationManifest]] = []
        seen: set[str] = set()
        overlap: set[str] = set()
        for instrument_ids, manifest in sorted(by_scope.items()):
            found = seen.intersection(instrument_ids)
            if found:
                overlap.update(found)
                continue
            seen.update(instrument_ids)
            selected.append((instrument_ids, manifest))

        snapshot: SnapshotManifest | None = None
        published = False
        if selected and not overlap:
            snapshot_instrument_ids = tuple(sorted(seen))
            selections: list[SourceSlice] = []
            for instrument_ids, manifest in selected:
                batch_id = str(
                    manifest.source_metadata.get("partition_quality", {}).get(
                        "batch_id", manifest.observation_id,
                    )
                )
                reason = f"assembled validated cohort {cohort_id} partition {batch_id}"
                selections.extend((
                    SourceSlice(
                        manifest.observation_id,
                        MarketTable.INSTRUMENTS,
                        reason,
                        instrument_ids,
                    ),
                    SourceSlice(
                        manifest.observation_id,
                        MarketTable.DAILY_BARS,
                        reason,
                        instrument_ids,
                        spec.start_date,
                        spec.end_date,
                    ),
                ))
            snapshot = self.warehouse.build_partitioned_snapshot(SnapshotPlan(
                tuple(selections),
                (
                    f"Issue #7 assembled research history {spec.start_date.isoformat()}"
                    f"..{spec.end_date.isoformat()} cohort={cohort_id}"
                ),
                readiness=ReadinessProfile.RESEARCH_PRICE,
                universe_scope=_universe_scope(spec, snapshot_instrument_ids),
            ))
            if publish and snapshot.quality.ready:
                self.warehouse.publish(snapshot.snapshot_id)
                published = True

        eligible_ids = set(map(str, eligible["instrument_id"]))
        included_ids = seen if not overlap else set()
        missing_ids = tuple(sorted(eligible_ids - included_ids))
        blockers: list[str] = []
        blockers.extend(
            f"historical_universe_missing:{item}"
            for item in sorted(set(spec.exchanges) - {"SH", "SZ"})
        )
        if not_in_master:
            blockers.append(f"not_in_historical_master:{len(not_in_master)}")
        if missing_ids:
            blockers.append(f"missing_reconciled_instruments:{len(missing_ids)}")
        if overlap:
            blockers.append(f"overlapping_partitions:{len(overlap)}")
        if snapshot is None:
            blockers.append("no_assembled_snapshot")
        elif not snapshot.quality.ready:
            blockers.extend(f"snapshot:{item}" for item in snapshot.quality.errors)

        source_ids: set[str] = set()
        for _, manifest in selected:
            quality = manifest.source_metadata.get("partition_quality", {})
            values = quality.get("source_observation_ids", {})
            if isinstance(values, Mapping):
                source_ids.update(map(str, values.values()))
        status = (
            "complete" if not blockers and snapshot is not None and snapshot.quality.ready
            else "ready_scoped" if snapshot is not None and snapshot.quality.ready
            else "incomplete"
        )
        build_id = f"assembly-{cohort_id}"
        checkpoint_path = self.warehouse.root / "builds" / cohort_id / "assembly.json"
        assembly_payload = {
            "schema_version": HISTORY_BUILD_SCHEMA_VERSION,
            "build_id": build_id,
            "cohort_id": cohort_id,
            "canonical_observation_ids": tuple(
                manifest.observation_id for _, manifest in selected
            ),
            "snapshot_id": None if snapshot is None else snapshot.snapshot_id,
            "published": published,
        }
        _write_atomic(checkpoint_path, assembly_payload)
        report_payload = {
            "decision_source": "https://github.com/zhan7653/miniQMT/issues/7",
            "kind": "history_cohort_assembly",
            "schema_version": HISTORY_BUILD_SCHEMA_VERSION,
            "build_id": build_id,
            "cohort_id": cohort_id,
            "status": status,
            "spec": to_primitive(spec),
            "policy": to_primitive(self.policy),
            "universe_observation_id": universe.observation_id,
            "eligible_instruments": len(eligible_ids),
            "eligible_instrument_ids": tuple(sorted(eligible_ids)),
            "eligible_universe_hash": stable_digest(tuple(sorted(eligible_ids))),
            "included_instrument_ids": tuple(sorted(included_ids)),
            "missing_instrument_ids": missing_ids,
            "not_in_master": not_in_master,
            "overlapping_instrument_ids": tuple(sorted(overlap)),
            "source_observation_ids": tuple(sorted(source_ids)),
            "canonical_observation_ids": assembly_payload["canonical_observation_ids"],
            "blockers": tuple(sorted(set(blockers))),
            "snapshot_id": None if snapshot is None else snapshot.snapshot_id,
            "snapshot_quality": None if snapshot is None else to_primitive(snapshot.quality),
            "published": published,
            "checkpoint": checkpoint_path,
        }
        report_hash = stable_digest(report_payload)[:16]
        report_path = self.report_root / f"history-assembly-{cohort_id}-{report_hash}.json"
        if report_path.exists():
            existing = json.loads(report_path.read_text(encoding="utf-8"))
            if existing != to_primitive(report_payload):
                raise ValueError(f"Immutable history assembly report collision: {report_path}")
        else:
            report_path.write_text(
                canonical_json(report_payload), encoding="utf-8", newline="\n",
            )
        return HistoryBuildResult(
            build_id,
            cohort_id,
            status,
            spec.start_date,
            spec.end_date,
            universe.observation_id,
            len(eligible_ids),
            len(included_ids),
            len(missing_ids) + len(not_in_master),
            len(source_ids),
            len(selected),
            tuple(sorted(set(blockers))),
            None if snapshot is None else snapshot.snapshot_id,
            bool(snapshot and snapshot.quality.ready),
            published,
            checkpoint_path,
            report_path,
        )

    def _build_batch(
        self,
        spec: HistoryBuildSpec,
        cohort_id: str,
        build_id: str,
        batch_id: str,
        instrument_ids: tuple[str, ...],
        batch_master: pd.DataFrame,
        universe: ObservationManifest,
    ) -> HistoryBatchResult:
        source_manifests: dict[str, ObservationManifest] = {}
        reused = True
        failures: dict[str, str] = {}

        def capture(provider: str) -> tuple[ObservationManifest, bool]:
            parameters: dict[str, Any] = {
                "batch_size": min(100, len(instrument_ids)),
                "count": 10000,
            }
            if provider == "xtquant":
                parameters.update({"download": True, "incrementally": False})
            request = ProviderRequest(
                ProviderCapability.DAILY_BARS_RAW,
                spec.start_date,
                spec.end_date,
                instrument_ids,
                parameters,
            )
            return self._capture_exact(provider, request, refresh=spec.refresh)

        # The two required backends are independent I/O channels.  Capture them in
        # parallel, but do not reconcile or checkpoint the batch until both immutable
        # observations have been recorded successfully.
        with ThreadPoolExecutor(
            max_workers=len(self.source_pair), thread_name_prefix="history-source",
        ) as executor:
            futures = {
                executor.submit(capture, provider): provider
                for provider in self.source_pair
            }
            for future in as_completed(futures):
                provider = futures[future]
                try:
                    observed, was_reused = future.result()
                except Exception as exc:
                    failures[provider] = f"{type(exc).__name__}:{str(exc)[:240]}"
                else:
                    source_manifests[provider] = observed
                    reused = reused and was_reused
        if failures:
            reason = "provider_error:" + ";".join(
                f"{provider}={detail}" for provider, detail in sorted(failures.items())
            )
            return HistoryBatchResult(
                batch_id,
                instrument_ids,
                (),
                {item: (reason,) for item in instrument_ids},
                {name: item.observation_id for name, item in source_manifests.items()},
                None,
                False,
            )

        payload, included, excluded = self._reconcile_partition(
            spec,
            cohort_id,
            build_id,
            batch_id,
            batch_master,
            universe,
            source_manifests,
        )
        conflict_ids = tuple(sorted(
            instrument_id
            for instrument_id, item_reasons in excluded.items()
            if any(
                reason == "source_session_keys_disagree"
                or reason.startswith("critical_conflict:")
                for reason in item_reasons
            )
        ))
        if self.adjudicator_provider and conflict_ids:
            parameters: dict[str, Any] = {
                "batch_size": min(100, len(conflict_ids)),
                "count": 10000,
                "adjudication_for": conflict_ids,
            }
            if self.adjudicator_provider == "xtquant":
                parameters.update({"download": True, "incrementally": False})
            request = ProviderRequest(
                ProviderCapability.DAILY_BARS_RAW,
                spec.start_date,
                spec.end_date,
                conflict_ids,
                parameters,
            )
            try:
                adjudicator, was_reused = self._capture_exact(
                    self.adjudicator_provider, request, refresh=spec.refresh,
                )
            except Exception as exc:
                reason = (
                    f"third_source_error:{self.adjudicator_provider}="
                    f"{type(exc).__name__}:{str(exc)[:240]}"
                )
                augmented = {
                    key: tuple(sorted({*value, reason})) if key in conflict_ids else value
                    for key, value in excluded.items()
                }
                excluded = MappingProxyType(augmented)
            else:
                source_manifests[self.adjudicator_provider] = adjudicator
                reused = reused and was_reused
                payload, included, excluded = self._reconcile_partition(
                    spec,
                    cohort_id,
                    build_id,
                    batch_id,
                    batch_master,
                    universe,
                    source_manifests,
                )
        canonical = None if payload is None else self.warehouse.record_observation(payload)
        return HistoryBatchResult(
            batch_id,
            instrument_ids,
            included,
            excluded,
            {name: item.observation_id for name, item in source_manifests.items()},
            None if canonical is None else canonical.observation_id,
            reused,
        )

    def _reconcile_partition(
        self,
        spec: HistoryBuildSpec,
        cohort_id: str,
        build_id: str,
        batch_id: str,
        batch_master: pd.DataFrame,
        universe: ObservationManifest,
        source_manifests: Mapping[str, ObservationManifest],
    ) -> tuple[ObservationPayload | None, tuple[str, ...], Mapping[str, tuple[str, ...]]]:
        primary_name, secondary_name = self.source_pair
        primary_manifest = source_manifests[primary_name]
        secondary_manifest = source_manifests[secondary_name]
        primary_backend = str(
            primary_manifest.source_metadata.get("backend_group", primary_name)
        )
        secondary_backend = str(
            secondary_manifest.source_metadata.get("backend_group", secondary_name)
        )
        requested = tuple(sorted(map(str, batch_master["instrument_id"])))
        reasons: dict[str, list[str]] = {item: [] for item in requested}
        if primary_backend == secondary_backend:
            for item in requested:
                reasons[item].append(f"same_backend:{primary_backend}")
            return None, (), _frozen_reasons(reasons)

        frames: dict[str, pd.DataFrame] = {}
        duplicate_ids: set[str] = set()
        for provider, manifest in source_manifests.items():
            frame = self.warehouse.read_observation_table(
                manifest.observation_id, MarketTable.DAILY_BARS,
            )
            frame = frame.loc[
                frame["instrument_id"].isin(requested)
                & frame["price_mode"].eq(PriceMode.RAW.value)
            ].copy()
            frames[provider] = frame.reset_index(drop=True)

        keys = ["instrument_id", "session_date", "price_mode"]
        safe_suspensions = _safe_suspension_keys(tuple(frames.values()), keys)
        for provider, frame in tuple(frames.items()):
            if safe_suspensions:
                index = pd.MultiIndex.from_frame(frame[keys])
                frame = frame.loc[~index.isin(safe_suspensions)].copy()
            # Research-price scope contains actual price observations.  Explicit
            # suspended rows are excluded.  If another source represents the same
            # suspended session as a zero-turnover flat bar, _safe_suspension_keys
            # removes that matching key from every source first.  A row with real
            # turnover is never hidden this way and still causes a scope mismatch.
            frame = frame.loc[~frame["suspended"].fillna(False)].reset_index(drop=True)
            duplicated = frame.duplicated(["instrument_id", "session_date", "price_mode"], keep=False)
            duplicate_ids.update(map(str, frame.loc[duplicated, "instrument_id"].unique()))
            frames[provider] = frame.loc[~duplicated].reset_index(drop=True)
        for instrument_id in duplicate_ids:
            reasons[instrument_id].append("duplicate_source_key")

        third_source_consensus = len(source_manifests) > 2
        adjudication_ids: set[str] = set()
        adjudication_payload_by_key: dict[tuple[str, str, str], str] = {}
        backends = {
            provider: str(manifest.source_metadata.get("backend_group", provider))
            for provider, manifest in source_manifests.items()
        }
        if third_source_consensus:
            adjudication_ids = set().union(*(
                set(manifest.request.instrument_ids)
                for provider, manifest in source_manifests.items()
                if provider not in {primary_name, secondary_name}
            ))
            adjudication_frames = {
                provider: frame.loc[
                    frame["instrument_id"].isin(adjudication_ids)
                ].reset_index(drop=True)
                for provider, frame in frames.items()
            }
            consensus, unresolved = _independent_consensus_frame(
                adjudication_frames,
                backends,
                self.policy,
            )
            for instrument_id, item_reasons in unresolved.items():
                reasons[instrument_id].extend(item_reasons)
            adjudication_payload_by_key = {
                tuple(str(row[key]) for key in keys): str(row["source_payload"])
                for row in consensus.to_dict("records")
            }
            # Feed the field-level majority result through the same partition
            # validation and publication path.  Both copies are synthetic views of
            # the independently derived consensus, not additional source evidence.
            # Instruments that already passed the baseline pair must remain on that
            # pair: the lazy adjudicator request intentionally contains no rows for
            # them and therefore cannot turn valid baseline data into false gaps.
            for provider in (primary_name, secondary_name):
                baseline = frames[provider].loc[
                    ~frames[provider]["instrument_id"].isin(adjudication_ids)
                ]
                frames[provider] = pd.concat(
                    (baseline, consensus), ignore_index=True,
                )

        primary = frames[primary_name]
        secondary = frames[secondary_name]
        primary_ids = set(map(str, primary["instrument_id"].unique()))
        secondary_ids = set(map(str, secondary["instrument_id"].unique()))
        for instrument_id in requested:
            if instrument_id not in primary_ids:
                reasons[instrument_id].append(f"missing_source:{primary_name}")
            if instrument_id not in secondary_ids:
                reasons[instrument_id].append(f"missing_source:{secondary_name}")

        critical = ("open", "high", "low", "close", "volume")
        optional = (
            "amount", "suspended", "price_limit_state", "previous_close",
            "price_limit_ratio", "limit_up", "limit_down",
        )
        primary_piece = primary[keys + list(critical) + list(optional)].copy()
        secondary_piece = secondary[keys + list(critical) + list(optional)].copy()
        merged = primary_piece.merge(
            secondary_piece,
            on=keys,
            how="outer",
            suffixes=("__primary", "__secondary"),
            indicator=True,
        )
        key_mismatch = set(map(
            str, merged.loc[merged["_merge"] != "both", "instrument_id"].unique(),
        ))
        for instrument_id in key_mismatch:
            reasons[instrument_id].append("source_session_keys_disagree")

        both = merged.loc[merged["_merge"] == "both"].copy()
        for field_name in critical:
            left = pd.to_numeric(both[f"{field_name}__primary"], errors="coerce")
            right = pd.to_numeric(both[f"{field_name}__secondary"], errors="coerce")
            rule = self.policy.rule(MarketTable.DAILY_BARS, field_name)
            difference = (left - right).abs()
            scale = pd.concat((left.abs(), right.abs()), axis=1).max(axis=1)
            agrees = (
                left.notna()
                & right.notna()
                & difference.le(rule.absolute_tolerance + rule.relative_tolerance * scale)
            )
            bad_ids = set(map(str, both.loc[~agrees, "instrument_id"].unique()))
            for instrument_id in bad_ids:
                reasons[instrument_id].append(f"critical_conflict:{field_name}")

        included = tuple(sorted(
            instrument_id for instrument_id in requested if not reasons[instrument_id]
        ))
        excluded = _frozen_reasons({
            key: value for key, value in reasons.items() if value
        })
        if not included:
            return None, (), excluded

        clean = both.loc[both["instrument_id"].isin(included)].copy()
        clean = clean.sort_values(keys, kind="stable").reset_index(drop=True)
        bars = pd.DataFrame({
            "instrument_id": clean["instrument_id"].astype(str),
            "session_date": clean["session_date"].astype(str),
            "price_mode": PriceMode.RAW.value,
            "open": clean["open__primary"],
            "high": clean["high__primary"],
            "low": clean["low__primary"],
            "close": clean["close__primary"],
            # Every adapter normalizes volume to shares.  Keep the explicitly chosen
            # secondary value after the two sources pass the calibrated tolerance.
            "volume": clean["volume__secondary"],
            "amount": clean["amount__primary"],
            "suspended": False,
            "price_limit_state": clean["price_limit_state__secondary"],
            "previous_close": clean["previous_close__secondary"],
            "price_limit_ratio": clean["price_limit_ratio__secondary"],
            "limit_up": clean["limit_up__secondary"],
            "limit_down": clean["limit_down__secondary"],
        })
        source_ids = {
            name: manifest.observation_id
            for name, manifest in source_manifests.items()
        }
        consensus_providers = tuple(source_manifests)
        consensus_backends = tuple(
            str(manifest.source_metadata.get("backend_group", name))
            for name, manifest in source_manifests.items()
        )
        baseline_ids = {
            name: source_ids[name] for name in (primary_name, secondary_name)
        }
        baseline_lineage = canonical_json({
            "policy_version": self.policy.version,
            "critical_consensus": {
                "providers": (primary_name, secondary_name),
                "backends": (primary_backend, secondary_backend),
                "source_observation_ids": baseline_ids,
                "method": "two-source-tolerance",
            },
            "field_selection": {
                "open": primary_name,
                "high": primary_name,
                "low": primary_name,
                "close": primary_name,
                "volume": secondary_name,
                "amount": primary_name,
                "suspended": secondary_name,
                "price_limit_state": secondary_name,
                "previous_close": secondary_name,
                "price_limit_ratio": secondary_name,
                "limit_up": secondary_name,
                "limit_down": secondary_name,
            },
        })
        baseline_payload = canonical_json({
            "kind": "two-source-reconciled-row",
            "source_observation_ids": baseline_ids,
        })
        row_lineage: list[str] = []
        row_payload: list[str] = []
        for row in bars[list(keys)].to_dict("records"):
            key = tuple(str(row[name]) for name in keys)
            if str(row["instrument_id"]) not in adjudication_ids:
                row_lineage.append(baseline_lineage)
                row_payload.append(baseline_payload)
                continue
            raw_consensus = adjudication_payload_by_key.get(key)
            if raw_consensus is None:
                raise ValueError(f"Adjudicated history row lost its consensus lineage: {key}")
            consensus_payload = json.loads(raw_consensus)
            present = tuple(map(str, consensus_payload["present_providers"]))
            actual_ids = {
                provider: source_ids[provider]
                for provider in present
            }
            row_lineage.append(canonical_json({
                "policy_version": self.policy.version,
                "critical_consensus": {
                    "providers": present,
                    "backends": tuple(backends[provider] for provider in present),
                    "source_observation_ids": actual_ids,
                    "method": "largest-independent-source-cluster",
                    "field_clusters": consensus_payload["field_clusters"],
                    "field_supporting_sources": consensus_payload["field_sources"],
                },
                "field_selection": {
                    "critical": consensus_payload["field_selected_provider"],
                    "optional": consensus_payload["optional_field_selected_provider"],
                },
            }))
            row_payload.append(canonical_json({
                "kind": "three-source-adjudicated-row",
                "source_observation_ids": actual_ids,
                "consensus": consensus_payload,
            }))
        bars["field_lineage"] = row_lineage
        bars["source_payload"] = row_payload

        instruments = materialize_order_quantity_rules(
            batch_master.loc[
                batch_master["instrument_id"].isin(included),
            ].copy()
        )
        instruments = instruments.reindex(
            columns=list(BUSINESS_SCHEMAS[MarketTable.INSTRUMENTS])
        )
        instrument_lineage = canonical_json({
            "selected_provider": universe.provider,
            "source_observation_id": universe.observation_id,
        })
        instruments["field_lineage"] = instrument_lineage
        instruments = materialize_order_quantity_rules(instruments)

        counts = bars.groupby("instrument_id", sort=False).size()
        if set(map(str, counts.index)) != set(included) or (counts <= 0).any():
            raise ValueError("Validated history partition lost an included instrument")
        if bars.duplicated(keys).any():
            raise ValueError("Validated history partition contains duplicate daily-bar keys")
        active = bars[["open", "high", "low", "close", "volume"]].apply(
            pd.to_numeric, errors="coerce",
        )
        if active.isna().any().any() or (active["volume"] < 0).any():
            raise ValueError("Validated history partition contains invalid critical values")
        invalid_ohlc = (
            active[["open", "high", "low", "close"]].min(axis=1).le(0)
            | active["high"].lt(active[["open", "close", "low"]].max(axis=1))
            | active["low"].gt(active[["open", "close", "high"]].min(axis=1))
        )
        if invalid_ohlc.any():
            raise ValueError("Validated history partition contains invalid OHLC relationships")

        observed_at = max(
            universe.observed_at,
            primary_manifest.observed_at,
            *(manifest.observed_at for manifest in source_manifests.values()),
        )
        request = ProviderRequest(
            ProviderCapability.CANONICAL_RECONCILIATION,
            spec.start_date,
            spec.end_date,
            included,
            {
                "input_observation_ids": tuple(sorted(source_ids.values())),
                "universe_observation_id": universe.observation_id,
                "policy_version": self.policy.version,
                "readiness": ReadinessProfile.RESEARCH_PRICE.value,
                "coverage_semantics": "instrument_lifecycle_active_price_sessions",
            },
        )
        partition_quality = {
            "validated": True,
            "cohort_id": cohort_id,
            "build_id": build_id,
            "batch_id": batch_id,
            "readiness": ReadinessProfile.RESEARCH_PRICE.value,
            "instrument_ids": included,
            "start_date": spec.start_date,
            "end_date": spec.end_date,
            "row_count": len(bars),
            "critical_fields": critical,
            "minimum_independent_backends": 2,
            "backends": consensus_backends,
            "source_observation_ids": source_ids,
            "coverage_semantics": (
                "at-least-two-independent-source consensus for every active-session key"
                if third_source_consensus
                else "matching active-session keys across both sources"
            ),
            "universe_definition": CURRENT_SH_SZ_STOCK_ETF_UNIVERSE,
            "universe_as_of": spec.universe_as_of,
        }
        payload = ObservationPayload(
            f"canonical-reconciler-{self.policy.version}",
            observed_at,
            request,
            {
                MarketTable.INSTRUMENTS: instruments,
                MarketTable.DAILY_BARS: bars,
            },
            (
                CoverageClaim(
                    MarketTable.INSTRUMENTS,
                    True,
                    instrument_ids=included,
                    detail="BaoStock lifecycle master for the exact reconciled partition",
                ),
                CoverageClaim(
                    MarketTable.DAILY_BARS,
                    True,
                    spec.start_date,
                    spec.end_date,
                    included,
                    "At least two independent backends agree for every active price-session key",
                ),
            ),
            {
                "kind": "field_level_reconciliation",
                "reconciliation_ready": True,
                "policy": to_primitive(self.policy),
                "report": {
                    "blockers": (),
                    "unresolved_conflicts": (),
                    "policy_version": self.policy.version,
                    "input_observation_ids": tuple(sorted(source_ids.values())),
                },
                "partition_quality": partition_quality,
            },
        )
        return payload, included, excluded

    def _capture_exact(
        self,
        provider: str,
        request: ProviderRequest,
        *,
        refresh: bool,
    ) -> tuple[ObservationManifest, bool]:
        if not refresh:
            matches = self.warehouse.matching_observations(provider=provider, request=request)
            if matches:
                return matches[-1], True
        payload = self.registry.observe(provider, request)
        return self.warehouse.record_observation(payload), False

    def _restore_batch(self, payload: Any) -> HistoryBatchResult | None:
        if not isinstance(payload, Mapping):
            return None
        canonical_id = payload.get("canonical_observation_id")
        # A failed or fully excluded batch is evidence for the previous attempt,
        # not a terminal success marker.  Retry it on the next normal resume so a
        # fixed adapter or recovered upstream can make progress without --refresh.
        if not canonical_id:
            return None
        if canonical_id:
            try:
                manifest = self.warehouse.load_observation(str(canonical_id))
            except Exception:
                return None
            if (
                manifest.source_metadata.get("kind") != "field_level_reconciliation"
                or not manifest.source_metadata.get("reconciliation_ready")
            ):
                return None
        source_ids = payload.get("source_observation_ids", {})
        if not isinstance(source_ids, Mapping):
            return None
        try:
            for observation_id in source_ids.values():
                self.warehouse.load_observation(str(observation_id))
            return HistoryBatchResult(
                str(payload["batch_id"]),
                tuple(payload.get("requested_instruments", ())),
                tuple(payload.get("included_instruments", ())),
                {
                    str(key): tuple(value)
                    for key, value in payload.get("excluded", {}).items()
                },
                {str(key): str(value) for key, value in source_ids.items()},
                None if canonical_id is None else str(canonical_id),
                True,
            )
        except (KeyError, TypeError, ValueError):
            return None


def compose_history_snapshot(
    warehouse: MarketDataWarehouse,
    observation_ids: tuple[str, ...],
    description: str,
    *,
    publish: bool = False,
) -> tuple[SnapshotManifest, str]:
    """Compose disjoint validated history partitions under one policy and date scope."""

    if not observation_ids:
        raise ValueError("History composition requires at least one observation")
    if len(set(observation_ids)) != len(observation_ids):
        raise ValueError("History composition contains duplicate observation ids")
    selections: list[SourceSlice] = []
    policy_versions: set[str] = set()
    date_scopes: set[tuple[date, date]] = set()
    universe_as_of_dates: set[date] = set()
    universe_definitions: set[str] = set()
    universe_instrument_ids: set[str] = set()
    for observation_id in observation_ids:
        manifest = warehouse.load_observation(observation_id)
        quality = manifest.source_metadata.get("partition_quality")
        policy = manifest.source_metadata.get("policy")
        if not isinstance(quality, Mapping) or not quality.get("validated"):
            raise ValueError(f"History partition is not validated: {observation_id}")
        if quality.get("readiness") != ReadinessProfile.RESEARCH_PRICE.value:
            raise ValueError(f"History partition readiness mismatch: {observation_id}")
        if not isinstance(policy, Mapping) or not policy.get("version"):
            raise ValueError(f"History partition has no policy version: {observation_id}")
        instrument_ids = tuple(sorted(map(str, quality.get("instrument_ids", ()))))
        if not instrument_ids:
            raise ValueError(f"History partition has no declared instruments: {observation_id}")
        start = date.fromisoformat(str(quality["start_date"])[:10])
        end = date.fromisoformat(str(quality["end_date"])[:10])
        policy_versions.add(str(policy["version"]))
        date_scopes.add((start, end))
        if not quality.get("universe_definition") or not quality.get("universe_as_of"):
            raise ValueError(
                f"History partition predates revision-2 universe evidence: {observation_id}"
            )
        universe_definitions.add(str(quality["universe_definition"]))
        universe_as_of_dates.add(date.fromisoformat(str(quality["universe_as_of"])[:10]))
        universe_instrument_ids.update(instrument_ids)
        reason = f"composed validated history partition policy={policy['version']}"
        selections.extend((
            SourceSlice(
                observation_id,
                MarketTable.INSTRUMENTS,
                reason,
                instrument_ids,
            ),
            SourceSlice(
                observation_id,
                MarketTable.DAILY_BARS,
                reason,
                instrument_ids,
                start,
                end,
            ),
        ))
    if len(policy_versions) != 1:
        raise ValueError(f"History composition mixes policy versions: {sorted(policy_versions)}")
    if len(date_scopes) != 1:
        raise ValueError(
            "History composition mixes date scopes: "
            + ",".join(f"{start}..{end}" for start, end in sorted(date_scopes))
        )
    if len(universe_definitions) != 1 or len(universe_as_of_dates) != 1:
        raise ValueError("History composition mixes universe definitions or as-of dates")
    start, end = next(iter(date_scopes))
    snapshot = warehouse.build_partitioned_snapshot(SnapshotPlan(
        tuple(selections),
        description,
        readiness=ReadinessProfile.RESEARCH_PRICE,
        universe_scope=UniverseScope(
            next(iter(universe_definitions)),
            next(iter(universe_as_of_dates)),
            start,
            end,
            survivorship_bias=True,
            instrument_ids=tuple(sorted(universe_instrument_ids)),
        ),
    ))
    if publish and snapshot.quality.ready:
        warehouse.publish(snapshot.snapshot_id)
    return snapshot, next(iter(policy_versions))


def derive_current_research_snapshot(
    warehouse: MarketDataWarehouse,
    report_root: str | Path,
    *,
    source_snapshot_id: str,
    supplement_snapshot_ids: tuple[str, ...] = (),
    universe_observation_id: str,
    universe_as_of: date,
    start_date: date,
    end_date: date,
    publish: bool = False,
) -> CurrentResearchResult:
    """Project disjoint immutable research snapshots onto one exact current universe."""

    if start_date > end_date or end_date > universe_as_of:
        raise ValueError("Current research dates must end no later than universe_as_of")
    source_snapshot_ids = tuple(dict.fromkeys((source_snapshot_id, *supplement_snapshot_ids)))
    sources = tuple(warehouse.load_snapshot(item) for item in source_snapshot_ids)
    if any(item.plan.readiness is not ReadinessProfile.RESEARCH_PRICE for item in sources):
        raise ValueError("Current research derivation requires research_price snapshots")
    universe_manifest = warehouse.load_observation(universe_observation_id)
    if not any(
        claim.table is MarketTable.INSTRUMENTS and claim.complete
        for claim in universe_manifest.coverage
    ):
        raise ValueError("Universe observation has no complete instrument claim")
    target_frame = warehouse.read_observation_table(
        universe_observation_id, MarketTable.INSTRUMENTS,
    )
    target_frame = target_frame.loc[
        target_frame["exchange"].isin(("SH", "SZ"))
        & target_frame["asset_type"].isin(("stock", "etf"))
    ]
    if universe_manifest.provider == "exchange-public":
        observed_as_of = universe_manifest.source_metadata.get("as_of_date")
        if str(observed_as_of)[:10] != universe_as_of.isoformat():
            raise ValueError(
                "Exchange universe observation as-of does not match requested universe_as_of"
            )
        claimed_ids = {
            instrument_id
            for claim in universe_manifest.coverage
            if claim.table is MarketTable.INSTRUMENTS and claim.complete
            for instrument_id in claim.instrument_ids
        }
        if claimed_ids != set(map(str, target_frame["instrument_id"])):
            raise ValueError("Exchange universe rows do not match the exact coverage claim")
    else:
        # Lifecycle masters contain historical members; only an exchange-public
        # observation is already an exact current-membership assertion.
        target_frame = target_frame.loc[
            target_frame["listed_date"].notna()
            & target_frame["listed_date"].le(universe_as_of.isoformat())
            & (
                target_frame["delisted_date"].isna()
                | target_frame["delisted_date"].gt(universe_as_of.isoformat())
            )
        ]
    target_ids = set(map(str, target_frame["instrument_id"]))
    if not target_ids:
        raise ValueError("Current universe observation has no target instruments")

    full_source_ids: dict[str, set[str]] = {}
    source_ids_by_snapshot: dict[str, set[str]] = {}
    owner_by_instrument: dict[str, str] = {}
    overlaps: set[str] = set()
    for snapshot_id, source in zip(source_snapshot_ids, sources, strict=True):
        source_instruments = warehouse.query_loaded_snapshot_table(
            source, MarketTable.INSTRUMENTS,
        )
        all_ids = set(map(str, source_instruments["instrument_id"]))
        full_source_ids[snapshot_id] = all_ids
        selected_ids = all_ids & target_ids
        source_ids_by_snapshot[snapshot_id] = selected_ids
        for instrument_id in selected_ids:
            if instrument_id in owner_by_instrument:
                overlaps.add(instrument_id)
            else:
                owner_by_instrument[instrument_id] = snapshot_id
    if overlaps:
        raise ValueError(
            f"Current research source snapshots overlap on {len(overlaps)} instruments"
        )
    all_target_source_ids = set().union(*source_ids_by_snapshot.values())
    included_ids = tuple(sorted(target_ids & all_target_source_ids))
    if not included_ids:
        raise ValueError("Research snapshots and target current universe do not overlap")
    missing_ids = tuple(sorted(target_ids - all_target_source_ids))
    every_source_id = set().union(*full_source_ids.values())
    excluded_source_ids = tuple(sorted(every_source_id - target_ids))

    selections: list[SourceSlice] = []
    source_master_parts = []
    for snapshot_id, source in zip(source_snapshot_ids, sources, strict=True):
        owned_ids = source_ids_by_snapshot[snapshot_id]
        master_part = warehouse.query_loaded_snapshot_table(
            source,
            MarketTable.INSTRUMENTS,
            instrument_ids=tuple(sorted(owned_ids)),
        )
        if not master_part.empty:
            source_master_parts.append(master_part)
        for selection in source.plan.selections:
            if selection.table is not MarketTable.DAILY_BARS:
                continue
            selection_ids = (
                set(selection.instrument_ids)
                if selection.instrument_ids else full_source_ids[snapshot_id]
            )
            selected_ids = tuple(sorted(selection_ids & owned_ids))
            if not selected_ids:
                continue
            selections.append(SourceSlice(
                selection.observation_id,
                selection.table,
                (
                    "revision-2 current-universe projection of immutable reconciled "
                    f"snapshot {snapshot_id}"
                ),
                selected_ids,
                selection.start_date,
                selection.end_date,
                selection.priority,
            ))
    current_master = _record_current_master_observation(
        warehouse,
        source_master=pd.concat(source_master_parts, ignore_index=True),
        official_master=target_frame.loc[
            target_frame["instrument_id"].astype(str).isin(included_ids)
        ].copy(),
        source_snapshot_ids=source_snapshot_ids,
        universe_manifest=universe_manifest,
        universe_as_of=universe_as_of,
        start_date=start_date,
        end_date=end_date,
        included_ids=included_ids,
    )
    selections.insert(0, SourceSlice(
        current_master.observation_id,
        MarketTable.INSTRUMENTS,
        "revision-2 reconciled official current membership and historical lifecycle master",
        included_ids,
    ))
    scope = UniverseScope(
        CURRENT_SH_SZ_STOCK_ETF_UNIVERSE,
        universe_as_of,
        start_date,
        end_date,
        survivorship_bias=True,
        instrument_ids=included_ids,
    )
    snapshot = warehouse.build_partitioned_snapshot(SnapshotPlan(
        tuple(selections),
        (
            f"Issue #7 revision-2 current SH/SZ research history "
            f"{start_date.isoformat()}..{end_date.isoformat()}"
        ),
        readiness=ReadinessProfile.RESEARCH_PRICE,
        universe_scope=scope,
    ))
    published = False
    if publish and snapshot.quality.ready:
        warehouse.publish(snapshot.snapshot_id)
        published = True
    status = (
        "complete" if snapshot.quality.ready and not missing_ids
        else "ready_scoped" if snapshot.quality.ready
        else "incomplete"
    )
    report_payload = {
        "decision_source": "https://github.com/zhan7653/miniQMT/issues/7",
        "decision_revision": 2,
        "kind": "current_research_projection",
        "status": status,
        "source_snapshot_id": source_snapshot_id,
        "source_snapshot_ids": source_snapshot_ids,
        "universe_observation_id": universe_observation_id,
        "universe_as_of": universe_as_of,
        "history_start": start_date,
        "history_end": end_date,
        "target_instrument_ids": tuple(sorted(target_ids)),
        "target_universe_hash": stable_digest(tuple(sorted(target_ids))),
        "included_instrument_ids": included_ids,
        "missing_instrument_ids": missing_ids,
        "excluded_source_instrument_ids": excluded_source_ids,
        "snapshot_id": snapshot.snapshot_id,
        "snapshot_quality": snapshot.quality,
        "published": published,
        "survivorship_bias": True,
    }
    root = Path(report_root).resolve()
    root.mkdir(parents=True, exist_ok=True)
    report_hash = stable_digest(report_payload)[:16]
    report = root / f"current-research-{snapshot.snapshot_id}-{report_hash}.json"
    primitive = to_primitive(report_payload)
    if report.exists():
        if json.loads(report.read_text(encoding="utf-8")) != primitive:
            raise ValueError(f"Immutable current-research report collision: {report}")
    else:
        report.write_text(canonical_json(report_payload), encoding="utf-8", newline="\n")
    return CurrentResearchResult(
        status,
        source_snapshot_id,
        source_snapshot_ids,
        snapshot.snapshot_id,
        len(target_ids),
        len(included_ids),
        missing_ids,
        excluded_source_ids,
        published,
        report,
    )


def _record_current_master_observation(
    warehouse: MarketDataWarehouse,
    *,
    source_master: pd.DataFrame,
    official_master: pd.DataFrame,
    source_snapshot_ids: tuple[str, ...],
    universe_manifest: ObservationManifest,
    universe_as_of: date,
    start_date: date,
    end_date: date,
    included_ids: tuple[str, ...],
) -> ObservationManifest:
    """Fuse official membership/classification with reconciled lifecycle metadata."""

    expected = set(included_ids)
    if (
        set(map(str, source_master["instrument_id"])) != expected
        or set(map(str, official_master["instrument_id"])) != expected
    ):
        raise ValueError("Current master inputs do not match the included universe")
    source = source_master.drop_duplicates("instrument_id").set_index("instrument_id")
    official = official_master.drop_duplicates("instrument_id").set_index("instrument_id")
    rows: list[dict[str, Any]] = []
    official_fields = (
        "name", "listed_date", "board", "exchange_product_class",
    )
    for instrument_id in included_ids:
        base = source.loc[instrument_id].to_dict()
        published = official.loc[instrument_id].to_dict()
        base["instrument_id"] = instrument_id
        for field in ("exchange", "local_code", "asset_type"):
            if str(base[field]) != str(published[field]):
                raise ValueError(f"Official/source master conflict: {instrument_id}/{field}")
        for field in official_fields:
            value = published.get(field)
            if value is not None and not pd.isna(value) and str(value).strip():
                base[field] = value
        base["delisted_date"] = pd.NA
        base["field_lineage"] = canonical_json({
            "kind": "current_master_reconciliation_r2_v2",
            "official_membership_observation_id": universe_manifest.observation_id,
            "historical_master_observation_id": base.get("source_observation_id"),
            "official_fields": official_fields,
            "historical_fields": (
                "buy_lot", "price_tick", "sell_delay_sessions", "price_limit_ratio",
            ),
        })
        base["source_payload"] = canonical_json({
            "official_membership_payload": published.get("source_payload"),
            "historical_master_payload": base.get("source_payload"),
        })
        rows.append({
            key: base.get(key)
            for key in BUSINESS_SCHEMAS[MarketTable.INSTRUMENTS]
        })
    frame = pd.DataFrame(rows)
    observed_at = max(
        (universe_manifest.observed_at, *(item.created_at for item in (
            warehouse.load_snapshot(snapshot_id) for snapshot_id in source_snapshot_ids
        ))),
    )
    evidence_ids = tuple(sorted({
        universe_manifest.observation_id,
        *map(str, source_master["source_observation_id"].dropna().unique()),
    }))
    partition_quality = {
        "validated": True,
        "readiness": ReadinessProfile.RESEARCH_PRICE.value,
        "instrument_ids": included_ids,
        "start_date": start_date,
        "end_date": end_date,
        "row_count": len(frame),
        "universe_definition": CURRENT_SH_SZ_STOCK_ETF_UNIVERSE,
        "universe_as_of": universe_as_of,
        "source_observation_ids": evidence_ids,
    }
    return warehouse.record_observation(ObservationPayload(
        "canonical-current-master-r2-v2",
        observed_at,
        ProviderRequest(
            ProviderCapability.CANONICAL_RECONCILIATION,
            start_date,
            end_date,
            included_ids,
            {
                "source_snapshot_ids": source_snapshot_ids,
                "universe_observation_id": universe_manifest.observation_id,
                "policy_version": "current-master-r2-v2",
            },
        ),
        {MarketTable.INSTRUMENTS: frame},
        (CoverageClaim(
            MarketTable.INSTRUMENTS,
            True,
            instrument_ids=included_ids,
            detail="Official exact membership/classification plus reconciled historical lifecycle",
        ),),
        {
            "kind": "field_level_reconciliation",
            "reconciliation_ready": True,
            "policy": {"version": "current-master-r2-v2"},
            "report": {"blockers": (), "unresolved_conflicts": ()},
            "partition_quality": partition_quality,
        },
    ))


def _validate_explicit_history_universe(
    universe: ObservationManifest,
    frame: pd.DataFrame,
    spec: HistoryBuildSpec,
) -> None:
    """Accept only a complete official current-universe proof for new listings."""

    if universe.provider != "exchange-public":
        raise ValueError(
            "Explicit history universe override must be an exchange-public observation"
        )
    if universe.request.capability is not ProviderCapability.INSTRUMENTS:
        raise ValueError(
            "Explicit history universe observation must carry instrument metadata"
        )
    if universe.request.instrument_ids:
        raise ValueError(
            "Explicit history universe observation must cover the full official request"
        )

    expected_exchanges = {"SH", "SZ"}
    expected_assets = {"stock", "etf"}
    request_exchanges = {
        str(item).upper()
        for item in universe.request.parameters.get("exchanges", ())
    }
    request_assets = {
        str(item).lower()
        for item in universe.request.parameters.get("asset_types", ())
    }
    if request_exchanges != expected_exchanges or request_assets != expected_assets:
        raise ValueError(
            "Explicit history universe observation must request the complete SH/SZ "
            "stock/ETF scope"
        )

    declared_as_of = universe.request.parameters.get("as_of_date")
    source_as_of = universe.source_metadata.get("as_of_date")
    if (
        str(declared_as_of)[:10] != spec.universe_as_of.isoformat()
        or str(source_as_of)[:10] != spec.universe_as_of.isoformat()
    ):
        raise ValueError(
            "Explicit history universe observation as_of_date does not match the "
            f"build scope: request={declared_as_of!r} source={source_as_of!r} "
            f"expected={spec.universe_as_of.isoformat()!r}"
        )
    requested_scope = universe.source_metadata.get("requested_scope")
    if not isinstance(requested_scope, Mapping):
        raise ValueError(
            "Explicit history universe observation has no source requested_scope"
        )
    source_exchanges = {
        str(item).upper() for item in requested_scope.get("exchanges", ())
    }
    source_assets = {
        str(item).lower() for item in requested_scope.get("asset_types", ())
    }
    if source_exchanges != expected_exchanges or source_assets != expected_assets:
        raise ValueError(
            "Explicit history universe source metadata does not prove the complete "
            "SH/SZ stock/ETF scope"
        )
    if str(universe.source_metadata.get("backend_group")) != "exchange-public":
        raise ValueError(
            "Explicit history universe observation has an unexpected backend group"
        )
    expected_endpoints = {
        "sse-main-stock-list",
        "sse-star-stock-list",
        "szse-a-stock-list",
        "sse-etf-scale-list",
        "sse-current-full-etf-list",
        "szse-etf-scale-daily",
        "szse-current-etf-list",
    }
    endpoint_counts = universe.source_metadata.get("endpoint_counts")
    response_hashes = universe.source_metadata.get("response_sha256")
    if (
        not isinstance(endpoint_counts, Mapping)
        or set(map(str, endpoint_counts)) != expected_endpoints
        or any(
            not isinstance(value, int) or isinstance(value, bool) or value <= 0
            for value in endpoint_counts.values()
        )
        or not isinstance(response_hashes, Mapping)
        or set(map(str, response_hashes)) != expected_endpoints
        or any(not str(value).strip() for value in response_hashes.values())
    ):
        raise ValueError(
            "Explicit history universe source metadata does not prove every official "
            "SH/SZ stock/ETF endpoint completed"
        )

    required_columns = {
        "instrument_id", "exchange", "local_code", "asset_type", "name",
        "currency", "listed_date", "board", "buy_lot", "price_tick",
    }
    missing_columns = sorted(required_columns - set(frame.columns))
    if missing_columns:
        raise ValueError(
            "Explicit history universe table misses required metadata columns: "
            + ",".join(missing_columns)
        )
    table_ids = tuple(map(str, frame["instrument_id"]))
    if not table_ids or len(table_ids) != len(set(table_ids)):
        raise ValueError(
            "Explicit history universe table is empty or has duplicate instruments"
        )
    if (
        not set(map(str, frame["exchange"])) <= expected_exchanges
        or not set(map(str, frame["asset_type"])) <= expected_assets
    ):
        raise ValueError(
            "Explicit history universe table contains instruments outside SH/SZ stock/ETF"
        )
    category_counts = {
        (exchange, asset_type): int(len(group))
        for (exchange, asset_type), group in frame.groupby(
            ["exchange", "asset_type"], dropna=False,
        )
    }
    expected_category_counts = {
        ("SH", "stock"): (
            endpoint_counts["sse-main-stock-list"]
            + endpoint_counts["sse-star-stock-list"]
        ),
        ("SZ", "stock"): endpoint_counts["szse-a-stock-list"],
        ("SH", "etf"): endpoint_counts["sse-current-full-etf-list"],
        ("SZ", "etf"): endpoint_counts["szse-etf-scale-daily"],
    }
    if category_counts != expected_category_counts:
        raise ValueError(
            "Explicit history universe table row counts do not match the completed "
            "official endpoint counts"
        )
    covered = tuple(
        claim for claim in universe.coverage
        if claim.table is MarketTable.INSTRUMENTS and claim.complete
    )
    if len(covered) != 1 or set(covered[0].instrument_ids) != set(table_ids):
        raise ValueError(
            "Explicit history universe rows do not match one exact complete coverage claim"
        )

    requested = set(spec.instrument_ids)
    if not requested:
        raise ValueError(
            "Explicit history universe override requires an exact non-empty instrument scope"
        )
    selected = frame.loc[frame["instrument_id"].astype(str).isin(requested)].copy()
    if set(map(str, selected["instrument_id"])) != requested:
        raise ValueError(
            "Explicit history universe observation does not contain every requested instrument"
        )
    if (
        not set(map(str, selected["exchange"])) <= set(spec.exchanges)
        or not set(map(str, selected["asset_type"])) <= set(spec.asset_types)
    ):
        raise ValueError(
            "Explicit history universe instruments fall outside the build categories"
        )

    invalid_fields: dict[str, tuple[str, ...]] = {}
    invalid_dates: dict[str, str] = {}
    for row in selected.to_dict("records"):
        instrument_id = str(row["instrument_id"])
        missing = tuple(
            field for field in sorted(required_columns - {"instrument_id"})
            if row.get(field) is None
            or bool(pd.isna(row.get(field)))
            or not str(row.get(field)).strip()
        )
        if missing:
            invalid_fields[instrument_id] = missing
            continue
        try:
            listed = date.fromisoformat(str(row["listed_date"])[:10])
        except ValueError:
            invalid_dates[instrument_id] = str(row["listed_date"])
            continue
        if not spec.start_date <= listed <= spec.end_date:
            invalid_dates[instrument_id] = listed.isoformat()
    if invalid_fields or invalid_dates:
        raise ValueError(
            "Explicit history universe instruments are not complete new listings inside "
            f"{spec.start_date.isoformat()}..{spec.end_date.isoformat()}: "
            f"missing={invalid_fields!r} listed_dates={invalid_dates!r}"
        )


def _select_universe(
    frame: pd.DataFrame,
    spec: HistoryBuildSpec,
) -> tuple[pd.DataFrame, tuple[str, ...]]:
    selected = frame.copy()
    selected = selected.loc[
        selected["exchange"].isin(set(spec.exchanges) & {"SH", "SZ"})
        & selected["asset_type"].isin(spec.asset_types)
        & selected["listed_date"].notna()
        & selected["listed_date"].le(spec.universe_as_of.isoformat())
        & (
            selected["delisted_date"].isna()
            | selected["delisted_date"].gt(spec.universe_as_of.isoformat())
        )
    ].copy()
    missing: tuple[str, ...] = ()
    if spec.instrument_ids:
        available = set(map(str, selected["instrument_id"]))
        missing = tuple(sorted(set(spec.instrument_ids) - available))
        selected = selected.loc[selected["instrument_id"].isin(spec.instrument_ids)]
    category = (
        selected["delisted_date"].notna().astype(int) * 2
        + selected["asset_type"].eq("etf").astype(int)
    )
    selected = (
        selected.assign(_history_order=category)
        .sort_values(["_history_order", "instrument_id"], kind="stable")
        .drop(columns=["_history_order"])
        .reset_index(drop=True)
    )
    if spec.max_instruments is not None:
        selected = selected.head(spec.max_instruments).reset_index(drop=True)
    return selected, missing


def _universe_scope(
    spec: HistoryBuildSpec,
    instrument_ids: tuple[str, ...],
) -> UniverseScope:
    assert spec.universe_as_of is not None
    return UniverseScope(
        CURRENT_SH_SZ_STOCK_ETF_UNIVERSE,
        spec.universe_as_of,
        spec.start_date,
        spec.end_date,
        survivorship_bias=True,
        instrument_ids=instrument_ids,
    )


def _safe_suspension_keys(
    frames: tuple[pd.DataFrame, ...],
    keys: list[str],
) -> set[tuple[str, str, str]]:
    """Return explicit suspension keys safely represented by every present source.

    Some price-only feeds retain a suspended session as a flat, zero-turnover bar but
    cannot mark it as suspended.  A key is removable from the research-price scope
    only when at least one source explicitly marks it suspended and every source row
    present for that key is either explicit suspension evidence or a flat zero-volume
    representation.  Any real turnover or non-flat OHLC keeps the key in the normal
    mismatch/conflict path.
    """

    candidates: set[tuple[str, str, str]] = set()
    for frame in frames:
        suspended = frame["suspended"].fillna(False).astype(bool)
        candidates.update(
            tuple(map(str, values))
            for values in frame.loc[suspended, keys].itertuples(index=False, name=None)
        )
    if not candidates:
        return set()

    safe = set(candidates)
    for frame in frames:
        index = pd.MultiIndex.from_frame(frame[keys])
        relevant = frame.loc[index.isin(candidates)].copy()
        if relevant.empty:
            continue
        explicit = relevant["suspended"].fillna(False).astype(bool)
        prices = relevant[["open", "high", "low", "close"]].apply(
            pd.to_numeric, errors="coerce",
        )
        volume = pd.to_numeric(relevant["volume"], errors="coerce")
        flat_zero = (
            prices.notna().all(axis=1)
            & prices.max(axis=1).sub(prices.min(axis=1)).abs().le(1e-12)
            & volume.notna()
            & volume.abs().le(1e-9)
        )
        unsafe = relevant.loc[~(explicit | flat_zero), keys]
        safe.difference_update(
            tuple(map(str, values))
            for values in unsafe.itertuples(index=False, name=None)
        )
    return safe


def _independent_consensus_frame(
    frames: Mapping[str, pd.DataFrame],
    backends: Mapping[str, str],
    policy: ReconciliationPolicy,
) -> tuple[pd.DataFrame, Mapping[str, tuple[str, ...]]]:
    """Build a row-level consensus without allowing provider priority to decide conflicts.

    A key and every critical value need an agreeing cluster backed by at least two
    distinct backend groups.  A tolerance cluster only identifies plausible peers;
    it may not be averaged.  The published value must be an actual provider value
    from one unique largest market-normalized exact-value subcluster backed by at
    least two independent backends.  Price normalization is far below the minimum
    exchange tick; volume normalization reflects the adapters' documented 100-share
    lot resolution.  Otherwise the critical field remains unresolved.
    """

    keys = ("instrument_id", "session_date", "price_mode")
    critical = ("open", "high", "low", "close", "volume")
    optional = (
        "amount", "suspended", "price_limit_state", "previous_close",
        "price_limit_ratio", "limit_up", "limit_down",
    )
    indexed = {
        provider: frame.set_index(list(keys), drop=False)
        for provider, frame in frames.items()
    }
    all_keys = sorted(set().union(*(set(frame.index) for frame in indexed.values())))
    rows: list[dict[str, Any]] = []
    reasons: dict[str, list[str]] = {}

    def exact_key(value: float, field_name: str) -> float:
        if field_name in {"open", "high", "low", "close"}:
            # xtquant frames commonly carry float32 representation noise while
            # HTTP sources decode the same exchange price as float64.  Six decimal
            # places remain three orders of magnitude below the 0.001 ETF tick.
            return round(value, 6)
        if field_name == "volume":
            # TickFlow and xtquant expose lot-rounded volume; BaoStock can retain
            # odd-lot shares.  Compare at the declared common 100-share resolution.
            # Upstream lot conversion uses conventional half-up rounding, whereas
            # Python's round() would send an exact 50-share tie to the even lot.
            return float(((int(value) + 50) // 100) * 100)
        return value

    def agreeing_cluster(
        values: Mapping[str, float], field_name: str,
    ) -> tuple[str, ...]:
        rule = policy.rule(MarketTable.DAILY_BARS, field_name)
        candidates: list[tuple[int, float, tuple[str, ...]]] = []
        names = tuple(sorted(values))
        for size in range(len(names), 1, -1):
            for members in combinations(names, size):
                if len({backends[name] for name in members}) != size:
                    continue
                numbers = [values[name] for name in members]
                agrees = all(
                    abs(left - right) <= (
                        rule.absolute_tolerance
                        + rule.relative_tolerance * max(abs(left), abs(right))
                    )
                    for left, right in combinations(numbers, 2)
                )
                if agrees:
                    scale = max(max(map(abs, numbers)), 1.0)
                    candidates.append((-size, (max(numbers) - min(numbers)) / scale, members))
            if candidates:
                break
        return min(candidates)[2] if candidates else ()

    for key in all_keys:
        present = {
            provider: frame.loc[key]
            for provider, frame in indexed.items()
            if key in frame.index
        }
        instrument_id = str(key[0])
        item_reasons = reasons.setdefault(instrument_id, [])
        if len({backends[name] for name in present}) < 2:
            item_reasons.append("source_session_keys_disagree")
            continue
        field_clusters: dict[str, tuple[str, ...]] = {}
        field_sources: dict[str, tuple[str, ...]] = {}
        field_selected_provider: dict[str, str] = {}
        output: dict[str, Any] = dict(zip(keys, map(str, key), strict=True))
        participation = {provider: 0 for provider in present}
        failed = False
        for field_name in critical:
            values = {
                provider: float(value)
                for provider, row in present.items()
                if not pd.isna(value := pd.to_numeric(row[field_name], errors="coerce"))
            }
            members = agreeing_cluster(values, field_name)
            if not members:
                item_reasons.append(f"critical_conflict:{field_name}")
                failed = True
                continue
            exact_groups: dict[float, list[str]] = {}
            for provider in members:
                exact_groups.setdefault(
                    exact_key(values[provider], field_name), []
                ).append(provider)
            largest = max(map(len, exact_groups.values()))
            winners = [
                (value, tuple(sorted(providers)))
                for value, providers in exact_groups.items()
                if len(providers) == largest and len({backends[name] for name in providers}) >= 2
            ]
            if len(winners) != 1:
                item_reasons.append(f"critical_conflict:no_unique_exact_value:{field_name}")
                failed = True
                continue
            normalized_value, selected_providers = winners[0]
            selected_provider = min(
                selected_providers,
                key=lambda provider: (
                    abs(values[provider] - normalized_value),
                    values[provider],
                    provider,
                ),
            )
            selected_value = values[selected_provider]
            field_clusters[field_name] = members
            field_sources[field_name] = selected_providers
            field_selected_provider[field_name] = selected_provider
            for provider in selected_providers:
                participation[provider] += 1
            output[field_name] = selected_value
        if failed:
            continue
        preferred = tuple(sorted(
            present,
            key=lambda provider: (-participation[provider], provider),
        ))
        optional_field_selected_provider: dict[str, str] = {}
        for field_name in optional:
            output[field_name] = pd.NA
            for provider in preferred:
                value = present[provider][field_name]
                if not pd.isna(value):
                    output[field_name] = value
                    optional_field_selected_provider[field_name] = provider
                    break
        output["suspended"] = False
        optional_field_selected_provider["suspended"] = "canonical-active-session-filter"
        output["source_payload"] = canonical_json({
            "kind": "independent-source-consensus",
            "present_providers": tuple(sorted(present)),
            "field_clusters": field_clusters,
            "field_sources": field_sources,
            "field_selected_provider": field_selected_provider,
            "optional_field_selected_provider": optional_field_selected_provider,
        })
        rows.append(output)

    unresolved = _frozen_reasons(reasons)
    failed_ids = set(unresolved)
    columns = [*keys, *critical, *optional, "source_payload"]
    consensus = pd.DataFrame(rows, columns=columns)
    if not consensus.empty and failed_ids:
        consensus = consensus.loc[
            ~consensus["instrument_id"].astype(str).isin(failed_ids)
        ].reset_index(drop=True)
    return consensus, unresolved


def _cohort_id(
    spec: HistoryBuildSpec,
    universe_observation_id: str,
    source_pair: tuple[str, str],
    policy_version: str,
    adjudicator_provider: str | None = None,
) -> str:
    return "cohort-" + stable_digest({
        "schema_version": HISTORY_BUILD_SCHEMA_VERSION,
        "start_date": spec.start_date,
        "end_date": spec.end_date,
        "instrument_ids": spec.instrument_ids,
        "exchanges": spec.exchanges,
        "asset_types": spec.asset_types,
        "max_instruments": spec.max_instruments,
        "source_pair": source_pair,
        "adjudicator_provider": adjudicator_provider,
        "policy_version": policy_version,
        "universe_observation_id": universe_observation_id,
    })[:24]


def _chunks(values: tuple[str, ...], size: int):
    for index in range(0, len(values), size):
        yield values[index:index + size]


def _frozen_reasons(values: Mapping[str, list[str]]) -> Mapping[str, tuple[str, ...]]:
    return MappingProxyType({
        key: tuple(sorted(set(reasons)))
        for key, reasons in sorted(values.items())
        if reasons
    })


def _read_checkpoint(path: Path, build_id: str) -> dict[str, Any]:
    if not path.exists():
        return {"build_id": build_id, "batches": {}}
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or payload.get("build_id") != build_id:
        raise ValueError(f"History checkpoint identity mismatch: {path}")
    return payload


def _write_atomic(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(
        f".{path.name}.{os.getpid()}.{uuid4().hex}.tmp"
    )
    try:
        temporary.write_text(canonical_json(payload), encoding="utf-8", newline="\n")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


@contextmanager
def _exclusive_build_lock(path: Path) -> Iterator[None]:
    """Hold a non-blocking process lock for one exact history-build identity."""

    resolved = path.resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    with _HELD_BUILD_LOCKS_GUARD:
        if resolved in _HELD_BUILD_LOCKS:
            raise RuntimeError(f"Identical history build is already running: {path.stem}")
        _HELD_BUILD_LOCKS.add(resolved)
    handle = path.open("a+b")
    locked = False
    try:
        handle.seek(0, os.SEEK_END)
        if handle.tell() == 0:
            handle.write(b"\0")
            handle.flush()
        handle.seek(0)
        try:
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            raise RuntimeError(
                f"Identical history build is already running: {path.stem}"
            ) from exc
        locked = True
        yield
    finally:
        if locked:
            handle.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        handle.close()
        with _HELD_BUILD_LOCKS_GUARD:
            _HELD_BUILD_LOCKS.discard(resolved)
