from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
import json
import os
from pathlib import Path
from typing import Any, Mapping

import pandas as pd

from fundlab.common.canonical import canonical_json, stable_digest, to_primitive
from fundlab.marketdata.contracts import (
    CURRENT_SH_SZ_STOCK_ETF_UNIVERSE,
    CorporateActionType,
    CoverageClaim,
    DATA_GAP_QUARANTINE_RULE_ID,
    MarketTable,
    ObservationManifest,
    ObservationPayload,
    ProviderCapability,
    ProviderRequest,
    ReadinessProfile,
    SIMULATION_PARTITION_VALIDATOR_VERSION,
    SnapshotManifest,
    SnapshotNotReadyError,
    UniverseScope,
)
from fundlab.marketdata.providers import ProviderRegistry
from fundlab.marketdata.reconciliation import default_reconciliation_policy
from fundlab.marketdata.schema import empty_table, validate_snapshot_tables
from fundlab.marketdata.sources import default_provider_registry
from fundlab.marketdata.sources.eastmoney_fund import EASTMONEY_ETF_ACTION_POLICY
from fundlab.marketdata.etf_rules import EtfRuleEvidenceBuilder
from fundlab.marketdata.trade_rules import (
    apply_corroborated_historical_limit_exceptions,
    audit_provider_price_limits,
    materialize_daily_trade_rules,
    materialize_order_quantity_rules,
)
from fundlab.marketdata.warehouse import MarketDataWarehouse


SIMULATION_CANONICAL_PROVIDER = "canonical-simulation-r2-v1"
SIMULATION_STATUS_CANONICAL_PROVIDER = "canonical-simulation-status-r2-v1"
ETF_ACTION_CANONICAL_PROVIDER = "canonical-etf-actions-r2-v5"
STOCK_ACTION_CANONICAL_PROVIDER = "canonical-stock-actions-r2-v1"
_JSON_MISSING = "\u0000"


@dataclass(frozen=True)
class SimulationBuildResult:
    snapshot_id: str
    ready: bool
    published: bool
    report: Path
    blockers: tuple[str, ...]


@dataclass(frozen=True)
class SimulationIncrementValidationResult:
    observation_id: str
    candidate_observation_id: str
    report: Path
    source_observation_ids: tuple[str, ...]


@dataclass(frozen=True)
class StatusCollectionSpec:
    source_snapshot_id: str
    calendar_observation_id: str
    provider_name: str = "baostock"
    batch_size: int = 50
    shard_count: int = 1
    shard_index: int = 0
    refresh: bool = False

    def __post_init__(self) -> None:
        if self.provider_name not in {"baostock", "xtquant"}:
            raise ValueError("Status provider must be baostock or xtquant")
        if self.batch_size < 1 or self.batch_size > 100:
            raise ValueError("Status batch_size must be between 1 and 100")
        if self.shard_count < 1 or self.shard_count > 16:
            raise ValueError("Status shard_count must be between 1 and 16")
        if self.shard_index < 0 or self.shard_index >= self.shard_count:
            raise ValueError("Status shard_index must be within shard_count")


@dataclass(frozen=True)
class StatusCollectionResult:
    status: str
    build_id: str
    requested_instruments: int
    completed_instruments: int
    observation_ids: tuple[str, ...]
    blockers: tuple[str, ...]
    unresolved_instrument_ids: tuple[str, ...]
    checkpoint: Path
    report: Path


@dataclass(frozen=True)
class EvidenceCollectionSpec:
    source_snapshot_id: str
    kind: str
    batch_size: int = 100
    refresh: bool = False
    predecessor_snapshot_id: str | None = None

    def __post_init__(self) -> None:
        if self.kind not in {"stock-actions", "etf-actions", "factors"}:
            raise ValueError("Evidence kind must be stock-actions, etf-actions, or factors")
        if self.batch_size < 1 or self.batch_size > 100:
            raise ValueError("Evidence batch_size must be between 1 and 100")
        if self.kind == "stock-actions" and not self.predecessor_snapshot_id:
            raise ValueError(
                "Incremental stock action evidence requires a predecessor snapshot"
            )


@dataclass(frozen=True)
class EvidenceCollectionResult:
    status: str
    build_id: str
    kind: str
    requested_instruments: int
    completed_instruments: int
    observation_ids: tuple[str, ...]
    blockers: tuple[str, ...]
    unresolved_instrument_ids: tuple[str, ...]
    checkpoint: Path
    report: Path


@dataclass(frozen=True)
class _ActionBatchCollectionResult:
    manifest: ObservationManifest | None
    unresolved_instrument_ids: tuple[str, ...]
    blocker: str | None = None


class SimulationEvidenceCollector:
    """Resumably collect action/factor evidence in immutable recovery batches."""

    def __init__(
        self,
        warehouse: MarketDataWarehouse,
        report_root: str | Path,
        *,
        registry: ProviderRegistry | None = None,
    ) -> None:
        self.warehouse = warehouse
        self.report_root = Path(report_root).resolve()
        self.registry = registry or default_provider_registry()

    def collect(
        self,
        spec: EvidenceCollectionSpec,
        *,
        loaded_snapshot: SnapshotManifest | None = None,
    ) -> EvidenceCollectionResult:
        from fundlab.marketdata.history import _exclusive_build_lock

        lock = self.warehouse.root / "builds" / ".locks" / (
            "evidence-" + stable_digest(spec)[:24] + ".lock"
        )
        with _exclusive_build_lock(lock):
            return self._collect_locked(spec, loaded_snapshot=loaded_snapshot)

    def _collect_locked(
        self,
        spec: EvidenceCollectionSpec,
        *,
        loaded_snapshot: SnapshotManifest | None,
    ) -> EvidenceCollectionResult:
        snapshot = loaded_snapshot or self.warehouse.load_snapshot(spec.source_snapshot_id)
        if snapshot.snapshot_id != spec.source_snapshot_id:
            raise ValueError("Loaded evidence snapshot does not match the collection spec")
        if snapshot.plan.readiness is not ReadinessProfile.RESEARCH_PRICE:
            raise ValueError("Evidence collection requires a research_price source snapshot")
        scope = snapshot.plan.universe_scope
        if scope is None or scope.definition != CURRENT_SH_SZ_STOCK_ETF_UNIVERSE:
            raise ValueError("Evidence collection requires an exact revision-2 universe scope")
        instruments = self.warehouse.query_loaded_snapshot_table(
            snapshot, MarketTable.INSTRUMENTS,
        ).sort_values("instrument_id", kind="stable").reset_index(drop=True)
        if set(map(str, instruments["instrument_id"])) != set(scope.instrument_ids):
            raise ValueError("Evidence source snapshot does not match its pinned universe")
        if spec.kind == "stock-actions":
            return self._collect_incremental_stock_actions(
                spec=spec,
                snapshot=snapshot,
                scope=scope,
                instruments=instruments,
            )

        provider, capability, table, asset_type = {
            "stock-actions": (
                "cninfo-public", ProviderCapability.CORPORATE_ACTIONS,
                MarketTable.CORPORATE_ACTIONS, "stock",
            ),
            "etf-actions": (
                "eastmoney-fund-public", ProviderCapability.CORPORATE_ACTIONS,
                MarketTable.CORPORATE_ACTIONS, "etf",
            ),
            "factors": (
                "xtquant", ProviderCapability.ADJUSTMENT_FACTORS,
                MarketTable.ADJUSTMENT_FACTORS, None,
            ),
        }[spec.kind]
        if asset_type is not None:
            instruments = instruments.loc[
                instruments["asset_type"].astype(str).eq(asset_type)
            ].reset_index(drop=True)
        instrument_ids = tuple(map(str, instruments["instrument_id"]))
        batches = tuple(_chunks(instrument_ids, spec.batch_size))
        collector_version = {
            "stock-actions": 2,
            "etf-actions": 6,
            "factors": 1,
        }[spec.kind]
        build_id = "evidence-" + stable_digest({
            "collector_version": collector_version,
            "source_snapshot_id": spec.source_snapshot_id,
            "kind": spec.kind,
            "provider": provider,
            "capability": capability,
            "batch_size": spec.batch_size,
        })[:24]
        checkpoint = self.warehouse.root / "builds" / build_id / "checkpoint.json"
        state = _read_json_if_present(checkpoint)
        completed: dict[str, str] = {
            str(key): str(value)
            for key, value in state.get("batches", {}).items()
        } if isinstance(state.get("batches", {}), Mapping) else {}
        observation_ids: list[str] = []
        blockers: list[str] = []

        indexed = instruments.set_index("instrument_id", drop=False)
        action_raw_observations: list[ObservationManifest] = []
        action_canonical_observations: list[ObservationManifest] = []
        if spec.kind.endswith("-actions"):
            canonical_provider = _action_canonical_provider(spec.kind)
            action_raw_observations.extend(self.warehouse.observations(provider=provider))
            action_canonical_observations.extend(
                self.warehouse.observations(provider=canonical_provider)
            )
        for batch in batches:
            batch_id = "batch-" + stable_digest(batch)[:16]
            parameters = _evidence_parameters(spec.kind, batch, indexed)
            request = ProviderRequest(
                capability, scope.history_start, scope.history_end, batch, parameters,
            )
            restored_id = None if spec.refresh else completed.get(batch_id)
            manifest: ObservationManifest | None = None
            if restored_id:
                try:
                    candidate = self.warehouse.load_observation(restored_id)
                    expected_request = (
                        _action_canonical_request(spec, scope, batch)
                        if spec.kind.endswith("-actions") else request
                    )
                    expected_provider = (
                        _action_canonical_provider(spec.kind)
                        if spec.kind.endswith("-actions") else provider
                    )
                    if candidate.provider == expected_provider and candidate.request == expected_request:
                        _require_complete_claim(candidate, table, request)
                        manifest = candidate
                except Exception:
                    manifest = None
            if manifest is None and not spec.refresh:
                if spec.kind.endswith("-actions"):
                    expected_request = _action_canonical_request(spec, scope, batch)
                    matches = tuple(
                        item for item in action_canonical_observations
                        if item.request == expected_request
                    )
                else:
                    matches = self.warehouse.matching_observations(
                        provider=provider, request=request,
                    )
                for candidate in reversed(matches):
                    try:
                        _require_complete_claim(candidate, table, request)
                    except Exception:
                        continue
                    manifest = candidate
                    break
            if manifest is None:
                try:
                    if spec.kind.endswith("-actions"):
                        action_batch = _collect_action_batch(
                            warehouse=self.warehouse,
                            registry=self.registry,
                            spec=spec,
                            scope=scope,
                            batch=batch,
                            instruments=indexed,
                            raw_observations=action_raw_observations,
                            reuse_existing=not spec.refresh,
                        )
                        candidate = action_batch.manifest
                        if candidate is not None:
                            action_canonical_observations.append(candidate)
                            observation_ids.append(candidate.observation_id)
                        if action_batch.blocker is not None:
                            blockers.append(f"{batch_id}:{action_batch.blocker}")
                        if action_batch.unresolved_instrument_ids:
                            continue
                        if candidate is None:
                            raise SnapshotNotReadyError(
                                f"{spec.kind} batch returned no canonical evidence"
                            )
                    else:
                        candidate = self.warehouse.record_observation(
                            self.registry.observe(provider, request)
                        )
                    _require_complete_claim(candidate, table, request)
                    manifest = candidate
                except Exception as exc:
                    blockers.append(f"{batch_id}:{type(exc).__name__}:{str(exc)[:240]}")
                    continue
            completed[batch_id] = manifest.observation_id
            observation_ids.append(manifest.observation_id)
            _write_atomic_json(checkpoint, {
                "schema_version": 2 if spec.kind.endswith("-actions") else 1,
                "build_id": build_id,
                "spec": spec,
                "universe_scope": scope,
                "provider": provider,
                "capability": capability,
                "table": table,
                "batches": dict(sorted(completed.items())),
            })

        completed_ids: set[str] = set()
        for observation_id in observation_ids:
            completed_ids.update(self.warehouse.load_observation(observation_id).request.instrument_ids)
        missing = set(instrument_ids) - completed_ids
        if missing:
            blockers.append(f"missing_{spec.kind}_instruments:{len(missing)}")
        status = "complete" if not blockers else "incomplete"
        report_payload = {
            "decision_source": "https://github.com/zhan7653/miniQMT/issues/7",
            "decision_revision": 2,
            "kind": f"simulation_{spec.kind}_collection",
            "status": status,
            "build_id": build_id,
            "spec": spec,
            "provider": provider,
            "canonical_provider": (
                _action_canonical_provider(spec.kind)
                if spec.kind.endswith("-actions") else None
            ),
            "capability": capability,
            "table": table,
            "universe_scope": scope,
            "requested_instrument_ids": instrument_ids,
            "completed_instrument_ids": tuple(sorted(completed_ids)),
            "unresolved_instrument_ids": tuple(sorted(missing)),
            "observation_ids": tuple(sorted(set(observation_ids))),
            "blockers": tuple(sorted(set(blockers))),
            "checkpoint": checkpoint,
        }
        self.report_root.mkdir(parents=True, exist_ok=True)
        report = self.report_root / (
            f"evidence-collection-{build_id}-{stable_digest(report_payload)[:16]}.json"
        )
        primitive = to_primitive(report_payload)
        if report.exists():
            if json.loads(report.read_text(encoding="utf-8")) != primitive:
                raise ValueError(f"Immutable evidence report collision: {report}")
        else:
            report.write_text(canonical_json(report_payload), encoding="utf-8", newline="\n")
        return EvidenceCollectionResult(
            status,
            build_id,
            spec.kind,
            len(instrument_ids),
            len(completed_ids & set(instrument_ids)),
            tuple(sorted(set(observation_ids))),
            tuple(sorted(set(blockers))),
            tuple(sorted(missing)),
            checkpoint,
            report,
        )

    def _collect_incremental_stock_actions(
        self,
        *,
        spec: EvidenceCollectionSpec,
        snapshot: SnapshotManifest,
        scope: UniverseScope,
        instruments: pd.DataFrame,
    ) -> EvidenceCollectionResult:
        """Use the disclosure index as the daily completeness boundary.

        The predecessor already owns every historical action.  A complete CNInfo
        announcement-index scan proves which stocks can have new evidence in this
        increment; only those stocks (plus durable pending disclosures) need the
        expensive per-symbol lifecycle endpoints.
        """

        if spec.predecessor_snapshot_id is None:
            raise ValueError("Stock action collection requires predecessor_snapshot_id")
        predecessor = self.warehouse.load_snapshot(spec.predecessor_snapshot_id)
        predecessor_scope = predecessor.plan.universe_scope
        if predecessor_scope is None:
            raise SnapshotNotReadyError("Predecessor snapshot has no universe scope")
        if scope.history_start != predecessor_scope.history_end + timedelta(days=1):
            raise SnapshotNotReadyError(
                "Stock action increment is not contiguous with its predecessor"
            )

        stocks = instruments.loc[
            instruments["asset_type"].astype(str).eq("stock")
        ].sort_values("instrument_id", kind="stable").reset_index(drop=True)
        stock_ids = tuple(map(str, stocks["instrument_id"]))
        stock_set = set(stock_ids)
        indexed = stocks.set_index("instrument_id", drop=False)

        index_root = self.warehouse.root / "indexes" / "cninfo-stock-actions"
        scan_start = scope.history_start - timedelta(days=1)
        scan_identity = {
            "policy": "cninfo-stock-action-announcement-index-v1",
            "start_date": scan_start,
            "end_date": scope.history_end,
        }
        scan_key = stable_digest(scan_identity)[:24]
        scan_path = index_root / "scans" / f"scan-{scan_key}.json"
        if scan_path.exists() and not spec.refresh:
            scan_payload = _read_json_if_present(scan_path)
            _require_complete_announcement_scan(
                scan_payload, start_date=scan_start, end_date=scope.history_end,
            )
        else:
            provider = self.registry.provider("cninfo-public")
            scanner = getattr(provider, "scan_announcements", None)
            if not callable(scanner):
                raise SnapshotNotReadyError(
                    "CNInfo provider does not expose the required announcement index"
                )
            try:
                scan = scanner(scan_start, scope.history_end)
            except Exception as exc:
                scan_audit = getattr(
                    provider, "last_announcement_scan_audit", {},
                )
                failure_payload = {
                    "schema_version": 1,
                    "policy": "cninfo-stock-action-announcement-scan-failure-v1",
                    "start_date": scan_start,
                    "end_date": scope.history_end,
                    "error": f"{type(exc).__name__}:{str(exc)[:1000]}",
                    "scan_audit": (
                        scan_audit if isinstance(scan_audit, Mapping) else {}
                    ),
                }
                failure_path = index_root / "failures" / (
                    f"failure-{stable_digest(failure_payload)[:24]}.json"
                )
                _write_immutable_json(failure_path, failure_payload)
                raise SnapshotNotReadyError(
                    "CNInfo announcement scan failed; immutable audit: "
                    f"{failure_path}"
                ) from exc
            scan_payload = to_primitive(scan)
            if not isinstance(scan_payload, Mapping):
                raise SnapshotNotReadyError("CNInfo announcement scan is not an object")
            _require_complete_announcement_scan(
                scan_payload, start_date=scan_start, end_date=scope.history_end,
            )
            _write_immutable_json(scan_path, scan_payload)

        scan_id = "scan-" + stable_digest(scan_payload)[:24]
        records = tuple(
            item for item in scan_payload.get("records", ())
            if isinstance(item, Mapping)
            and str(item.get("instrument_id", "")) in stock_set
        )
        affected_ids = tuple(sorted({str(item["instrument_id"]) for item in records}))
        ignored_announcements: dict[str, Mapping[str, Any]] = {}
        actionable_records: list[Mapping[str, Any]] = []
        for item in records:
            reason = _ignored_stock_action_announcement_reason(item)
            if reason is None:
                actionable_records.append(item)
                continue
            ignored_announcements[str(item["announcement_id"])] = {
                **dict(item),
                "reason": reason,
            }
        actionable_affected_ids = tuple(sorted({
            str(item["instrument_id"]) for item in actionable_records
        }))
        by_instrument: dict[str, list[Mapping[str, Any]]] = {}
        for item in actionable_records:
            by_instrument.setdefault(str(item["instrument_id"]), []).append(item)

        pending_path = index_root / "pending.json"
        pending_payload = _read_json_if_present(pending_path)
        pending: dict[str, Any] = {
            str(key): value
            for key, value in pending_payload.get("instruments", {}).items()
        } if isinstance(pending_payload.get("instruments", {}), Mapping) else {}
        candidate_ids = set(by_instrument) | set(pending)
        for instrument_id in tuple(sorted(candidate_ids)):
            items = by_instrument.get(instrument_id, ())
            existing = pending.get(instrument_id, {})
            old_records = existing.get("announcements", ()) if isinstance(existing, Mapping) else ()
            merged = {
                str(item["announcement_id"]): dict(item)
                for item in (*old_records, *items)
                if isinstance(item, Mapping) and item.get("announcement_id")
            }
            retained: dict[str, Mapping[str, Any]] = {}
            for announcement_id, announcement in merged.items():
                reason = _ignored_stock_action_announcement_reason(announcement)
                if reason is None:
                    retained[announcement_id] = announcement
                    continue
                ignored_announcements[announcement_id] = {
                    **announcement,
                    "reason": reason,
                }
            if not retained:
                pending.pop(instrument_id, None)
                continue
            pending[instrument_id] = {
                "first_seen_date": (
                    str(existing.get("first_seen_date"))
                    if isinstance(existing, Mapping) and existing.get("first_seen_date")
                    else scope.history_end.isoformat()
                ),
                "last_attempt_date": scope.history_end.isoformat(),
                "state": "awaiting_structured_detail",
                "announcements": tuple(
                    retained[key] for key in sorted(retained)
                ),
            }

        target_ids = tuple(sorted(
            (set(actionable_affected_ids) | set(pending)) & stock_set
        ))
        raw_observations = list(self.warehouse.observations(provider="cninfo-public"))
        selected: dict[str, ObservationManifest] = {}
        request_errors: dict[str, str] = {}
        invalid_details: dict[str, Any] = {}
        detail_request_attempts: list[Mapping[str, Any]] = []

        def absorb(manifest: ObservationManifest) -> None:
            if (
                manifest.provider != "cninfo-public"
                or manifest.request.capability is not ProviderCapability.CORPORATE_ACTIONS
                or manifest.request.end_date != scope.history_end
                or manifest.request.parameters.get("collection_mode")
                != "announcement-targeted-v2"
                or manifest.request.parameters.get("validation_start_date")
                != scope.history_start.isoformat()
            ):
                return
            hashes = manifest.source_metadata.get("response_sha256")
            errors = manifest.source_metadata.get("request_errors")
            invalid = manifest.source_metadata.get("invalid_lifecycle")
            if not isinstance(hashes, Mapping):
                return
            error_ids = set(errors) if isinstance(errors, Mapping) else set()
            invalid_ids = set(invalid) if isinstance(invalid, Mapping) else set()
            for instrument_id in manifest.request.instrument_ids:
                if instrument_id not in target_ids:
                    continue
                listed = _instrument_listed_date(indexed, instrument_id)
                if manifest.request.start_date is None or manifest.request.start_date > listed:
                    continue
                if instrument_id in invalid_ids:
                    invalid_details[instrument_id] = invalid[instrument_id]
                    continue
                if instrument_id in error_ids:
                    request_errors[instrument_id] = str(errors[instrument_id])
                    continue
                if instrument_id in hashes:
                    current = selected.get(instrument_id)
                    if current is None or manifest.observed_at > current.observed_at:
                        selected[instrument_id] = manifest

        if not spec.refresh:
            for manifest in raw_observations:
                absorb(manifest)
        reused_detail_ids = set(selected)

        recovery_errors: dict[str, str] = {}
        detail_requested_ids: set[str] = set()
        for round_index in range(2):
            unresolved = tuple(sorted(set(target_ids) - set(selected)))
            if not unresolved:
                break
            for batch in _chunks(unresolved, 10):
                detail_requested_ids.update(batch)
                start = min(_instrument_listed_date(indexed, item) for item in batch)
                request = ProviderRequest(
                    ProviderCapability.CORPORATE_ACTIONS,
                    start,
                    scope.history_end,
                    batch,
                    {
                        "max_workers": min(4, len(batch)),
                        "retries": 2,
                        "retry_backoff_seconds": 0.5,
                        "collection_mode": "announcement-targeted-v2",
                        "validation_start_date": scope.history_start.isoformat(),
                        "announcement_dates_by_instrument": {
                            instrument_id: tuple(sorted({
                                str(item.get("announcement_time", ""))[:10]
                                for item in pending.get(instrument_id, {}).get(
                                    "announcements", ()
                                )
                                if isinstance(item, Mapping)
                                and len(str(item.get("announcement_time", ""))) >= 10
                            }))
                            for instrument_id in batch
                        },
                    },
                )
                try:
                    manifest = self.warehouse.record_observation(
                        self.registry.observe("cninfo-public", request)
                    )
                except Exception as exc:
                    detail = f"{type(exc).__name__}:{str(exc)[:240]}"
                    detail_request_attempts.append({
                        "round": round_index + 1,
                        "instrument_ids": batch,
                        "status": "error",
                        "error": detail,
                    })
                    for instrument_id in batch:
                        recovery_errors[instrument_id] = detail
                    continue
                raw_observations.append(manifest)
                absorb(manifest)
                detail_request_attempts.append({
                    "round": round_index + 1,
                    "instrument_ids": batch,
                    "status": "observed",
                    "observation_id": manifest.observation_id,
                })

        selected_ids = set(selected)
        detail_pieces: list[pd.DataFrame] = []
        input_ids = tuple(sorted({item.observation_id for item in selected.values()}))
        per_instrument_evidence: dict[str, Any] = {}
        known_pending: dict[str, Any] = {}
        for observation_id in input_ids:
            frame = self.warehouse.read_observation_table(
                observation_id, MarketTable.CORPORATE_ACTIONS,
            )
            owned = {
                instrument_id for instrument_id, manifest in selected.items()
                if manifest.observation_id == observation_id
            }
            frame = frame.loc[frame["instrument_id"].astype(str).isin(owned)].copy()
            frame["source_observation_id"] = observation_id
            detail_pieces.append(frame)
            manifest = self.warehouse.load_observation(observation_id)
            hashes = manifest.source_metadata.get("response_sha256", {})
            future = manifest.source_metadata.get("known_pending_after_cutoff", {})
            for instrument_id in owned:
                per_instrument_evidence[instrument_id] = {
                    "observation_id": observation_id,
                    "response_sha256": hashes.get(instrument_id),
                }
                if isinstance(future, Mapping) and instrument_id in future:
                    known_pending[instrument_id] = future[instrument_id]

        detailed_actions = (
            pd.concat(detail_pieces, ignore_index=True).reset_index(drop=True)
            if detail_pieces else empty_table(
                MarketTable.CORPORATE_ACTIONS, include_lineage=True,
            )
        )
        detailed_actions, _ = _collapse_same_lifecycle_cash_components(
            detailed_actions,
        )
        predecessor_actions = (
            self.warehouse.query_loaded_snapshot_table(
                predecessor,
                MarketTable.CORPORATE_ACTIONS,
                instrument_ids=tuple(sorted(selected_ids)),
                start_date=predecessor_scope.history_start,
                end_date=predecessor_scope.history_end,
            )
            if selected_ids else empty_table(
                MarketTable.CORPORATE_ACTIONS, include_lineage=True,
            )
        )
        historical_corrections = _historical_action_corrections(
            predecessor_actions=predecessor_actions,
            current_actions=detailed_actions,
            instrument_ids=tuple(sorted(selected_ids)),
            history_start=predecessor_scope.history_start,
            history_end=predecessor_scope.history_end,
        )

        current_actions = detailed_actions.loc[
            detailed_actions["ex_date"].astype(str).between(
                scope.history_start.isoformat(), scope.history_end.isoformat(),
            )
        ].copy().reset_index(drop=True)
        awaiting: set[str] = set()
        for instrument_id in tuple(sorted(set(pending) & stock_set)):
            item = pending.get(instrument_id, {})
            announcements = tuple(
                announcement for announcement in item.get("announcements", ())
                if isinstance(announcement, Mapping)
            ) if isinstance(item, Mapping) else ()
            if instrument_id not in selected_ids:
                awaiting.add(instrument_id)
                continue
            if instrument_id in historical_corrections:
                continue
            instrument_actions = current_actions.loc[
                current_actions["instrument_id"].astype(str).eq(instrument_id)
            ]
            future_actions = known_pending.get(instrument_id, ())
            remaining: list[Mapping[str, Any]] = []
            has_known_future = False
            for announcement in announcements:
                if _announcement_matches_action_evidence(
                    announcement, instrument_actions,
                ):
                    continue
                if _announcement_matches_action_evidence(
                    announcement, future_actions,
                ):
                    remaining.append({**announcement, "state": "known_future_event"})
                    has_known_future = True
                    continue
                if _is_correction_announcement(announcement):
                    confirmations = set(map(
                        str, announcement.get("confirmation_observation_ids", ()),
                    ))
                    confirmations.add(selected[instrument_id].observation_id)
                    if len(confirmations) >= 2:
                        continue
                    remaining.append({
                        **announcement,
                        "state": "confirming_no_action",
                        "confirmation_observation_ids": tuple(sorted(confirmations)),
                    })
                    continue
                remaining.append({
                    **announcement,
                    "state": "awaiting_structured_detail",
                })
            if not remaining:
                pending.pop(instrument_id, None)
                continue
            pending[instrument_id] = {
                **item,
                "last_attempt_date": scope.history_end.isoformat(),
                "state": (
                    "awaiting_structured_detail"
                    if any(
                        str(value.get("state")) == "awaiting_structured_detail"
                        for value in remaining
                    )
                    else "known_future_event"
                    if has_known_future and all(
                        str(value.get("state")) == "known_future_event"
                        for value in remaining
                    )
                    else "confirming_no_action"
                ),
                "announcements": tuple(remaining),
                "known_pending_after_cutoff": future_actions,
            }
            if any(
                str(value.get("state")) == "awaiting_structured_detail"
                for value in remaining
            ):
                awaiting.add(instrument_id)

        unresolved = set(target_ids) - selected_ids
        unresolved.update(awaiting)
        for instrument_id in unresolved:
            if instrument_id in invalid_details:
                continue
            if instrument_id in recovery_errors:
                request_errors[instrument_id] = recovery_errors[instrument_id]
            elif instrument_id in awaiting:
                announcement_ids = tuple(
                    str(item.get("announcement_id"))
                    for item in pending.get(instrument_id, {}).get("announcements", ())
                    if isinstance(item, Mapping)
                    and str(item.get("state")) == "awaiting_structured_detail"
                ) if isinstance(pending.get(instrument_id), Mapping) else ()
                request_errors[instrument_id] = (
                    "PendingAnnouncement:structured lifecycle not available for "
                    + ",".join(filter(None, announcement_ids))
                )
            else:
                request_errors[instrument_id] = "ObservationError:Source request failed: no evidence"

        _write_atomic_json(pending_path, {
            "schema_version": 3,
            "policy": "cninfo-stock-action-pending-v3",
            "updated_for": scope.history_end,
            "instruments": dict(sorted(pending.items())),
        })
        watermark_path = index_root / "watermark.json"
        existing_watermark = _read_json_if_present(watermark_path)
        existing_end = str(existing_watermark.get("last_successful_end_date", ""))
        if not existing_end or existing_end <= scope.history_end.isoformat():
            _write_atomic_json(watermark_path, {
                "schema_version": 1,
                "policy": "cninfo-stock-action-announcement-index-v1",
                "last_successful_end_date": scope.history_end,
                "scan_id": scan_id,
                "scan_path": scan_path,
            })

        blockers: list[str] = []
        if historical_corrections:
            blockers.append(
                "HistoricalActionCorrectionError:exact historical action differences detected: "
                + canonical_json(historical_corrections)[:4000]
            )
        if unresolved:
            invalid = {
                item: invalid_details[item] for item in sorted(unresolved)
                if item in invalid_details
            }
            errors = ";".join(
                f"{item}:{request_errors.get(item, 'InvalidLifecycle:see invalid details')}"
                for item in sorted(unresolved)
            )
            blockers.append(
                "SnapshotNotReadyError:stock-actions evidence remains unresolved: "
                f"count={len(unresolved)} ids={','.join(sorted(unresolved))} "
                f"invalid={canonical_json(invalid)} request_errors={errors}"
            )
            blockers.append(f"missing_stock-actions_instruments:{len(unresolved)}")

        complete_ids = tuple(sorted(stock_set - unresolved))
        observation_ids: tuple[str, ...] = ()
        if complete_ids and not historical_corrections:
            current_actions = current_actions.loc[
                current_actions["instrument_id"].astype(str).isin(complete_ids)
            ].reset_index(drop=True)
            current_actions, component_aggregations = _collapse_same_lifecycle_cash_components(
                current_actions,
            )
            canonical_request = ProviderRequest(
                ProviderCapability.CANONICAL_RECONCILIATION,
                scope.history_start,
                scope.history_end,
                complete_ids,
                {
                    "kind": "stock-actions-announcement-index-r5",
                    "scan_id": scan_id,
                    "predecessor_snapshot_id": predecessor.snapshot_id,
                },
            )
            matches = self.warehouse.matching_observations(
                provider=STOCK_ACTION_CANONICAL_PROVIDER,
                request=canonical_request,
            )
            manifest = matches[-1] if matches else self.warehouse.record_observation(
                ObservationPayload(
                    STOCK_ACTION_CANONICAL_PROVIDER,
                    max(
                        (item.observed_at for item in selected.values()),
                        default=snapshot.created_at,
                    ),
                    canonical_request,
                    {MarketTable.CORPORATE_ACTIONS: current_actions},
                    (CoverageClaim(
                        MarketTable.CORPORATE_ACTIONS,
                        True,
                        scope.history_start,
                        scope.history_end,
                        complete_ids,
                        "Complete announcement index plus targeted CNInfo lifecycle evidence",
                    ),),
                    {
                        "kind": "field_level_reconciliation",
                        "reconciliation_ready": True,
                        "policy": "stock-actions-announcement-index-r5-v1",
                        "source_snapshot_id": spec.source_snapshot_id,
                        "predecessor_snapshot_id": predecessor.snapshot_id,
                        "announcement_scan_id": scan_id,
                        "announcement_scan_path": str(scan_path),
                        "announcement_scan_hash": stable_digest(scan_payload),
                        "affected_instrument_ids": affected_ids,
                        "actionable_affected_instrument_ids": actionable_affected_ids,
                        "ignored_announcements": tuple(
                            ignored_announcements[key]
                            for key in sorted(ignored_announcements)
                        ),
                        "targeted_instrument_ids": target_ids,
                        "detail_requested_instrument_ids": tuple(sorted(
                            detail_requested_ids
                        )),
                        "reused_detail_instrument_ids": tuple(sorted(
                            reused_detail_ids
                        )),
                        "input_observation_ids": input_ids,
                        "per_instrument_evidence": per_instrument_evidence,
                        "known_pending_after_cutoff": known_pending,
                        "component_aggregation_policy": "same-lifecycle-cash-sum-r2-v1",
                        "component_aggregations": component_aggregations,
                        "instrument_count": len(complete_ids),
                        "action_count": len(current_actions),
                    },
                )
            )
            observation_ids = (manifest.observation_id,)

        build_id = "evidence-" + stable_digest({
            "collector_version": 5,
            "source_snapshot_id": spec.source_snapshot_id,
            "predecessor_snapshot_id": predecessor.snapshot_id,
            "kind": spec.kind,
            "scan_id": scan_id,
        })[:24]
        checkpoint = self.warehouse.root / "builds" / build_id / "checkpoint.json"
        _write_atomic_json(checkpoint, {
            "schema_version": 5,
            "build_id": build_id,
            "spec": spec,
            "universe_scope": scope,
            "scan_id": scan_id,
            "scan_path": scan_path,
            "pending_path": pending_path,
            "affected_instrument_ids": affected_ids,
            "actionable_affected_instrument_ids": actionable_affected_ids,
            "ignored_announcements": tuple(
                ignored_announcements[key]
                for key in sorted(ignored_announcements)
            ),
            "targeted_instrument_ids": target_ids,
            "detail_requested_instrument_ids": tuple(sorted(detail_requested_ids)),
            "reused_detail_instrument_ids": tuple(sorted(reused_detail_ids)),
            "detail_request_attempts": tuple(detail_request_attempts),
            "unresolved_instrument_ids": tuple(sorted(unresolved)),
            "observation_ids": observation_ids,
        })
        status = "complete" if not blockers else "incomplete"
        report_payload = {
            "decision_source": "direct-user-decision-2026-08-01",
            "kind": "simulation_stock-actions_incremental_collection",
            "status": status,
            "build_id": build_id,
            "spec": spec,
            "universe_scope": scope,
            "scan_id": scan_id,
            "scan_path": scan_path,
            "scan_window": {"start": scan_start, "end": scope.history_end},
            "announcement_count": len(records),
            "affected_instrument_ids": affected_ids,
            "actionable_affected_instrument_ids": actionable_affected_ids,
            "ignored_announcement_count": len(ignored_announcements),
            "ignored_announcements": tuple(
                ignored_announcements[key]
                for key in sorted(ignored_announcements)
            ),
            "targeted_instrument_ids": target_ids,
            "pending_instrument_ids": tuple(sorted(set(pending) & stock_set)),
            "universe_instrument_ids": stock_ids,
            "requested_instrument_ids": tuple(sorted(detail_requested_ids)),
            "reused_detail_instrument_ids": tuple(sorted(reused_detail_ids)),
            "new_detail_instrument_ids": tuple(sorted(selected_ids - reused_detail_ids)),
            "completed_target_instrument_ids": tuple(sorted(set(target_ids) - unresolved)),
            "completed_instrument_ids": complete_ids,
            "unresolved_instrument_ids": tuple(sorted(unresolved)),
            "detail_request_attempts": tuple(detail_request_attempts),
            "historical_corrections": historical_corrections,
            "observation_ids": observation_ids,
            "blockers": tuple(blockers),
            "checkpoint": checkpoint,
        }
        self.report_root.mkdir(parents=True, exist_ok=True)
        report = self.report_root / (
            f"evidence-collection-{build_id}-{stable_digest(report_payload)[:16]}.json"
        )
        _write_immutable_json(report, report_payload)
        return EvidenceCollectionResult(
            status,
            build_id,
            spec.kind,
            len(stock_ids),
            len(complete_ids),
            observation_ids,
            tuple(blockers),
            tuple(sorted(unresolved)),
            checkpoint,
            report,
        )


class SimulationStatusCollector:
    """Resumably collect and validate daily trading-status evidence."""

    def __init__(
        self,
        warehouse: MarketDataWarehouse,
        report_root: str | Path,
        *,
        registry: ProviderRegistry | None = None,
    ) -> None:
        self.warehouse = warehouse
        self.report_root = Path(report_root).resolve()
        self.registry = registry or default_provider_registry()

    def collect(
        self,
        spec: StatusCollectionSpec,
        *,
        loaded_snapshot: SnapshotManifest | None = None,
    ) -> StatusCollectionResult:
        # BaoStock's login is an account-wide session: another process logging in
        # invalidates the first one.  A warehouse-wide non-blocking lock prevents
        # accidental concurrent shards while retaining deterministic recovery units.
        from fundlab.marketdata.history import _exclusive_build_lock

        if spec.provider_name == "baostock":
            lock = self.warehouse.root / "builds" / ".locks" / "baostock-status.lock"
            with _exclusive_build_lock(lock):
                return self._collect_locked(spec, loaded_snapshot=loaded_snapshot)
        lock = self.warehouse.root / "builds" / ".locks" / (
            "status-" + stable_digest(spec)[:24] + ".lock"
        )
        with _exclusive_build_lock(lock):
            return self._collect_locked(spec, loaded_snapshot=loaded_snapshot)

    def _collect_locked(
        self,
        spec: StatusCollectionSpec,
        *,
        loaded_snapshot: SnapshotManifest | None,
    ) -> StatusCollectionResult:
        snapshot = loaded_snapshot or self.warehouse.load_snapshot(spec.source_snapshot_id)
        if snapshot.snapshot_id != spec.source_snapshot_id:
            raise ValueError("Loaded status snapshot does not match the collection spec")
        if snapshot.plan.readiness is not ReadinessProfile.RESEARCH_PRICE:
            raise ValueError("Status collection requires a research_price source snapshot")
        scope = snapshot.plan.universe_scope
        if (
            scope is None
            or scope.definition != CURRENT_SH_SZ_STOCK_ETF_UNIVERSE
            or set(scope.instrument_ids) == set()
        ):
            raise ValueError("Status collection requires an exact revision-2 universe scope")
        calendar_manifest = self.warehouse.load_observation(spec.calendar_observation_id)
        calendar_quality = calendar_manifest.source_metadata.get("calendar_quality")
        if not isinstance(calendar_quality, Mapping) or not calendar_quality.get("validated"):
            raise ValueError("Status collection requires a validated canonical calendar")
        calendar = self.warehouse.read_observation_table(
            spec.calendar_observation_id, MarketTable.CALENDAR,
        )
        instruments = self.warehouse.query_loaded_snapshot_table(
            snapshot, MarketTable.INSTRUMENTS,
        )
        instruments = instruments.loc[
            instruments["instrument_id"].astype(str).isin(scope.instrument_ids)
        ].sort_values("instrument_id", kind="stable").reset_index(drop=True)
        if set(map(str, instruments["instrument_id"])) != set(scope.instrument_ids):
            raise ValueError("Research snapshot instrument rows do not match its universe scope")
        if spec.provider_name == "baostock":
            # ETFs never carry stock ST designations; BaoStock is used only for
            # stock ST while xtquant supplies dense suspension state for all assets.
            instruments = instruments.loc[
                instruments["asset_type"].astype(str).eq("stock")
            ].reset_index(drop=True)

        all_batches = tuple(_chunks(tuple(map(str, instruments["instrument_id"])), spec.batch_size))
        batches = tuple(
            batch for ordinal, batch in enumerate(all_batches)
            if ordinal % spec.shard_count == spec.shard_index
        )
        selected_ids = tuple(sorted(item for batch in batches for item in batch))
        build_id = "status-" + stable_digest({
            "schema_version": 3,
            "source_snapshot_id": spec.source_snapshot_id,
            "calendar_observation_id": spec.calendar_observation_id,
            "provider_name": spec.provider_name,
            "batch_size": spec.batch_size,
            "shard_count": spec.shard_count,
            "shard_index": spec.shard_index,
        })[:24]
        checkpoint = self.warehouse.root / "builds" / build_id / "checkpoint.json"
        state = _read_json_if_present(checkpoint)
        completed: dict[str, str] = {
            str(key): str(value)
            for key, value in state.get("batches", {}).items()
        } if isinstance(state.get("batches", {}), Mapping) else {}
        blockers: list[str] = []
        observation_ids: list[str] = []
        for batch in batches:
            batch_id = "batch-" + stable_digest(batch)[:16]
            request = ProviderRequest(
                ProviderCapability.DAILY_STATUS,
                scope.history_start,
                scope.history_end,
                batch,
            )
            restored_id = None if spec.refresh else completed.get(batch_id)
            if restored_id and not spec.refresh:
                try:
                    candidate = self.warehouse.load_observation(restored_id)
                    expected_provider = (
                        SIMULATION_STATUS_CANONICAL_PROVIDER
                        if spec.provider_name == "xtquant" else spec.provider_name
                    )
                    if candidate.provider != expected_provider:
                        raise SnapshotNotReadyError("Status checkpoint provider mismatch")
                    if (
                        candidate.request.start_date != request.start_date
                        or candidate.request.end_date != request.end_date
                        or set(candidate.request.instrument_ids) != set(request.instrument_ids)
                    ):
                        raise SnapshotNotReadyError("Status checkpoint observation identity mismatch")
                    if spec.provider_name == "xtquant":
                        metadata = candidate.source_metadata.get("status_quality")
                        if (
                            candidate.request.capability
                            is not ProviderCapability.CANONICAL_RECONCILIATION
                            or not isinstance(metadata, Mapping)
                            or not metadata.get("validated")
                            or candidate.source_metadata.get("source_snapshot_id")
                            != spec.source_snapshot_id
                            or candidate.source_metadata.get("calendar_observation_id")
                            != spec.calendar_observation_id
                        ):
                            raise SnapshotNotReadyError(
                                "Canonical status checkpoint evidence mismatch"
                            )
                    elif candidate.request != request:
                        raise SnapshotNotReadyError("Status checkpoint request mismatch")
                    if spec.provider_name == "baostock":
                        _require_complete_status_claim(
                            candidate,
                            request,
                            instruments.loc[instruments["instrument_id"].isin(batch)],
                            calendar,
                            scope,
                        )
                    else:
                        _require_complete_claim(candidate, MarketTable.DAILY_BARS, request)
                except Exception:
                    restored_id = None
            if restored_id:
                observation_ids.append(restored_id)
                continue
            matches = () if spec.refresh else self.warehouse.matching_observations(
                provider=spec.provider_name, request=request,
            )
            try:
                source_manifest = None
                for candidate in reversed(matches):
                    try:
                        if spec.provider_name == "xtquant":
                            _require_sparse_status_source(candidate, request)
                        else:
                            _require_complete_status_claim(
                                candidate,
                                request,
                                instruments.loc[instruments["instrument_id"].isin(batch)],
                                calendar,
                                scope,
                            )
                    except Exception:
                        continue
                    source_manifest = candidate
                    break
                if source_manifest is None:
                    source_manifest = self.warehouse.record_observation(
                        self.registry.observe(spec.provider_name, request)
                    )
                if spec.provider_name == "xtquant":
                    _require_sparse_status_source(source_manifest, request)
                else:
                    _require_complete_status_claim(
                        source_manifest,
                        request,
                        instruments.loc[instruments["instrument_id"].isin(batch)],
                        calendar,
                        scope,
                    )
                frame = self.warehouse.read_observation_table(
                    source_manifest.observation_id, MarketTable.DAILY_BARS,
                )
                research = self.warehouse.query_loaded_snapshot_table(
                    snapshot,
                    MarketTable.DAILY_BARS,
                    instrument_ids=batch,
                    start_date=scope.history_start,
                    end_date=scope.history_end,
                    price_mode="raw",
                )
                _validate_status_scope(
                    instruments.loc[instruments["instrument_id"].isin(batch)],
                    frame,
                    calendar,
                    scope,
                    required_active_keys=set(map(
                        tuple, research[["instrument_id", "session_date"]].astype(str).to_numpy(),
                    )),
                    mode=spec.provider_name,
                )
                manifest = source_manifest
                if spec.provider_name == "xtquant":
                    dense_status, status_quality = canonicalize_suspension_status(
                        instruments=instruments.loc[
                            instruments["instrument_id"].isin(batch)
                        ],
                        research_bars=research,
                        status_bars=frame,
                        calendar=calendar,
                        universe_scope=scope,
                        status_observation_id=source_manifest.observation_id,
                        source_snapshot_id=spec.source_snapshot_id,
                        calendar_observation_id=spec.calendar_observation_id,
                    )
                    canonical_request = ProviderRequest(
                        ProviderCapability.CANONICAL_RECONCILIATION,
                        scope.history_start,
                        scope.history_end,
                        batch,
                        {
                            "kind": "simulation-suspension-status-r2",
                            "source_snapshot_id": spec.source_snapshot_id,
                            "calendar_observation_id": spec.calendar_observation_id,
                            "status_observation_id": source_manifest.observation_id,
                        },
                    )
                    canonical_matches = self.warehouse.matching_observations(
                        provider=SIMULATION_STATUS_CANONICAL_PROVIDER,
                        request=canonical_request,
                    )
                    manifest = canonical_matches[-1] if canonical_matches else None
                    if manifest is None:
                        manifest = self.warehouse.record_observation(ObservationPayload(
                            SIMULATION_STATUS_CANONICAL_PROVIDER,
                            max(source_manifest.observed_at, calendar_manifest.observed_at),
                            canonical_request,
                            {MarketTable.DAILY_BARS: dense_status},
                            (CoverageClaim(
                                MarketTable.DAILY_BARS,
                                True,
                                scope.history_start,
                                scope.history_end,
                                batch,
                                (
                                    "Exact in-lifecycle open-session suspension state from "
                                    "calendar minus independently reconciled active-price presence"
                                ),
                            ),),
                            {
                                "kind": "field_level_reconciliation",
                                "reconciliation_ready": True,
                                "source_snapshot_id": spec.source_snapshot_id,
                                "calendar_observation_id": spec.calendar_observation_id,
                                "input_observation_ids": (
                                    source_manifest.observation_id,
                                    spec.calendar_observation_id,
                                    *status_quality["research_observation_ids"],
                                ),
                                "status_quality": status_quality,
                            },
                        ))
                    _require_complete_claim(manifest, MarketTable.DAILY_BARS, request)
            except Exception as exc:
                blockers.append(f"{batch_id}:{type(exc).__name__}:{str(exc)[:240]}")
                continue
            completed[batch_id] = manifest.observation_id
            observation_ids.append(manifest.observation_id)
            _write_atomic_json(checkpoint, {
                "schema_version": 3,
                "build_id": build_id,
                "spec": spec,
                "universe_scope": scope,
                "batches": dict(sorted(completed.items())),
            })

        completed_ids: set[str] = set()
        for observation_id in observation_ids:
            manifest = self.warehouse.load_observation(observation_id)
            completed_ids.update(manifest.request.instrument_ids)
        missing = set(selected_ids) - completed_ids
        if missing:
            blockers.append(f"missing_status_instruments:{len(missing)}")
        status = "complete" if not blockers else "incomplete"
        report_payload = {
            "decision_source": "https://github.com/zhan7653/miniQMT/issues/7",
            "decision_revision": 2,
            "kind": (
                "simulation_stock_st_collection"
                if spec.provider_name == "baostock" else
                "simulation_dense_suspension_collection"
            ),
            "status": status,
            "build_id": build_id,
            "spec": spec,
            "universe_scope": scope,
            "requested_instrument_ids": selected_ids,
            "completed_instrument_ids": tuple(sorted(completed_ids)),
            "unresolved_instrument_ids": tuple(sorted(missing)),
            "observation_ids": tuple(sorted(set(observation_ids))),
            "blockers": tuple(sorted(set(blockers))),
            "checkpoint": checkpoint,
        }
        self.report_root.mkdir(parents=True, exist_ok=True)
        report = self.report_root / (
            f"status-collection-{build_id}-{stable_digest(report_payload)[:16]}.json"
        )
        primitive = to_primitive(report_payload)
        if report.exists():
            if json.loads(report.read_text(encoding="utf-8")) != primitive:
                raise ValueError(f"Immutable status report collision: {report}")
        else:
            report.write_text(canonical_json(report_payload), encoding="utf-8", newline="\n")
        return StatusCollectionResult(
            status,
            build_id,
            len(selected_ids),
            len(completed_ids & set(selected_ids)),
            tuple(sorted(set(observation_ids))),
            tuple(sorted(set(blockers))),
            tuple(sorted(missing)),
            checkpoint,
            report,
        )


def build_dense_simulation_bars(
    *,
    instruments: pd.DataFrame,
    research_bars: pd.DataFrame,
    status_bars: pd.DataFrame,
    calendar: pd.DataFrame,
    universe_scope: UniverseScope,
) -> pd.DataFrame:
    """Combine trusted active prices with an exhaustive daily status source.

    Research bars remain the price authority.  The status source contributes the
    explicit suspension/ST/previous-close facts and must contain exactly one row
    for every in-lifecycle open session.  Nontradable rows may retain a genuinely
    unknown ST/previous-close value; no price is invented.
    """

    master = instruments.copy(deep=True)
    target_ids = set(map(str, master["instrument_id"]))
    if not target_ids or not target_ids <= set(universe_scope.instrument_ids):
        raise SnapshotNotReadyError("Dense-bar instruments are outside the pinned universe")
    raw_research = research_bars.loc[
        research_bars["price_mode"].astype(str).eq("raw")
        & research_bars["instrument_id"].astype(str).isin(target_ids)
        & research_bars["session_date"].astype(str).between(
            universe_scope.history_start.isoformat(), universe_scope.history_end.isoformat(),
        )
    ].copy()
    raw_status = status_bars.loc[
        status_bars["price_mode"].astype(str).eq("raw")
        & status_bars["instrument_id"].astype(str).isin(target_ids)
        & status_bars["session_date"].astype(str).between(
            universe_scope.history_start.isoformat(), universe_scope.history_end.isoformat(),
        )
    ].copy()
    keys = ["instrument_id", "session_date"]
    if raw_research.duplicated(keys).any():
        raise SnapshotNotReadyError("Research bars contain duplicate daily keys")
    if raw_status.duplicated(keys).any():
        raise SnapshotNotReadyError("Status bars contain duplicate daily keys")

    open_by_exchange = {
        str(exchange): tuple(sorted(map(str, group.loc[
            group["is_open"].fillna(False), "session_date",
        ])))
        for exchange, group in calendar.loc[
            calendar["session_date"].astype(str).between(
                universe_scope.history_start.isoformat(), universe_scope.history_end.isoformat(),
            )
        ].groupby("exchange")
    }
    expected: set[tuple[str, str]] = set()
    for item in master.to_dict("records"):
        if pd.isna(item.get("listed_date")):
            raise SnapshotNotReadyError(f"Instrument has no listed_date: {item['instrument_id']}")
        start = max(str(item["listed_date"]), universe_scope.history_start.isoformat())
        end = universe_scope.history_end.isoformat()
        if not pd.isna(item.get("delisted_date")):
            end = min(end, str(item["delisted_date"]))
        expected.update(
            (str(item["instrument_id"]), day)
            for day in open_by_exchange.get(str(item["exchange"]), ())
            if start <= day <= end
        )
    status_keys = set(map(tuple, raw_status[keys].astype(str).to_numpy()))
    if status_keys != expected:
        raise SnapshotNotReadyError(
            f"Status daily coverage mismatch: missing={len(expected - status_keys)} "
            f"extra={len(status_keys - expected)}"
        )
    if raw_status["suspended"].isna().any():
        raise SnapshotNotReadyError("Status source has unknown suspension facts")

    active_keys = {
        key for key, suspended in zip(
            map(tuple, raw_status[keys].astype(str).to_numpy()),
            raw_status["suspended"].astype(bool),
            strict=True,
        )
        if not suspended
    }
    active_status = raw_status.loc[~raw_status["suspended"].astype(bool)]
    if active_status[["is_st", "previous_close"]].isna().any().any():
        raise SnapshotNotReadyError(
            "Active status source has unknown ST/previous-close facts"
        )
    research_keys = set(map(tuple, raw_research[keys].astype(str).to_numpy()))
    missing_price = active_keys - research_keys
    price_on_suspended = research_keys - active_keys
    placeholder_keys = set(map(tuple, raw_research.loc[
        pd.to_numeric(raw_research["volume"], errors="coerce").fillna(0).le(0),
        keys,
    ].astype(str).to_numpy()))
    invalid_price_on_suspended = price_on_suspended - placeholder_keys
    if missing_price or invalid_price_on_suspended:
        raise SnapshotNotReadyError(
            f"Research/status active-key mismatch: missing_price={len(missing_price)} "
            f"price_on_suspended={len(invalid_price_on_suspended)}"
        )

    research_authority_columns = tuple(
        column for column in raw_research.columns
        if column not in {
            *keys,
            "price_mode",
            "suspended",
            "is_st",
            "previous_close",
            "field_lineage",
            "source_payload",
        }
    )
    price_columns = raw_research[[*keys, *research_authority_columns]].rename(
        columns={column: f"_research_{column}" for column in research_authority_columns}
    )
    dense = raw_status.sort_values(keys, kind="stable").merge(
        price_columns,
        on=keys,
        how="left",
        sort=False,
        validate="one_to_one",
    )
    suspended = dense["suspended"].astype(bool)
    dense["_status_observation_id"] = dense["source_observation_id"]
    dense["_price_observation_id"] = dense["_research_source_observation_id"].where(
        ~suspended, pd.NA,
    )
    for column in research_authority_columns:
        research_column = f"_research_{column}"
        dense[column] = dense[research_column].where(~suspended, dense[column])
    dense.loc[suspended, ["open", "high", "low", "close", "amount"]] = pd.NA
    dense.loc[suspended, "volume"] = 0
    dense["price_mode"] = "raw"
    dense["field_lineage"] = _json_for_unique_rows(
        dense,
        ("_price_observation_id", "_status_observation_id"),
        lambda item: canonical_json({
            "kind": "dense_simulation_bar_r2",
            "price_observation_id": _optional_json_text(item["_price_observation_id"]),
            "status_observation_id": _optional_json_text(item["_status_observation_id"]),
            "status_fields": ("suspended", "is_st", "previous_close"),
        }),
    )
    dense["source_payload"] = _json_for_unique_rows(
        dense,
        ("_price_observation_id", "_status_observation_id"),
        lambda item: canonical_json({
            "kind": "dense_simulation_bar_r2",
            "price_observation_id": _optional_json_text(item["_price_observation_id"]),
            "status_observation_id": _optional_json_text(item["_status_observation_id"]),
            "payload_retention": "referenced immutable observations",
        }),
    )
    temporary_columns = [
        column for column in dense.columns
        if column.startswith("_research_")
        or column in {"_status_observation_id", "_price_observation_id"}
    ]
    return dense.drop(columns=temporary_columns).reset_index(drop=True)


def canonicalize_suspension_status(
    *,
    instruments: pd.DataFrame,
    research_bars: pd.DataFrame,
    status_bars: pd.DataFrame,
    calendar: pd.DataFrame,
    universe_scope: UniverseScope,
    status_observation_id: str,
    source_snapshot_id: str,
    calendar_observation_id: str,
) -> tuple[pd.DataFrame, Mapping[str, Any]]:
    """Turn sparse MiniQMT trading presence into explicit suspension state.

    A missing source row is classified as suspended only after the same key is
    absent from the already reconciled research-price snapshot.  The resulting
    row deliberately has no OHLC and no invented previous close.
    """

    keys = ["instrument_id", "session_date"]
    target_ids = set(map(str, instruments["instrument_id"]))
    research = research_bars.loc[
        research_bars["instrument_id"].astype(str).isin(target_ids)
        & research_bars["price_mode"].astype(str).eq("raw")
    ].copy()
    status = status_bars.loc[
        status_bars["instrument_id"].astype(str).isin(target_ids)
        & status_bars["price_mode"].astype(str).eq("raw")
    ].copy()
    research_keys = set(map(tuple, research[keys].astype(str).to_numpy()))
    _validate_status_scope(
        instruments,
        status,
        calendar,
        universe_scope,
        required_active_keys=research_keys,
        mode="xtquant",
    )
    expected = _expected_status_keys(instruments, calendar, universe_scope)
    actual = set(map(tuple, status[keys].astype(str).to_numpy()))
    missing = expected - actual
    if missing & research_keys:
        raise SnapshotNotReadyError(
            "A trusted active-price session cannot be inferred as suspended"
        )

    research_observation_ids = tuple(sorted({
        str(value) for value in research.get(
            "source_observation_id", pd.Series(dtype="string")
        ).dropna()
        if str(value).strip() not in {"", "<NA>", "None"}
    }))
    expected_frame = pd.DataFrame(sorted(expected), columns=keys)
    dense = expected_frame.merge(
        status,
        on=keys,
        how="left",
        sort=False,
        indicator="_status_presence",
        validate="one_to_one",
    )
    present = dense["_status_presence"].eq("both")
    source_suspended = dense["suspended"].fillna(False).astype(bool)
    dense["price_mode"] = "raw"
    for column in ("open", "high", "low", "close", "amount"):
        dense[column] = pd.NA
    dense["volume"] = 0
    dense["suspended"] = dense["suspended"].where(present, True)
    dense["is_st"] = dense["is_st"].where(present, pd.NA)
    dense["previous_close"] = dense["previous_close"].where(present, pd.NA)

    active_lineage = canonical_json({
        "kind": "simulation_suspension_status_r2",
        "semantics": "miniQMT_active_presence_confirmed_by_reconciled_price",
        "status_observation_id": status_observation_id,
        "research_snapshot_id": source_snapshot_id,
        "calendar_observation_id": calendar_observation_id,
    })
    explicit_suspension_lineage = canonical_json({
        "kind": "simulation_suspension_status_r2",
        "semantics": "explicit_suspended_row",
        "status_observation_id": status_observation_id,
        "research_snapshot_id": source_snapshot_id,
        "calendar_observation_id": calendar_observation_id,
    })
    inferred_suspension_lineage = canonical_json({
        "kind": "simulation_suspension_status_r2",
        "semantics": "calendar_open_minus_two_source_active_presence",
        "status_observation_id": status_observation_id,
        "research_snapshot_id": source_snapshot_id,
        "research_observation_ids": research_observation_ids,
        "calendar_observation_id": calendar_observation_id,
    })
    dense["field_lineage"] = inferred_suspension_lineage
    dense.loc[present & source_suspended, "field_lineage"] = explicit_suspension_lineage
    dense.loc[present & ~source_suspended, "field_lineage"] = active_lineage
    dense["source_payload"] = canonical_json({
        "inference": "no MiniQMT row and no reconciled active-price row",
        "status_observation_id": status_observation_id,
        "research_snapshot_id": source_snapshot_id,
        "calendar_observation_id": calendar_observation_id,
    })
    dense.loc[present, "source_payload"] = canonical_json({
        "status_observation_id": status_observation_id,
        "payload_retention": "referenced immutable observation",
    })
    dense = dense.drop(columns="_status_presence").reset_index(drop=True)
    quality = {
        "policy": "calendar-minus-two-source-active-presence-r2-v1",
        "validated": True,
        "instrument_ids": tuple(sorted(target_ids)),
        "expected_session_count": len(expected),
        "source_row_count": len(actual),
        "active_session_count": len(research_keys),
        "inferred_suspension_count": len(missing),
        "explicit_suspension_count": int(status["suspended"].astype(bool).sum()),
        "active_key_hash": stable_digest(tuple(sorted(research_keys))),
        "dense_key_hash": stable_digest(tuple(sorted(expected))),
        "status_observation_id": status_observation_id,
        "research_snapshot_id": source_snapshot_id,
        "research_observation_ids": research_observation_ids,
        "calendar_observation_id": calendar_observation_id,
    }
    return dense, quality


def reconcile_simulation_status(
    *,
    instruments: pd.DataFrame,
    research_bars: pd.DataFrame,
    dense_status_bars: pd.DataFrame,
    stock_st_bars: pd.DataFrame,
    calendar: pd.DataFrame,
    universe_scope: UniverseScope,
) -> pd.DataFrame:
    """Reconcile dense local suspension state with BaoStock's historical stock ST facts."""

    master = instruments.set_index("instrument_id").to_dict("index")
    target_ids = set(master)
    research = research_bars.loc[
        research_bars["instrument_id"].astype(str).isin(target_ids)
        & research_bars["price_mode"].astype(str).eq("raw")
    ].copy()
    dense = dense_status_bars.loc[
        dense_status_bars["instrument_id"].astype(str).isin(target_ids)
        & dense_status_bars["price_mode"].astype(str).eq("raw")
    ].copy()
    st = stock_st_bars.loc[
        stock_st_bars["instrument_id"].astype(str).isin(target_ids)
        & stock_st_bars["price_mode"].astype(str).eq("raw")
    ].copy()
    keys = ["instrument_id", "session_date"]
    if dense.duplicated(keys).any() or st.duplicated(keys).any():
        raise SnapshotNotReadyError("Status reconciliation source has duplicate daily keys")
    _validate_status_scope(
        instruments,
        dense,
        calendar,
        universe_scope,
        required_active_keys=set(map(
            tuple, research[keys].astype(str).to_numpy(),
        )),
        mode="canonical",
    )
    result = dense.sort_values(keys, kind="stable").reset_index(drop=True)
    st_evidence = st[[*keys, "suspended", "is_st", "source_observation_id"]].rename(columns={
        "suspended": "_st_suspended",
        "is_st": "_st_is_st",
        "source_observation_id": "_st_observation_id",
    })
    research_close = research[[*keys, "close", "volume"]].rename(
        columns={"close": "_research_close", "volume": "_research_volume"},
    )
    result = result.merge(
        st_evidence,
        on=keys,
        how="left",
        sort=False,
        validate="one_to_one",
    ).merge(
        research_close,
        on=keys,
        how="left",
        sort=False,
        validate="one_to_one",
    )
    result["_asset_type"] = result["instrument_id"].map({
        instrument_id: str(item["asset_type"])
        for instrument_id, item in master.items()
    })
    is_etf = result["_asset_type"].eq("etf")
    source_suspended = result["suspended"].astype(bool)
    result["_suspension_semantics"] = "canonical_dense_status"
    zero_volume_placeholder = (
        ~source_suspended
        & result["_st_suspended"].fillna(False).astype(bool)
        & result["_research_volume"].notna()
        & result["_research_volume"].le(0)
    )
    result.loc[zero_volume_placeholder, "suspended"] = True
    result.loc[zero_volume_placeholder, "_suspension_semantics"] = (
        "baostock_suspended_plus_zero_volume_price_placeholder"
    )
    suspended = result["suspended"].astype(bool)
    has_st_observation = result["_st_observation_id"].notna()
    unknown_observed_st = ~is_etf & has_st_observation & result["_st_is_st"].isna()
    if unknown_observed_st.any():
        key = tuple(map(str, result.loc[unknown_observed_st, keys].iloc[0]))
        raise SnapshotNotReadyError(f"Stock ST state is unknown: {key}")
    result["_effective_st"] = result.groupby(
        "instrument_id", sort=False,
    )["_st_is_st"].ffill()
    result["_effective_st_observation_id"] = result.groupby(
        "instrument_id", sort=False,
    )["_st_observation_id"].ffill()
    result.loc[is_etf, "_effective_st"] = False
    result.loc[is_etf, "_effective_st_observation_id"] = (
        "exchange-rule:not-applicable-to-etf"
    )
    missing_active_point_in_time_st = ~suspended & result["_effective_st"].isna()
    if missing_active_point_in_time_st.any():
        key = tuple(map(str, result.loc[missing_active_point_in_time_st, keys].iloc[0]))
        raise SnapshotNotReadyError(f"Active stock has no point-in-time ST state: {key}")

    original_previous_close = result["previous_close"].copy()
    missing_active_previous_close = ~suspended & original_previous_close.isna()
    if missing_active_previous_close.any():
        key = tuple(map(str, result.loc[missing_active_previous_close, keys].iloc[0]))
        raise SnapshotNotReadyError(f"Status row has no defensible previous close: {key}")
    last_close_through_session = result.groupby(
        "instrument_id", sort=False,
    )["_research_close"].ffill()
    result["_last_close_before_session"] = last_close_through_session.groupby(
        result["instrument_id"], sort=False,
    ).shift(1)
    result["previous_close"] = original_previous_close.fillna(
        result["_last_close_before_session"],
    )
    result["_previous_close_semantics"] = "source"
    result.loc[
        original_previous_close.isna() & result["_last_close_before_session"].isna(),
        "_previous_close_semantics",
    ] = "unavailable_before_scope_start"
    result.loc[
        original_previous_close.isna() & result["_last_close_before_session"].notna(),
        "_previous_close_semantics",
    ] = "last_traded_close_for_nontradable_session"
    result["is_st"] = result["_effective_st"]
    for column in ("open", "high", "low", "close", "amount"):
        result[column] = pd.NA
    result["volume"] = 0
    result["_suspension_observation_id"] = result["source_observation_id"]
    result["_st_semantics"] = "last_point_in_time_observation"
    result.loc[is_etf, "_st_semantics"] = "not_applicable"
    result["field_lineage"] = _json_for_unique_rows(
        result,
        (
            "_suspension_observation_id",
            "_effective_st_observation_id",
            "_st_semantics",
            "_previous_close_semantics",
            "_suspension_semantics",
        ),
        lambda item: canonical_json({
            "kind": "simulation_status_reconciliation_r2",
            "suspension_observation_id": _optional_json_text(
                item["_suspension_observation_id"]
            ),
            "st_observation_id": _optional_json_text(
                item["_effective_st_observation_id"]
            ),
            "suspended_st_semantics": item["_st_semantics"],
            "previous_close_semantics": item["_previous_close_semantics"],
            "suspension_resolution": item["_suspension_semantics"],
        }),
    )
    result["source_payload"] = _json_for_unique_rows(
        result,
        ("_suspension_observation_id", "_st_observation_id"),
        lambda item: canonical_json({
            "dense_status_observation_id": _optional_json_text(
                item["_suspension_observation_id"]
            ),
            "stock_st_observation_id": _optional_json_text(item["_st_observation_id"]),
            "payload_retention": "referenced immutable observations",
        }),
    )
    return result.drop(columns=[
        "_st_is_st",
        "_st_suspended",
        "_st_observation_id",
        "_research_close",
        "_research_volume",
        "_asset_type",
        "_effective_st",
        "_effective_st_observation_id",
        "_last_close_before_session",
        "_previous_close_semantics",
        "_suspension_observation_id",
        "_st_semantics",
        "_suspension_semantics",
    ]).reset_index(drop=True)


class SimulationIncrementValidator:
    """Assemble and validate one exact EOD partition before publication.

    The input must already be a field-level reconciliation observation.  This
    stage owns the simulation-only semantics that a generic reconciler cannot:
    exchange-derived quantity/rule materialization, exact open-session coverage,
    provider price-limit corroboration, and the immutable partition-quality
    marker consumed by the incremental publisher.
    """

    def __init__(self, warehouse: MarketDataWarehouse, report_root: str | Path) -> None:
        self.warehouse = warehouse
        self.report_root = Path(report_root).resolve()

    def validate_and_record(
        self,
        *,
        candidate_observation_id: str,
        calendar_observation_id: str,
        universe_scope: UniverseScope,
        description: str,
    ) -> SimulationIncrementValidationResult:
        candidate = self.warehouse.load_observation(candidate_observation_id)
        if (
            candidate.source_metadata.get("kind") != "field_level_reconciliation"
            or not candidate.source_metadata.get("reconciliation_ready")
        ):
            raise SnapshotNotReadyError(
                "Simulation increment candidate must be a ready field-level reconciliation"
            )
        required_tables = {
            MarketTable.INSTRUMENTS,
            MarketTable.DAILY_BARS,
            MarketTable.CORPORATE_ACTIONS,
            MarketTable.ADJUSTMENT_FACTORS,
        }
        available_tables = {item.table for item in candidate.files}
        missing = sorted(item.value for item in required_tables - available_tables)
        if missing:
            raise SnapshotNotReadyError(
                "Simulation increment candidate is missing tables: " + ", ".join(missing)
            )

        tables = {
            table: self.warehouse.read_observation_table(candidate_observation_id, table)
            for table in required_tables
        }
        for frame in tables.values():
            frame["source_observation_id"] = candidate_observation_id
        instruments = materialize_order_quantity_rules(tables[MarketTable.INSTRUMENTS])
        instrument_ids = tuple(sorted(map(str, instruments["instrument_id"])))
        if instrument_ids != universe_scope.instrument_ids:
            raise SnapshotNotReadyError(
                "Simulation increment candidate does not match its pinned instrument scope"
            )
        exchanges = set(map(str, instruments["exchange"]))

        calendar_manifest = self.warehouse.load_observation(calendar_observation_id)
        if not any(item.table is MarketTable.CALENDAR for item in calendar_manifest.files):
            raise SnapshotNotReadyError("Simulation increment calendar observation has no calendar")
        full_calendar = self.warehouse.read_observation_table(
            calendar_observation_id, MarketTable.CALENDAR,
        )
        rule_calendar = full_calendar.loc[
            full_calendar["exchange"].astype(str).isin(exchanges)
            & full_calendar["session_date"].astype(str).le(
                universe_scope.history_end.isoformat()
            )
        ].reset_index(drop=True)
        calendar = rule_calendar.loc[
            rule_calendar["session_date"].astype(str).between(
                universe_scope.history_start.isoformat(),
                universe_scope.history_end.isoformat(),
            )
        ].reset_index(drop=True)
        calendar["source_observation_id"] = calendar_observation_id
        target_open = calendar.loc[
            calendar["session_date"].astype(str).eq(
                universe_scope.history_end.isoformat()
            )
            & calendar["is_open"].fillna(False).astype(bool)
        ]
        if target_open.empty:
            raise SnapshotNotReadyError(
                "Simulation increment must end on a completed open exchange session"
            )

        bars = tables[MarketTable.DAILY_BARS].loc[
            tables[MarketTable.DAILY_BARS]["instrument_id"].astype(str).isin(instrument_ids)
            & tables[MarketTable.DAILY_BARS]["session_date"].astype(str).between(
                universe_scope.history_start.isoformat(),
                universe_scope.history_end.isoformat(),
            )
            & tables[MarketTable.DAILY_BARS]["price_mode"].astype(str).eq("raw")
        ].reset_index(drop=True)
        if len(bars) != len(tables[MarketTable.DAILY_BARS]):
            raise SnapshotNotReadyError(
                "Routine simulation increment contains rows outside its exact raw date scope"
            )
        etf_rules = None
        if instruments["asset_type"].astype(str).eq("etf").any():
            etf_rules = EtfRuleEvidenceBuilder(self.report_root).build(
                instruments,
                universe_as_of=universe_scope.as_of_date,
                universe_observation_id=candidate_observation_id,
            ).rules
        bars = materialize_daily_trade_rules(
            bars, instruments, rule_calendar, etf_rules=etf_rules,
        )
        quarantine_mask = bars["field_lineage"].astype(str).str.contains(
            "daily_instrument_data_gap_quarantine_v1",
            regex=False,
            na=False,
        )
        if quarantine_mask.any():
            bars.loc[quarantine_mask, "trade_rule_id"] = DATA_GAP_QUARANTINE_RULE_ID
            bars.loc[quarantine_mask, "trade_rule_known_date"] = bars.loc[
                quarantine_mask, "session_date"
            ]

        provider_bars, upstream_manifests = self._provider_audit_bars(
            candidate,
            instrument_ids=instrument_ids,
            start_date=universe_scope.history_start,
            end_date=universe_scope.history_end,
        )
        bars, exception_audit = apply_corroborated_historical_limit_exceptions(
            bars,
            provider_bars,
            protected_dates=(universe_scope.history_end,),
        )
        latest_historical_exception = bars.loc[
            bars["session_date"].astype(str).eq(universe_scope.history_end.isoformat())
            & bars["trade_rule_id"].astype(str).eq(
                "cn-historical-exchange-exception-corroborated-v1"
            )
        ]
        if not latest_historical_exception.empty:
            raise SnapshotNotReadyError(
                "Latest EOD rules cannot use a historical price-limit exception"
            )
        price_limit_audit = audit_provider_price_limits(
            bars,
            provider_bars,
            required_direct_limit_date=universe_scope.history_end,
        )

        scoped_tables = {
            MarketTable.INSTRUMENTS: instruments,
            MarketTable.CALENDAR: calendar,
            MarketTable.DAILY_BARS: bars,
            MarketTable.CORPORATE_ACTIONS: self._exact_event_scope(
                tables[MarketTable.CORPORATE_ACTIONS],
                "ex_date",
                universe_scope,
            ),
            MarketTable.ADJUSTMENT_FACTORS: self._exact_event_scope(
                tables[MarketTable.ADJUSTMENT_FACTORS],
                "effective_date",
                universe_scope,
            ),
        }
        quality = validate_snapshot_tables(
            scoped_tables,
            profile=ReadinessProfile.SIMULATION,
            universe_scope=universe_scope,
        )
        if not quality.ready:
            raise SnapshotNotReadyError(
                "Simulation increment validation failed: " + "; ".join(quality.errors)
            )

        source_observation_ids = tuple(sorted({
            candidate.observation_id,
            calendar_manifest.observation_id,
            *(item.observation_id for item in upstream_manifests),
        }))
        partition_quality = {
            "validated": True,
            "validator_version": SIMULATION_PARTITION_VALIDATOR_VERSION,
            "readiness": ReadinessProfile.SIMULATION.value,
            "calendar_observation_id": calendar_observation_id,
            "start_date": universe_scope.history_start,
            "end_date": universe_scope.history_end,
            "universe_as_of": universe_scope.as_of_date,
            "universe_definition": universe_scope.definition,
            "instrument_ids": instrument_ids,
            "row_counts": quality.row_counts,
            "candidate_observation_id": candidate_observation_id,
            "source_observation_ids": source_observation_ids,
            "price_limit_audit": price_limit_audit,
            "historical_limit_exception_audit": exception_audit,
            "degraded_quarantine": candidate.source_metadata.get(
                "degraded_quarantine"
            ),
        }
        observed_at = max(
            candidate.observed_at,
            calendar_manifest.observed_at,
            *(item.observed_at for item in upstream_manifests),
        )
        claims = tuple(
            CoverageClaim(
                table,
                True,
                None if table is MarketTable.INSTRUMENTS else universe_scope.history_start,
                None if table is MarketTable.INSTRUMENTS else universe_scope.history_end,
                instrument_ids,
                f"Validated by {SIMULATION_PARTITION_VALIDATOR_VERSION}",
            )
            for table in (
                MarketTable.INSTRUMENTS,
                MarketTable.DAILY_BARS,
                MarketTable.CORPORATE_ACTIONS,
                MarketTable.ADJUSTMENT_FACTORS,
            )
        )
        payload = ObservationPayload(
            f"canonical-{SIMULATION_PARTITION_VALIDATOR_VERSION}",
            observed_at,
            ProviderRequest(
                ProviderCapability.CANONICAL_RECONCILIATION,
                universe_scope.history_start,
                universe_scope.history_end,
                instrument_ids,
                {
                    "description": description,
                    "candidate_observation_id": candidate_observation_id,
                    "calendar_observation_id": calendar_observation_id,
                    "validator_version": SIMULATION_PARTITION_VALIDATOR_VERSION,
                    "source_observation_ids": source_observation_ids,
                },
            ),
            {
                table: scoped_tables[table]
                for table in (
                    MarketTable.INSTRUMENTS,
                    MarketTable.DAILY_BARS,
                    MarketTable.CORPORATE_ACTIONS,
                    MarketTable.ADJUSTMENT_FACTORS,
                )
            },
            claims,
            {
                "kind": "field_level_reconciliation",
                "reconciliation_ready": True,
                "description": description,
                "partition_quality": partition_quality,
                "report": {
                    "blockers": (),
                    "unresolved_conflicts": (),
                    "candidate_observation_id": candidate_observation_id,
                    "source_observation_ids": source_observation_ids,
                    "price_limit_audit": to_primitive(price_limit_audit),
                    "historical_limit_exception_audit": to_primitive(exception_audit),
                    "degraded_quarantine": candidate.source_metadata.get(
                        "degraded_quarantine"
                    ),
                },
                "degraded_quarantine": candidate.source_metadata.get(
                    "degraded_quarantine"
                ),
            },
        )
        validated = self.warehouse.record_observation(payload)
        report_payload = {
            "decision_source": "https://github.com/zhan7653/miniQMT/issues/8",
            "decision_revision": 1,
            "kind": "simulation_increment_validation",
            "candidate_observation_id": candidate_observation_id,
            "validated_observation_id": validated.observation_id,
            "calendar_observation_id": calendar_observation_id,
            "universe_scope": universe_scope,
            "partition_quality": partition_quality,
        }
        self.report_root.mkdir(parents=True, exist_ok=True)
        report_hash = stable_digest(report_payload)[:16]
        report = self.report_root / (
            f"simulation-increment-{validated.observation_id}-{report_hash}.json"
        )
        primitive = to_primitive(report_payload)
        if report.exists():
            if json.loads(report.read_text(encoding="utf-8")) != primitive:
                raise ValueError(f"Immutable increment validation report collision: {report}")
        else:
            report.write_text(canonical_json(report_payload), encoding="utf-8", newline="\n")
        return SimulationIncrementValidationResult(
            validated.observation_id,
            candidate_observation_id,
            report,
            source_observation_ids,
        )

    @staticmethod
    def _exact_event_scope(
        frame: pd.DataFrame,
        date_column: str,
        scope: UniverseScope,
    ) -> pd.DataFrame:
        inside = (
            frame["instrument_id"].astype(str).isin(scope.instrument_ids)
            & frame[date_column].astype(str).between(
                scope.history_start.isoformat(), scope.history_end.isoformat(),
            )
        )
        if not inside.all():
            raise SnapshotNotReadyError(
                "Routine simulation increment contains an event outside its exact scope"
            )
        return frame.reset_index(drop=True)

    def _provider_audit_bars(
        self,
        candidate: ObservationManifest,
        *,
        instrument_ids: tuple[str, ...],
        start_date: date,
        end_date: date,
    ) -> tuple[Mapping[str, pd.DataFrame], tuple[ObservationManifest, ...]]:
        policy = default_reconciliation_policy(ReadinessProfile.SIMULATION)
        queue = list(_declared_input_observation_ids(candidate))
        visited: set[str] = set()
        upstream: dict[str, ObservationManifest] = {}
        grouped: dict[str, list[pd.DataFrame]] = {}
        while queue:
            observation_id = queue.pop()
            if observation_id in visited or observation_id == candidate.observation_id:
                continue
            visited.add(observation_id)
            manifest = self.warehouse.load_observation(observation_id)
            nested = _declared_input_observation_ids(manifest)
            queue.extend(item for item in nested if item not in visited)
            if (
                manifest.provider.startswith("canonical-")
                or manifest.request.capability not in {
                    ProviderCapability.DAILY_BARS_RAW,
                    ProviderCapability.DAILY_STATUS,
                }
                or not any(item.table is MarketTable.DAILY_BARS for item in manifest.files)
            ):
                continue
            frame = self.warehouse.read_observation_table(
                observation_id, MarketTable.DAILY_BARS,
            )
            if manifest.request.parameters.get("instrument_limit_snapshot"):
                hashes = manifest.source_metadata.get("response_sha256")
                errors = manifest.source_metadata.get("request_errors")
                if not isinstance(hashes, Mapping):
                    continue
                error_ids = set(errors) if isinstance(errors, Mapping) else set()
                approved_ids = set(map(str, hashes)) - set(map(str, error_ids))
                frame = frame.loc[
                    frame["instrument_id"].astype(str).isin(approved_ids)
                ]
            frame = frame.loc[
                frame["instrument_id"].astype(str).isin(instrument_ids)
                & frame["session_date"].astype(str).between(
                    start_date.isoformat(), end_date.isoformat(),
                )
                & frame["price_mode"].astype(str).eq("raw")
            ].copy()
            if frame.empty:
                continue
            frame["_audit_observation_id"] = observation_id
            grouped.setdefault(policy.backend(manifest.provider), []).append(frame)
            upstream[observation_id] = manifest
        provider_bars: dict[str, pd.DataFrame] = {}
        keys = ["instrument_id", "session_date", "price_mode"]
        for backend, pieces in sorted(grouped.items()):
            combined = pd.concat(pieces, ignore_index=True).sort_values(
                [*keys, "observed_at"], kind="stable",
            )
            # Daily direct-limit snapshots intentionally carry no OHLC, while
            # historical/status rows usually carry OHLC but no direct limits.
            # Preserve the latest independently sourced row of each evidence
            # type instead of letting the newer sparse snapshot erase prices.
            price_rows = combined.loc[
                combined[["high", "low"]].notna().all(axis=1)
            ].drop_duplicates(keys, keep="last")
            limit_rows = combined.loc[
                combined[["limit_up", "limit_down"]].notna().all(axis=1)
            ].drop_duplicates(keys, keep="last")
            selected = pd.concat((price_rows, limit_rows), ignore_index=True)
            provider_bars[backend] = selected.drop_duplicates(
                [*keys, "_audit_observation_id"], keep="last",
            ).reset_index(drop=True)
        if len(provider_bars) < 2:
            raise SnapshotNotReadyError(
                "Simulation increment needs two independent daily provider backends"
            )
        return provider_bars, tuple(
            upstream[item] for item in sorted(upstream)
        )


def _declared_input_observation_ids(
    manifest: ObservationManifest,
) -> tuple[str, ...]:
    values: list[Any] = [
        manifest.request.parameters.get("input_observation_ids"),
        manifest.request.parameters.get("source_observation_ids"),
        manifest.source_metadata.get("source_observation_ids"),
    ]
    report = manifest.source_metadata.get("report")
    if isinstance(report, Mapping):
        values.extend((
            report.get("input_observation_ids"),
            report.get("source_observation_ids"),
        ))
    quality = manifest.source_metadata.get("partition_quality")
    if isinstance(quality, Mapping):
        values.append(quality.get("source_observation_ids"))
    found: set[str] = set()

    def collect(value: Any) -> None:
        if isinstance(value, str):
            if value.startswith("obs-"):
                found.add(value)
            return
        if isinstance(value, Mapping):
            for item in value.values():
                collect(item)
            return
        if isinstance(value, (tuple, list, set, frozenset)):
            for item in value:
                collect(item)

    for value in values:
        collect(value)
    found.discard(manifest.observation_id)
    return tuple(sorted(found))


class SimulationSnapshotBuilder:
    """Advance a componentized simulation snapshot with a validated EOD increment."""

    def __init__(self, warehouse: MarketDataWarehouse, report_root: str | Path) -> None:
        self.warehouse = warehouse
        self.report_root = Path(report_root).resolve()

    def extend(
        self,
        *,
        predecessor_snapshot_id: str,
        calendar_observation_id: str,
        increment_observation_ids: tuple[str, ...],
        universe_scope: UniverseScope,
        description: str,
        publish: bool = False,
    ) -> SimulationBuildResult:
        """Validate a componentized EOD increment and atomically advance publication."""

        from fundlab.marketdata.incremental import IncrementalCanonicalPublisher

        snapshot, incremental_audit = IncrementalCanonicalPublisher(
            self.warehouse,
        ).extend(
            predecessor_snapshot_id=predecessor_snapshot_id,
            calendar_observation_id=calendar_observation_id,
            increment_observation_ids=increment_observation_ids,
            universe_scope=universe_scope,
            description=description,
        )
        published = False
        if publish:
            self.warehouse.publish_if_current(
                predecessor_snapshot_id, snapshot.snapshot_id,
            )
            published = True
        report_payload = {
            "decision_source": "https://github.com/zhan7653/miniQMT/issues/7",
            "decision_revision": 2,
            "kind": "simulation_eod_extension",
            "predecessor_snapshot_id": predecessor_snapshot_id,
            "snapshot_id": snapshot.snapshot_id,
            "universe_scope": universe_scope,
            "calendar_observation_id": calendar_observation_id,
            "increment_observation_ids": tuple(sorted(set(increment_observation_ids))),
            "incremental_audit": incremental_audit,
            "quality": snapshot.quality,
            "published": published,
        }
        self.report_root.mkdir(parents=True, exist_ok=True)
        report_hash = stable_digest(report_payload)[:16]
        report = self.report_root / f"simulation-eod-{snapshot.snapshot_id}-{report_hash}.json"
        primitive = to_primitive(report_payload)
        if report.exists():
            if json.loads(report.read_text(encoding="utf-8")) != primitive:
                raise ValueError(f"Immutable simulation EOD report collision: {report}")
        else:
            report.write_text(canonical_json(report_payload), encoding="utf-8", newline="\n")
        return SimulationBuildResult(
            snapshot.snapshot_id,
            snapshot.quality.ready,
            published,
            report,
            snapshot.quality.errors,
        )


def _chunks(values: tuple[str, ...], size: int):
    for index in range(0, len(values), size):
        yield values[index:index + size]


def _validate_status_scope(
    instruments: pd.DataFrame,
    status: pd.DataFrame,
    calendar: pd.DataFrame,
    scope: UniverseScope,
    *,
    required_active_keys: set[tuple[str, str]] | None = None,
    mode: str = "exact",
) -> None:
    keys = ["instrument_id", "session_date"]
    if status.duplicated(keys).any():
        raise SnapshotNotReadyError("Status observation has duplicate daily keys")
    if status["suspended"].isna().any():
        raise SnapshotNotReadyError("Status observation has unknown suspension fields")
    active_rows = status.loc[~status["suspended"].astype(bool)]
    if mode == "baostock" and status[["is_st", "previous_close"]].isna().any().any():
        raise SnapshotNotReadyError(
            "BaoStock status observation has unknown ST/previous-close fields"
        )
    expected = _expected_status_keys(instruments, calendar, scope)
    actual = set(map(tuple, status[keys].astype(str).to_numpy()))
    if not actual <= expected:
        raise SnapshotNotReadyError(
            f"Status daily coverage mismatch: extra={len(actual - expected)}"
        )
    if required_active_keys is None:
        if actual != expected:
            raise SnapshotNotReadyError(
                f"Status daily coverage mismatch: missing={len(expected - actual)} extra=0"
            )
        return
    if mode in {"xtquant", "canonical"}:
        if mode == "canonical" and actual != expected:
            raise SnapshotNotReadyError(
                f"Dense suspension coverage mismatch: missing={len(expected - actual)}"
            )
        active = set(map(tuple, status.loc[
            ~status["suspended"].astype(bool), keys,
        ].astype(str).to_numpy()))
        if active != required_active_keys:
            raise SnapshotNotReadyError(
                f"Dense suspension/price-key mismatch: missing={len(required_active_keys - active)} "
                f"extra={len(active - required_active_keys)}"
            )
        if status.loc[
            ~status["suspended"].astype(bool), "previous_close",
        ].isna().any():
            raise SnapshotNotReadyError("Active dense status row has no previous_close")
        return
    missing_active = required_active_keys - actual
    unresolved_missing = _unfillable_point_in_time_st_gaps(
        missing_active,
        expected,
        status,
    )
    if unresolved_missing:
        raise SnapshotNotReadyError(
            f"Stock ST source misses {len(unresolved_missing)} active price sessions"
        )


def _unfillable_point_in_time_st_gaps(
    missing: set[tuple[str, str]],
    expected: set[tuple[str, str]],
    status: pd.DataFrame,
) -> set[tuple[str, str]]:
    """Allow one isolated source hole only when the prior session has ST evidence."""

    if not missing:
        return set()
    actual = set(map(tuple, status[["instrument_id", "session_date"]].astype(str).to_numpy()))
    expected_by_instrument: dict[str, list[str]] = {}
    for instrument_id, session in sorted(expected):
        expected_by_instrument.setdefault(instrument_id, []).append(session)
    unresolved: set[tuple[str, str]] = set()
    for key in missing:
        instrument_id, session = key
        sessions = expected_by_instrument.get(instrument_id, [])
        try:
            position = sessions.index(session)
        except ValueError:
            unresolved.add(key)
            continue
        if position == 0 or (instrument_id, sessions[position - 1]) not in actual:
            unresolved.add(key)
    return unresolved


def _expected_status_keys(
    instruments: pd.DataFrame,
    calendar: pd.DataFrame,
    scope: UniverseScope,
) -> set[tuple[str, str]]:
    open_by_exchange = {
        str(exchange): tuple(sorted(map(str, group.loc[group["is_open"], "session_date"])))
        for exchange, group in calendar.loc[
            calendar["session_date"].astype(str).between(
                scope.history_start.isoformat(), scope.history_end.isoformat(),
            )
        ].groupby("exchange")
    }
    expected: set[tuple[str, str]] = set()
    for item in instruments.to_dict("records"):
        if pd.isna(item.get("listed_date")):
            raise SnapshotNotReadyError(
                f"Instrument has no listed_date: {item['instrument_id']}"
            )
        start = max(str(item["listed_date"]), scope.history_start.isoformat())
        end = scope.history_end.isoformat()
        if not pd.isna(item.get("delisted_date")):
            end = min(end, str(item["delisted_date"]))
        expected.update(
            (str(item["instrument_id"]), day)
            for day in open_by_exchange.get(str(item["exchange"]), ())
            if start <= day <= end
        )
    return expected


def _evidence_parameters(
    kind: str,
    batch: tuple[str, ...],
    instruments: pd.DataFrame,
) -> Mapping[str, Any]:
    if kind == "stock-actions":
        return {
            "max_workers": 2,
            "retries": 5,
            "retry_backoff_seconds": 1.0,
        }
    if kind == "etf-actions":
        return {
            # Recovery scopes contain at most five funds.  Four workers keep
            # one slow announcement/PDF from serially blocking the remaining
            # archive checks while staying below the provider's hard cap of 8.
            "max_workers": 4,
            # The accumulator retries unresolved funds in a second recovery
            # round and on the next command.  Keep each network attempt bounded
            # so one rate-limited archive page cannot stall all 1,602 ETFs.
            "retries": 2,
            "retry_backoff_seconds": 0.5,
            "timeout_seconds": 15,
            "listed_dates": {
                instrument_id: str(instruments.loc[instrument_id, "listed_date"])[:10]
                for instrument_id in batch
            },
        }
    return {}


def _action_canonical_provider(kind: str) -> str:
    return {
        "stock-actions": STOCK_ACTION_CANONICAL_PROVIDER,
        "etf-actions": ETF_ACTION_CANONICAL_PROVIDER,
    }[kind]


def _action_canonical_request(
    spec: EvidenceCollectionSpec,
    scope: UniverseScope,
    batch: tuple[str, ...],
) -> ProviderRequest:
    parameters: dict[str, Any] = {
        "kind": f"{spec.kind}-evidence-r2",
        "source_snapshot_id": spec.source_snapshot_id,
    }
    if spec.kind == "etf-actions":
        parameters["source_parser_policy"] = EASTMONEY_ETF_ACTION_POLICY
    return ProviderRequest(
        ProviderCapability.CANONICAL_RECONCILIATION,
        scope.history_start,
        scope.history_end,
        batch,
        parameters,
    )


def _collapse_same_lifecycle_cash_components(
    actions: pd.DataFrame,
) -> tuple[pd.DataFrame, Mapping[str, Any]]:
    """Sum distinct cash components paid on one identical entitlement lifecycle."""

    keys = ["instrument_id", "action_type", "ex_date"]
    if actions.empty or not actions.duplicated(keys, keep=False).any():
        return actions, {}
    output: list[dict[str, Any]] = []
    evidence: dict[str, Any] = {}
    for key, group in actions.groupby(keys, sort=False, dropna=False):
        if len(group) == 1:
            output.append(group.iloc[0].to_dict())
            continue
        instrument_id, action_type, ex_date = map(str, key)
        lifecycle = group[["record_date", "ex_date", "pay_date", "listing_date"]].astype(
            "string"
        ).fillna(_JSON_MISSING)
        cash = pd.to_numeric(group["cash_per_share"], errors="coerce")
        incompatible = (
            action_type != CorporateActionType.CASH_DIVIDEND.value
            or len(lifecycle.drop_duplicates()) != 1
            or cash.isna().any()
            or (cash <= 0).any()
            or group[[
                "share_ratio", "rights_price", "quantity_multiplier",
            ]].notna().any().any()
            or group["known_date"].isna().any()
        )
        if incompatible:
            output.extend(group.to_dict("records"))
            continue
        component_hashes = tuple(sorted(
            stable_digest({
                "known_date": str(item["known_date"]),
                "cash_per_share": str(item["cash_per_share"]),
                "source_payload": item.get("source_payload"),
                "source_observation_id": item.get("source_observation_id"),
            })
            for item in group.to_dict("records")
        ))
        total = sum(
            (
                Decimal(str(value)).quantize(Decimal("0.000000000001"))
                for value in cash
            ),
            Decimal("0"),
        )
        row = group.iloc[0].to_dict()
        row.update({
            "action_id": "act-" + stable_digest({
                "policy": "same-lifecycle-cash-sum-r2-v1",
                "instrument_id": instrument_id,
                "ex_date": ex_date,
                "component_hashes": component_hashes,
            })[:24],
            "known_date": max(map(str, group["known_date"])),
            "cash_per_share": float(total),
            "field_lineage": canonical_json({
                "kind": "same_lifecycle_cash_component_aggregation_r2",
                "component_hashes": component_hashes,
                "known_date_semantics": "latest_component_implementation_announcement",
            }),
            "source_payload": canonical_json({
                "component_source_payloads": tuple(sorted(map(str, group["source_payload"]))),
                "payload_retention": "embedded immutable component provenance",
            }),
        })
        output.append(row)
        evidence[f"{instrument_id}|{action_type}|{ex_date}"] = {
            "component_count": len(group),
            "component_hashes": component_hashes,
            "cash_per_share": float(total),
            "known_date": row["known_date"],
        }
    return pd.DataFrame(output, columns=actions.columns), dict(sorted(evidence.items()))


def _collect_action_batch(
    *,
    warehouse: MarketDataWarehouse,
    registry: ProviderRegistry,
    spec: EvidenceCollectionSpec,
    scope: UniverseScope,
    batch: tuple[str, ...],
    instruments: pd.DataFrame,
    raw_observations: list[ObservationManifest],
    reuse_existing: bool,
) -> _ActionBatchCollectionResult:
    """Accumulate per-instrument action successes and retry only failures."""

    source_provider = {
        "stock-actions": "cninfo-public",
        "etf-actions": "eastmoney-fund-public",
    }[spec.kind]
    canonical_provider = _action_canonical_provider(spec.kind)
    recovery_batch_size = 10 if spec.kind == "stock-actions" else 5

    selected: dict[str, ObservationManifest] = {}

    def absorb(manifest: ObservationManifest) -> None:
        if (
            manifest.provider != source_provider
            or manifest.request.capability is not ProviderCapability.CORPORATE_ACTIONS
            or manifest.request.start_date != scope.history_start
            or manifest.request.end_date != scope.history_end
            or not set(manifest.request.instrument_ids) <= set(batch)
        ):
            return
        metadata = manifest.source_metadata
        hashes = metadata.get("response_sha256")
        errors = metadata.get("request_errors")
        invalid = metadata.get("invalid_lifecycle")
        if not isinstance(hashes, Mapping):
            return
        legacy_incompatible_action_ids: set[str] = set()
        if (
            spec.kind == "etf-actions"
            and metadata.get("parser_policy") != EASTMONEY_ETF_ACTION_POLICY
        ):
            # V6 discovers distribution notices even when the older per-fund
            # archive table has no row.  An old positive observation remains
            # replayable; an old negative is no longer exhaustive and must be
            # fetched again under the current policy.
            try:
                legacy_actions = warehouse.read_observation_table(
                    manifest.observation_id, MarketTable.CORPORATE_ACTIONS,
                )
            except Exception:
                return
            legacy_positive_ids = set(map(str, legacy_actions.get(
                "instrument_id", pd.Series(dtype="string"),
            )))
            legacy_incompatible_action_ids = (
                set(manifest.request.instrument_ids) - legacy_positive_ids
            )
        error_ids = set(errors) if isinstance(errors, Mapping) else set()
        invalid_ids = set(invalid) if isinstance(invalid, Mapping) else set()
        for instrument_id in manifest.request.instrument_ids:
            if (
                instrument_id in hashes
                and instrument_id not in error_ids
                and instrument_id not in invalid_ids
                and instrument_id not in legacy_incompatible_action_ids
                and instrument_id in batch
            ):
                previous = selected.get(instrument_id)
                if previous is None or manifest.observed_at > previous.observed_at:
                    selected[instrument_id] = manifest

    if reuse_existing:
        for item in raw_observations:
            absorb(item)

    recovery_errors: list[str] = []
    for _round in range(2):
        unresolved = tuple(sorted(set(batch) - set(selected)))
        if not unresolved:
            break
        for recovery_batch in _chunks(unresolved, recovery_batch_size):
            request = ProviderRequest(
                ProviderCapability.CORPORATE_ACTIONS,
                scope.history_start,
                scope.history_end,
                recovery_batch,
                _evidence_parameters(spec.kind, recovery_batch, instruments),
            )
            try:
                manifest = warehouse.record_observation(
                    registry.observe(source_provider, request)
                )
            except Exception as exc:
                recovery_errors.append(
                    f"{','.join(recovery_batch)}:{type(exc).__name__}:{str(exc)[:240]}"
                )
                continue
            raw_observations.append(manifest)
            absorb(manifest)

    unresolved = tuple(sorted(set(batch) - set(selected)))
    invalid_details: dict[str, Any] = {}
    if unresolved:
        for manifest in reversed(raw_observations):
            invalid = manifest.source_metadata.get("invalid_lifecycle")
            if not isinstance(invalid, Mapping):
                continue
            for instrument_id in unresolved:
                if instrument_id in invalid and instrument_id not in invalid_details:
                    invalid_details[instrument_id] = invalid[instrument_id]

    if not selected:
        return _ActionBatchCollectionResult(
            None,
            unresolved,
            (
                f"SnapshotNotReadyError:{spec.kind} evidence remains unresolved: "
                f"count={len(unresolved)} ids={','.join(unresolved)} "
                f"invalid={canonical_json(invalid_details)[:500]} "
                f"request_errors={';'.join(recovery_errors[-3:])[:500]}"
            ),
        )

    pieces: list[pd.DataFrame] = []
    input_ids = tuple(sorted({item.observation_id for item in selected.values()}))
    for observation_id in input_ids:
        frame = warehouse.read_observation_table(
            observation_id, MarketTable.CORPORATE_ACTIONS,
        )
        owned = {
            instrument_id for instrument_id, manifest in selected.items()
            if manifest.observation_id == observation_id
        }
        frame = frame.loc[frame["instrument_id"].astype(str).isin(owned)].copy()
        frame["source_observation_id"] = observation_id
        pieces.append(frame)
    actions = (
        pd.concat(pieces, ignore_index=True).reset_index(drop=True)
        if pieces else empty_table(MarketTable.CORPORATE_ACTIONS, include_lineage=True)
    )
    actions, component_aggregations = _collapse_same_lifecycle_cash_components(actions)
    if actions.duplicated(["instrument_id", "action_type", "ex_date"]).any():
        raise SnapshotNotReadyError(
            f"Accumulated {spec.kind} evidence has duplicate events"
        )

    per_instrument = {
        instrument_id: {
            "observation_id": manifest.observation_id,
            "response_sha256": manifest.source_metadata["response_sha256"][instrument_id],
        }
        for instrument_id, manifest in sorted(selected.items())
    }
    pending = {
        instrument_id: manifest.source_metadata.get(
            "known_pending_after_cutoff", {}
        ).get(instrument_id, ())
        for instrument_id, manifest in sorted(selected.items())
        if isinstance(manifest.source_metadata.get("known_pending_after_cutoff"), Mapping)
        and instrument_id in manifest.source_metadata["known_pending_after_cutoff"]
    }
    completed_batch = tuple(sorted(selected))
    request = _action_canonical_request(spec, scope, completed_batch)
    observed_at = max(item.observed_at for item in selected.values())
    manifest = warehouse.record_observation(ObservationPayload(
        canonical_provider,
        observed_at,
        request,
        {MarketTable.CORPORATE_ACTIONS: actions},
        (CoverageClaim(
            MarketTable.CORPORATE_ACTIONS,
            True,
            scope.history_start,
            scope.history_end,
            completed_batch,
            (
                "Per-instrument CNInfo implementation lifecycle evidence"
                if spec.kind == "stock-actions" else
                "Per-instrument Eastmoney archive and announcement lifecycle evidence"
            ),
        ),),
        {
            "kind": "field_level_reconciliation",
            "reconciliation_ready": True,
            "policy": f"{spec.kind}-per-instrument-accumulation-r2-v1",
            "source_snapshot_id": spec.source_snapshot_id,
            "input_observation_ids": input_ids,
            "per_instrument_evidence": per_instrument,
            "known_pending_after_cutoff": pending,
            "component_aggregation_policy": "same-lifecycle-cash-sum-r2-v1",
            "component_aggregations": component_aggregations,
            "instrument_count": len(completed_batch),
            "action_count": len(actions),
            "evidence_hash": stable_digest({
                "per_instrument_evidence": per_instrument,
                "known_pending_after_cutoff": pending,
                "component_aggregations": component_aggregations,
            }),
        },
    ))
    blocker = None
    if unresolved:
        blocker = (
            f"SnapshotNotReadyError:{spec.kind} evidence remains unresolved: "
            f"count={len(unresolved)} ids={','.join(unresolved)} "
            f"invalid={canonical_json(invalid_details)[:500]} "
            f"request_errors={';'.join(recovery_errors[-3:])[:500]}"
        )
    return _ActionBatchCollectionResult(manifest, unresolved, blocker)


def _require_complete_status_claim(
    manifest: ObservationManifest,
    request: ProviderRequest,
    instruments: pd.DataFrame,
    calendar: pd.DataFrame,
    scope: UniverseScope,
) -> None:
    """Validate status coverage against listing lifecycle, not a pre-listing date."""

    claims = [item for item in manifest.coverage if item.table is MarketTable.DAILY_BARS]
    if len(claims) != 1 or not claims[0].complete:
        raise SnapshotNotReadyError(
            f"Observation coverage is incomplete: {manifest.observation_id}/daily_bars"
        )
    claim = claims[0]
    if set(claim.instrument_ids) != set(request.instrument_ids):
        raise SnapshotNotReadyError(
            f"Status coverage instruments differ: {manifest.observation_id}"
        )
    open_by_exchange = {
        str(exchange): tuple(sorted(map(str, group.loc[
            group["is_open"].fillna(False), "session_date",
        ])))
        for exchange, group in calendar.groupby("exchange")
    }
    lifecycle_starts: list[date] = []
    for item in instruments.to_dict("records"):
        if pd.isna(item.get("listed_date")):
            raise SnapshotNotReadyError(
                f"Status instrument has no listed_date: {item['instrument_id']}"
            )
        civil_start = max(
            scope.history_start,
            date.fromisoformat(str(item["listed_date"])[:10]),
        )
        first_open = next((
            day for day in open_by_exchange.get(str(item["exchange"]), ())
            if day >= civil_start.isoformat()
        ), None)
        if first_open is None:
            raise SnapshotNotReadyError(
                f"Status calendar has no in-scope session: {item['instrument_id']}"
            )
        lifecycle_starts.append(date.fromisoformat(first_open))
    expected_start = min(lifecycle_starts)
    if (
        claim.start_date is None
        or claim.end_date is None
        or claim.start_date > expected_start
        or claim.end_date < scope.history_end
    ):
        raise SnapshotNotReadyError(
            f"Status coverage does not span in-lifecycle scope: {manifest.observation_id}"
        )


def _require_sparse_status_source(
    manifest: ObservationManifest,
    request: ProviderRequest,
) -> None:
    """Accept sparse MiniQMT presence only for later canonical suspension inference."""

    if manifest.provider != "xtquant" or manifest.request != request:
        raise SnapshotNotReadyError(
            f"Sparse status request identity differs: {manifest.observation_id}"
        )
    files = [item for item in manifest.files if item.table is MarketTable.DAILY_BARS]
    if len(files) != 1:
        raise SnapshotNotReadyError(
            f"Sparse status observation has no unique daily table: {manifest.observation_id}"
        )


def _require_complete_claim(
    manifest: ObservationManifest,
    table: MarketTable,
    request: ProviderRequest,
) -> None:
    matches = [item for item in manifest.coverage if item.table is table]
    if len(matches) != 1:
        raise SnapshotNotReadyError(
            f"Observation has no unique {table.value} coverage claim: {manifest.observation_id}"
        )
    claim = matches[0]
    if (
        not claim.complete
        or claim.start_date != request.start_date
        or claim.end_date != request.end_date
        or set(claim.instrument_ids) != set(request.instrument_ids)
    ):
        raise SnapshotNotReadyError(
            f"Observation coverage is incomplete: {manifest.observation_id}/{table.value}"
        )


def _json_for_unique_rows(
    frame: pd.DataFrame,
    columns: tuple[str, ...],
    encoder,
) -> pd.Series:
    """Encode the few distinct lineage combinations, then join them vectorially."""

    keys = frame.loc[:, columns].astype("string").fillna(_JSON_MISSING)
    unique = keys.drop_duplicates(ignore_index=True)
    unique["_encoded_json"] = [encoder(item) for item in unique.to_dict("records")]
    joined = keys.merge(
        unique,
        on=list(columns),
        how="left",
        sort=False,
        validate="many_to_one",
    )
    return pd.Series(joined["_encoded_json"].array, index=frame.index, dtype="string")


def _optional_json_text(value: Any) -> str | None:
    text = str(value)
    return None if text == _JSON_MISSING else text


def _require_complete_announcement_scan(
    payload: Mapping[str, Any], *, start_date: date, end_date: date,
) -> None:
    required_categories = {
        "category_qyfpxzcs_szsh",
        "category_pg_szsh",
        "category_bcgz_szsh",
    }
    if payload.get("complete") is not True:
        raise SnapshotNotReadyError("CNInfo announcement scan is not complete")
    if payload.get("policy_version") != "cninfo-corporate-actions-v1":
        raise SnapshotNotReadyError("CNInfo announcement scan policy version mismatch")
    if (
        str(payload.get("start_date")) != start_date.isoformat()
        or str(payload.get("end_date")) != end_date.isoformat()
    ):
        raise SnapshotNotReadyError("CNInfo announcement scan window mismatch")
    categories = payload.get("categories")
    if not isinstance(categories, (list, tuple)) or not required_categories <= set(map(str, categories)):
        raise SnapshotNotReadyError("CNInfo announcement scan categories are incomplete")
    page_evidence = payload.get("page_evidence")
    if not isinstance(page_evidence, (list, tuple)) or not page_evidence:
        raise SnapshotNotReadyError("CNInfo announcement scan has no page evidence")
    for category in required_categories:
        entries = tuple(
            item for item in page_evidence
            if isinstance(item, Mapping) and str(item.get("category")) == category
        )
        initial = tuple(item for item in entries if item.get("check") == "initial")
        recheck = tuple(item for item in entries if item.get("check") == "recheck")
        pages = tuple(
            item for item in entries if item.get("check") in {"initial", "page"}
        )
        if len(initial) != 1 or len(recheck) != 1 or not pages:
            raise SnapshotNotReadyError(
                f"CNInfo announcement scan page checks are incomplete: {category}"
            )
        totals = {item.get("reported_total") for item in entries}
        if len(totals) != 1 or any(
            isinstance(value, bool) or not isinstance(value, int) or value < 0
            for value in totals
        ):
            raise SnapshotNotReadyError(
                f"CNInfo announcement scan page totals are inconsistent: {category}"
            )
        total = next(iter(totals))
        if sum(int(item.get("record_count", -1)) for item in pages) != total:
            raise SnapshotNotReadyError(
                f"CNInfo announcement scan page counts are incomplete: {category}"
            )
        if initial[0].get("response_hash") != recheck[0].get("response_hash"):
            raise SnapshotNotReadyError(
                f"CNInfo announcement scan first-page evidence drifted: {category}"
            )
        for item in entries:
            response_json = item.get("response_json")
            response_hash = item.get("response_hash")
            if not isinstance(response_json, str) or not response_json:
                raise SnapshotNotReadyError(
                    f"CNInfo announcement scan raw response is missing: {category}"
                )
            try:
                response_payload = json.loads(response_json)
            except json.JSONDecodeError as exc:
                raise SnapshotNotReadyError(
                    f"CNInfo announcement scan raw response is invalid: {category}"
                ) from exc
            if stable_digest(response_payload) != response_hash:
                raise SnapshotNotReadyError(
                    f"CNInfo announcement scan response hash mismatch: {category}"
                )
    records = payload.get("records")
    affected = payload.get("affected_instrument_ids")
    if not isinstance(records, (list, tuple)) or not isinstance(affected, (list, tuple)):
        raise SnapshotNotReadyError("CNInfo announcement scan record structure is invalid")
    record_ids: set[str] = set()
    record_instruments: set[str] = set()
    for item in records:
        if not isinstance(item, Mapping):
            raise SnapshotNotReadyError("CNInfo announcement scan record is not an object")
        required = {
            "announcement_id", "instrument_id", "announcement_time",
            "category", "title", "document_url",
        }
        if not required <= set(item) or any(not str(item[key]).strip() for key in required):
            raise SnapshotNotReadyError("CNInfo announcement scan record is incomplete")
        announcement_id = str(item["announcement_id"])
        if announcement_id in record_ids:
            raise SnapshotNotReadyError("CNInfo announcement scan contains duplicate IDs")
        record_ids.add(announcement_id)
        record_instruments.add(str(item["instrument_id"]))
    if record_instruments != set(map(str, affected)):
        raise SnapshotNotReadyError("CNInfo announcement affected-instrument set mismatch")


def _instrument_listed_date(instruments: pd.DataFrame, instrument_id: str) -> date:
    value = instruments.loc[instrument_id, "listed_date"]
    if pd.isna(value):
        raise SnapshotNotReadyError(f"Instrument has no listed_date: {instrument_id}")
    return date.fromisoformat(str(value)[:10])


def _ignored_stock_action_announcement_reason(
    announcement: Mapping[str, Any],
) -> str | None:
    """Return an audited reason when a disclosure cannot affect A-share lifecycle data.

    The CNInfo distribution category contains proposals and shareholder suggestions
    as well as implementation notices.  Only the latter can supply an ex-date or a
    payable/listing event.  Corrections remain actionable because the exact accepted
    historical business fields must be compared before they can be cleared.
    """

    title = str(announcement.get("title", "")).strip()
    compact = "".join(title.split())
    if "H股" in compact and "A股" not in compact:
        return "non_a_share_h_only"
    if str(announcement.get("category", "")) != "category_qyfpxzcs_szsh":
        return None
    if _is_correction_announcement(announcement):
        return None
    if any(token in compact for token in ("预案", "提议")):
        return "non_executable_proposal"
    if any(token in compact for token in (
        "实施", "除权除息", "股权登记", "派息日", "股份到账", "配股缴款", "配股上市",
    )):
        return None
    if any(token in compact for token in (
        "提示性", "利润分配方案", "分红方案", "可分配利润进行现金分红",
    )):
        return "non_executable_proposal"
    return None


def _is_correction_announcement(announcement: Mapping[str, Any]) -> bool:
    title = str(announcement.get("title", ""))
    return (
        str(announcement.get("category", "")) == "category_bcgz_szsh"
        or any(token in title for token in ("更正", "补充", "调整", "修订"))
    )


def _announcement_matches_action_evidence(
    announcement: Mapping[str, Any],
    evidence: pd.DataFrame | Any,
) -> bool:
    category = str(announcement.get("category", ""))
    expected_types = {
        "category_qyfpxzcs_szsh": {"cash_dividend", "stock_dividend"},
        "category_pg_szsh": {"rights_issue"},
        "category_bcgz_szsh": {
            "cash_dividend", "stock_dividend", "rights_issue",
        },
    }.get(category, set())
    announcement_date = str(announcement.get("announcement_time", ""))[:10]
    if not expected_types or len(announcement_date) != 10:
        return False
    records = (
        evidence.to_dict("records")
        if isinstance(evidence, pd.DataFrame)
        else tuple(evidence or ())
    )
    for item in records:
        if not isinstance(item, Mapping):
            continue
        action_type = str(item.get("action_type", item.get("kind", "")))
        known_date = str(item.get("known_date", ""))[:10]
        if action_type in expected_types and known_date == announcement_date:
            return True
    return False


def _historical_action_corrections(
    *,
    predecessor_actions: pd.DataFrame,
    current_actions: pd.DataFrame,
    instrument_ids: tuple[str, ...],
    history_start: date,
    history_end: date,
) -> Mapping[str, Any]:
    """Return exact historical business-field differences without mutating history."""

    fields = (
        "known_date", "record_date", "pay_date", "listing_date",
        "cash_per_share", "share_ratio", "rights_price", "quantity_multiplier",
    )

    def rows(frame: pd.DataFrame, instrument_id: str) -> dict[str, Mapping[str, Any]]:
        selected = frame.loc[
            frame["instrument_id"].astype(str).eq(instrument_id)
            & frame["ex_date"].astype(str).ge(history_start.isoformat())
            & frame["ex_date"].astype(str).le(history_end.isoformat())
        ]
        result: dict[str, Mapping[str, Any]] = {}
        for item in selected.to_dict("records"):
            key = f"{item['action_type']}|{str(item['ex_date'])[:10]}"
            values = {
                field: _source_native_action_value(item, field) for field in fields
            }
            if key in result and result[key] != values:
                raise SnapshotNotReadyError(
                    f"Historical action comparison has duplicate semantic key: "
                    f"{instrument_id}/{key}"
                )
            result[key] = values
        return result

    differences: dict[str, Any] = {}
    for instrument_id in instrument_ids:
        before = rows(predecessor_actions, instrument_id)
        after = rows(current_actions, instrument_id)
        added = tuple(sorted(set(after) - set(before)))
        removed = tuple(sorted(set(before) - set(after)))
        changed = {
            key: {"before": before[key], "after": after[key]}
            for key in sorted(set(before) & set(after))
            if before[key] != after[key]
        }
        if added or removed or changed:
            differences[instrument_id] = {
                "added_keys": added,
                "removed_keys": removed,
                "changed": changed,
            }
    return dict(sorted(differences.items()))


def _source_native_action_value(item: Mapping[str, Any], field: str) -> Any:
    if field not in {"pay_date", "listing_date", "quantity_multiplier"}:
        return _action_comparison_value(item.get(field))
    payload: Any = item.get("source_payload")
    for _ in range(2):
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except json.JSONDecodeError:
                break
        if not isinstance(payload, Mapping):
            break
        if isinstance(payload.get("raw"), Mapping):
            raw = payload["raw"]
            if field == "pay_date":
                action_type = str(item.get("action_type"))
                if action_type == "cash_dividend":
                    return _action_comparison_value(raw.get("派息日"))
                if action_type == "rights_issue":
                    return _action_comparison_value(raw.get("配股缴款截止日"))
                return None
            if field == "listing_date":
                key = (
                    "配股上市日"
                    if str(item.get("action_type")) == "rights_issue"
                    else "股份到账日"
                )
                return _action_comparison_value(raw.get(key))
            return _action_comparison_value(raw.get("quantity_multiplier"))
        payload = payload.get("upstream_action_payload")
    return _action_comparison_value(item.get(field))


def _action_comparison_value(value: Any) -> Any:
    if value is None or value is pd.NA:
        return None
    try:
        if bool(pd.isna(value)):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, (date, pd.Timestamp)):
        return str(value)[:10]
    if isinstance(value, float):
        return round(value, 12)
    if hasattr(value, "item"):
        return _action_comparison_value(value.item())
    return str(value)[:10] if "-" in str(value) and len(str(value)) >= 10 else value


def _write_immutable_json(path: Path, payload: Mapping[str, Any]) -> None:
    primitive = to_primitive(payload)
    if path.exists():
        if json.loads(path.read_text(encoding="utf-8")) != primitive:
            raise ValueError(f"Immutable JSON collision: {path}")
        return
    _write_atomic_json(path, payload)


def _read_json_if_present(path: Path) -> Mapping[str, Any]:
    if not path.exists():
        return {}
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, Mapping):
        raise ValueError(f"Status checkpoint is not an object: {path}")
    return value


def _write_atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(canonical_json(payload), encoding="utf-8", newline="\n")
    os.replace(temporary, path)
