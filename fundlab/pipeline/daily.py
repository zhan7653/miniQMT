"""One idempotent daily cycle: extend the published snapshot, advance paper accounts.

This module only orchestrates.  Systemic trust failures remain fail-closed.
Small, explicitly bounded instrument-level availability gaps are published as
non-tradable quarantine rows so the rest of the daily system can advance while
the full error is kept visible and recoverable.
"""

from __future__ import annotations

import json
import os
import re
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from math import isfinite
from pathlib import Path
from typing import Any, Callable, Mapping

import pandas as pd

from fundlab.common.canonical import canonical_json, stable_digest, to_primitive
from fundlab.marketdata import (
    CURRENT_SH_SZ_STOCK_ETF_UNIVERSE,
    CanonicalMarketData,
    CorporateActionReconciliationResult,
    CoverageClaim,
    EvidenceCollectionSpec,
    MarketDataWarehouse,
    MarketDataError,
    MarketIngestionService,
    MarketTable,
    ObservationPayload,
    ProviderCapability,
    ProviderRequest,
    ReadinessProfile,
    SimulationEvidenceCollector,
    SimulationIncrementValidator,
    SimulationSnapshotBuilder,
    SimulationStatusCollector,
    SnapshotNotReadyError,
    SnapshotPlan,
    SourceSlice,
    StatusCollectionSpec,
    UniverseScope,
    build_factor_audit_candidates,
    build_dense_simulation_bars,
    default_provider_registry,
    derive_current_research_snapshot,
    reconcile_corporate_action_factors,
    record_no_trade_research_partition,
)
from fundlab.marketdata.schema import empty_table
from fundlab.marketdata.history import (
    HistoryBuildSpec,
    HistoryDatabaseBuilder,
    NoTradeSourceActiveError,
    find_no_trade_research_partition,
)
from fundlab.marketdata.simulation_data import reconcile_simulation_status
from fundlab.marketdata.contracts import (
    DATA_GAP_QUARANTINE_RULE_ID,
    EXECUTION_EVIDENCE_GAP_RULE_ID,
)
from fundlab.settings import DailyAccountSettings, FoundationSettings
from fundlab.strategies import (
    FileIntentSource,
    MovingAverageGridSource,
    StaticAllocationSource,
)
from fundlab.strategies.moving_average_grid import moving_average_grid_config
from fundlab.trading import (
    PortfolioState,
    SimulationService,
    TradingRepository,
    build_simulation_feedback,
)


PIPELINE_VERSION = "daily-pipeline-v2"
CALENDAR_PROVIDERS = ("baostock", "sina-calendar")
NO_TRADE_PROVIDERS = ("tickflow", "xtquant", "baostock")
DIRECT_LIMIT_PROVIDERS = ("xtquant", "eastmoney-efinance")
FACTOR_AUDIT_PROVIDER = "canonical-tickflow-adjusted-factor-audit-r2-v1"
FACTOR_AUDIT_VERSION = "adjusted-price-factor-audit-r2-v1"
DATA_GAP_QUARANTINE_PROVIDER = "fundlab-data-gap-quarantine"


class DailyRunInProgress(RuntimeError):
    """Another process already holds the daily-run lock."""


@contextmanager
def _exclusive_daily_lock(path: Path):
    """One daily run at a time across processes (web trigger vs scheduled task).

    Uses OS-level file locking, so the lock dies with the process and can
    never go stale after a crash.
    """

    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a+b")
    try:
        handle.seek(0, os.SEEK_END)
        if handle.tell() == 0:
            handle.write(b"\0")
            handle.flush()
        handle.seek(0)
        if os.name == "nt":
            import msvcrt

            try:
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            except OSError as exc:
                raise DailyRunInProgress(str(path)) from exc
            try:
                yield
            finally:
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as exc:
                raise DailyRunInProgress(str(path)) from exc
            yield
    finally:
        handle.close()


class DailyPipelineBlocked(RuntimeError):
    """A fail-closed gate stopped the run; the reason is user-facing."""

    def __init__(self, stage: str, reason: str, detail: Mapping[str, Any] | None = None) -> None:
        super().__init__(f"{stage}: {reason}")
        self.stage = stage
        self.reason = reason
        self.detail = dict(detail or {})


@dataclass
class DailyStage:
    name: str
    status: str
    detail: dict[str, Any] = field(default_factory=dict)


@dataclass
class DailyRunResult:
    status: str
    target_date: date | None
    snapshot_id: str | None
    stages: list[DailyStage]
    accounts: list[dict[str, Any]]
    report_path: Path | None

    @property
    def exit_code(self) -> int:
        return 0 if self.status in {"ok", "up_to_date", "degraded"} else 2


@dataclass(frozen=True)
class _NoTradeResolution:
    observation_id: str | None
    confirmed_instrument_ids: tuple[str, ...]
    active_conflicts: Mapping[str, tuple[str, ...]]


@dataclass(frozen=True)
class _UniverseResolution:
    observation_id: str
    frame: pd.DataFrame
    degraded_detail: Mapping[str, Any] | None = None
    execution_guard_ids: tuple[str, ...] = ()


@dataclass(frozen=True)
class _DegradedCollectionResult:
    status: str
    build_id: str
    requested_instruments: int
    completed_instruments: int
    observation_ids: tuple[str, ...]
    blockers: tuple[str, ...]
    unresolved_instrument_ids: tuple[str, ...]
    checkpoint: Path | None = None
    report: Path | None = None


class DailyPipeline:
    def __init__(
        self,
        settings: FoundationSettings,
        *,
        registry=None,
        now_fn: Callable[[], datetime] = datetime.now,
    ) -> None:
        self.settings = settings
        self.warehouse = MarketDataWarehouse(settings.paths.market_data)
        self.registry = registry or default_provider_registry()
        self.ingestion = MarketIngestionService(self.registry, self.warehouse)
        self.report_root = Path(settings.paths.report_root)
        self.daily_report_root = Path(settings.daily.report_root)
        self.now_fn = now_fn

    # ------------------------------------------------------------------ run

    def run(
        self,
        *,
        target_date: date | None = None,
        skip_data: bool = False,
        skip_accounts: bool = False,
    ) -> DailyRunResult:
        stages: list[DailyStage] = []
        accounts: list[dict[str, Any]] = []
        snapshot_id: str | None = None
        resolved_target: date | None = None
        status = "ok"
        lock_path = Path(self.settings.paths.market_data) / "builds" / ".locks" / "daily-run.lock"
        try:
            with _exclusive_daily_lock(lock_path):
                predecessor = self.warehouse.load_snapshot(self.warehouse.current_snapshot_id())
                previous_scope = predecessor.plan.universe_scope
                if previous_scope is None:
                    raise DailyPipelineBlocked("resolve", "current snapshot has no universe scope")

                if skip_data:
                    # Fully offline: no provider is contacted; accounts advance
                    # against whatever is already published.
                    resolved_target = target_date
                    snapshot_id = predecessor.snapshot_id
                    stages.append(DailyStage("resolve", "ok", {
                        "predecessor_snapshot_id": predecessor.snapshot_id,
                        "predecessor_end": previous_scope.history_end.isoformat(),
                        "calendar": "skipped (--skip-data)",
                    }))
                    stages.append(DailyStage("data", "skipped", {"reason": "--skip-data"}))
                else:
                    calendar_obs, calendar_frame = self._validated_calendar(
                        previous_scope.history_start,
                    )
                    resolved_target = target_date or self._latest_completed_session(calendar_frame)
                    stages.append(DailyStage("resolve", "ok", {
                        "predecessor_snapshot_id": predecessor.snapshot_id,
                        "predecessor_end": previous_scope.history_end.isoformat(),
                        "calendar_observation_id": calendar_obs,
                        "target_date": resolved_target.isoformat(),
                    }))
                    if resolved_target <= previous_scope.history_end:
                        stages.append(DailyStage("data", "up_to_date", {
                            "published_end": previous_scope.history_end.isoformat(),
                        }))
                        snapshot_id = predecessor.snapshot_id
                    else:
                        snapshot_id = self._extend_data(
                            stages,
                            predecessor=predecessor,
                            previous_scope=previous_scope,
                            calendar_observation_id=calendar_obs,
                            calendar_frame=calendar_frame,
                            target=resolved_target,
                        )

                if snapshot_id is not None:
                    persistent_quarantine = self._snapshot_degraded_quarantine(
                        self.warehouse.load_snapshot(snapshot_id)
                    )
                    if (
                        persistent_quarantine is not None
                        and not any(item.name == "quarantine" for item in stages)
                    ):
                        stages.append(DailyStage(
                            "quarantine", "degraded", persistent_quarantine,
                        ))

                if skip_accounts:
                    stages.append(DailyStage("accounts", "skipped", {"reason": "--skip-accounts"}))
                else:
                    accounts = self._advance_accounts(stages)
        except DailyRunInProgress:
            status = "blocked"
            stages.append(DailyStage("lock", "blocked", {
                "reason": "another daily run is already in progress",
            }))
        except DailyPipelineBlocked as exc:
            status = "blocked"
            stages.append(DailyStage(exc.stage, "blocked", {
                "reason": exc.reason, **exc.detail,
            }))
        if status == "ok" and any(item.status == "blocked" for item in stages):
            status = "blocked"
        if status == "ok" and any(item.status == "degraded" for item in stages):
            status = "degraded"
        report_path = self._write_report(status, resolved_target, snapshot_id, stages, accounts)
        return DailyRunResult(status, resolved_target, snapshot_id, stages, accounts, report_path)

    # ------------------------------------------------------- calendar/target

    def _validated_calendar(self, history_start: date) -> tuple[str, pd.DataFrame]:
        """Capture both calendar channels, require exact agreement, record one canonical observation.

        The capture window deliberately reaches ``calendar_horizon_days`` past
        today: exchanges publish their calendars ahead of time, and carrying
        those future sessions in the canonical snapshot is what lets an intent
        decided at the published data head schedule its T+1 order.  Agreement
        between both sources is required over the full window, future included.
        """

        capture_end = self.now_fn().date() + timedelta(
            days=self.settings.daily.calendar_horizon_days,
        )
        request = ProviderRequest(
            ProviderCapability.TRADING_CALENDAR,
            history_start,
            capture_end,
            parameters={"exchanges": ("SH", "SZ")},
        )
        manifests = []
        for provider in CALENDAR_PROVIDERS:
            observed, _ = self.ingestion.capture_resumable(provider, request)
            manifests.append(observed)
        frames = {
            manifest.provider: self.warehouse.read_observation_table(
                manifest.observation_id, MarketTable.CALENDAR,
            )
            for manifest in manifests
        }
        open_sets = {
            provider: {
                (str(row.exchange), str(row.session_date))
                for row in frame.loc[frame["is_open"].fillna(False).astype(bool)].itertuples(index=False)
            }
            for provider, frame in frames.items()
        }
        names = tuple(open_sets)
        first, second = open_sets[names[0]], open_sets[names[1]]
        if first != second:
            difference = sorted(first.symmetric_difference(second))[:20]
            raise DailyPipelineBlocked("calendar", "calendar sources disagree", {
                "providers": names, "difference_sample": difference,
            })
        reference = frames[names[0]].loc[:, ["exchange", "session_date", "is_open"]].copy()
        reference = reference.sort_values(
            ["exchange", "session_date"], kind="stable",
        ).reset_index(drop=True)
        source_ids = tuple(sorted(item.observation_id for item in manifests))
        existing = self._matching_validated_calendar(history_start, capture_end, source_ids)
        if existing is not None:
            return existing, reference
        payload = ObservationPayload(
            f"canonical-calendar-{PIPELINE_VERSION}",
            max(item.observed_at for item in manifests),
            ProviderRequest(
                ProviderCapability.CANONICAL_RECONCILIATION,
                history_start,
                capture_end,
                parameters={
                    "input_observation_ids": source_ids,
                    "pipeline": PIPELINE_VERSION,
                },
            ),
            {MarketTable.CALENDAR: reference},
            (CoverageClaim(MarketTable.CALENDAR, True, history_start, capture_end),),
            {
                "kind": "field_level_reconciliation",
                "reconciliation_ready": True,
                "description": "daily pipeline two-source exchange calendar",
                "calendar_quality": {
                    "validated": True,
                    "start_date": history_start,
                    "end_date": capture_end,
                    "providers": names,
                },
                "report": {
                    "blockers": (),
                    "unresolved_conflicts": (),
                    "input_observation_ids": source_ids,
                },
            },
        )
        recorded = self.warehouse.record_observation(payload)
        return recorded.observation_id, reference

    def _matching_validated_calendar(
        self, start: date, end: date, source_ids: tuple[str, ...],
    ) -> str | None:
        matches = self.warehouse.matching_observations(
            provider=f"canonical-calendar-{PIPELINE_VERSION}",
            request=ProviderRequest(
                ProviderCapability.CANONICAL_RECONCILIATION,
                start,
                end,
                parameters={
                    "input_observation_ids": source_ids,
                    "pipeline": PIPELINE_VERSION,
                },
            ),
        )
        for manifest in reversed(matches):
            quality = manifest.source_metadata.get("calendar_quality")
            if isinstance(quality, Mapping) and quality.get("validated"):
                return manifest.observation_id
        return None

    def _latest_completed_session(self, calendar_frame: pd.DataFrame) -> date:
        now = self.now_fn()
        boundary = now.date()
        if now.time() < self.settings.daily.session_cutoff:
            boundary = boundary - timedelta(days=1)
        open_dates = sorted({
            str(item) for item in calendar_frame.loc[
                calendar_frame["is_open"].fillna(False).astype(bool), "session_date",
            ]
        })
        candidates = [item for item in open_dates if item <= boundary.isoformat()]
        if not candidates:
            raise DailyPipelineBlocked("resolve", "no completed trading session in calendar window")
        return date.fromisoformat(candidates[-1])

    # ------------------------------------------------------------- data path

    def _extend_data(
        self,
        stages: list[DailyStage],
        *,
        predecessor,
        previous_scope: UniverseScope,
        calendar_observation_id: str,
        calendar_frame: pd.DataFrame,
        target: date,
    ) -> str:
        increment_start = previous_scope.history_end + timedelta(days=1)

        universe = self._official_universe(target, predecessor=predecessor)
        universe_obs, official_frame = universe.observation_id, universe.frame
        official_ids = set(map(str, official_frame["instrument_id"]))
        previous_ids = set(previous_scope.instrument_ids)
        quarantine_reasons: dict[str, list[str]] = {}
        execution_guard_reasons: dict[str, list[str]] = {}
        all_date_execution_guard_ids: set[str] = set(universe.execution_guard_ids)
        self._add_quarantine_reasons(
            execution_guard_reasons,
            universe.execution_guard_ids,
            source="universe",
            blockers=(
                str((universe.degraded_detail or {}).get("reason", "carried_universe")),
            ),
        )
        new_ids = tuple(sorted(official_ids - previous_ids))
        target_ids = tuple(sorted(official_ids))
        stages.append(DailyStage(
            "universe", "degraded" if universe.degraded_detail else "ok", {
            "universe_observation_id": universe_obs,
            "official_instruments": len(official_ids),
            "new_instruments": len(new_ids),
            **dict(universe.degraded_detail or {}),
        }))

        builder = HistoryDatabaseBuilder(
            self.warehouse,
            self.report_root,
            registry=self.registry,
            source_pair=self.settings.daily.source_pair,
            adjudicator_provider=self.settings.daily.adjudicator,
        )
        history_spec = HistoryBuildSpec(
            start_date=increment_start,
            end_date=target,
            universe_as_of=target,
            instrument_ids=target_ids,
            exchanges=("SH", "SZ"),
            asset_types=("stock", "etf"),
            batch_size=self.settings.daily.batch_size,
            publish=False,
        )
        refresh_historical_universe = self._historical_universe_refresh_required(
            builder.universe_provider,
            builder.default_universe_request(history_spec),
            target,
        )
        build = builder.build(
            history_spec,
            refresh_universe=refresh_historical_universe,
        )
        build_report = json.loads(Path(build.report).read_text(encoding="utf-8"))
        included_ids = set(map(str, build_report.get("included_instrument_ids", ())))
        excluded: Mapping[str, Any] = build_report.get("excluded", {})
        if build.snapshot_id is None:
            raise DailyPipelineBlocked("bars", "history build produced no reconciled partition", {
                "blockers": build.blockers,
            })
        stages.append(DailyStage("bars", "ok", {
            "build_id": build.build_id,
            "snapshot_id": build.snapshot_id,
            "included": len(included_ids),
            "excluded": len(excluded),
            "historical_universe_observation_id": build_report.get(
                "universe_observation_id"
            ),
            "historical_universe_refreshed": refresh_historical_universe,
        }))

        missing = tuple(sorted(set(target_ids) - included_ids))
        no_trade_only = tuple(
            item for item in missing
            if item in previous_ids and self._only_missing_sources(excluded.get(item))
        )
        new_master_missing = tuple(
            item for item in missing
            if item in new_ids and self._only_not_in_historical_master(excluded.get(item))
        )
        carried_master_missing = tuple(
            item for item in missing
            if item in previous_ids and self._only_not_in_historical_master(
                excluded.get(item)
            )
        )
        unexplained = tuple(sorted(
            set(missing)
            - set(no_trade_only)
            - set(new_master_missing)
            - set(carried_master_missing)
        ))
        for instrument_id in unexplained:
            quarantine_reasons.setdefault(instrument_id, []).append(
                f"bars:{canonical_json(excluded.get(instrument_id))}"
            )

        research_source_id = build.snapshot_id
        supplement_snapshot_ids: tuple[str, ...] = ()
        build_partition_ids = tuple(
            map(str, build_report.get("canonical_observation_ids", ()))
        )
        official_master_missing = tuple(sorted({
            *new_master_missing,
            *carried_master_missing,
        }))
        if official_master_missing:
            try:
                supplement_id, supplement_partitions, detail = (
                    self._build_new_instrument_supplement(
                        builder=builder,
                        universe_observation_id=universe_obs,
                        official_frame=official_frame,
                        instrument_ids=official_master_missing,
                        start=increment_start,
                        end=target,
                        trusted_predecessor_snapshot_id=(
                            predecessor.snapshot_id if carried_master_missing else None
                        ),
                    )
                )
            except Exception as exc:
                self._add_quarantine_reasons(
                    quarantine_reasons,
                    official_master_missing,
                    source="new_instruments",
                    blockers=(f"{type(exc).__name__}:{str(exc)[:500]}",),
                )
                stages.append(DailyStage(
                    "new_instruments" if new_master_missing else "carried_instruments",
                    "degraded",
                    {
                        "instrument_ids": official_master_missing,
                        "error_type": type(exc).__name__,
                        "error": str(exc)[:500],
                        "policy": "retain_official_master_and_quarantine_missing_prices",
                    },
                ))
            else:
                supplement_snapshot_ids = (supplement_id,)
                build_partition_ids = tuple(sorted({
                    *build_partition_ids,
                    *supplement_partitions,
                }))
                detail = {
                    **detail,
                    "new_instrument_ids": new_master_missing,
                    "carried_instrument_ids": carried_master_missing,
                }
                stages.append(DailyStage(
                    "new_instruments" if new_master_missing else "carried_instruments",
                    "ok",
                    detail,
                ))

        no_trade_observation_id: str | None = None
        confirmed_no_trade_ids: tuple[str, ...] = ()
        if no_trade_only:
            try:
                no_trade = self._record_no_trade(
                    predecessor_snapshot_id=predecessor.snapshot_id,
                    universe_observation_id=universe_obs,
                    calendar_observation_id=calendar_observation_id,
                    start=increment_start,
                    end=target,
                    instrument_ids=no_trade_only,
                    quarantine_reasons=quarantine_reasons,
                    universe_size=len(target_ids),
                )
            except Exception as exc:
                self._add_quarantine_reasons(
                    quarantine_reasons,
                    no_trade_only,
                    source="no_trade",
                    blockers=(f"{type(exc).__name__}:{str(exc)[:500]}",),
                )
                stages.append(DailyStage("no_trade", "degraded", {
                    "instruments": no_trade_only,
                    "error_type": type(exc).__name__,
                    "error": str(exc)[:500],
                    "policy": "quarantine_unresolved_missing_prices",
                }))
            else:
                no_trade_observation_id = no_trade.observation_id
                confirmed_no_trade_ids = no_trade.confirmed_instrument_ids
                stages.append(DailyStage(
                    "no_trade", "degraded" if no_trade.active_conflicts else "ok", {
                    "instruments": confirmed_no_trade_ids,
                    "observation_id": no_trade_observation_id,
                    "quarantined_active_conflicts": dict(no_trade.active_conflicts),
                }))

        quarantine_research_observation_id: str | None = None
        bar_quarantine_ids = tuple(sorted(quarantine_reasons))
        if bar_quarantine_ids:
            quarantine_research_observation_id = self._record_quarantined_research_partition(
                predecessor_snapshot_id=predecessor.snapshot_id,
                universe_observation_id=universe_obs,
                calendar_observation_id=calendar_observation_id,
                official_frame=official_frame,
                start=increment_start,
                end=target,
                instrument_ids=bar_quarantine_ids,
                reasons={
                    item: tuple(quarantine_reasons[item]) for item in bar_quarantine_ids
                },
            )

        if (
            supplement_snapshot_ids
            or no_trade_observation_id is not None
            or quarantine_research_observation_id is not None
        ):
            research_source_id = self._combine_partitions(
                main_snapshot_id=build.snapshot_id,
                supplement_snapshot_ids=supplement_snapshot_ids,
                no_trade_observation_id=no_trade_observation_id,
                no_trade_ids=confirmed_no_trade_ids,
                quarantine_observation_id=quarantine_research_observation_id,
                quarantine_ids=bar_quarantine_ids,
                target_ids=target_ids,
                start=increment_start,
                end=target,
            )

        research = derive_current_research_snapshot(
            self.warehouse,
            self.report_root,
            source_snapshot_id=research_source_id,
            universe_observation_id=universe_obs,
            universe_as_of=target,
            start_date=increment_start,
            end_date=target,
            publish=False,
        )
        if research.status != "complete" or research.missing_instrument_ids:
            raise DailyPipelineBlocked("research", "current research increment is incomplete", {
                "status": research.status,
                "missing_sample": tuple(research.missing_instrument_ids[:20]),
            })
        stages.append(DailyStage("research", "ok", {
            "snapshot_id": research.snapshot_id,
            "instruments": research.included_instruments,
        }))

        status_results = {}
        status_collector = SimulationStatusCollector(
            self.warehouse, self.report_root, registry=self.registry,
        )
        for provider in ("xtquant", "baostock"):
            try:
                result = status_collector.collect(StatusCollectionSpec(
                    source_snapshot_id=research.snapshot_id,
                    calendar_observation_id=calendar_observation_id,
                    provider_name=provider,
                    batch_size=50,
                ))
            except Exception as exc:
                result = _DegradedCollectionResult(
                    "incomplete",
                    f"unavailable-status-{provider}-{target.isoformat()}",
                    len(target_ids),
                    0,
                    (),
                    (f"{type(exc).__name__}:{str(exc)[:500]}",),
                    target_ids,
                )
            status_results[provider] = result
            if result.status != "complete":
                unresolved_ids = tuple(getattr(
                    result, "unresolved_instrument_ids", (),
                )) or target_ids
                self._add_quarantine_reasons(
                    execution_guard_reasons,
                    unresolved_ids,
                    source=f"status:{provider}",
                    blockers=result.blockers,
                )
                all_date_execution_guard_ids.update(unresolved_ids)
        stages.append(DailyStage(
            "status",
            "degraded" if any(
                result.status != "complete" for result in status_results.values()
            ) else "ok",
            {
            provider: {
                "observations": len(result.observation_ids),
                "blockers": tuple(result.blockers),
                "unresolved_instrument_ids": tuple(getattr(
                    result, "unresolved_instrument_ids", (),
                )),
            }
            for provider, result in status_results.items()
        }))

        direct_limit_results = {
            provider: self._collect_direct_limit_observations(
                provider=provider,
                target=target,
                instrument_ids=target_ids,
            )
            for provider in DIRECT_LIMIT_PROVIDERS
        }
        empty_limit_providers = tuple(
            provider for provider, result in direct_limit_results.items()
            if not result["observation_ids"]
        )
        directly_verified_ids = set(target_ids)
        for result in direct_limit_results.values():
            directly_verified_ids.intersection_update(result["_covered_instrument_ids"])
        missing_limit_ids = tuple(sorted(set(target_ids) - directly_verified_ids))
        self._add_quarantine_reasons(
            execution_guard_reasons,
            missing_limit_ids,
            source="limits:direct",
            blockers=("direct_price_limit_evidence_incomplete",),
        )
        stages.append(DailyStage("limits", "degraded" if missing_limit_ids else "ok", {
            provider: {
                "observations": len(result["observation_ids"]),
                "observation_ids": result["observation_ids"],
                "covered_instruments": result["covered_instruments"],
                "unresolved_instruments": len(result["unresolved_instrument_ids"]),
                "unresolved_sample": result["unresolved_instrument_ids"][:20],
                "request_errors": result["request_errors"][-5:],
            }
            for provider, result in direct_limit_results.items()
        } | {
            "empty_providers": empty_limit_providers,
            "execution_guard_instruments": len(missing_limit_ids),
            "execution_guard_sample": missing_limit_ids[:20],
            "policy": (
                "publish_prices_and_disable_execution_for_missing_direct_limit_evidence"
            ),
        }))

        evidence_results = {}
        evidence_guard_ids: set[str] = set()
        asset_types = official_frame.set_index("instrument_id")["asset_type"].astype(str)
        evidence_collector = SimulationEvidenceCollector(
            self.warehouse, self.report_root, registry=self.registry,
        )
        for kind in ("stock-actions", "etf-actions", "factors"):
            kind_ids = tuple(sorted(
                target_ids
                if kind == "factors"
                else (
                    instrument_id
                    for instrument_id in target_ids
                    if asset_types.get(instrument_id) == (
                        "stock" if kind == "stock-actions" else "etf"
                    )
                )
            ))
            try:
                result = evidence_collector.collect(EvidenceCollectionSpec(
                    source_snapshot_id=research.snapshot_id,
                    kind=kind,
                    predecessor_snapshot_id=(
                        predecessor.snapshot_id if kind == "stock-actions" else None
                    ),
                ))
            except Exception as exc:
                error = f"{type(exc).__name__}:{str(exc)[:500]}"
                result = _DegradedCollectionResult(
                    "incomplete",
                    f"unavailable-evidence-{kind}-{target.isoformat()}",
                    len(kind_ids),
                    0,
                    (),
                    (error,),
                    kind_ids,
                )
            evidence_results[kind] = result
            if result.status != "complete":
                unresolved_ids = tuple(getattr(
                    result, "unresolved_instrument_ids", (),
                )) or kind_ids
                evidence_guard_ids.update(unresolved_ids)
                self._add_quarantine_reasons(
                    execution_guard_reasons,
                    unresolved_ids,
                    source=f"evidence:{kind}",
                    blockers=result.blockers,
                )
                all_date_execution_guard_ids.update(unresolved_ids)
        stages.append(DailyStage(
            "evidence",
            "degraded" if any(
                result.status != "complete" for result in evidence_results.values()
            ) else "ok",
            {
            kind: {
                "observations": len(result.observation_ids),
                "requested_instruments": getattr(result, "requested_instruments", None),
                "completed_instruments": getattr(result, "completed_instruments", None),
                "unresolved_instrument_ids": tuple(getattr(
                    result, "unresolved_instrument_ids", (),
                )),
                "blockers": tuple(result.blockers),
                "report": str(getattr(result, "report", "")) or None,
            }
            for kind, result in evidence_results.items()
        }))

        quarantine_detail = self._finalize_quarantine(
            predecessor=predecessor,
            official_frame=official_frame,
            calendar_frame=calendar_frame,
            increment_start=increment_start,
            target=target,
            reasons=quarantine_reasons,
            universe_size=len(target_ids),
        )
        if quarantine_detail is not None:
            stages.append(DailyStage("quarantine", "degraded", quarantine_detail))

        increment_scope = UniverseScope(
            CURRENT_SH_SZ_STOCK_ETF_UNIVERSE,
            target,
            increment_start,
            target,
            survivorship_bias=previous_scope.survivorship_bias,
            instrument_ids=target_ids,
        )
        candidate_id, factor_detail = self._compose_candidate(
            research_snapshot_id=research.snapshot_id,
            calendar_frame=calendar_frame,
            increment_scope=increment_scope,
            status_results=status_results,
            evidence_results=evidence_results,
            build_partition_ids=build_partition_ids,
            no_trade_observation_id=no_trade_observation_id,
            universe_observation_id=universe_obs,
            direct_limit_observation_ids=tuple(sorted({
                observation_id
                for result in direct_limit_results.values()
                for observation_id in result["observation_ids"]
            })),
            execution_guard_ids=tuple(sorted(execution_guard_reasons)),
            all_date_execution_guard_ids=tuple(sorted(all_date_execution_guard_ids)),
            target_only_execution_guard_ids=tuple(sorted(
                set(missing_limit_ids) - all_date_execution_guard_ids
            )),
            execution_guard_reasons={
                item: tuple(sorted(set(reasons)))
                for item, reasons in sorted(execution_guard_reasons.items())
            },
            evidence_guard_ids=tuple(sorted(evidence_guard_ids)),
            quarantine_detail=quarantine_detail,
        )
        stages.append(DailyStage(
            "factor_reconciliation",
            "degraded" if factor_detail.get("status") == "degraded" else "ok",
            factor_detail,
        ))
        stages.append(DailyStage("candidate", "ok", {"observation_id": candidate_id}))

        try:
            validated = SimulationIncrementValidator(
                self.warehouse, self.report_root,
            ).validate_and_record(
                candidate_observation_id=candidate_id,
                calendar_observation_id=calendar_observation_id,
                universe_scope=increment_scope,
                description=f"daily pipeline EOD increment through {target.isoformat()}",
            )
        except MarketDataError as exc:
            retryable = self._retryable_direct_limit_validation_failure(
                str(exc), direct_limit_results,
            )
            raise DailyPipelineBlocked("validate", str(exc), {
                "error_type": type(exc).__name__,
                "candidate_observation_id": candidate_id,
                "retryable": retryable,
                "retry_class": "transient_direct_limit_gap" if retryable else None,
            }) from exc
        validated_manifest = self.warehouse.load_observation(
            validated.observation_id,
        )
        validated_guard = validated_manifest.source_metadata.get(
            "partition_quality", {}
        ).get("execution_evidence_guard", {})
        validator_added_guard = (
            validated_guard.get("validator_added_reasons", {})
            if isinstance(validated_guard, Mapping) else {}
        )
        stages.append(DailyStage(
            "validate",
            "degraded" if validator_added_guard else "ok",
            {
                "observation_id": validated.observation_id,
                "validator_added_execution_guard": validator_added_guard,
            },
        ))

        extended_ids = tuple(sorted(previous_ids.union(target_ids)))
        target_scope = UniverseScope(
            previous_scope.definition,
            target,
            previous_scope.history_start,
            target,
            survivorship_bias=previous_scope.survivorship_bias,
            instrument_ids=extended_ids,
        )
        result = SimulationSnapshotBuilder(
            self.warehouse, self.report_root,
        ).extend(
            predecessor_snapshot_id=predecessor.snapshot_id,
            calendar_observation_id=calendar_observation_id,
            increment_observation_ids=(validated.observation_id,),
            universe_scope=target_scope,
            description=f"daily pipeline EOD through {target.isoformat()}",
            publish=True,
        )
        if not result.ready or not result.published:
            raise DailyPipelineBlocked("extend", "extension did not publish", {
                "blockers": result.blockers,
            })
        stages.append(DailyStage("extend", "ok", {
            "snapshot_id": result.snapshot_id,
            "published": result.published,
        }))
        return result.snapshot_id

    def _official_universe(self, target: date, *, predecessor) -> _UniverseResolution:
        request = ProviderRequest(
            ProviderCapability.INSTRUMENTS,
            parameters={
                "exchanges": ("SH", "SZ"),
                "asset_types": ("stock", "etf"),
                "as_of_date": target.isoformat(),
            },
        )
        prior = self.warehouse.query_loaded_snapshot_table(
            predecessor, MarketTable.INSTRUMENTS,
        ).sort_values("instrument_id", kind="stable").reset_index(drop=True)
        try:
            observed, _ = self.ingestion.capture_resumable("exchange-public", request)
            frame = self.warehouse.read_observation_table(
                observed.observation_id, MarketTable.INSTRUMENTS,
            )
        except Exception as exc:
            detail = {
                "reason": "official_universe_unavailable",
                "error_type": type(exc).__name__,
                "error": str(exc)[:500],
                "carried_instruments": len(prior),
                "source_snapshot_id": predecessor.snapshot_id,
            }
            carried = self._record_carried_universe(
                target=target,
                frame=prior,
                predecessor_snapshot_id=predecessor.snapshot_id,
                official_observation_id=None,
                reason="official_universe_unavailable",
                detail=detail,
            )
            return _UniverseResolution(
                carried,
                prior,
                detail,
                tuple(sorted(map(str, prior["instrument_id"]))),
            )

        official_ids = set(map(str, frame["instrument_id"]))
        prior_ids = set(map(str, prior["instrument_id"]))
        removed = tuple(sorted(prior_ids - official_ids))
        if not removed:
            return _UniverseResolution(observed.observation_id, frame)
        carried_rows = prior.loc[prior["instrument_id"].astype(str).isin(removed)]
        merged = pd.concat((frame, carried_rows), ignore_index=True)
        merged = merged.drop_duplicates("instrument_id", keep="first").sort_values(
            "instrument_id", kind="stable",
        ).reset_index(drop=True)
        detail = {
            "reason": "official_universe_removed_predecessor_instruments",
            "removed_count": len(removed),
            "removed_sample": removed[:20],
            "carried_instruments": len(removed),
            "source_snapshot_id": predecessor.snapshot_id,
        }
        carried = self._record_carried_universe(
            target=target,
            frame=merged,
            predecessor_snapshot_id=predecessor.snapshot_id,
            official_observation_id=observed.observation_id,
            reason="official_universe_removed_predecessor_instruments",
            detail=detail,
        )
        return _UniverseResolution(carried, merged, detail, removed)

    def _record_carried_universe(
        self,
        *,
        target: date,
        frame: pd.DataFrame,
        predecessor_snapshot_id: str,
        official_observation_id: str | None,
        reason: str,
        detail: Mapping[str, Any],
    ) -> str:
        instrument_ids = tuple(sorted(map(str, frame["instrument_id"])))
        input_ids = tuple(item for item in (official_observation_id,) if item)
        request = ProviderRequest(
            ProviderCapability.CANONICAL_RECONCILIATION,
            parameters={
                "target_date": target.isoformat(),
                "predecessor_snapshot_id": predecessor_snapshot_id,
                "official_observation_ids": input_ids,
                "reason": reason,
                "pipeline": PIPELINE_VERSION,
            },
        )
        provider = f"canonical-universe-carry-forward-{PIPELINE_VERSION}"
        matches = self.warehouse.matching_observations(provider=provider, request=request)
        if matches:
            return matches[-1].observation_id
        payload = ObservationPayload(
            provider,
            datetime.now(timezone.utc),
            request,
            {MarketTable.INSTRUMENTS: frame},
            (CoverageClaim(
                MarketTable.INSTRUMENTS,
                True,
                instrument_ids=instrument_ids,
                detail="Complete carried membership from the last trusted published scope",
            ),),
            {
                "kind": "degraded_universe_carry_forward",
                "target_date": target,
                "predecessor_snapshot_id": predecessor_snapshot_id,
                "input_observation_ids": input_ids,
                "degraded_detail": dict(detail),
            },
        )
        return self.warehouse.record_observation(payload).observation_id

    def _historical_universe_refresh_required(
        self,
        provider: str,
        request: ProviderRequest,
        target: date,
    ) -> bool:
        """Refresh once per target boundary, then pin retries to that observation.

        The default historical master has no as-of parameter.  Refreshing it on
        every retry changes the history/research identities and strands otherwise
        valid downstream checkpoints.  An exact, complete observation captured no
        earlier than the target session is already fresh enough for that target;
        the next target refreshes naturally once this boundary becomes stale.
        """

        matches = self.warehouse.matching_observations(
            provider=provider,
            request=request,
        )
        if not matches:
            return True
        latest = matches[-1]
        complete = any(
            claim.table is MarketTable.INSTRUMENTS and claim.complete
            for claim in latest.coverage
        )
        return not complete or latest.observed_at.date() < target

    def _collect_direct_limit_observations(
        self,
        *,
        provider: str,
        target: date,
        instrument_ids: tuple[str, ...],
    ) -> dict[str, Any]:
        """Accumulate immutable per-instrument direct limit successes across retries."""

        requested = tuple(sorted(set(map(str, instrument_ids))))
        requested_set = set(requested)
        selected: dict[str, Any] = {}
        provider_errors: set[str] = set()
        errors_by_instrument: dict[str, set[str]] = {}

        def absorb(manifest) -> None:
            request = manifest.request
            if (
                manifest.provider != provider
                or request.capability is not ProviderCapability.DAILY_STATUS
                or request.start_date != target
                or request.end_date != target
                or not request.parameters.get("instrument_limit_snapshot")
                or not request.instrument_ids
                or not set(request.instrument_ids) <= requested_set
            ):
                return
            hashes = manifest.source_metadata.get("response_sha256")
            errors = manifest.source_metadata.get("request_errors")
            if isinstance(errors, Mapping):
                provider_errors.update(
                    str(error)[:240] for error in errors.values()
                    if str(error).strip()
                )
                for instrument_id, error in errors.items():
                    if str(instrument_id) in requested_set and str(error).strip():
                        errors_by_instrument.setdefault(
                            str(instrument_id), set(),
                        ).add(str(error)[:240])
            if not isinstance(hashes, Mapping):
                return
            error_ids = set(errors) if isinstance(errors, Mapping) else set()
            try:
                frame = self.warehouse.read_observation_table(
                    manifest.observation_id, MarketTable.DAILY_BARS,
                )
            except Exception:
                return
            if frame.empty or "instrument_id" not in frame:
                return
            frame = frame.loc[
                frame["instrument_id"].astype(str).isin(request.instrument_ids)
                & frame["session_date"].astype(str).eq(target.isoformat())
                & frame["price_mode"].astype(str).eq("raw")
            ].copy()
            if frame.empty:
                return
            for column in ("previous_close", "limit_up", "limit_down"):
                frame[column] = pd.to_numeric(frame[column], errors="coerce")
            valid = frame.loc[
                frame[["previous_close", "limit_up", "limit_down"]].notna().all(axis=1)
                & frame["previous_close"].gt(0)
                & frame["limit_up"].gt(0)
                & frame["limit_down"].gt(0)
            ]
            valid_ids = tuple(map(str, valid["instrument_id"]))
            duplicate_ids = set(map(
                str,
                valid.loc[
                    valid["instrument_id"].astype(str).duplicated(keep=False),
                    "instrument_id",
                ],
            ))
            for instrument_id in valid_ids:
                if (
                    instrument_id not in duplicate_ids
                    and instrument_id in hashes
                    and instrument_id not in error_ids
                    and instrument_id not in selected
                ):
                    selected[instrument_id] = manifest

        for manifest in self.warehouse.observations(provider=provider):
            absorb(manifest)

        request_errors: list[str] = []
        batch_size = min(self.settings.daily.batch_size, 100)
        parameters: dict[str, Any] = {"instrument_limit_snapshot": True}
        if provider == "eastmoney-efinance":
            parameters.update({
                "max_workers": 16,
                "retries": 3,
                "retry_backoff_seconds": 0.25,
                "timeout_seconds": 20,
            })
        for _round in range(2):
            unresolved = tuple(sorted(requested_set - set(selected)))
            if not unresolved:
                break
            for index in range(0, len(unresolved), batch_size):
                batch = unresolved[index:index + batch_size]
                request = ProviderRequest(
                    ProviderCapability.DAILY_STATUS,
                    target,
                    target,
                    batch,
                    parameters,
                )
                try:
                    manifest = self.ingestion.capture(provider, request)
                except Exception as exc:
                    detail = f"{type(exc).__name__}:{str(exc)[:240]}"
                    request_errors.append(f"{','.join(batch[:3])}:{detail}")
                    for instrument_id in batch:
                        errors_by_instrument.setdefault(instrument_id, set()).add(detail)
                    continue
                absorb(manifest)

        unresolved = tuple(sorted(requested_set - set(selected)))
        return {
            "observation_ids": tuple(sorted({
                manifest.observation_id for manifest in selected.values()
            })),
            "covered_instruments": len(selected),
            "unresolved_instrument_ids": unresolved,
            "request_errors": tuple(request_errors),
            "provider_errors": tuple(sorted(provider_errors)),
            "_covered_instrument_ids": tuple(sorted(selected)),
            "_errors_by_instrument": {
                instrument_id: tuple(sorted(errors))
                for instrument_id, errors in sorted(errors_by_instrument.items())
            },
        }

    @staticmethod
    def _only_missing_sources(reasons: Any) -> bool:
        if not isinstance(reasons, (list, tuple)) or not reasons:
            return False
        return all(str(item).startswith("missing_source:") for item in reasons)

    @staticmethod
    def _only_not_in_historical_master(reasons: Any) -> bool:
        return (
            isinstance(reasons, (list, tuple))
            and tuple(map(str, reasons)) == ("not_in_historical_master",)
        )

    @staticmethod
    def _only_retryable_provider_errors(reasons: Any) -> bool:
        if not isinstance(reasons, (list, tuple)) or not reasons:
            return False
        for reason in map(str, reasons):
            if not reason.startswith("provider_error:"):
                return False
            failures = reason.removeprefix("provider_error:").split(";")
            if not failures or any("=" not in failure for failure in failures):
                return False
            for failure in failures:
                detail = failure.split("=", 1)[1]
                if not DailyPipeline._retryable_failure_text(detail):
                    return False
        return True

    @staticmethod
    def _only_retryable_capture_errors(errors: Any) -> bool:
        if not isinstance(errors, (list, tuple)) or not errors:
            return False
        return all(DailyPipeline._retryable_failure_text(error) for error in errors)

    @staticmethod
    def _only_retryable_collection_blockers(
        blockers: Any,
        *,
        consequence_prefixes: tuple[str, ...],
    ) -> bool:
        if not isinstance(blockers, (list, tuple)) or not blockers:
            return False
        primary = tuple(
            str(blocker) for blocker in blockers
            if not any(
                str(blocker).startswith(prefix)
                or f":{prefix}" in str(blocker)
                for prefix in consequence_prefixes
            )
        )
        return bool(primary) and all(
            DailyPipeline._retryable_failure_text(blocker) for blocker in primary
        )

    @staticmethod
    def _quarantinable_action_collection(
        blockers: Any,
        unresolved_instrument_ids: Any,
    ) -> bool:
        """Only availability gaps qualify; parser/schema/lifecycle errors stay hard."""

        unresolved = set(map(str, unresolved_instrument_ids))
        if not unresolved or not isinstance(blockers, (list, tuple)) or not blockers:
            return False
        primary = tuple(
            str(blocker) for blocker in blockers
            if "missing_stock-actions_instruments:" not in str(blocker)
            and "missing_etf-actions_instruments:" not in str(blocker)
        )
        if not primary:
            return False
        claimed: set[str] = set()
        transport_covered: set[str] = set()
        for blocker in primary:
            if "evidence remains unresolved:" not in blocker:
                return False
            match = re.search(
                r"count=(\d+)\s+ids=([^\s]+)\s+invalid=(.*?)\s+"
                r"request_errors=(.*)$",
                blocker,
            )
            if match is None or match.group(3).strip() != "{}":
                return False
            blocker_ids = set(filter(None, match.group(2).split(",")))
            if int(match.group(1)) != len(blocker_ids):
                return False
            claimed.update(blocker_ids)
            request_errors = tuple(
                item.strip() for item in match.group(4).split(";") if item.strip()
            )
            if not request_errors or not all(
                DailyPipeline._retryable_atomic_failure(item)
                for item in request_errors
            ):
                return False
            for error in request_errors:
                error_match = re.match(
                    r"([^:]+):(?:TimeoutError|ConnectionError|PermissionError|"
                    r"ObservationError|PendingAnnouncement|HTTP Error|Source HTTP)",
                    error,
                )
                if error_match is None:
                    return False
                transport_covered.update(filter(None, error_match.group(1).split(",")))
        return claimed == unresolved and unresolved <= transport_covered

    @staticmethod
    def _action_gap_instrument_ids(
        quarantine_detail: Mapping[str, Any] | None,
    ) -> set[str]:
        reasons_by_instrument = (quarantine_detail or {}).get(
            "reasons_by_instrument", {}
        )
        if not isinstance(reasons_by_instrument, Mapping):
            return set()
        return {
            str(instrument_id)
            for instrument_id, reasons in reasons_by_instrument.items()
            if any(
                str(reason).startswith("evidence:") and "-actions:" in str(reason)
                for reason in reasons
            )
        }

    @staticmethod
    def _retryable_failure_text(error: Any) -> bool:
        text = str(error)
        if "request_errors=" in text:
            prefix, payload = text.split("request_errors=", 1)
            invalid = re.search(r"invalid=(.*)\s*$", prefix)
            if invalid and invalid.group(1).strip() not in {"", "{}"}:
                return False
            embedded = tuple(
                item.strip() for item in payload.split(";") if item.strip()
            )
            return bool(embedded) and all(
                DailyPipeline._retryable_atomic_failure(item) for item in embedded
            )
        return DailyPipeline._retryable_atomic_failure(text)

    @staticmethod
    def _retryable_atomic_failure(text: str) -> bool:
        if re.search(
            r"(?:^|[;=,:])(?:ValueError|TypeError|IntegrityError|"
            r"SchemaError|SourceConflictError|TradeRuleError):",
            text,
        ):
            return False
        if any(token in text for token in (
            "TimeoutError:",
            "ConnectionError:",
            "PermissionError:[WinError 5]",
            "ObservationError:Source request failed:",
            "ObservationError: Source request failed:",
            "PendingAnnouncement:",
        )):
            return True
        match = re.search(r"Source HTTP (\d{3}):", text)
        if not match:
            match = re.search(r"HTTP Error (\d{3})(?:\D|$)", text)
        if not match:
            return False
        status = int(match.group(1))
        return status in {408, 429} or 500 <= status <= 599

    @staticmethod
    def _retryable_direct_limit_validation_failure(
        reason: str,
        direct_limit_results: Mapping[str, Mapping[str, Any]],
    ) -> bool:
        match = re.search(
            r"Price-limit audit needs two direct provider limit values for "
            r"([^/]+)/\d{4}-\d{2}-\d{2}",
            reason,
        )
        if not match:
            return False
        instrument_id = match.group(1)
        missing_results = tuple(
            result for result in direct_limit_results.values()
            if instrument_id not in set(result.get("_covered_instrument_ids", ()))
        )
        if not missing_results:
            return False
        for result in missing_results:
            by_instrument = result.get("_errors_by_instrument", {})
            errors = (
                by_instrument.get(instrument_id, ())
                if isinstance(by_instrument, Mapping) else ()
            )
            if not DailyPipeline._only_retryable_capture_errors(errors):
                return False
        return True

    def _snapshot_degraded_quarantine(
        self,
        snapshot,
    ) -> Mapping[str, Any] | None:
        records: list[Mapping[str, Any]] = []
        seen: set[str] = set()
        for selection in snapshot.plan.selections:
            observation_id = str(selection.observation_id)
            if observation_id in seen:
                continue
            seen.add(observation_id)
            manifest = self.warehouse.load_observation(observation_id)
            detail = manifest.source_metadata.get("degraded_quarantine")
            if isinstance(detail, Mapping) and detail.get("instrument_ids"):
                records.append(detail)
        if not records:
            return None
        candidate_ids = tuple(sorted({
            str(instrument_id)
            for record in records
            for instrument_id in record.get("instrument_ids", ())
        }))
        scope = snapshot.plan.universe_scope
        if scope is None:
            return None
        bars = self.warehouse.query_loaded_snapshot_table(
            snapshot,
            MarketTable.DAILY_BARS,
            instrument_ids=candidate_ids,
            start_date=scope.history_end - timedelta(days=45),
            end_date=scope.history_end,
            price_mode="raw",
        )
        active_ids: list[str] = []
        consecutive: dict[str, int] = {}
        reasons: dict[str, tuple[str, ...]] = {}
        active_records: list[Mapping[str, Any]] = []
        for instrument_id in candidate_ids:
            rows = bars.loc[
                bars["instrument_id"].astype(str).eq(instrument_id)
            ].sort_values("session_date", ascending=False, kind="stable")
            if rows.empty or str(rows.iloc[0]["trade_rule_id"]) != DATA_GAP_QUARANTINE_RULE_ID:
                continue
            count = 0
            for rule in map(str, rows["trade_rule_id"]):
                if rule != DATA_GAP_QUARANTINE_RULE_ID:
                    break
                count += 1
            latest_session = str(rows.iloc[0]["session_date"])[:10]
            matching_records = []
            for record in records:
                sessions_by_instrument = record.get("increment_sessions", {})
                if not isinstance(sessions_by_instrument, Mapping):
                    continue
                sessions = tuple(map(str, sessions_by_instrument.get(instrument_id, ())))
                if latest_session in sessions:
                    matching_records.append((max(sessions), record))
            if not matching_records:
                continue
            selected_record = max(matching_records, key=lambda item: item[0])[1]
            record_reasons = selected_record.get("reasons_by_instrument", {})
            values = (
                record_reasons.get(instrument_id, ())
                if isinstance(record_reasons, Mapping) else ()
            )
            active_ids.append(instrument_id)
            consecutive[instrument_id] = count
            reasons[instrument_id] = tuple(sorted(set(map(str, values))))
            active_records.append(selected_record)
        instrument_ids = tuple(sorted(active_ids))
        if not instrument_ids:
            return None
        return {
            "policy": "daily-instrument-data-gap-quarantine-v1",
            "persistent": True,
            "instrument_ids": instrument_ids,
            "instrument_count": len(instrument_ids),
            "consecutive_sessions": dict(sorted(consecutive.items())),
            "reasons_by_instrument": {
                item: tuple(sorted(reasons[item])) for item in instrument_ids
            },
            "source_partition_count": len({id(record) for record in active_records}),
            "valuation_policy": "last_trusted_price_stale",
            "execution_policy": "prohibit_and_defer_pending_orders",
        }

    def _build_new_instrument_supplement(
        self,
        *,
        builder: HistoryDatabaseBuilder,
        universe_observation_id: str,
        official_frame: pd.DataFrame,
        instrument_ids: tuple[str, ...],
        start: date,
        end: date,
        trusted_predecessor_snapshot_id: str | None = None,
    ) -> tuple[str, tuple[str, ...], Mapping[str, Any]]:
        """Build an exact official-master partition for new or already trusted listings."""
        selected = official_frame.loc[
            official_frame["instrument_id"].astype(str).isin(instrument_ids)
        ].copy()
        if (
            len(selected) != len(instrument_ids)
            or selected["instrument_id"].astype(str).nunique() != len(instrument_ids)
        ):
            raise DailyPipelineBlocked(
                "new_instruments",
                "official new-instrument master is missing or duplicated",
                {"instrument_ids": instrument_ids},
            )
        required = (
            "exchange", "local_code", "asset_type", "name", "currency",
            "listed_date", "board", "buy_lot", "price_tick",
        )
        invalid_fields: dict[str, tuple[str, ...]] = {}
        invalid_dates: dict[str, str] = {}
        for row in selected.to_dict("records"):
            instrument_id = str(row["instrument_id"])
            missing_fields = tuple(
                field for field in required
                if row.get(field) is None
                or pd.isna(row.get(field))
                or not str(row.get(field)).strip()
            )
            if missing_fields:
                invalid_fields[instrument_id] = missing_fields
                continue
            try:
                listed = date.fromisoformat(str(row["listed_date"]))
            except ValueError:
                invalid_dates[instrument_id] = str(row["listed_date"])
                continue
            if listed > end or (
                listed < start and trusted_predecessor_snapshot_id is None
            ):
                invalid_dates[instrument_id] = listed.isoformat()
        if invalid_fields or invalid_dates:
            raise DailyPipelineBlocked(
                "new_instruments",
                "new-instrument metadata is outside the exact onboarding scope",
                {
                    "invalid_fields": invalid_fields,
                    "invalid_listed_dates": invalid_dates,
                    "onboarding_start": start.isoformat(),
                    "onboarding_end": end.isoformat(),
                },
            )

        build_kwargs: dict[str, str] = {
            "universe_observation_id": universe_observation_id,
        }
        if trusted_predecessor_snapshot_id is not None:
            build_kwargs["trusted_predecessor_snapshot_id"] = (
                trusted_predecessor_snapshot_id
            )
        result = builder.build(
            HistoryBuildSpec(
                start_date=start,
                end_date=end,
                universe_as_of=end,
                instrument_ids=instrument_ids,
                exchanges=("SH", "SZ"),
                asset_types=("stock", "etf"),
                batch_size=min(self.settings.daily.batch_size, len(instrument_ids)),
                publish=False,
            ),
            **build_kwargs,
        )
        report = json.loads(Path(result.report).read_text(encoding="utf-8"))
        included = tuple(sorted(map(str, report.get("included_instrument_ids", ()))))
        excluded = report.get("excluded", {})
        if (
            result.snapshot_id is None
            or included != tuple(sorted(instrument_ids))
            or excluded
        ):
            raise DailyPipelineBlocked(
                "new_instruments",
                "new-instrument evidence did not produce an exact reconciled partition",
                {
                    "instrument_ids": instrument_ids,
                    "included": included,
                    "excluded": excluded,
                    "blockers": result.blockers,
                },
            )
        partitions = tuple(map(str, report.get("canonical_observation_ids", ())))
        if not partitions:
            raise DailyPipelineBlocked(
                "new_instruments",
                "new-instrument partition has no canonical observation evidence",
                {"instrument_ids": instrument_ids},
            )
        return result.snapshot_id, partitions, {
            "instrument_ids": instrument_ids,
            "build_id": result.build_id,
            "snapshot_id": result.snapshot_id,
            "canonical_observation_ids": partitions,
            "universe_observation_id": universe_observation_id,
            "trusted_predecessor_snapshot_id": trusted_predecessor_snapshot_id,
        }

    def _record_no_trade(
        self,
        *,
        predecessor_snapshot_id: str,
        universe_observation_id: str,
        calendar_observation_id: str,
        start: date,
        end: date,
        instrument_ids: tuple[str, ...],
        quarantine_reasons: dict[str, list[str]],
        universe_size: int,
    ) -> _NoTradeResolution:
        existing = find_no_trade_research_partition(
            self.warehouse,
            predecessor_snapshot_id=predecessor_snapshot_id,
            universe_observation_id=universe_observation_id,
            calendar_observation_id=calendar_observation_id,
            start_date=start,
            end_date=end,
            instrument_ids=instrument_ids,
        )
        if existing is not None:
            return _NoTradeResolution(existing.observation_id, instrument_ids, {})
        source_ids = []
        request = ProviderRequest(
            ProviderCapability.DAILY_BARS_RAW, start, end, instrument_ids,
        )
        for provider in NO_TRADE_PROVIDERS:
            observed, _ = self.ingestion.capture_resumable(provider, request)
            source_ids.append(observed.observation_id)
        common = {
            "predecessor_snapshot_id": predecessor_snapshot_id,
            "universe_observation_id": universe_observation_id,
            "calendar_observation_id": calendar_observation_id,
            "source_observation_ids": tuple(source_ids),
            "start_date": start,
            "end_date": end,
        }
        try:
            manifest = record_no_trade_research_partition(
                self.warehouse, **common, instrument_ids=instrument_ids,
            )
        except NoTradeSourceActiveError as exc:
            unexpected = set(exc.active_observation_ids) - set(instrument_ids)
            if unexpected:
                raise ValueError(
                    f"No-trade active evidence escaped requested scope: {sorted(unexpected)}"
                ) from exc
            active_conflicts = dict(exc.active_observation_ids)
            for instrument_id, observation_ids in active_conflicts.items():
                quarantine_reasons.setdefault(instrument_id, []).append(
                    "no_trade:active_source_rows:"
                    + canonical_json({"observation_ids": observation_ids})
                )
            confirmed = tuple(sorted(set(instrument_ids) - set(active_conflicts)))
            if not confirmed:
                return _NoTradeResolution(None, (), active_conflicts)
            existing = find_no_trade_research_partition(
                self.warehouse,
                predecessor_snapshot_id=predecessor_snapshot_id,
                universe_observation_id=universe_observation_id,
                calendar_observation_id=calendar_observation_id,
                start_date=start,
                end_date=end,
                instrument_ids=confirmed,
            )
            if existing is None:
                existing = record_no_trade_research_partition(
                    self.warehouse, **common, instrument_ids=confirmed,
                )
            return _NoTradeResolution(
                existing.observation_id, confirmed, active_conflicts,
            )
        return _NoTradeResolution(manifest.observation_id, instrument_ids, {})

    @staticmethod
    def _add_quarantine_reasons(
        target: dict[str, list[str]],
        instrument_ids: Any,
        *,
        source: str,
        blockers: Any,
    ) -> None:
        detail = tuple(sorted({str(item) for item in blockers if str(item).strip()}))
        rendered = f"{source}:{canonical_json(detail)}"
        for instrument_id in sorted(set(map(str, instrument_ids))):
            target.setdefault(instrument_id, []).append(rendered)

    def _finalize_quarantine(
        self,
        *,
        predecessor,
        official_frame: pd.DataFrame,
        calendar_frame: pd.DataFrame,
        increment_start: date,
        target: date,
        reasons: Mapping[str, list[str]],
        universe_size: int,
    ) -> Mapping[str, Any] | None:
        if not reasons:
            return None
        instrument_ids = tuple(sorted(reasons))
        prior = self.warehouse.query_loaded_snapshot_table(
            predecessor,
            MarketTable.DAILY_BARS,
            instrument_ids=instrument_ids,
            start_date=predecessor.plan.universe_scope.history_end - timedelta(days=45),
            end_date=predecessor.plan.universe_scope.history_end,
            price_mode="raw",
        )
        prior_counts: dict[str, int] = {}
        for instrument_id in instrument_ids:
            rows = prior.loc[
                prior["instrument_id"].astype(str).eq(instrument_id)
            ].sort_values("session_date", ascending=False, kind="stable")
            count = 0
            for rule in map(str, rows["trade_rule_id"]):
                if rule != DATA_GAP_QUARANTINE_RULE_ID:
                    break
                count += 1
            prior_counts[instrument_id] = count

        indexed = official_frame.set_index("instrument_id", drop=False)
        open_by_exchange = {
            str(exchange): tuple(sorted(map(str, group.loc[
                group["is_open"].fillna(False).astype(bool), "session_date",
            ])))
            for exchange, group in calendar_frame.groupby("exchange")
        }
        consecutive: dict[str, int] = {}
        increment_sessions: dict[str, tuple[str, ...]] = {}
        for instrument_id in instrument_ids:
            row = indexed.loc[instrument_id]
            listed = str(row["listed_date"])[:10]
            delisted = None if pd.isna(row.get("delisted_date")) else str(row["delisted_date"])[:10]
            sessions = tuple(
                session
                for session in open_by_exchange.get(str(row["exchange"]), ())
                if increment_start.isoformat() <= session <= target.isoformat()
                and session >= listed
                and (delisted is None or session <= delisted)
            )
            increment_sessions[instrument_id] = sessions
            consecutive[instrument_id] = prior_counts[instrument_id] + len(sessions)
        return {
            "policy": "daily-instrument-data-gap-quarantine-never-block-v2",
            "instrument_ids": instrument_ids,
            "instrument_count": len(instrument_ids),
            "universe_size": universe_size,
            "fraction": len(instrument_ids) / universe_size,
            "blocking_thresholds_enforced": False,
            "consecutive_sessions": dict(sorted(consecutive.items())),
            "increment_sessions": dict(sorted(increment_sessions.items())),
            "reasons_by_instrument": {
                item: tuple(sorted(set(reasons[item]))) for item in instrument_ids
            },
            "valuation_policy": "last_trusted_price_stale",
            "execution_policy": "prohibit_and_defer_pending_orders",
        }

    def _record_quarantined_research_partition(
        self,
        *,
        predecessor_snapshot_id: str,
        universe_observation_id: str,
        calendar_observation_id: str,
        official_frame: pd.DataFrame,
        start: date,
        end: date,
        instrument_ids: tuple[str, ...],
        reasons: Mapping[str, Any],
    ) -> str:
        request = ProviderRequest(
            ProviderCapability.CANONICAL_RECONCILIATION,
            start,
            end,
            instrument_ids,
            {
                "kind": "daily-instrument-data-gap-quarantine-research-v1",
                "predecessor_snapshot_id": predecessor_snapshot_id,
                "universe_observation_id": universe_observation_id,
                "calendar_observation_id": calendar_observation_id,
                "reasons": reasons,
            },
        )
        matches = self.warehouse.matching_observations(
            provider=DATA_GAP_QUARANTINE_PROVIDER,
            request=request,
        )
        if matches:
            return matches[-1].observation_id
        instruments = official_frame.loc[
            official_frame["instrument_id"].astype(str).isin(instrument_ids)
        ].copy()
        if set(map(str, instruments["instrument_id"])) != set(instrument_ids):
            raise DailyPipelineBlocked(
                "bars", "quarantine instrument master is incomplete",
                {"instrument_ids": instrument_ids},
            )
        bars = empty_table(MarketTable.DAILY_BARS, include_lineage=True)
        dependencies = tuple(sorted({
            universe_observation_id,
            calendar_observation_id,
        }))
        quality = {
            "validated": True,
            "validator_version": "daily-data-gap-quarantine-research-v1",
            "readiness": ReadinessProfile.RESEARCH_PRICE.value,
            "instrument_ids": instrument_ids,
            "start_date": start,
            "end_date": end,
            "universe_definition": CURRENT_SH_SZ_STOCK_ETF_UNIVERSE,
            "universe_as_of": end,
            "row_count": 0,
            "degraded": True,
            "reasons_by_instrument": reasons,
            "source_observation_ids": dependencies,
        }
        observed_at = max(
            self.warehouse.load_observation(item).observed_at for item in dependencies
        )
        manifest = self.warehouse.record_observation(ObservationPayload(
            DATA_GAP_QUARANTINE_PROVIDER,
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
                    instrument_ids=instrument_ids,
                    detail="Official current master retained under explicit data-gap quarantine",
                ),
                CoverageClaim(
                    MarketTable.DAILY_BARS,
                    True,
                    start,
                    end,
                    instrument_ids,
                    "No trusted price rows published; simulation materializes non-tradable quarantine",
                ),
            ),
            {
                "kind": "field_level_reconciliation",
                "reconciliation_ready": True,
                "partition_quality": quality,
                "degraded_quarantine": quality,
                "report": {"blockers": (), "unresolved_conflicts": ()},
            },
        ))
        return manifest.observation_id

    def _combine_partitions(
        self,
        *,
        main_snapshot_id: str,
        supplement_snapshot_ids: tuple[str, ...],
        no_trade_observation_id: str | None,
        no_trade_ids: tuple[str, ...],
        quarantine_observation_id: str | None,
        quarantine_ids: tuple[str, ...],
        target_ids: tuple[str, ...],
        start: date,
        end: date,
    ) -> str:
        main = self.warehouse.load_snapshot(main_snapshot_id)
        selections = [*main.plan.selections]
        for snapshot_id in supplement_snapshot_ids:
            supplement = self.warehouse.load_snapshot(snapshot_id)
            selections.extend(supplement.plan.selections)
        if no_trade_observation_id is not None:
            reason = "daily pipeline no-trade consensus for full-window suspensions"
            selections.extend((
                SourceSlice(
                    no_trade_observation_id, MarketTable.INSTRUMENTS, reason, no_trade_ids,
                ),
                SourceSlice(
                    no_trade_observation_id, MarketTable.DAILY_BARS, reason, no_trade_ids,
                    start, end,
                ),
            ))
        if quarantine_observation_id is not None:
            reason = "daily pipeline bounded instrument data-gap quarantine"
            selections.extend((
                SourceSlice(
                    quarantine_observation_id,
                    MarketTable.INSTRUMENTS,
                    reason,
                    quarantine_ids,
                ),
                SourceSlice(
                    quarantine_observation_id,
                    MarketTable.DAILY_BARS,
                    reason,
                    quarantine_ids,
                    start,
                    end,
                ),
            ))
        scope = UniverseScope(
            CURRENT_SH_SZ_STOCK_ETF_UNIVERSE,
            end,
            start,
            end,
            survivorship_bias=True,
            instrument_ids=target_ids,
        )
        combined = self.warehouse.build_partitioned_snapshot(SnapshotPlan(
            tuple(selections),
            f"daily pipeline research increment {start.isoformat()}..{end.isoformat()}",
            readiness=ReadinessProfile.RESEARCH_PRICE,
            universe_scope=scope,
        ))
        if not combined.quality.ready:
            raise DailyPipelineBlocked("research", "combined research increment is not ready", {
                "errors": combined.quality.errors,
            })
        return combined.snapshot_id

    def _reconcile_action_factor_evidence(
        self,
        *,
        instruments: pd.DataFrame,
        actions: pd.DataFrame,
        primary_factors: pd.DataFrame,
        bars: pd.DataFrame,
        calendar_frame: pd.DataFrame,
        increment_scope: UniverseScope,
    ) -> tuple[
        CorporateActionReconciliationResult,
        tuple[str, ...],
        Mapping[str, Any],
    ]:
        candidates = build_factor_audit_candidates(
            instruments=instruments,
            actions=actions,
            factors=primary_factors,
            universe_scope=increment_scope,
            daily_bars=bars,
        )
        audit_observation_ids: list[str] = []
        corroborating = empty_table(
            MarketTable.ADJUSTMENT_FACTORS, include_lineage=True,
        )
        baostock_reused = False
        adjusted_reused: dict[str, bool] = {}
        adjusted_candidate_count = 0
        if candidates:
            candidate_ids = tuple(sorted(candidates))
            baostock_request = ProviderRequest(
                ProviderCapability.ADJUSTMENT_FACTORS,
                increment_scope.history_start,
                increment_scope.history_end,
                candidate_ids,
            )
            baostock, baostock_reused = self.ingestion.capture_resumable(
                "baostock", baostock_request,
            )
            baostock_factors = self.warehouse.read_observation_table(
                baostock.observation_id, MarketTable.ADJUSTMENT_FACTORS,
            )
            audit_observation_ids.append(baostock.observation_id)
            unresolved = self._unmatched_factor_candidates(
                candidates, baostock_factors,
            )
            corroborating = baostock_factors
            if unresolved:
                prior_open = tuple(sorted({
                    str(value) for value in calendar_frame.loc[
                        calendar_frame["is_open"].fillna(False).astype(bool)
                        & calendar_frame["session_date"].astype(str).lt(
                            increment_scope.history_start.isoformat()
                        ),
                        "session_date",
                    ]
                }))
                if not prior_open:
                    raise SnapshotNotReadyError(
                        "Adjusted-price factor audit has no prior open session"
                    )
                audit_start = date.fromisoformat(prior_open[-1])
                audit_ids = tuple(sorted(unresolved))
                request_parameters = {
                    "batch_size": min(self.settings.daily.batch_size, 100),
                }
                raw, raw_reused = self.ingestion.capture_resumable(
                    "tickflow",
                    ProviderRequest(
                        ProviderCapability.DAILY_BARS_RAW,
                        audit_start,
                        increment_scope.history_end,
                        audit_ids,
                        request_parameters,
                    ),
                )
                adjusted, adjusted_was_reused = self.ingestion.capture_resumable(
                    "tickflow",
                    ProviderRequest(
                        ProviderCapability.DAILY_BARS_ADJUSTED,
                        audit_start,
                        increment_scope.history_end,
                        audit_ids,
                        request_parameters,
                    ),
                )
                adjusted_reused = {
                    "raw": raw_reused,
                    "adjusted": adjusted_was_reused,
                }
                factor_rows = self._derive_adjusted_price_factor_rows(
                    raw_bars=self.warehouse.read_observation_table(
                        raw.observation_id, MarketTable.DAILY_BARS,
                    ),
                    adjusted_bars=self.warehouse.read_observation_table(
                        adjusted.observation_id, MarketTable.DAILY_BARS,
                    ),
                    candidates=unresolved,
                    raw_observation_id=raw.observation_id,
                    adjusted_observation_id=adjusted.observation_id,
                )
                adjusted_candidate_count = sum(map(len, unresolved.values()))
                dependency_ids = {
                    baostock.observation_id,
                    raw.observation_id,
                    adjusted.observation_id,
                }
                for frame in (actions, primary_factors, bars):
                    if "source_observation_id" not in frame:
                        continue
                    relevant = frame.loc[
                        frame["instrument_id"].astype(str).isin(audit_ids),
                        "source_observation_id",
                    ].dropna()
                    dependency_ids.update(
                        str(value) for value in relevant
                        if str(value).strip() not in {"", "<NA>", "None"}
                    )
                input_ids = tuple(sorted(dependency_ids))
                quality = {
                    "validated": True,
                    "validator_version": FACTOR_AUDIT_VERSION,
                    "audit_start": audit_start,
                    "start_date": increment_scope.history_start,
                    "end_date": increment_scope.history_end,
                    "factor_candidates": {
                        key: dict(sorted(value.items()))
                        for key, value in sorted(unresolved.items())
                    },
                    "input_observation_ids": input_ids,
                    "row_count": len(factor_rows),
                }
                audit = self.warehouse.record_observation(ObservationPayload(
                    FACTOR_AUDIT_PROVIDER,
                    max(
                        self.warehouse.load_observation(item).observed_at
                        for item in input_ids
                    ),
                    ProviderRequest(
                        ProviderCapability.CANONICAL_RECONCILIATION,
                        increment_scope.history_start,
                        increment_scope.history_end,
                        audit_ids,
                        {
                            "kind": "adjusted-price-factor-audit",
                            "validator_version": FACTOR_AUDIT_VERSION,
                            "audit_start": audit_start,
                            "input_observation_ids": input_ids,
                        },
                    ),
                    {MarketTable.ADJUSTMENT_FACTORS: factor_rows},
                    (CoverageClaim(
                        MarketTable.ADJUSTMENT_FACTORS,
                        True,
                        increment_scope.history_start,
                        increment_scope.history_end,
                        audit_ids,
                        "TickFlow raw/forward-adjusted ratio shift matches official action",
                    ),),
                    {
                        "kind": "field_level_reconciliation",
                        "reconciliation_ready": True,
                        "factor_audit_quality": quality,
                        "input_observation_ids": input_ids,
                    },
                ))
                audit_observation_ids.append(audit.observation_id)
                tickflow_factors = self.warehouse.read_observation_table(
                    audit.observation_id, MarketTable.ADJUSTMENT_FACTORS,
                )
                corroborating = pd.concat(
                    (corroborating, tickflow_factors), ignore_index=True,
                )

        reconciled = reconcile_corporate_action_factors(
            instruments=instruments,
            actions=actions,
            factors=primary_factors,
            corroborating_factors=corroborating,
            universe_scope=increment_scope,
            daily_bars=bars,
        )
        detail = {
            "candidate_events": sum(map(len, candidates.values())),
            "baostock_observation_id": (
                audit_observation_ids[0] if candidates else None
            ),
            "baostock_reused": baostock_reused,
            "adjusted_price_candidates": adjusted_candidate_count,
            "adjusted_price_reused": adjusted_reused,
            "audit_observation_ids": tuple(audit_observation_ids),
            "reconciled_actions": len(reconciled.actions),
            "reconciled_factors": len(reconciled.factors),
            "evidence_hash": reconciled.evidence_hash,
        }
        return reconciled, tuple(audit_observation_ids), detail

    @staticmethod
    def _unmatched_factor_candidates(
        candidates: Mapping[str, Mapping[str, float]],
        factors: pd.DataFrame,
    ) -> dict[str, dict[str, float]]:
        rows = factors.to_dict("records")
        unresolved: dict[str, dict[str, float]] = {}
        for instrument_id, events in candidates.items():
            for event_date, expected in events.items():
                matched = False
                for row in rows:
                    if str(row.get("instrument_id")) != instrument_id:
                        continue
                    try:
                        factor_date = date.fromisoformat(
                            str(row.get("effective_date"))[:10]
                        )
                        multiplier = float(row.get("price_multiplier"))
                    except (TypeError, ValueError):
                        continue
                    distance = abs(
                        (factor_date - date.fromisoformat(event_date)).days
                    )
                    if (
                        isfinite(multiplier)
                        and multiplier > 0
                        and distance <= 31
                        and abs(multiplier - expected) / expected <= 0.03
                    ):
                        matched = True
                        break
                if not matched:
                    unresolved.setdefault(instrument_id, {})[event_date] = expected
        return unresolved

    @staticmethod
    def _derive_adjusted_price_factor_rows(
        *,
        raw_bars: pd.DataFrame,
        adjusted_bars: pd.DataFrame,
        candidates: Mapping[str, Mapping[str, float]],
        raw_observation_id: str,
        adjusted_observation_id: str,
    ) -> pd.DataFrame:
        keys = ["instrument_id", "session_date"]
        raw = raw_bars.loc[
            raw_bars["price_mode"].astype(str).eq("raw"),
            [*keys, "close"],
        ].rename(columns={"close": "raw_close"})
        adjusted = adjusted_bars.loc[
            adjusted_bars["price_mode"].astype(str).eq("adjusted"),
            [*keys, "close"],
        ].rename(columns={"close": "adjusted_close"})
        if raw.duplicated(keys).any() or adjusted.duplicated(keys).any():
            raise SnapshotNotReadyError(
                "Adjusted-price factor audit source has duplicate daily keys"
            )
        joined = raw.merge(adjusted, on=keys, how="inner", validate="one_to_one")
        joined["raw_close"] = pd.to_numeric(joined["raw_close"], errors="coerce")
        joined["adjusted_close"] = pd.to_numeric(
            joined["adjusted_close"], errors="coerce",
        )
        joined = joined.loc[
            joined[["raw_close", "adjusted_close"]].notna().all(axis=1)
            & joined["raw_close"].gt(0)
            & joined["adjusted_close"].gt(0)
        ].copy()
        joined["adjustment_ratio"] = (
            joined["adjusted_close"] / joined["raw_close"]
        )
        rows: list[dict[str, Any]] = []
        failures: list[str] = []
        for instrument_id, events in sorted(candidates.items()):
            instrument = joined.loc[
                joined["instrument_id"].astype(str).eq(instrument_id)
            ].sort_values("session_date", kind="stable")
            for event_date, expected in sorted(events.items()):
                before = instrument.loc[
                    instrument["session_date"].astype(str).lt(event_date)
                ].tail(1)
                event = instrument.loc[
                    instrument["session_date"].astype(str).eq(event_date)
                ]
                if before.empty or len(event) != 1:
                    failures.append(f"{instrument_id}/{event_date}:missing_ratio_boundary")
                    continue
                prior_ratio = float(before.iloc[0]["adjustment_ratio"])
                event_ratio = float(event.iloc[0]["adjustment_ratio"])
                multiplier = prior_ratio / event_ratio
                relative = abs(multiplier - expected) / expected
                if (
                    not isfinite(multiplier)
                    or multiplier <= 0
                    or relative > 0.03
                ):
                    failures.append(
                        f"{instrument_id}/{event_date}:expected={expected:.12g},"
                        f"observed={multiplier:.12g},relative={relative:.6g}"
                    )
                    continue
                evidence = {
                    "provider": "tickflow",
                    "raw_observation_id": raw_observation_id,
                    "adjusted_observation_id": adjusted_observation_id,
                    "prior_session": str(before.iloc[0]["session_date"])[:10],
                    "event_session": event_date,
                    "prior_adjusted_to_raw_ratio": prior_ratio,
                    "event_adjusted_to_raw_ratio": event_ratio,
                    "observed_price_multiplier": multiplier,
                    "expected_action_multiplier": expected,
                    "relative_difference": relative,
                }
                rows.append({
                    "factor_id": "factor-" + stable_digest({
                        "kind": FACTOR_AUDIT_VERSION,
                        "instrument_id": instrument_id,
                        "event_date": event_date,
                        "evidence": evidence,
                    })[:24],
                    "instrument_id": instrument_id,
                    "effective_date": event_date,
                    "known_date": event_date,
                    "price_multiplier": multiplier,
                    "field_lineage": canonical_json({
                        "kind": FACTOR_AUDIT_VERSION,
                        "semantics": "forward-adjusted/raw ratio shift at official event",
                    }),
                    "source_payload": canonical_json({
                        "raw": {},
                        "adjusted_price_audit": evidence,
                    }),
                })
        if failures:
            raise SnapshotNotReadyError(
                "Adjusted-price factor audit failed: " + "; ".join(failures[:10])
            )
        return pd.DataFrame(
            rows,
            columns=empty_table(
                MarketTable.ADJUSTMENT_FACTORS, include_lineage=True,
            ).columns,
        )

    def _compose_candidate(
        self,
        *,
        research_snapshot_id: str,
        calendar_frame: pd.DataFrame,
        increment_scope: UniverseScope,
        status_results: Mapping[str, Any],
        evidence_results: Mapping[str, Any],
        build_partition_ids: tuple[str, ...],
        no_trade_observation_id: str | None,
        universe_observation_id: str,
        direct_limit_observation_ids: tuple[str, ...],
        execution_guard_ids: tuple[str, ...],
        all_date_execution_guard_ids: tuple[str, ...],
        target_only_execution_guard_ids: tuple[str, ...],
        execution_guard_reasons: Mapping[str, tuple[str, ...]],
        evidence_guard_ids: tuple[str, ...],
        quarantine_detail: Mapping[str, Any] | None,
    ) -> tuple[str, Mapping[str, Any]]:
        research = self.warehouse.load_snapshot(research_snapshot_id)
        instruments = self.warehouse.query_loaded_snapshot_table(
            research, MarketTable.INSTRUMENTS,
        )
        quarantine_ids = tuple(
            map(str, (quarantine_detail or {}).get("instrument_ids", ()))
        )
        guard_reasons = {
            str(item): tuple(map(str, values))
            for item, values in execution_guard_reasons.items()
        }
        available_ids = tuple(sorted(
            set(increment_scope.instrument_ids) - set(quarantine_ids)
        ))
        if not available_ids:
            raise DailyPipelineBlocked(
                "quarantine", "instrument data gaps cover the whole daily universe"
            )
        execution_guard = set(map(str, execution_guard_ids)) & set(available_ids)
        all_date_guard = (
            set(map(str, all_date_execution_guard_ids)) & set(available_ids)
        )
        target_only_guard = (
            set(map(str, target_only_execution_guard_ids)) & set(available_ids)
        )
        modeled_ids = tuple(sorted(set(available_ids) - all_date_guard))
        research_bars = self.warehouse.query_loaded_snapshot_table(
            research,
            MarketTable.DAILY_BARS,
            start_date=increment_scope.history_start,
            end_date=increment_scope.history_end,
            price_mode="raw",
        )
        research_bars = research_bars.loc[
            research_bars["instrument_id"].astype(str).isin(available_ids)
        ].reset_index(drop=True)
        dense_status = self._concat_observation_tables(
            status_results["xtquant"].observation_ids, MarketTable.DAILY_BARS,
        )
        stock_st = self._concat_observation_tables(
            status_results["baostock"].observation_ids, MarketTable.DAILY_BARS,
        )
        calendar_window = calendar_frame.loc[
            calendar_frame["session_date"].astype(str).between(
                increment_scope.history_start.isoformat(),
                increment_scope.history_end.isoformat(),
            )
        ].reset_index(drop=True)
        if modeled_ids:
            modeled_scope = UniverseScope(
                increment_scope.definition,
                increment_scope.as_of_date,
                increment_scope.history_start,
                increment_scope.history_end,
                survivorship_bias=increment_scope.survivorship_bias,
                instrument_ids=modeled_ids,
            )
            modeled_instruments = instruments.loc[
                instruments["instrument_id"].astype(str).isin(modeled_ids)
            ].reset_index(drop=True)
            modeled_research = research_bars.loc[
                research_bars["instrument_id"].astype(str).isin(modeled_ids)
            ].reset_index(drop=True)
            dense_status = dense_status.loc[
                dense_status["instrument_id"].astype(str).isin(modeled_ids)
            ].reset_index(drop=True)
            stock_st = stock_st.loc[
                stock_st["instrument_id"].astype(str).isin(modeled_ids)
            ].reset_index(drop=True)
            status_bars = reconcile_simulation_status(
                instruments=modeled_instruments,
                research_bars=modeled_research,
                dense_status_bars=dense_status,
                stock_st_bars=stock_st,
                calendar=calendar_window,
                universe_scope=modeled_scope,
            )
            bars = build_dense_simulation_bars(
                instruments=modeled_instruments,
                research_bars=modeled_research,
                status_bars=status_bars,
                calendar=calendar_window,
                universe_scope=modeled_scope,
            )
        else:
            bars = empty_table(MarketTable.DAILY_BARS, include_lineage=True)
        if all_date_guard:
            guarded_bars = self._build_execution_guard_bars(
                instruments=instruments,
                research_bars=research_bars,
                calendar=calendar_window,
                scope=increment_scope,
                instrument_ids=tuple(sorted(all_date_guard)),
                reasons=guard_reasons,
                source_observation_id=universe_observation_id,
            )
            bars = pd.concat((bars, guarded_bars), ignore_index=True)
            bars = bars.sort_values(
                ["instrument_id", "session_date", "price_mode"], kind="stable",
            ).reset_index(drop=True)
        if target_only_guard:
            bars = self._mark_execution_guard_bars(
                bars,
                instrument_ids=target_only_guard,
                reasons={item: guard_reasons.get(item, ()) for item in target_only_guard},
                session_dates={increment_scope.history_end.isoformat()},
            )
        if quarantine_ids:
            quarantine_bars = self._build_quarantine_bars(
                instruments=instruments,
                calendar=calendar_window,
                scope=increment_scope,
                quarantine_detail=quarantine_detail or {},
                source_observation_id=universe_observation_id,
            )
            bars = pd.concat((bars, quarantine_bars), ignore_index=True)
            bars = bars.sort_values(
                ["instrument_id", "session_date", "price_mode"], kind="stable",
            ).reset_index(drop=True)
        action_gap_ids = self._action_gap_instrument_ids(quarantine_detail)
        event_ids = tuple(sorted(
            set(increment_scope.instrument_ids)
            - action_gap_ids
            - set(map(str, evidence_guard_ids))
        ))
        raw_actions = self._concat_observation_tables(
            (
                *evidence_results["stock-actions"].observation_ids,
                *evidence_results["etf-actions"].observation_ids,
            ),
            MarketTable.CORPORATE_ACTIONS,
        )
        raw_factors = self._concat_observation_tables(
            evidence_results["factors"].observation_ids,
            MarketTable.ADJUSTMENT_FACTORS,
        )
        event_evidence_guard = set(map(str, evidence_guard_ids))
        remaining_event_ids = set(event_ids)
        factor_failures: list[Mapping[str, Any]] = []
        while remaining_event_ids:
            active_event_ids = tuple(sorted(remaining_event_ids))
            event_scope = UniverseScope(
                increment_scope.definition,
                increment_scope.as_of_date,
                increment_scope.history_start,
                increment_scope.history_end,
                survivorship_bias=increment_scope.survivorship_bias,
                instrument_ids=active_event_ids,
            )
            event_instruments = instruments.loc[
                instruments["instrument_id"].astype(str).isin(active_event_ids)
            ].reset_index(drop=True)
            event_bars = bars.loc[
                bars["instrument_id"].astype(str).isin(active_event_ids)
            ].reset_index(drop=True)
            actions = self._scoped_events(
                raw_actions, date_column="ex_date", scope=event_scope,
            )
            factors = self._scoped_events(
                raw_factors, date_column="effective_date", scope=event_scope,
            )
            try:
                reconciled, factor_audit_observation_ids, factor_detail = (
                    self._reconcile_action_factor_evidence(
                        instruments=event_instruments,
                        actions=actions,
                        primary_factors=factors,
                        bars=event_bars,
                        calendar_frame=calendar_frame,
                        increment_scope=event_scope,
                    )
                )
                break
            except Exception as exc:
                error = f"{type(exc).__name__}:{str(exc)[:1000]}"
                mentioned = set(re.findall(r"\b\d{6}\.(?:SH|SZ)\b", error))
                affected = mentioned & remaining_event_ids
                if not affected:
                    try:
                        affected = set(build_factor_audit_candidates(
                            instruments=event_instruments,
                            actions=actions,
                            factors=factors,
                            universe_scope=event_scope,
                            daily_bars=event_bars,
                        ))
                    except Exception:
                        affected = set()
                    affected &= remaining_event_ids
                if not affected:
                    affected = set(remaining_event_ids)
                for instrument_id in affected:
                    guard_reasons[instrument_id] = (
                        *guard_reasons.get(instrument_id, ()),
                        f"factors:{error}",
                    )
                execution_guard.update(affected)
                event_evidence_guard.update(affected)
                bars = self._mark_execution_guard_bars(
                    bars,
                    instrument_ids=affected,
                    reasons={item: guard_reasons[item] for item in affected},
                )
                factor_failures.append({
                    "error": error,
                    "instrument_ids": tuple(sorted(affected)),
                })
                remaining_event_ids.difference_update(affected)
        else:
            actions = empty_table(MarketTable.CORPORATE_ACTIONS, include_lineage=True)
            factors = empty_table(MarketTable.ADJUSTMENT_FACTORS, include_lineage=True)
            report = {
                "policy": "corporate-action-factor-r2-v1",
                "status": "degraded_no_trusted_event_scope",
                "instrument_ids": (),
                "action_count": 0,
                "factor_count": 0,
                "failures": tuple(factor_failures),
            }
            reconciled = CorporateActionReconciliationResult(
                actions, factors, stable_digest(report), report,
            )
            factor_audit_observation_ids = ()
            factor_detail = {
                "status": "degraded",
                "failures": tuple(factor_failures),
                "guarded_instruments": len(event_evidence_guard),
                "audit_observation_ids": (),
                "reconciled_actions": 0,
                "reconciled_factors": 0,
                "evidence_hash": reconciled.evidence_hash,
            }
        if factor_failures and remaining_event_ids:
            factor_detail = {
                **dict(factor_detail),
                "status": "degraded",
                "failures": tuple(factor_failures),
                "guarded_instruments": len(event_evidence_guard),
            }
        actions = reconciled.actions
        factors = reconciled.factors
        input_ids = tuple(sorted({
            *build_partition_ids,
            *(item for item in (no_trade_observation_id,) if item),
            *status_results["xtquant"].observation_ids,
            *status_results["baostock"].observation_ids,
            *evidence_results["stock-actions"].observation_ids,
            *evidence_results["etf-actions"].observation_ids,
            *evidence_results["factors"].observation_ids,
            *factor_audit_observation_ids,
            *direct_limit_observation_ids,
            universe_observation_id,
        }))
        observed_at = max(
            self.warehouse.load_observation(item).observed_at for item in input_ids
        )
        description = (
            "daily pipeline simulation increment candidate "
            f"{increment_scope.history_start.isoformat()}..{increment_scope.history_end.isoformat()}"
        )
        if quarantine_ids:
            description += f" degraded_quarantine={len(quarantine_ids)}"
        if execution_guard:
            description += f" execution_evidence_guard={len(execution_guard)}"
        complete_event_ids = tuple(sorted(
            set(increment_scope.instrument_ids) - event_evidence_guard
        ))
        incomplete_event_ids = tuple(sorted(event_evidence_guard))
        claims: list[CoverageClaim] = [
            CoverageClaim(
                MarketTable.INSTRUMENTS,
                True,
                instrument_ids=increment_scope.instrument_ids,
                detail=f"Composed by {PIPELINE_VERSION}",
            ),
            CoverageClaim(
                MarketTable.DAILY_BARS,
                True,
                increment_scope.history_start,
                increment_scope.history_end,
                increment_scope.instrument_ids,
                f"Composed by {PIPELINE_VERSION}",
            ),
        ]
        for table in (
            MarketTable.CORPORATE_ACTIONS,
            MarketTable.ADJUSTMENT_FACTORS,
        ):
            if complete_event_ids:
                claims.append(CoverageClaim(
                    table,
                    True,
                    increment_scope.history_start,
                    increment_scope.history_end,
                    complete_event_ids,
                    f"Complete trusted event scope composed by {PIPELINE_VERSION}",
                ))
            if incomplete_event_ids:
                claims.append(CoverageClaim(
                    table,
                    False,
                    increment_scope.history_start,
                    increment_scope.history_end,
                    incomplete_event_ids,
                    "Auxiliary event evidence unresolved; execution prohibited",
                ))
        auxiliary_evidence_gap = {
            "instrument_ids": incomplete_event_ids,
            "start_date": increment_scope.history_start,
            "end_date": increment_scope.history_end,
            "tables": (
                MarketTable.CORPORATE_ACTIONS.value,
                MarketTable.ADJUSTMENT_FACTORS.value,
            ),
            "reasons_by_instrument": {
                item: tuple(guard_reasons.get(item, ()))
                for item in incomplete_event_ids
            },
        } if incomplete_event_ids else None
        payload = ObservationPayload(
            f"canonical-reconciler-{PIPELINE_VERSION}",
            observed_at,
            ProviderRequest(
                ProviderCapability.CANONICAL_RECONCILIATION,
                increment_scope.history_start,
                increment_scope.history_end,
                increment_scope.instrument_ids,
                {
                    "description": description,
                    "input_observation_ids": input_ids,
                    "pipeline": PIPELINE_VERSION,
                },
            ),
            {
                MarketTable.INSTRUMENTS: instruments,
                MarketTable.DAILY_BARS: bars,
                MarketTable.CORPORATE_ACTIONS: actions,
                MarketTable.ADJUSTMENT_FACTORS: factors,
            },
            tuple(claims),
            {
                "kind": "field_level_reconciliation",
                "reconciliation_ready": True,
                "description": description,
                "report": {
                    "blockers": (),
                    "unresolved_conflicts": (),
                    "input_observation_ids": input_ids,
                    "corporate_action_reconciliation": to_primitive(
                        reconciled.report
                    ),
                    "corporate_action_evidence_hash": reconciled.evidence_hash,
                    "degraded_quarantine": to_primitive(quarantine_detail),
                    "execution_evidence_guard": {
                        "instrument_ids": tuple(sorted(execution_guard)),
                        "rule_id": EXECUTION_EVIDENCE_GAP_RULE_ID,
                        "reasons_by_instrument": {
                            item: tuple(guard_reasons.get(item, ()))
                            for item in sorted(execution_guard)
                        },
                    },
                    "degraded_auxiliary_evidence": auxiliary_evidence_gap,
                    "non_blocking_degradations": (
                        ("auxiliary_event_evidence_incomplete",)
                        if incomplete_event_ids else ()
                    ),
                },
                "degraded_quarantine": to_primitive(quarantine_detail),
                "execution_evidence_guard": {
                    "instrument_ids": tuple(sorted(execution_guard)),
                    "rule_id": EXECUTION_EVIDENCE_GAP_RULE_ID,
                    "reasons_by_instrument": {
                        item: tuple(guard_reasons.get(item, ()))
                        for item in sorted(execution_guard)
                    },
                },
                "degraded_auxiliary_evidence": auxiliary_evidence_gap,
            },
        )
        return self.warehouse.record_observation(payload).observation_id, {
            **dict(factor_detail),
            "degraded_quarantine": to_primitive(quarantine_detail),
        }

    @staticmethod
    def _build_quarantine_bars(
        *,
        instruments: pd.DataFrame,
        calendar: pd.DataFrame,
        scope: UniverseScope,
        quarantine_detail: Mapping[str, Any],
        source_observation_id: str,
    ) -> pd.DataFrame:
        ids = tuple(map(str, quarantine_detail.get("instrument_ids", ())))
        counts = quarantine_detail.get("consecutive_sessions", {})
        reasons = quarantine_detail.get("reasons_by_instrument", {})
        instrument_rows = instruments.loc[
            instruments["instrument_id"].astype(str).isin(ids)
        ].set_index("instrument_id", drop=False)
        open_by_exchange = {
            str(exchange): tuple(sorted(map(str, group.loc[
                group["is_open"].fillna(False).astype(bool), "session_date",
            ])))
            for exchange, group in calendar.groupby("exchange")
        }
        records: list[dict[str, Any]] = []
        for instrument_id in ids:
            row = instrument_rows.loc[instrument_id]
            listed = str(row["listed_date"])[:10]
            delisted = None if pd.isna(row.get("delisted_date")) else str(row["delisted_date"])[:10]
            for session in open_by_exchange.get(str(row["exchange"]), ()):
                if not scope.history_start.isoformat() <= session <= scope.history_end.isoformat():
                    continue
                if session < listed or (delisted is not None and session > delisted):
                    continue
                lineage = {
                    "kind": "daily_instrument_data_gap_quarantine_v1",
                    "policy": {
                        "name": "daily-instrument-data-gap-quarantine-never-block-v2",
                        "blocking_thresholds_enforced": False,
                    },
                    "consecutive_sessions": counts.get(instrument_id),
                    "reasons": reasons.get(instrument_id, ()),
                    "valuation": "last_trusted_price_only",
                    "execution": "prohibited_and_deferred",
                }
                records.append({
                    "instrument_id": instrument_id,
                    "session_date": session,
                    "price_mode": "raw",
                    "open": pd.NA,
                    "high": pd.NA,
                    "low": pd.NA,
                    "close": pd.NA,
                    "volume": 0,
                    "amount": pd.NA,
                    # A quarantine row means tradability is unresolved.  It is
                    # not evidence that the instrument was suspended.
                    "suspended": pd.NA,
                    "is_st": pd.NA,
                    "trade_rule_id": DATA_GAP_QUARANTINE_RULE_ID,
                    "trade_rule_known_date": session,
                    "buy_lot": row["buy_lot"],
                    "quantity_step": row.get("quantity_step"),
                    "odd_lot_sell_all": row.get("odd_lot_sell_all"),
                    "price_tick": row["price_tick"],
                    "sell_delay_sessions": row.get("sell_delay_sessions"),
                    "price_limit_state": "unknown",
                    "previous_close": pd.NA,
                    "price_limit_ratio": pd.NA,
                    "limit_up": pd.NA,
                    "limit_down": pd.NA,
                    "field_lineage": canonical_json(lineage),
                    "source_payload": canonical_json(lineage),
                    "source_provider": DATA_GAP_QUARANTINE_PROVIDER,
                    "source_observation_id": source_observation_id,
                    "observed_at": pd.NA,
                })
        return pd.DataFrame(
            records,
            columns=empty_table(
                MarketTable.DAILY_BARS, include_lineage=True,
            ).columns,
        )

    @staticmethod
    def _build_execution_guard_bars(
        *,
        instruments: pd.DataFrame,
        research_bars: pd.DataFrame,
        calendar: pd.DataFrame,
        scope: UniverseScope,
        instrument_ids: tuple[str, ...],
        reasons: Mapping[str, tuple[str, ...]],
        source_observation_id: str,
    ) -> pd.DataFrame:
        """Keep every trustworthy research price while making execution impossible."""

        selected = instruments.loc[
            instruments["instrument_id"].astype(str).isin(instrument_ids)
        ].set_index("instrument_id", drop=False)
        research = research_bars.loc[
            research_bars["instrument_id"].astype(str).isin(instrument_ids)
            & research_bars["price_mode"].astype(str).eq("raw")
        ].copy()
        keys = ["instrument_id", "session_date"]
        if research.duplicated(keys).any():
            raise DailyPipelineBlocked(
                "research", "execution-guard research prices contain duplicate daily keys"
            )
        research_by_key = {
            (str(row["instrument_id"]), str(row["session_date"])[:10]): row
            for row in research.to_dict("records")
        }
        open_by_exchange = {
            str(exchange): tuple(sorted(map(str, group.loc[
                group["is_open"].fillna(False).astype(bool), "session_date",
            ])))
            for exchange, group in calendar.groupby("exchange")
        }
        records: list[dict[str, Any]] = []
        for instrument_id in instrument_ids:
            item = selected.loc[instrument_id]
            listed = str(item["listed_date"])[:10]
            delisted = (
                None if pd.isna(item.get("delisted_date"))
                else str(item["delisted_date"])[:10]
            )
            for session in open_by_exchange.get(str(item["exchange"]), ()):
                if not scope.history_start.isoformat() <= session <= scope.history_end.isoformat():
                    continue
                if session < listed or (delisted is not None and session > delisted):
                    continue
                upstream = research_by_key.get((instrument_id, session), {})
                row = dict(upstream)
                upstream_lineage = row.get("field_lineage")
                lineage = {
                    "kind": "daily_execution_evidence_gap_guard_v1",
                    "rule_id": EXECUTION_EVIDENCE_GAP_RULE_ID,
                    "reasons": tuple(reasons.get(instrument_id, ())),
                    "price_semantics": (
                        "trusted_research_price_preserved"
                        if upstream else "no_trusted_price_available"
                    ),
                    "execution": "prohibited_and_deferred",
                    "upstream_lineage": upstream_lineage,
                    "upstream_observation_id": row.get("source_observation_id"),
                }
                row.update({
                    "instrument_id": instrument_id,
                    "session_date": session,
                    "price_mode": "raw",
                    "volume": row.get("volume", 0) if upstream else 0,
                    "amount": row.get("amount", pd.NA),
                    "suspended": pd.NA,
                    "is_st": pd.NA,
                    "trade_rule_id": EXECUTION_EVIDENCE_GAP_RULE_ID,
                    "trade_rule_known_date": session,
                    "buy_lot": item["buy_lot"],
                    "quantity_step": item.get("quantity_step"),
                    "odd_lot_sell_all": item.get("odd_lot_sell_all"),
                    "price_tick": item["price_tick"],
                    "sell_delay_sessions": item.get("sell_delay_sessions"),
                    "price_limit_state": "unknown",
                    "previous_close": pd.NA,
                    "price_limit_ratio": pd.NA,
                    "limit_up": pd.NA,
                    "limit_down": pd.NA,
                    "field_lineage": canonical_json(lineage),
                    "source_payload": canonical_json(lineage),
                    "source_provider": row.get(
                        "source_provider", DATA_GAP_QUARANTINE_PROVIDER,
                    ),
                    "source_observation_id": row.get(
                        "source_observation_id", source_observation_id,
                    ),
                    "observed_at": row.get("observed_at", pd.NA),
                })
                for column in ("open", "high", "low", "close"):
                    row.setdefault(column, pd.NA)
                records.append(row)
        return pd.DataFrame(
            records,
            columns=empty_table(
                MarketTable.DAILY_BARS, include_lineage=True,
            ).columns,
        )

    @staticmethod
    def _mark_execution_guard_bars(
        bars: pd.DataFrame,
        *,
        instrument_ids: set[str],
        reasons: Mapping[str, tuple[str, ...]],
        session_dates: set[str] | None = None,
    ) -> pd.DataFrame:
        result = bars.copy(deep=True)
        mask = result["instrument_id"].astype(str).isin(instrument_ids)
        if session_dates is not None:
            mask &= result["session_date"].astype(str).isin(session_dates)
        result.loc[mask, "suspended"] = pd.NA
        result.loc[mask, "is_st"] = pd.NA
        result.loc[mask, "trade_rule_id"] = EXECUTION_EVIDENCE_GAP_RULE_ID
        result.loc[mask, "trade_rule_known_date"] = result.loc[mask, "session_date"]
        result.loc[mask, "price_limit_state"] = "unknown"
        result.loc[mask, [
            "previous_close", "price_limit_ratio", "limit_up", "limit_down",
        ]] = pd.NA
        for index in result.index[mask]:
            value = result.at[index, "field_lineage"]
            try:
                lineage = json.loads(str(value)) if str(value).strip() else {}
            except json.JSONDecodeError:
                lineage = {"upstream_lineage": str(value)}
            instrument_id = str(result.at[index, "instrument_id"])
            lineage["daily_execution_evidence_gap_guard_v1"] = {
                "rule_id": EXECUTION_EVIDENCE_GAP_RULE_ID,
                "reasons": tuple(reasons.get(instrument_id, ())),
                "execution": "prohibited_and_deferred",
            }
            result.at[index, "field_lineage"] = canonical_json(lineage)
        return result

    def _concat_observation_tables(
        self, observation_ids, table: MarketTable,
    ) -> pd.DataFrame:
        frames = []
        for observation_id in observation_ids:
            manifest = self.warehouse.load_observation(observation_id)
            if not any(item.table is table for item in manifest.files):
                continue
            frames.append(self.warehouse.read_observation_table(observation_id, table))
        if not frames:
            return empty_table(table, include_lineage=True)
        return pd.concat(frames, ignore_index=True)

    @staticmethod
    def _scoped_events(
        frame: pd.DataFrame, *, date_column: str, scope: UniverseScope,
    ) -> pd.DataFrame:
        if frame.empty:
            return frame
        keep = (
            frame["instrument_id"].astype(str).isin(scope.instrument_ids)
            & frame[date_column].astype(str).between(
                scope.history_start.isoformat(), scope.history_end.isoformat(),
            )
        )
        return frame.loc[keep].reset_index(drop=True)

    # ------------------------------------------------------------- accounts

    def _advance_accounts(self, stages: list[DailyStage]) -> list[dict[str, Any]]:
        configured = self.settings.daily.accounts
        if not configured:
            stages.append(DailyStage("accounts", "skipped", {"reason": "no accounts configured"}))
            return []
        market = CanonicalMarketData.open(self.settings.paths.market_data)
        repository = TradingRepository(self.settings.paths.trading_database)
        service = SimulationService(
            market_data=market,
            repository=repository,
            execution_policy=self.settings.execution_policy,
            risk_policy=self.settings.risk_policy,
            fee_schedule=self.settings.fee_schedule,
        )
        scope = market.manifest.plan.universe_scope
        if scope is None:
            raise DailyPipelineBlocked("accounts", "published snapshot has no universe scope")
        # The snapshot calendar carries exchange-announced future sessions, so
        # an intent decided exactly at the published data head can schedule its
        # T+1 order; tomorrow's publication executes it with real T+1 prices.
        # The calendar itself extends past the head, so the data head — not the
        # calendar end — is the account clock boundary.
        published_end = scope.history_end
        results = []
        for account in configured:
            results.append(self._advance_one_account(
                account, market, repository, service, published_end,
            ))
        blocked = [item for item in results if item["status"] == "blocked"]
        stages.append(DailyStage(
            "accounts",
            "blocked" if blocked else "ok",
            {
                "snapshot_id": market.snapshot_id,
                "published_end": published_end.isoformat(),
                "advanced": sum(1 for item in results if item["status"] == "ok"),
                "blocked": [item["account_id"] for item in blocked],
            },
        ))
        return results

    def _advance_one_account(
        self,
        account: DailyAccountSettings,
        market: CanonicalMarketData,
        repository: TradingRepository,
        service: SimulationService,
        published_end: date,
    ) -> dict[str, Any]:
        try:
            try:
                repository.account(account.account_id)
            except Exception:
                repository.create_account(
                    account.account_id,
                    account.name,
                    PortfolioState.with_cash(account.initial_cash),
                )
            _, selected_parent = repository.selected_state(account.account_id)
            if selected_parent is None:
                sessions = (published_end,)
            else:
                head = repository.run(selected_parent).binding.end_date
                if head >= published_end:
                    sessions = ()
                else:
                    sessions = market.trading_days(
                        head + timedelta(days=1), published_end,
                    )
            last_run_id = selected_parent
            persistent_source = None
            if sessions and account.strategy != "agent-file":
                persistent_source = self._intent_source(account, sessions[0])
                if (
                    account.strategy == "moving-average-grid"
                    and selected_parent is not None
                ):
                    assert isinstance(persistent_source, MovingAverageGridSource)
                    previous_weight = self._last_moving_average_grid_weight(
                        repository,
                        selected_parent,
                        persistent_source,
                    )
                    persistent_source = self._intent_source(
                        account,
                        sessions[0],
                        initial_emitted_weight=previous_weight,
                    )
            for session in sessions:
                source = (
                    self._intent_source(account, session)
                    if account.strategy == "agent-file"
                    else persistent_source
                )
                assert source is not None
                outcome = service.run_daily(
                    account.account_id,
                    session,
                    source,
                )
                last_run_id = outcome.run.run_id
            payload: dict[str, Any] = {
                "account_id": account.account_id,
                "status": "ok",
                "strategy": account.strategy,
                "sessions_advanced": len(sessions),
                "head": max(
                    published_end,
                    date.min if selected_parent is None else
                    repository.run(selected_parent).binding.end_date,
                ).isoformat(),
            }
            if last_run_id is not None:
                feedback = build_simulation_feedback(repository, last_run_id)
                payload["run_id"] = last_run_id
                payload["quality"] = feedback.quality
                payload["equity"] = str(feedback.final_equity)
                payload["overall_return"] = str(feedback.overall_return)
            return payload
        except Exception as exc:
            return {
                "account_id": account.account_id,
                "status": "blocked",
                "strategy": account.strategy,
                "error_type": type(exc).__name__,
                "error": str(exc),
            }

    def _intent_source(
        self,
        account: DailyAccountSettings,
        session: date,
        *,
        initial_emitted_weight: Decimal | None = None,
    ):
        if account.strategy == "static":
            return StaticAllocationSource(account.weights)
        if account.strategy == "moving-average-grid":
            policy = self.settings.agent.policies.get(account.account_id)
            if policy is None:
                raise ValueError(
                    "moving-average-grid daily account has no matching policy: "
                    f"{account.account_id}"
                )
            if policy.kind != "moving-average-grid":
                raise ValueError(
                    "moving-average-grid daily account has mismatched policy kind: "
                    f"{account.account_id}={policy.kind}"
                )
            return MovingAverageGridSource(
                moving_average_grid_config(policy.params),
                initial_emitted_weight=initial_emitted_weight,
            )
        return FileIntentSource(
            self.settings.daily.agent_decision_root,
            account.account_id,
            session,
        )

    @staticmethod
    def _last_moving_average_grid_weight(
        repository: TradingRepository,
        run_id: str,
        source: MovingAverageGridSource,
    ) -> Decimal | None:
        current_run_id: str | None = run_id
        while current_run_id is not None:
            run = repository.run(current_run_id)
            binding = run.binding
            if (
                binding.strategy_id != source.strategy_id
                or binding.strategy_version != source.strategy_version
                or binding.strategy_config_hash != source.config_hash
            ):
                return None
            for event in reversed(repository.events(current_run_id)):
                if event["event_type"] != "portfolio_intent_received":
                    continue
                payload = event["payload"]
                if (
                    payload.get("strategy_id") != source.strategy_id
                    or payload.get("strategy_version") != source.strategy_version
                    or payload.get("strategy_config_hash") != source.config_hash
                ):
                    continue
                target_weights = payload.get("target_weights")
                if not isinstance(target_weights, Mapping):
                    raise ValueError("moving-average-grid ledger intent has invalid weights")
                value = target_weights.get(source.config.instrument)
                if value is None:
                    raise ValueError(
                        "moving-average-grid ledger intent omits its configured instrument"
                    )
                return Decimal(str(value))
            current_run_id = binding.parent_run_id
        return None

    # --------------------------------------------------------------- report

    def _write_report(
        self,
        status: str,
        target: date | None,
        snapshot_id: str | None,
        stages: list[DailyStage],
        accounts: list[dict[str, Any]],
    ) -> Path | None:
        try:
            self.daily_report_root.mkdir(parents=True, exist_ok=True)
            payload = {
                "pipeline": PIPELINE_VERSION,
                "generated_at": self.now_fn().isoformat(),
                "status": status,
                "target_date": None if target is None else target.isoformat(),
                "snapshot_id": snapshot_id,
                "stages": [to_primitive(item) for item in stages],
                "accounts": accounts,
            }
            token = stable_digest(payload)[:10]
            day = "unknown" if target is None else target.isoformat()
            path = self.daily_report_root / f"daily-{day}-{token}.json"
            path.write_text(canonical_json(payload), encoding="utf-8", newline="\n")
            summary = self.daily_report_root / f"daily-{day}-{token}.md"
            summary.write_text(self._markdown_summary(payload), encoding="utf-8", newline="\n")
            return path
        except Exception:
            return None

    @staticmethod
    def _markdown_summary(payload: Mapping[str, Any]) -> str:
        lines = [
            f"# Daily run {payload['target_date']} — {payload['status']}",
            "",
            f"- snapshot: `{payload['snapshot_id']}`",
            f"- generated: {payload['generated_at']}",
            "",
            "## Stages",
            "",
        ]
        for stage in payload["stages"]:
            lines.append(f"- **{stage['name']}**: {stage['status']}")
            for key, value in stage.get("detail", {}).items():
                lines.append(f"    - {key}: {value}")
        if payload["accounts"]:
            lines.extend(["", "## Accounts", ""])
            for account in payload["accounts"]:
                lines.append(
                    f"- **{account['account_id']}** ({account.get('strategy')}): "
                    f"{account['status']}"
                )
                for key in ("sessions_advanced", "head", "quality", "error"):
                    if account.get(key) is not None:
                        lines.append(f"    - {key}: {account[key]}")
        lines.append("")
        return "\n".join(lines)
