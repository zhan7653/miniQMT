"""One idempotent daily cycle: extend the published snapshot, advance paper accounts.

This module only orchestrates.  Every trust decision stays inside the existing
machinery (two-source history reconciliation, no-trade consensus, status and
evidence collectors, the increment validator and the atomic incremental
publisher).  Any gate that fails blocks the run with a structured reason
instead of publishing a partial result; re-running resumes from the durable
observation warehouse.
"""

from __future__ import annotations

import json
import os
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Mapping

import pandas as pd

from fundlab.common.canonical import canonical_json, stable_digest, to_primitive
from fundlab.marketdata import (
    CURRENT_SH_SZ_STOCK_ETF_UNIVERSE,
    CanonicalMarketData,
    CoverageClaim,
    EvidenceCollectionSpec,
    MarketDataWarehouse,
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
    SnapshotPlan,
    SourceSlice,
    StatusCollectionSpec,
    UniverseScope,
    default_provider_registry,
    derive_current_research_snapshot,
    record_no_trade_research_partition,
)
from fundlab.marketdata.history import HistoryBuildSpec, HistoryDatabaseBuilder
from fundlab.marketdata.simulation_data import reconcile_simulation_status
from fundlab.settings import DailyAccountSettings, FoundationSettings
from fundlab.strategies import FileIntentSource, StaticAllocationSource
from fundlab.trading import (
    PortfolioState,
    SimulationService,
    TradingRepository,
    build_simulation_feedback,
)


PIPELINE_VERSION = "daily-pipeline-v1"
CALENDAR_PROVIDERS = ("baostock", "sina-calendar")
NO_TRADE_PROVIDERS = ("tickflow", "xtquant", "baostock")


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
        return 0 if self.status in {"ok", "up_to_date"} else 2


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

        universe_obs, official_frame = self._official_universe(target)
        official_ids = set(map(str, official_frame["instrument_id"]))
        previous_ids = set(previous_scope.instrument_ids)
        new_ids = tuple(sorted(official_ids - previous_ids))
        removed = tuple(sorted(previous_ids - official_ids))
        if removed:
            raise DailyPipelineBlocked("universe", "official universe removed predecessor instruments", {
                "removed_sample": removed[:20], "removed_count": len(removed),
            })
        target_ids = tuple(sorted(official_ids))
        stages.append(DailyStage("universe", "ok", {
            "universe_observation_id": universe_obs,
            "official_instruments": len(official_ids),
            "new_instruments": len(new_ids),
        }))

        builder = HistoryDatabaseBuilder(
            self.warehouse,
            self.report_root,
            registry=self.registry,
            source_pair=self.settings.daily.source_pair,
            adjudicator_provider=self.settings.daily.adjudicator,
        )
        build = builder.build(HistoryBuildSpec(
            start_date=increment_start,
            end_date=target,
            universe_as_of=target,
            instrument_ids=target_ids,
            exchanges=("SH", "SZ"),
            asset_types=("stock", "etf"),
            batch_size=self.settings.daily.batch_size,
            publish=False,
        ))
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
        unexplained = tuple(sorted(
            set(missing) - set(no_trade_only) - set(new_master_missing)
        ))
        if unexplained:
            raise DailyPipelineBlocked("bars", "instruments missing for reasons other than no-trade", {
                "unexplained_sample": unexplained[:20],
                "unexplained_count": len(unexplained),
                "excluded_reasons_sample": {
                    item: excluded.get(item) for item in unexplained[:10]
                },
            })

        research_source_id = build.snapshot_id
        supplement_snapshot_ids: tuple[str, ...] = ()
        build_partition_ids = tuple(
            map(str, build_report.get("canonical_observation_ids", ()))
        )
        if new_master_missing:
            supplement_id, supplement_partitions, detail = (
                self._build_new_instrument_supplement(
                    builder=builder,
                    universe_observation_id=universe_obs,
                    official_frame=official_frame,
                    instrument_ids=new_master_missing,
                    start=increment_start,
                    end=target,
                )
            )
            supplement_snapshot_ids = (supplement_id,)
            build_partition_ids = tuple(sorted({
                *build_partition_ids,
                *supplement_partitions,
            }))
            stages.append(DailyStage("new_instruments", "ok", detail))

        no_trade_observation_id: str | None = None
        if no_trade_only:
            no_trade_observation_id = self._record_no_trade(
                predecessor_snapshot_id=predecessor.snapshot_id,
                universe_observation_id=universe_obs,
                calendar_observation_id=calendar_observation_id,
                start=increment_start,
                end=target,
                instrument_ids=no_trade_only,
            )
            stages.append(DailyStage("no_trade", "ok", {
                "instruments": no_trade_only,
                "observation_id": no_trade_observation_id,
            }))

        if supplement_snapshot_ids or no_trade_observation_id is not None:
            research_source_id = self._combine_partitions(
                main_snapshot_id=build.snapshot_id,
                supplement_snapshot_ids=supplement_snapshot_ids,
                no_trade_observation_id=no_trade_observation_id,
                no_trade_ids=no_trade_only,
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
        for provider in ("xtquant", "baostock"):
            result = SimulationStatusCollector(
                self.warehouse, self.report_root, registry=self.registry,
            ).collect(StatusCollectionSpec(
                source_snapshot_id=research.snapshot_id,
                calendar_observation_id=calendar_observation_id,
                provider_name=provider,
                batch_size=50,
            ))
            if result.status != "complete":
                raise DailyPipelineBlocked("status", f"{provider} status collection incomplete", {
                    "blockers": result.blockers,
                })
            status_results[provider] = result
        stages.append(DailyStage("status", "ok", {
            provider: len(result.observation_ids)
            for provider, result in status_results.items()
        }))

        evidence_results = {}
        for kind in ("stock-actions", "etf-actions", "factors"):
            result = SimulationEvidenceCollector(
                self.warehouse, self.report_root, registry=self.registry,
            ).collect(EvidenceCollectionSpec(
                source_snapshot_id=research.snapshot_id,
                kind=kind,
            ))
            if result.status != "complete":
                raise DailyPipelineBlocked("evidence", f"{kind} evidence collection incomplete", {
                    "blockers": result.blockers,
                })
            evidence_results[kind] = result
        stages.append(DailyStage("evidence", "ok", {
            kind: len(result.observation_ids)
            for kind, result in evidence_results.items()
        }))

        increment_scope = UniverseScope(
            CURRENT_SH_SZ_STOCK_ETF_UNIVERSE,
            target,
            increment_start,
            target,
            survivorship_bias=previous_scope.survivorship_bias,
            instrument_ids=target_ids,
        )
        candidate_id = self._compose_candidate(
            research_snapshot_id=research.snapshot_id,
            calendar_frame=calendar_frame,
            increment_scope=increment_scope,
            status_results=status_results,
            evidence_results=evidence_results,
            build_partition_ids=build_partition_ids,
            no_trade_observation_id=no_trade_observation_id,
            universe_observation_id=universe_obs,
        )
        stages.append(DailyStage("candidate", "ok", {"observation_id": candidate_id}))

        validated = SimulationIncrementValidator(
            self.warehouse, self.report_root,
        ).validate_and_record(
            candidate_observation_id=candidate_id,
            calendar_observation_id=calendar_observation_id,
            universe_scope=increment_scope,
            description=f"daily pipeline EOD increment through {target.isoformat()}",
        )
        stages.append(DailyStage("validate", "ok", {
            "observation_id": validated.observation_id,
        }))

        extended_ids = tuple(sorted(previous_ids.union(target_ids)))
        target_scope = UniverseScope(
            previous_scope.definition,
            previous_scope.as_of_date,
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

    def _official_universe(self, target: date) -> tuple[str, pd.DataFrame]:
        request = ProviderRequest(
            ProviderCapability.INSTRUMENTS,
            parameters={
                "exchanges": ("SH", "SZ"),
                "asset_types": ("stock", "etf"),
                "as_of_date": target.isoformat(),
            },
        )
        observed, _ = self.ingestion.capture_resumable("exchange-public", request)
        frame = self.warehouse.read_observation_table(
            observed.observation_id, MarketTable.INSTRUMENTS,
        )
        return observed.observation_id, frame

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

    def _build_new_instrument_supplement(
        self,
        *,
        builder: HistoryDatabaseBuilder,
        universe_observation_id: str,
        official_frame: pd.DataFrame,
        instrument_ids: tuple[str, ...],
        start: date,
        end: date,
    ) -> tuple[str, tuple[str, ...], Mapping[str, Any]]:
        """Build a disjoint exact partition for exchange-announced new listings."""
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
            if not start <= listed <= end:
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
            universe_observation_id=universe_observation_id,
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
    ) -> str:
        source_ids = []
        request = ProviderRequest(
            ProviderCapability.DAILY_BARS_RAW, start, end, instrument_ids,
        )
        for provider in NO_TRADE_PROVIDERS:
            observed, _ = self.ingestion.capture_resumable(provider, request)
            source_ids.append(observed.observation_id)
        manifest = record_no_trade_research_partition(
            self.warehouse,
            predecessor_snapshot_id=predecessor_snapshot_id,
            universe_observation_id=universe_observation_id,
            calendar_observation_id=calendar_observation_id,
            source_observation_ids=tuple(source_ids),
            start_date=start,
            end_date=end,
            instrument_ids=instrument_ids,
        )
        return manifest.observation_id

    def _combine_partitions(
        self,
        *,
        main_snapshot_id: str,
        supplement_snapshot_ids: tuple[str, ...],
        no_trade_observation_id: str | None,
        no_trade_ids: tuple[str, ...],
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
    ) -> str:
        research = self.warehouse.load_snapshot(research_snapshot_id)
        instruments = self.warehouse.query_loaded_snapshot_table(
            research, MarketTable.INSTRUMENTS,
        )
        research_bars = self.warehouse.query_loaded_snapshot_table(
            research,
            MarketTable.DAILY_BARS,
            start_date=increment_scope.history_start,
            end_date=increment_scope.history_end,
            price_mode="raw",
        )
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
        bars = reconcile_simulation_status(
            instruments=instruments,
            research_bars=research_bars,
            dense_status_bars=dense_status,
            stock_st_bars=stock_st,
            calendar=calendar_window,
            universe_scope=increment_scope,
        )
        actions = self._scoped_events(
            self._concat_observation_tables(
                (
                    *evidence_results["stock-actions"].observation_ids,
                    *evidence_results["etf-actions"].observation_ids,
                ),
                MarketTable.CORPORATE_ACTIONS,
            ),
            date_column="ex_date",
            scope=increment_scope,
        )
        factors = self._scoped_events(
            self._concat_observation_tables(
                evidence_results["factors"].observation_ids,
                MarketTable.ADJUSTMENT_FACTORS,
            ),
            date_column="effective_date",
            scope=increment_scope,
        )
        input_ids = tuple(sorted({
            *build_partition_ids,
            *(item for item in (no_trade_observation_id,) if item),
            *status_results["xtquant"].observation_ids,
            *status_results["baostock"].observation_ids,
            *evidence_results["stock-actions"].observation_ids,
            *evidence_results["etf-actions"].observation_ids,
            *evidence_results["factors"].observation_ids,
            universe_observation_id,
        }))
        observed_at = max(
            self.warehouse.load_observation(item).observed_at for item in input_ids
        )
        description = (
            "daily pipeline simulation increment candidate "
            f"{increment_scope.history_start.isoformat()}..{increment_scope.history_end.isoformat()}"
        )
        claims = tuple(
            CoverageClaim(
                table,
                True,
                None if table is MarketTable.INSTRUMENTS else increment_scope.history_start,
                None if table is MarketTable.INSTRUMENTS else increment_scope.history_end,
                increment_scope.instrument_ids,
                f"Composed by {PIPELINE_VERSION}",
            )
            for table in (
                MarketTable.INSTRUMENTS,
                MarketTable.DAILY_BARS,
                MarketTable.CORPORATE_ACTIONS,
                MarketTable.ADJUSTMENT_FACTORS,
            )
        )
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
            claims,
            {
                "kind": "field_level_reconciliation",
                "reconciliation_ready": True,
                "description": description,
                "report": {
                    "blockers": (),
                    "unresolved_conflicts": (),
                    "input_observation_ids": input_ids,
                },
            },
        )
        return self.warehouse.record_observation(payload).observation_id

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
            return pd.DataFrame()
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
            for session in sessions:
                outcome = service.run_daily(
                    account.account_id,
                    session,
                    self._intent_source(account, session),
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

    def _intent_source(self, account: DailyAccountSettings, session: date):
        if account.strategy == "static":
            return StaticAllocationSource(account.weights)
        return FileIntentSource(
            self.settings.daily.agent_decision_root,
            account.account_id,
            session,
        )

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
