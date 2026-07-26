from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from math import isclose
from types import MappingProxyType
from typing import Any, Iterable, Mapping

import pandas as pd

from fundlab.common.canonical import canonical_json, to_primitive
from fundlab.marketdata.contracts import (
    CoverageClaim,
    MarketTable,
    ObservationManifest,
    ProviderCapability,
    ProviderRequest,
    ReadinessProfile,
    ReconciliationError,
)
from fundlab.marketdata.schema import BUSINESS_SCHEMAS, TABLE_KEYS
from fundlab.marketdata.warehouse import MarketDataWarehouse


@dataclass(frozen=True)
class FieldRule:
    table: MarketTable
    field: str
    priorities: tuple[str, ...] = ()
    minimum_independent_backends: int = 1
    absolute_tolerance: float = 0.0
    relative_tolerance: float = 0.0

    def __post_init__(self) -> None:
        if self.field not in BUSINESS_SCHEMAS[self.table]:
            raise ValueError(f"Unknown policy field: {self.table.value}.{self.field}")
        if self.minimum_independent_backends < 1:
            raise ValueError("minimum_independent_backends must be positive")
        if self.absolute_tolerance < 0 or self.relative_tolerance < 0:
            raise ValueError("Reconciliation tolerances cannot be negative")
        object.__setattr__(self, "priorities", tuple(self.priorities))


@dataclass(frozen=True)
class ReconciliationPolicy:
    version: str
    backend_groups: Mapping[str, str]
    rules: tuple[FieldRule, ...]
    default_priorities: Mapping[MarketTable, tuple[str, ...]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.version.strip():
            raise ValueError("Reconciliation policy version cannot be empty")
        keyed: set[tuple[MarketTable, str]] = set()
        for rule in self.rules:
            key = (rule.table, rule.field)
            if key in keyed:
                raise ValueError(f"Duplicate reconciliation rule: {rule.table.value}.{rule.field}")
            keyed.add(key)
        object.__setattr__(self, "backend_groups", MappingProxyType(dict(self.backend_groups)))
        object.__setattr__(self, "rules", tuple(self.rules))
        object.__setattr__(
            self,
            "default_priorities",
            MappingProxyType({MarketTable(key): tuple(value) for key, value in self.default_priorities.items()}),
        )

    def backend(self, provider: str) -> str:
        return self.backend_groups.get(provider, provider)

    def rule(self, table: MarketTable, field_name: str) -> FieldRule:
        explicit = next(
            (item for item in self.rules if item.table is table and item.field == field_name),
            None,
        )
        if explicit is not None:
            return explicit
        return FieldRule(
            table,
            field_name,
            self.default_priorities.get(table, ()),
        )


@dataclass(frozen=True)
class FieldConflict:
    table: MarketTable
    key: Mapping[str, Any]
    field: str
    reason: str
    candidate_backends: tuple[str, ...]


@dataclass(frozen=True)
class ReconciliationReport:
    policy_version: str
    readiness: ReadinessProfile
    input_observation_ids: tuple[str, ...]
    declared_scope: Mapping[str, Mapping[str, Any]]
    blockers: tuple[str, ...]
    unresolved_conflicts: tuple[FieldConflict, ...]
    resolved_conflicts: int
    selected_field_counts: Mapping[str, int]
    candidate_field_counts: Mapping[str, int]

    @property
    def ready(self) -> bool:
        return not self.blockers and not self.unresolved_conflicts


@dataclass(frozen=True)
class ReconciliationResult:
    tables: Mapping[MarketTable, pd.DataFrame]
    report: ReconciliationReport
    table_complete: Mapping[MarketTable, bool]


@dataclass(frozen=True)
class _Candidate:
    provider: str
    backend: str
    observation_id: str
    observed_at: str
    value: Any


def default_reconciliation_policy(
    readiness: ReadinessProfile = ReadinessProfile.SIMULATION,
) -> ReconciliationPolicy:
    """Return the immutable first calibrated policy; it never learns priorities at runtime."""

    readiness = ReadinessProfile(readiness)
    market_priority = (
        "tickflow", "eastmoney-efinance", "baostock", "xtquant", "xtquant-legacy-import",
    )
    official_priority = ("exchange-public", "cninfo-public", "baostock", "tickflow")
    rules = [
        FieldRule(
            MarketTable.DAILY_BARS,
            field_name,
            market_priority,
            2,
            absolute_tolerance=(
                # The three-source calibration sample (379,037 rows) agreed to
                # floating-point precision.  Keep the production gate below the
                # smallest supported CNY price tick (ETF: 0.001) so two values one
                # tick apart can never be treated as consensus.
                0.0005 if field_name in {"open", "high", "low", "close"}
                else 100.0 if field_name == "volume"
                else 0.0
            ),
            relative_tolerance=1e-8 if field_name in {"open", "high", "low", "close"} else 1e-6,
        )
        for field_name in ("open", "high", "low", "close", "volume")
    ]
    if readiness is ReadinessProfile.SIMULATION:
        rules.extend((FieldRule(
            MarketTable.DAILY_BARS,
            "previous_close",
            ("baostock", "eastmoney-efinance", "tickflow", "xtquant-legacy-import"),
            2,
            absolute_tolerance=0.0005,
            relative_tolerance=1e-8,
        ), FieldRule(
            MarketTable.DAILY_BARS,
            "suspended",
            ("baostock", "eastmoney-efinance", "tickflow", "xtquant-legacy-import"),
            2,
        )))
    return ReconciliationPolicy(
        version=f"a-share-daily-{readiness.value}-v4",
        backend_groups={
            "eastmoney-efinance": "eastmoney",
            "eastmoney-akshare": "eastmoney",
            "tickflow": "tickflow-unverified",
            "baostock": "baostock",
            "xtquant-legacy-import": "xtquant",
            "xtquant": "xtquant",
            "exchange-public": "exchange-public",
            "cninfo-public": "cninfo-public",
        },
        rules=tuple(rules),
        default_priorities={
            MarketTable.INSTRUMENTS: official_priority,
            MarketTable.CALENDAR: official_priority,
            MarketTable.DAILY_BARS: market_priority,
            MarketTable.CORPORATE_ACTIONS: official_priority,
            MarketTable.ADJUSTMENT_FACTORS: official_priority,
        },
    )


class ReconciliationService:
    """Create one replayable canonical observation from explicit source observations."""

    def __init__(self, warehouse: MarketDataWarehouse, policy: ReconciliationPolicy) -> None:
        self.warehouse = warehouse
        self.policy = policy

    def reconcile(
        self,
        observation_ids: Iterable[str],
        *,
        readiness: ReadinessProfile,
    ) -> ReconciliationResult:
        readiness = ReadinessProfile(readiness)
        ids = tuple(sorted(set(observation_ids)))
        if not ids:
            raise ValueError("At least one observation is required for reconciliation")
        manifests = tuple(self.warehouse.load_observation(item) for item in ids)
        frames_by_table: dict[MarketTable, list[pd.DataFrame]] = {}
        for manifest in manifests:
            for stored in manifest.files:
                frame = self.warehouse.read_observation_table(manifest.observation_id, stored.table)
                if stored.table is MarketTable.DAILY_BARS:
                    frame = frame.loc[frame["price_mode"] == "raw"].reset_index(drop=True)
                if frame.duplicated(list(TABLE_KEYS[stored.table])).any():
                    raise ReconciliationError(
                        f"Observation {manifest.observation_id} has duplicate {stored.table.value} keys"
                    )
                frames_by_table.setdefault(stored.table, []).append(frame)

        selected_counts: dict[str, int] = {}
        candidate_counts: dict[str, int] = {}
        conflicts: list[FieldConflict] = []
        resolved_conflicts = 0
        output: dict[MarketTable, pd.DataFrame] = {}
        table_conflicts: dict[MarketTable, bool] = {}
        for table, frames in sorted(frames_by_table.items(), key=lambda item: item[0].value):
            reconciled, found, resolved = _reconcile_table(
                table,
                frames,
                self.policy,
                selected_counts,
                candidate_counts,
            )
            output[table] = reconciled
            conflicts.extend(found)
            resolved_conflicts += resolved
            table_conflicts[table] = bool(found)

        required = {
            ReadinessProfile.RESEARCH_PRICE: {
                MarketTable.INSTRUMENTS,
                MarketTable.DAILY_BARS,
            },
            ReadinessProfile.SIMULATION: set(MarketTable),
        }[readiness]
        blockers = [f"missing_table:{item.value}" for item in sorted(required - set(output), key=lambda item: item.value)]
        table_complete: dict[MarketTable, bool] = {}
        declared_scope = {
            table.value: _table_scope(table, frame, manifests)
            for table, frame in sorted(output.items(), key=lambda item: item[0].value)
        }
        for table in output:
            table_scope = declared_scope[table.value]
            scope_start = _optional_date(table_scope.get("start_date"))
            scope_end = _optional_date(table_scope.get("end_date"))
            scope_instruments = tuple(map(str, table_scope.get("instrument_ids", ())))
            complete_backends = {
                self.policy.backend(manifest.provider)
                for manifest in manifests
                if any(
                    claim.table is table
                    and claim.complete
                    and _claim_covers_scope(
                        claim,
                        table,
                        output[table],
                        scope_start,
                        scope_end,
                        scope_instruments,
                    )
                    for claim in manifest.coverage
                )
            }
            minimum = 2 if table is MarketTable.DAILY_BARS else 1
            coverage_ok = len(complete_backends) >= minimum
            if not coverage_ok and table in required:
                blockers.append(f"insufficient_complete_backends:{table.value}:{len(complete_backends)}/{minimum}")
            table_complete[table] = coverage_ok and not table_conflicts.get(table, False)

        report = ReconciliationReport(
            self.policy.version,
            readiness,
            ids,
            declared_scope,
            tuple(sorted(set(blockers))),
            tuple(conflicts),
            resolved_conflicts,
            dict(sorted(selected_counts.items())),
            dict(sorted(candidate_counts.items())),
        )
        return ReconciliationResult(MappingProxyType(output), report, MappingProxyType(table_complete))

    def reconcile_and_record(
        self,
        observation_ids: Iterable[str],
        *,
        readiness: ReadinessProfile,
        description: str,
    ) -> tuple[ObservationManifest, ReconciliationReport]:
        ids = tuple(sorted(set(observation_ids)))
        result = self.reconcile(ids, readiness=readiness)
        manifests = tuple(self.warehouse.load_observation(item) for item in ids)
        preferred_scope = result.report.declared_scope.get(MarketTable.DAILY_BARS.value, {})
        if not preferred_scope:
            preferred_scope = result.report.declared_scope.get(MarketTable.CALENDAR.value, {})
        start_date = _optional_date(preferred_scope.get("start_date"))
        end_date = _optional_date(preferred_scope.get("end_date"))
        instruments = tuple(map(str, preferred_scope.get("instrument_ids", ())))
        if not instruments:
            instruments = tuple(sorted({
                item for manifest in manifests for item in manifest.request.instrument_ids
            }))
        request = ProviderRequest(
            ProviderCapability.CANONICAL_RECONCILIATION,
            start_date,
            end_date,
            instruments,
            {
                "description": description,
                "input_observation_ids": ids,
                "policy_version": self.policy.version,
                "readiness": ReadinessProfile(readiness).value,
            },
        )
        claims = tuple(
            _coverage_claim(table, frame, result.table_complete.get(table, False), request)
            for table, frame in sorted(result.tables.items(), key=lambda item: item[0].value)
        )
        from fundlab.marketdata.contracts import ObservationPayload

        payload = ObservationPayload(
            f"canonical-reconciler-{self.policy.version}",
            max(manifest.observed_at for manifest in manifests).astimezone(timezone.utc),
            request,
            result.tables,
            claims,
            {
                "kind": "field_level_reconciliation",
                "reconciliation_ready": result.report.ready,
                "description": description,
                "policy": to_primitive(self.policy),
                "report": to_primitive(result.report),
            },
        )
        return self.warehouse.record_observation(payload), result.report


def _reconcile_table(
    table: MarketTable,
    frames: list[pd.DataFrame],
    policy: ReconciliationPolicy,
    selected_counts: dict[str, int],
    candidate_counts: dict[str, int],
) -> tuple[pd.DataFrame, list[FieldConflict], int]:
    combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if combined.empty:
        return combined.drop(columns=list(combined.columns), errors="ignore").reindex(
            columns=list(BUSINESS_SCHEMAS[table])
        ), [], 0
    keys = list(TABLE_KEYS[table])
    excluded = set(keys) | {"source_payload", "field_lineage"}
    fields = [item for item in BUSINESS_SCHEMAS[table] if item not in excluded]
    rows: list[dict[str, Any]] = []
    conflicts: list[FieldConflict] = []
    resolved = 0
    grouped = combined.groupby(keys, sort=True, dropna=False)
    for raw_key, group in grouped:
        key_values = raw_key if isinstance(raw_key, tuple) else (raw_key,)
        key = {name: _plain(value) for name, value in zip(keys, key_values, strict=True)}
        row = dict(key)
        lineage: dict[str, Any] = {}
        for field_name in fields:
            rule = policy.rule(table, field_name)
            if (
                table is MarketTable.DAILY_BARS
                and field_name in {"open", "high", "low", "close"}
                and _all_known_rows_suspended(group)
            ):
                row[field_name] = pd.NA
                lineage[field_name] = {
                    "selected": None,
                    "reason": "not_applicable_on_suspended_session",
                    "candidate_observation_ids": (),
                }
                continue
            candidates = [
                _Candidate(
                    str(item["source_provider"]),
                    policy.backend(str(item["source_provider"])),
                    str(item["source_observation_id"]),
                    str(item["observed_at"]),
                    _plain(item[field_name]),
                )
                for item in group.to_dict("records")
                if not _missing(item.get(field_name))
            ]
            for candidate in candidates:
                candidate_counts[candidate.provider] = candidate_counts.get(candidate.provider, 0) + 1
            chosen, agreeing, had_disagreement, consensus_met = _select_candidate(candidates, rule)
            if chosen is None:
                row[field_name] = pd.NA
                if rule.minimum_independent_backends > 1:
                    conflicts.append(FieldConflict(
                        table,
                        key,
                        field_name,
                        "independent_backend_consensus_not_met",
                        tuple(sorted({item.backend for item in candidates})),
                    ))
                lineage[field_name] = {
                    "selected": None,
                    "minimum_independent_backends": rule.minimum_independent_backends,
                    "candidate_observation_ids": tuple(sorted({
                        item.observation_id for item in candidates
                    })),
                }
                continue
            row[field_name] = chosen.value
            selected_counts[chosen.provider] = selected_counts.get(chosen.provider, 0) + 1
            if not consensus_met:
                conflicts.append(FieldConflict(
                    table,
                    key,
                    field_name,
                    "independent_backend_consensus_not_met",
                    tuple(sorted({item.backend for item in candidates})),
                ))
            elif had_disagreement:
                resolved += 1
            lineage[field_name] = {
                "selected": _candidate_reference(chosen),
                "agreeing_backends": tuple(sorted(agreeing)),
                "minimum_independent_backends": rule.minimum_independent_backends,
                "candidate_observation_ids": tuple(sorted({
                    item.observation_id for item in candidates
                })),
            }
        row["field_lineage"] = canonical_json({
            "policy_version": policy.version,
            "fields": lineage,
        })
        row["source_payload"] = canonical_json({
            "kind": "reconciled_row",
            "policy_version": policy.version,
            "source_observation_ids": tuple(sorted(set(map(str, group["source_observation_id"])))),
        })
        rows.append(row)
    return pd.DataFrame(rows, columns=list(BUSINESS_SCHEMAS[table])), conflicts, resolved


def _select_candidate(
    candidates: list[_Candidate], rule: FieldRule,
) -> tuple[_Candidate | None, set[str], bool, bool]:
    if not candidates:
        return None, set(), False, False
    clusters: list[list[_Candidate]] = []
    for candidate in candidates:
        cluster = next(
            (items for items in clusters if _equivalent(candidate.value, items[0].value, rule)),
            None,
        )
        if cluster is None:
            clusters.append([candidate])
        else:
            cluster.append(candidate)
    eligible = [
        items for items in clusters
        if len({candidate.backend for candidate in items}) >= rule.minimum_independent_backends
    ]
    maximum_support = max(
        (len({candidate.backend for candidate in items}) for items in eligible),
        default=0,
    )
    tied_consensus = sum(
        len({candidate.backend for candidate in items}) == maximum_support
        for items in eligible
    ) > 1
    consensus_met = bool(eligible) and not (
        rule.minimum_independent_backends > 1 and tied_consensus
    )
    candidates_to_rank = eligible or clusters
    candidates_to_rank.sort(key=lambda items: (
        -len({candidate.backend for candidate in items}),
        min(_priority(candidate.provider, rule.priorities) for candidate in items),
        canonical_json(_plain(items[0].value)),
    ))
    winners = candidates_to_rank[0]
    winners.sort(key=lambda item: (
        _priority(item.provider, rule.priorities),
        item.provider,
        item.observation_id,
    ))
    return winners[0], {item.backend for item in winners}, len(clusters) > 1, consensus_met


def _equivalent(left: Any, right: Any, rule: FieldRule) -> bool:
    if isinstance(left, (int, float)) and not isinstance(left, bool) and isinstance(right, (int, float)) and not isinstance(right, bool):
        return isclose(
            float(left),
            float(right),
            rel_tol=rule.relative_tolerance,
            abs_tol=rule.absolute_tolerance,
        )
    return left == right


def _priority(provider: str, priorities: tuple[str, ...]) -> int:
    try:
        return priorities.index(provider)
    except ValueError:
        return len(priorities)


def _candidate_reference(candidate: _Candidate) -> Mapping[str, Any]:
    return {
        "provider": candidate.provider,
        "backend": candidate.backend,
        "observation_id": candidate.observation_id,
    }


def _coverage_claim(
    table: MarketTable,
    frame: pd.DataFrame,
    complete: bool,
    request: ProviderRequest,
) -> CoverageClaim:
    instrument_ids = ()
    if "instrument_id" in frame:
        instrument_ids = tuple(sorted(set(map(str, frame["instrument_id"].dropna()))))
        if not instrument_ids:
            instrument_ids = request.instrument_ids
    date_column = {
        MarketTable.CALENDAR: "session_date",
        MarketTable.DAILY_BARS: "session_date",
        MarketTable.CORPORATE_ACTIONS: "ex_date",
        MarketTable.ADJUSTMENT_FACTORS: "effective_date",
    }.get(table)
    if date_column is None:
        start = end = None
    elif frame.empty:
        start, end = request.start_date, request.end_date
    else:
        start = date.fromisoformat(str(frame[date_column].min())[:10])
        end = date.fromisoformat(str(frame[date_column].max())[:10])
    return CoverageClaim(
        table,
        complete,
        start,
        end,
        instrument_ids,
        f"field-level reconciliation complete={complete}",
    )


def _table_scope(
    table: MarketTable,
    frame: pd.DataFrame,
    manifests: tuple[ObservationManifest, ...],
) -> Mapping[str, Any]:
    date_column = {
        MarketTable.CALENDAR: "session_date",
        MarketTable.DAILY_BARS: "session_date",
        MarketTable.CORPORATE_ACTIONS: "ex_date",
        MarketTable.ADJUSTMENT_FACTORS: "effective_date",
    }.get(table)
    if date_column is not None and not frame.empty:
        start = str(frame[date_column].min())[:10]
        end = str(frame[date_column].max())[:10]
    elif date_column is not None:
        claims = [
            claim
            for manifest in manifests
            for claim in manifest.coverage
            if claim.table is table and claim.start_date is not None
        ]
        start = min((claim.start_date for claim in claims), default=None)
        end = max((claim.end_date for claim in claims), default=None)
        start = None if start is None else start.isoformat()
        end = None if end is None else end.isoformat()
    else:
        start = end = None
    if "instrument_id" in frame and not frame.empty:
        instruments = tuple(sorted(set(map(str, frame["instrument_id"].dropna()))))
    else:
        instruments = tuple(sorted({
            item
            for manifest in manifests
            for claim in manifest.coverage
            if claim.table is table
            for item in claim.instrument_ids
        }))
    return {
        "start_date": start,
        "end_date": end,
        "instrument_ids": instruments,
        "row_count": len(frame),
    }


def _claim_covers_scope(
    claim: CoverageClaim,
    table: MarketTable,
    frame: pd.DataFrame,
    scope_start: date | None,
    scope_end: date | None,
    scope_instruments: tuple[str, ...],
) -> bool:
    if table is not MarketTable.INSTRUMENTS and scope_start is not None:
        if claim.start_date is None or claim.end_date is None:
            return False
        if claim.start_date > scope_start or claim.end_date < scope_end:
            return False
    required_instruments = set(scope_instruments)
    if "instrument_id" in frame and not frame.empty:
        required_instruments.update(map(str, frame["instrument_id"].dropna()))
    if required_instruments and claim.instrument_ids:
        if not required_instruments <= set(claim.instrument_ids):
            return False
    return True


def _plain(value: Any) -> Any:
    if value is None or value is pd.NA or pd.isna(value):
        return None
    if hasattr(value, "item"):
        value = value.item()
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return value


def _missing(value: Any) -> bool:
    return value is None or value is pd.NA or bool(pd.isna(value))


def _optional_date(value: Any) -> date | None:
    return None if value is None else date.fromisoformat(str(value)[:10])


def _all_known_rows_suspended(group: pd.DataFrame) -> bool:
    known = group["suspended"].dropna()
    return not known.empty and bool(known.astype(bool).all())
