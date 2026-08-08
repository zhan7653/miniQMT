from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from enum import StrEnum
from types import MappingProxyType
from typing import Any, Mapping, Protocol, runtime_checkable


class MarketTable(StrEnum):
    INSTRUMENTS = "instruments"
    CALENDAR = "calendar"
    DAILY_BARS = "daily_bars"
    CORPORATE_ACTIONS = "corporate_actions"
    ADJUSTMENT_FACTORS = "adjustment_factors"


class ProviderCapability(StrEnum):
    INSTRUMENTS = "instruments"
    TRADING_CALENDAR = "trading_calendar"
    DAILY_BARS_RAW = "daily_bars_raw"
    DAILY_BARS_ADJUSTED = "daily_bars_adjusted"
    DAILY_STATUS = "daily_status"
    CORPORATE_ACTIONS = "corporate_actions"
    ADJUSTMENT_FACTORS = "adjustment_factors"
    CANONICAL_RECONCILIATION = "canonical_reconciliation"


class PriceMode(StrEnum):
    RAW = "raw"
    ADJUSTED = "adjusted"


class PriceLimitState(StrEnum):
    BOUNDED = "bounded"
    UNBOUNDED = "unbounded"
    UNKNOWN = "unknown"


class AssetType(StrEnum):
    STOCK = "stock"
    ETF = "etf"


class CorporateActionType(StrEnum):
    CASH_DIVIDEND = "cash_dividend"
    STOCK_DIVIDEND = "stock_dividend"
    SPLIT = "split"
    RIGHTS_ISSUE = "rights_issue"


class SnapshotState(StrEnum):
    READY = "ready"
    INCOMPLETE = "incomplete"


class ReadinessProfile(StrEnum):
    """The declared use whose mandatory fields gate a snapshot."""

    LEGACY_UNKNOWN = "legacy_unknown"
    RESEARCH_PRICE = "research_price"
    SIMULATION = "simulation"


CURRENT_SH_SZ_STOCK_ETF_UNIVERSE = "current_sh_sz_stock_etf"
SIMULATION_PARTITION_VALIDATOR_VERSION = "simulation-partition-r2-v3"
DATA_GAP_QUARANTINE_RULE_ID = "cn-data-gap-quarantine-no-execution-v1"
EXECUTION_EVIDENCE_GAP_RULE_ID = "cn-execution-evidence-gap-no-execution-v1"


@dataclass(frozen=True)
class UniverseScope:
    """The immutable universe and history boundary declared by a snapshot."""

    definition: str
    as_of_date: date
    history_start: date
    history_end: date
    survivorship_bias: bool = False
    instrument_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.definition.strip():
            raise ValueError("Universe definition cannot be empty")
        if self.history_start > self.history_end:
            raise ValueError("Universe history_start must not exceed history_end")
        if self.history_end > self.as_of_date:
            raise ValueError("Universe history_end must not exceed as_of_date")
        normalized = tuple(sorted(set(map(str, self.instrument_ids))))
        if not normalized:
            raise ValueError("Universe scope must pin at least one instrument_id")
        object.__setattr__(self, "instrument_ids", normalized)


@dataclass(frozen=True)
class ProviderRequest:
    capability: ProviderCapability
    start_date: date | None = None
    end_date: date | None = None
    instrument_ids: tuple[str, ...] = ()
    parameters: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if (self.start_date is None) != (self.end_date is None):
            raise ValueError("Provider request dates must be supplied together")
        if self.start_date is not None and self.start_date > self.end_date:
            raise ValueError("Provider request start_date must not exceed end_date")
        object.__setattr__(self, "instrument_ids", tuple(sorted(set(self.instrument_ids))))
        object.__setattr__(self, "parameters", MappingProxyType({
            str(key): _freeze_parameter(value)
            for key, value in self.parameters.items()
        }))


@dataclass(frozen=True)
class CoverageClaim:
    table: MarketTable
    complete: bool
    start_date: date | None = None
    end_date: date | None = None
    instrument_ids: tuple[str, ...] = ()
    detail: str | None = None

    def __post_init__(self) -> None:
        if (self.start_date is None) != (self.end_date is None):
            raise ValueError("Coverage dates must be supplied together")
        if self.start_date is not None and self.start_date > self.end_date:
            raise ValueError("Coverage start_date must not exceed end_date")
        object.__setattr__(self, "instrument_ids", tuple(sorted(set(self.instrument_ids))))


@dataclass(frozen=True)
class ObservationPayload:
    provider: str
    observed_at: datetime
    request: ProviderRequest
    tables: Mapping[MarketTable, Any]
    coverage: tuple[CoverageClaim, ...]
    source_metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.provider.strip():
            raise ValueError("Provider name cannot be empty")
        if self.observed_at.tzinfo is None:
            raise ValueError("observed_at must be timezone-aware")
        normalized = {MarketTable(key): value for key, value in self.tables.items()}
        if not normalized:
            raise ValueError("Observation payload must contain at least one table")
        claimed = {claim.table for claim in self.coverage}
        if not set(normalized) <= claimed:
            raise ValueError("Every observed table requires an explicit coverage claim")
        object.__setattr__(self, "tables", MappingProxyType(normalized))
        object.__setattr__(self, "coverage", tuple(self.coverage))
        object.__setattr__(self, "source_metadata", MappingProxyType(dict(self.source_metadata)))


@runtime_checkable
class MarketDataProvider(Protocol):
    name: str
    capabilities: frozenset[ProviderCapability]

    def observe(self, request: ProviderRequest) -> ObservationPayload: ...


@dataclass(frozen=True)
class StoredFile:
    table: MarketTable
    path: str
    sha256: str
    row_count: int


@dataclass(frozen=True)
class ObservationManifest:
    observation_id: str
    provider: str
    observed_at: datetime
    request: ProviderRequest
    coverage: tuple[CoverageClaim, ...]
    files: tuple[StoredFile, ...]
    source_metadata: Mapping[str, Any]
    schema_version: int = 1


@dataclass(frozen=True)
class SourceSlice:
    observation_id: str
    table: MarketTable
    reason: str
    instrument_ids: tuple[str, ...] = ()
    start_date: date | None = None
    end_date: date | None = None
    priority: int = 0

    def __post_init__(self) -> None:
        if not self.observation_id or not self.reason.strip():
            raise ValueError("Source slice requires an observation_id and an explicit reason")
        if (self.start_date is None) != (self.end_date is None):
            raise ValueError("Source slice dates must be supplied together")
        if self.start_date is not None and self.start_date > self.end_date:
            raise ValueError("Source slice start_date must not exceed end_date")
        object.__setattr__(self, "instrument_ids", tuple(sorted(set(self.instrument_ids))))


@dataclass(frozen=True)
class SnapshotPlan:
    selections: tuple[SourceSlice, ...]
    description: str
    require_complete_coverage: bool = True
    readiness: ReadinessProfile = ReadinessProfile.SIMULATION
    universe_scope: UniverseScope | None = None

    def __post_init__(self) -> None:
        if not self.selections or not self.description.strip():
            raise ValueError("Snapshot plan requires selections and a description")
        object.__setattr__(self, "selections", tuple(self.selections))
        object.__setattr__(self, "readiness", ReadinessProfile(self.readiness))
        if self.universe_scope is not None and not isinstance(self.universe_scope, UniverseScope):
            raise TypeError("universe_scope must be a UniverseScope")


@dataclass(frozen=True)
class QualityReport:
    state: SnapshotState
    errors: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()
    row_counts: Mapping[str, int] = field(default_factory=dict)

    @property
    def ready(self) -> bool:
        return self.state is SnapshotState.READY


@dataclass(frozen=True)
class SnapshotManifest:
    snapshot_id: str
    created_at: datetime
    plan: SnapshotPlan
    quality: QualityReport
    files: tuple[StoredFile, ...]
    schema_version: int = 1
    component_selections: tuple["SnapshotComponentSelection", ...] = ()


@dataclass(frozen=True)
class SnapshotComponentSelection:
    """One internal component selected by an otherwise stable public snapshot.

    Component storage and schemas deliberately remain private implementation
    details.  The snapshot only pins immutable component identities and their
    deterministic overlay order.
    """

    component_id: str
    priority: int = 0
    ordinal: int = 0

    def __post_init__(self) -> None:
        if not self.component_id.startswith("cmp-"):
            raise ValueError("Snapshot component ids must start with cmp-")
        if self.priority < 0 or self.ordinal < 0:
            raise ValueError("Snapshot component priority and ordinal must be non-negative")


class MarketDataError(RuntimeError):
    pass


class ProviderSelectionError(MarketDataError):
    pass


class ObservationError(MarketDataError):
    pass


class SourceConflictError(MarketDataError):
    pass


class ReconciliationError(MarketDataError):
    pass


class TradeRuleError(MarketDataError):
    pass


class SnapshotNotReadyError(MarketDataError):
    pass


class IntegrityError(MarketDataError):
    pass


def _freeze_parameter(value: Any) -> Any:
    """Make request parameters stable across JSON manifest round trips."""

    if isinstance(value, Mapping):
        return MappingProxyType({
            str(key): _freeze_parameter(item) for key, item in value.items()
        })
    if isinstance(value, (list, tuple, set, frozenset)):
        return tuple(_freeze_parameter(item) for item in value)
    return value
