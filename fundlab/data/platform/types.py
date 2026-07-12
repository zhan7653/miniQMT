from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from enum import StrEnum
from hashlib import sha256
import json
from typing import Any, Mapping, Protocol, Sequence, runtime_checkable


class ProviderCapability(StrEnum):
    TRADING_CALENDAR = "trading_calendar"
    INSTRUMENTS = "instruments"
    DAILY_BARS_RAW = "daily_bars_raw"
    DAILY_BARS_ADJUSTED = "daily_bars_adjusted"


class ProviderHealth(StrEnum):
    AVAILABLE = "available"
    SDK_MISSING = "sdk_missing"
    SERVICE_UNAVAILABLE = "service_unavailable"
    UNHEALTHY = "unhealthy"


class PriceMode(StrEnum):
    RAW = "raw"
    ADJUSTED = "adjusted"


class BatchStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETE = "complete"
    FAILED = "failed"


class VersionStatus(StrEnum):
    BUILDING = "building"
    COMPLETE = "complete"
    FAILED = "failed"
    SUPERSEDED = "superseded"


class CollectionPhase(StrEnum):
    DISCOVER = "discover"
    CANARY = "canary"
    COLLECT = "collect"
    PUBLISH = "publish"
    DAILY = "daily"


class CollectionRunStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETE = "complete"
    FAILED = "failed"


class PartitionStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETE = "complete"
    FAILED = "failed"
    QUARANTINED = "quarantined"


class AttemptStatus(StrEnum):
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class TrustState(StrEnum):
    TRUSTED = "trusted"
    QUARANTINED = "quarantined"
    UNTRUSTED = "untrusted"


class QualityDisposition(StrEnum):
    BLOCK_BATCH = "block_batch"
    BLOCK_SYMBOL = "block_symbol"
    VALID_SUSPENDED = "valid_suspended"
    WARNING = "warning"
    PASS = "pass"


def stable_fingerprint(value: Mapping[str, Any] | Sequence[Any]) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return sha256(encoded.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class ProviderRequest:
    symbols: tuple[str, ...]
    start_date: date
    end_date: date
    capability: ProviderCapability

    def __post_init__(self) -> None:
        if not self.symbols:
            raise ValueError("Provider request symbols cannot be empty")
        if self.start_date > self.end_date:
            raise ValueError("Provider request start_date must not exceed end_date")


@dataclass(frozen=True)
class SymbolResult:
    symbol: str
    rows: int
    error: str | None = None


@dataclass(frozen=True)
class ProviderResult:
    provider: str
    capability: ProviderCapability
    symbols: tuple[SymbolResult, ...]
    observed_at: datetime

    @property
    def row_count(self) -> int:
        return sum(item.rows for item in self.symbols)


@dataclass(frozen=True)
class PreflightResult:
    provider: str
    health: ProviderHealth
    observed_at: datetime
    detail: str | None = None

    @property
    def available(self) -> bool:
        return self.health is ProviderHealth.AVAILABLE


@runtime_checkable
class DataProvider(Protocol):
    name: str
    capabilities: frozenset[ProviderCapability]

    def preflight(self) -> PreflightResult: ...


@dataclass(frozen=True)
class ManifestIdentity:
    provider: str
    batch_id: str
    version_id: str
    published_at: datetime
    quality_state: TrustState
    content_fingerprint: str
    schema_version: int
    row_counts: Mapping[str, int] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.schema_version < 1:
            raise ValueError("Manifest schema_version must be positive")
        if any(count < 0 for count in self.row_counts.values()):
            raise ValueError("Manifest row counts cannot be negative")


@dataclass(frozen=True)
class PartitionIdentity:
    provider: str
    symbol: str
    price_mode: PriceMode
    start_date: date
    end_date: date
    source_identity: str

    def __post_init__(self) -> None:
        if not self.provider or not self.symbol or not self.source_identity:
            raise ValueError("Partition identity fields cannot be empty")
        if self.start_date > self.end_date:
            raise ValueError("Partition start_date must not exceed end_date")

    @property
    def fingerprint(self) -> str:
        return stable_fingerprint({
            "provider": self.provider,
            "symbol": self.symbol,
            "price_mode": self.price_mode.value,
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat(),
            "source_identity": self.source_identity,
        })


@dataclass(frozen=True)
class PartitionArtifact:
    identity: PartitionIdentity
    path: str
    checksum: str
    row_count: int

    def __post_init__(self) -> None:
        if not self.path or not self.checksum:
            raise ValueError("Partition artifact path and checksum cannot be empty")
        if self.row_count < 0:
            raise ValueError("Partition artifact row_count cannot be negative")
