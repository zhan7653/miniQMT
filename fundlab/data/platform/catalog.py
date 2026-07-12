from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime
import json
from pathlib import Path
import sqlite3
from typing import Iterator

from fundlab.common.dates import audit_now

from .types import (AttemptStatus, BatchStatus, CollectionPhase, CollectionRunStatus, PartitionIdentity,
                    PartitionStatus, PriceMode, VersionStatus)


CATALOG_SCHEMA_VERSION = 2


class CatalogError(RuntimeError):
    pass


class InvalidTransition(CatalogError):
    pass


@dataclass(frozen=True)
class BatchRecord:
    batch_id: str
    provider: str
    start_date: date
    end_date: date
    symbols: tuple[str, ...]
    universe_version: str
    config_hash: str
    request_fingerprint: str
    status: BatchStatus
    row_count: int
    error: str | None


@dataclass(frozen=True)
class VersionRecord:
    version_id: str
    batch_id: str
    manifest_fingerprint: str
    content_fingerprint: str
    status: VersionStatus
    previous_version_id: str | None
    published_at: datetime | None
    error: str | None


@dataclass(frozen=True)
class CollectionRunRecord:
    run_id: str
    provider: str
    phase: CollectionPhase
    target_date: date
    config_hash: str
    approved_speed_level: str
    status: CollectionRunStatus
    error: str | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None


@dataclass(frozen=True)
class CollectionPartitionRecord:
    partition_id: str
    run_id: str
    identity: PartitionIdentity
    status: PartitionStatus
    row_count: int = 0
    checksum: str | None = None
    storage_path: str | None = None
    retry_count: int = 0
    error: str | None = None


@dataclass(frozen=True)
class CollectionAttemptRecord:
    attempt_id: str
    partition_id: str
    attempt_number: int
    status: AttemptStatus
    started_at: datetime
    finished_at: datetime | None = None
    error: str | None = None


@dataclass(frozen=True)
class ThrottleEventRecord:
    event_id: str
    run_id: str
    speed_level: str
    workers: int
    request_interval_seconds: float
    cooldown_every_symbols: int
    cooldown_seconds: float
    reason: str
    observed_at: datetime


_BATCH_TRANSITIONS = {
    BatchStatus.PENDING: {BatchStatus.RUNNING, BatchStatus.FAILED},
    BatchStatus.RUNNING: {BatchStatus.COMPLETE, BatchStatus.FAILED},
    BatchStatus.COMPLETE: set(),
    BatchStatus.FAILED: set(),
}
_VERSION_TRANSITIONS = {
    VersionStatus.BUILDING: {VersionStatus.COMPLETE, VersionStatus.FAILED},
    VersionStatus.COMPLETE: {VersionStatus.SUPERSEDED},
    VersionStatus.FAILED: set(),
    VersionStatus.SUPERSEDED: set(),
}
_RUN_TRANSITIONS = {
    CollectionRunStatus.PENDING: {CollectionRunStatus.RUNNING, CollectionRunStatus.FAILED},
    CollectionRunStatus.RUNNING: {CollectionRunStatus.PAUSED, CollectionRunStatus.COMPLETE, CollectionRunStatus.FAILED},
    CollectionRunStatus.PAUSED: {CollectionRunStatus.RUNNING, CollectionRunStatus.FAILED},
    CollectionRunStatus.COMPLETE: set(),
    CollectionRunStatus.FAILED: set(),
}
_PARTITION_TRANSITIONS = {
    PartitionStatus.PENDING: {PartitionStatus.RUNNING},
    PartitionStatus.RUNNING: {PartitionStatus.COMPLETE, PartitionStatus.FAILED, PartitionStatus.QUARANTINED},
    PartitionStatus.FAILED: {PartitionStatus.RUNNING, PartitionStatus.QUARANTINED},
    PartitionStatus.COMPLETE: set(),
    PartitionStatus.QUARANTINED: set(),
}


class DataCatalog:
    """Audited v2 catalog. Schema changes require a new migration version."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS ingestion_batches (
                    batch_id TEXT PRIMARY KEY, provider TEXT NOT NULL,
                    start_date TEXT NOT NULL, end_date TEXT NOT NULL,
                    symbols_json TEXT NOT NULL, universe_version TEXT NOT NULL,
                    config_hash TEXT NOT NULL, request_fingerprint TEXT NOT NULL UNIQUE,
                    status TEXT NOT NULL, row_count INTEGER NOT NULL DEFAULT 0,
                    error TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS published_versions (
                    version_id TEXT PRIMARY KEY, batch_id TEXT NOT NULL,
                    manifest_fingerprint TEXT NOT NULL,
                    content_fingerprint TEXT NOT NULL,
                    status TEXT NOT NULL, previous_version_id TEXT,
                    published_at TEXT, error TEXT, created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(batch_id, content_fingerprint),
                    FOREIGN KEY(batch_id) REFERENCES ingestion_batches(batch_id)
                );
                CREATE TABLE IF NOT EXISTS catalog_transitions (
                    transition_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    entity_type TEXT NOT NULL, entity_id TEXT NOT NULL,
                    from_status TEXT, to_status TEXT NOT NULL,
                    occurred_at TEXT NOT NULL, detail TEXT
                );
                CREATE TABLE IF NOT EXISTS catalog_pointers (
                    name TEXT PRIMARY KEY, version_id TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                """
            )
            current = connection.execute("SELECT MAX(version) FROM schema_migrations").fetchone()[0]
            if current is not None and current > CATALOG_SCHEMA_VERSION:
                raise CatalogError(f"Catalog schema {current} is newer than supported {CATALOG_SCHEMA_VERSION}")
            if current is None:
                connection.execute(
                    "INSERT INTO schema_migrations(version, applied_at) VALUES (1, ?)",
                    (audit_now().isoformat(),),
                )
                current = 1
            if current < 2:
                connection.executescript(
                    """
                    CREATE TABLE collection_runs (
                        run_id TEXT PRIMARY KEY, provider TEXT NOT NULL, phase TEXT NOT NULL,
                        target_date TEXT NOT NULL, config_hash TEXT NOT NULL,
                        approved_speed_level TEXT NOT NULL, status TEXT NOT NULL, error TEXT,
                        started_at TEXT, finished_at TEXT, created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    );
                    CREATE TABLE collection_partitions (
                        partition_id TEXT PRIMARY KEY, run_id TEXT NOT NULL,
                        scope_fingerprint TEXT NOT NULL UNIQUE, provider TEXT NOT NULL,
                        symbol TEXT NOT NULL, price_mode TEXT NOT NULL,
                        start_date TEXT NOT NULL, end_date TEXT NOT NULL,
                        source_identity TEXT NOT NULL, status TEXT NOT NULL,
                        row_count INTEGER NOT NULL DEFAULT 0, checksum TEXT, storage_path TEXT,
                        retry_count INTEGER NOT NULL DEFAULT 0, error TEXT,
                        created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
                        FOREIGN KEY(run_id) REFERENCES collection_runs(run_id)
                    );
                    CREATE TABLE collection_attempts (
                        attempt_id TEXT PRIMARY KEY, partition_id TEXT NOT NULL,
                        attempt_number INTEGER NOT NULL, status TEXT NOT NULL,
                        started_at TEXT NOT NULL, finished_at TEXT, error TEXT,
                        UNIQUE(partition_id, attempt_number),
                        FOREIGN KEY(partition_id) REFERENCES collection_partitions(partition_id)
                    );
                    CREATE TABLE throttle_events (
                        event_id TEXT PRIMARY KEY, run_id TEXT NOT NULL, speed_level TEXT NOT NULL,
                        workers INTEGER NOT NULL, request_interval_seconds REAL NOT NULL,
                        cooldown_every_symbols INTEGER NOT NULL, cooldown_seconds REAL NOT NULL,
                        reason TEXT NOT NULL, observed_at TEXT NOT NULL,
                        FOREIGN KEY(run_id) REFERENCES collection_runs(run_id)
                    );
                    CREATE INDEX collection_partitions_run_status_idx
                        ON collection_partitions(run_id, status);
                    CREATE INDEX collection_attempts_partition_idx
                        ON collection_attempts(partition_id, attempt_number);
                    CREATE INDEX throttle_events_run_idx ON throttle_events(run_id, observed_at);
                    """
                )
                connection.execute(
                    "INSERT INTO schema_migrations(version, applied_at) VALUES (2, ?)",
                    (audit_now().isoformat(),),
                )

    def create_batch(self, record: BatchRecord) -> BatchRecord:
        if record.start_date > record.end_date or not record.symbols:
            raise ValueError("Batch scope must contain symbols and a valid date range")
        if record.status is not BatchStatus.PENDING or record.row_count != 0 or record.error is not None:
            raise ValueError("New batches must be pending with no rows or error")
        now = audit_now().isoformat()
        with self._transaction() as connection:
            existing = connection.execute(
                "SELECT * FROM ingestion_batches WHERE request_fingerprint=?", (record.request_fingerprint,)
            ).fetchone()
            if existing:
                return self._batch(existing)
            connection.execute(
                """INSERT INTO ingestion_batches VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (record.batch_id, record.provider, record.start_date.isoformat(), record.end_date.isoformat(),
                 json.dumps(sorted(set(record.symbols))), record.universe_version, record.config_hash,
                 record.request_fingerprint, record.status.value, 0, None, now, now),
            )
            self._audit(connection, "batch", record.batch_id, None, record.status.value, None)
        return record

    def transition_batch(self, batch_id: str, target: BatchStatus, *, row_count: int = 0, error: str | None = None) -> BatchRecord:
        if row_count < 0:
            raise ValueError("row_count cannot be negative")
        with self._transaction() as connection:
            row = self._required(connection, "ingestion_batches", "batch_id", batch_id)
            current = BatchStatus(row["status"])
            if target not in _BATCH_TRANSITIONS[current]:
                raise InvalidTransition(f"Batch cannot transition from {current} to {target}")
            if target is BatchStatus.FAILED and not error:
                raise ValueError("Failed batch requires an error")
            connection.execute(
                "UPDATE ingestion_batches SET status=?, row_count=?, error=?, updated_at=? WHERE batch_id=?",
                (target.value, row_count, error, audit_now().isoformat(), batch_id),
            )
            self._audit(connection, "batch", batch_id, current.value, target.value, error)
            return self._batch(self._required(connection, "ingestion_batches", "batch_id", batch_id))

    def create_version(self, record: VersionRecord) -> VersionRecord:
        if record.status is not VersionStatus.BUILDING or record.published_at is not None or record.error is not None:
            raise ValueError("New versions must be building and unpublished")
        now = audit_now().isoformat()
        with self._transaction() as connection:
            existing = connection.execute(
                "SELECT * FROM published_versions WHERE batch_id=? AND content_fingerprint=?",
                (record.batch_id, record.content_fingerprint),
            ).fetchone()
            if existing:
                return self._version(existing)
            self._required(connection, "ingestion_batches", "batch_id", record.batch_id)
            connection.execute(
                "INSERT INTO published_versions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (record.version_id, record.batch_id, record.manifest_fingerprint, record.content_fingerprint,
                 record.status.value, record.previous_version_id, None, None, now, now),
            )
            self._audit(connection, "version", record.version_id, None, record.status.value, None)
        return record

    def complete_version(self, version_id: str) -> VersionRecord:
        with self._transaction() as connection:
            row = self._required(connection, "published_versions", "version_id", version_id)
            current = VersionStatus(row["status"])
            if VersionStatus.COMPLETE not in _VERSION_TRANSITIONS[current]:
                raise InvalidTransition(f"Version cannot transition from {current} to complete")
            now = audit_now().isoformat()
            previous_id = row["previous_version_id"]
            pointer = connection.execute(
                "SELECT version_id FROM catalog_pointers WHERE name='latest_complete'"
            ).fetchone()
            latest_id = pointer["version_id"] if pointer else None
            if latest_id != previous_id:
                raise InvalidTransition(
                    "A new version must revise the current latest complete version; "
                    f"expected previous_version_id={latest_id!r}"
                )
            if previous_id:
                previous = self._required(connection, "published_versions", "version_id", previous_id)
                if VersionStatus(previous["status"]) is not VersionStatus.COMPLETE:
                    raise InvalidTransition("Revision predecessor must be complete")
                connection.execute(
                    "UPDATE published_versions SET status=?, updated_at=? WHERE version_id=?",
                    (VersionStatus.SUPERSEDED.value, now, previous_id),
                )
                self._audit(connection, "version", previous_id, VersionStatus.COMPLETE.value, VersionStatus.SUPERSEDED.value, version_id)
            connection.execute(
                "UPDATE published_versions SET status=?, published_at=?, updated_at=? WHERE version_id=?",
                (VersionStatus.COMPLETE.value, now, now, version_id),
            )
            connection.execute(
                "INSERT INTO catalog_pointers(name, version_id, updated_at) VALUES ('latest_complete', ?, ?) "
                "ON CONFLICT(name) DO UPDATE SET version_id=excluded.version_id, updated_at=excluded.updated_at",
                (version_id, now),
            )
            self._audit(connection, "version", version_id, current.value, VersionStatus.COMPLETE.value, None)
            return self._version(self._required(connection, "published_versions", "version_id", version_id))

    def fail_version(self, version_id: str, error: str) -> VersionRecord:
        if not error:
            raise ValueError("Failed version requires an error")
        with self._transaction() as connection:
            row = self._required(connection, "published_versions", "version_id", version_id)
            current = VersionStatus(row["status"])
            if VersionStatus.FAILED not in _VERSION_TRANSITIONS[current]:
                raise InvalidTransition(f"Version cannot transition from {current} to failed")
            connection.execute(
                "UPDATE published_versions SET status=?, error=?, updated_at=? WHERE version_id=?",
                (VersionStatus.FAILED.value, error, audit_now().isoformat(), version_id),
            )
            self._audit(connection, "version", version_id, current.value, VersionStatus.FAILED.value, error)
            return self._version(self._required(connection, "published_versions", "version_id", version_id))

    def latest_complete(self) -> VersionRecord | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT v.* FROM catalog_pointers p JOIN published_versions v ON v.version_id=p.version_id "
                "WHERE p.name='latest_complete' AND v.status=?", (VersionStatus.COMPLETE.value,)
            ).fetchone()
            return self._version(row) if row else None

    def create_collection_run(self, record: CollectionRunRecord) -> CollectionRunRecord:
        if (record.status is not CollectionRunStatus.PENDING or record.error is not None
                or record.started_at is not None or record.finished_at is not None):
            raise ValueError("New collection runs must be pending and unstarted")
        if not all((record.run_id, record.provider, record.config_hash, record.approved_speed_level)):
            raise ValueError("Collection run identity and configuration fields cannot be empty")
        now = audit_now().isoformat()
        with self._transaction() as connection:
            existing = connection.execute("SELECT * FROM collection_runs WHERE run_id=?", (record.run_id,)).fetchone()
            if existing:
                return self._run(existing)
            connection.execute(
                """INSERT INTO collection_runs
                   (run_id, provider, phase, target_date, config_hash, approved_speed_level, status,
                    error, started_at, finished_at, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, ?, ?)""",
                (record.run_id, record.provider, record.phase.value, record.target_date.isoformat(),
                 record.config_hash, record.approved_speed_level, record.status.value, now, now),
            )
            self._audit(connection, "collection_run", record.run_id, None, record.status.value, None)
        return record

    def transition_collection_run(self, run_id: str, target: CollectionRunStatus, *, error: str | None = None) -> CollectionRunRecord:
        with self._transaction() as connection:
            row = self._required(connection, "collection_runs", "run_id", run_id)
            current = CollectionRunStatus(row["status"])
            if target not in _RUN_TRANSITIONS[current]:
                raise InvalidTransition(f"Collection run cannot transition from {current} to {target}")
            if target is CollectionRunStatus.FAILED and not error:
                raise ValueError("Failed collection run requires an error")
            if target is not CollectionRunStatus.FAILED and error is not None:
                raise ValueError("Only failed collection runs may record an error")
            now = audit_now().isoformat()
            started_at = now if target is CollectionRunStatus.RUNNING and row["started_at"] is None else row["started_at"]
            finished_at = now if target in {CollectionRunStatus.COMPLETE, CollectionRunStatus.FAILED} else None
            connection.execute(
                "UPDATE collection_runs SET status=?, error=?, started_at=?, finished_at=?, updated_at=? WHERE run_id=?",
                (target.value, error, started_at, finished_at, now, run_id),
            )
            self._audit(connection, "collection_run", run_id, current.value, target.value, error)
            return self._run(self._required(connection, "collection_runs", "run_id", run_id))

    def get_collection_run(self, run_id: str) -> CollectionRunRecord:
        with self._connect() as connection:
            return self._run(self._required(connection, "collection_runs", "run_id", run_id))

    def create_partition(self, record: CollectionPartitionRecord) -> CollectionPartitionRecord:
        if (record.status is not PartitionStatus.PENDING or record.row_count != 0 or record.checksum is not None
                or record.storage_path is not None or record.retry_count != 0 or record.error is not None):
            raise ValueError("New collection partitions must be pending without result state")
        if record.partition_id != record.identity.fingerprint:
            raise ValueError("partition_id must equal the partition identity fingerprint")
        now = audit_now().isoformat()
        with self._transaction() as connection:
            existing = connection.execute(
                "SELECT * FROM collection_partitions WHERE scope_fingerprint=?", (record.identity.fingerprint,)
            ).fetchone()
            if existing:
                return self._partition(existing)
            self._required(connection, "collection_runs", "run_id", record.run_id)
            identity = record.identity
            connection.execute(
                """INSERT INTO collection_partitions
                   (partition_id, run_id, scope_fingerprint, provider, symbol, price_mode, start_date,
                    end_date, source_identity, status, row_count, checksum, storage_path, retry_count,
                    error, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, NULL, NULL, 0, NULL, ?, ?)""",
                (record.partition_id, record.run_id, identity.fingerprint, identity.provider, identity.symbol,
                 identity.price_mode.value, identity.start_date.isoformat(), identity.end_date.isoformat(),
                 identity.source_identity, record.status.value, now, now),
            )
            self._audit(connection, "collection_partition", record.partition_id, None, record.status.value, None)
        return record

    def transition_partition(
        self, partition_id: str, target: PartitionStatus, *, row_count: int = 0,
        checksum: str | None = None, storage_path: str | None = None, error: str | None = None,
    ) -> CollectionPartitionRecord:
        if row_count < 0:
            raise ValueError("row_count cannot be negative")
        with self._transaction() as connection:
            row = self._required(connection, "collection_partitions", "partition_id", partition_id)
            current = PartitionStatus(row["status"])
            if target not in _PARTITION_TRANSITIONS[current]:
                raise InvalidTransition(f"Partition cannot transition from {current} to {target}")
            if target is PartitionStatus.COMPLETE and (not checksum or not storage_path):
                raise ValueError("Complete partition requires checksum and storage_path")
            if target in {PartitionStatus.FAILED, PartitionStatus.QUARANTINED} and not error:
                raise ValueError(f"{target.value.title()} partition requires an error")
            if target in {PartitionStatus.PENDING, PartitionStatus.RUNNING} and any((checksum, storage_path, error)):
                raise ValueError("Pending/running partition cannot contain result state")
            connection.execute(
                """UPDATE collection_partitions
                   SET status=?, row_count=?, checksum=?, storage_path=?, error=?, updated_at=?
                   WHERE partition_id=?""",
                (target.value, row_count, checksum, storage_path, error, audit_now().isoformat(), partition_id),
            )
            self._audit(connection, "collection_partition", partition_id, current.value, target.value, error)
            return self._partition(self._required(connection, "collection_partitions", "partition_id", partition_id))

    def get_partition(self, partition_id: str) -> CollectionPartitionRecord:
        with self._connect() as connection:
            return self._partition(self._required(connection, "collection_partitions", "partition_id", partition_id))

    def list_partitions(
        self, *, run_id: str | None = None, status: PartitionStatus | None = None,
    ) -> tuple[CollectionPartitionRecord, ...]:
        clauses: list[str] = []
        parameters: list[str] = []
        if run_id is not None:
            clauses.append("run_id=?")
            parameters.append(run_id)
        if status is not None:
            clauses.append("status=?")
            parameters.append(status.value)
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._connect() as connection:
            rows = connection.execute(
                f"SELECT * FROM collection_partitions{where} ORDER BY symbol, start_date, price_mode",
                tuple(parameters),
            ).fetchall()
            return tuple(self._partition(row) for row in rows)

    def start_attempt(self, partition_id: str, attempt_id: str) -> CollectionAttemptRecord:
        if not attempt_id:
            raise ValueError("attempt_id cannot be empty")
        now = audit_now()
        with self._transaction() as connection:
            partition = self._required(connection, "collection_partitions", "partition_id", partition_id)
            if PartitionStatus(partition["status"]) is not PartitionStatus.RUNNING:
                raise InvalidTransition("Attempts can only start for running partitions")
            next_number = connection.execute(
                "SELECT COALESCE(MAX(attempt_number), 0) + 1 FROM collection_attempts WHERE partition_id=?",
                (partition_id,),
            ).fetchone()[0]
            connection.execute(
                "INSERT INTO collection_attempts VALUES (?, ?, ?, ?, ?, NULL, NULL)",
                (attempt_id, partition_id, next_number, AttemptStatus.RUNNING.value, now.isoformat()),
            )
            connection.execute(
                "UPDATE collection_partitions SET retry_count=?, updated_at=? WHERE partition_id=?",
                (max(0, next_number - 1), now.isoformat(), partition_id),
            )
            self._audit(connection, "collection_attempt", attempt_id, None, AttemptStatus.RUNNING.value, None)
            return self._attempt(self._required(connection, "collection_attempts", "attempt_id", attempt_id))

    def finish_attempt(self, attempt_id: str, target: AttemptStatus, *, error: str | None = None) -> CollectionAttemptRecord:
        if target not in {AttemptStatus.SUCCEEDED, AttemptStatus.FAILED}:
            raise ValueError("Attempt target must be succeeded or failed")
        if target is AttemptStatus.FAILED and not error:
            raise ValueError("Failed attempt requires an error")
        if target is AttemptStatus.SUCCEEDED and error is not None:
            raise ValueError("Succeeded attempt cannot record an error")
        with self._transaction() as connection:
            row = self._required(connection, "collection_attempts", "attempt_id", attempt_id)
            current = AttemptStatus(row["status"])
            if current is not AttemptStatus.RUNNING:
                raise InvalidTransition(f"Attempt cannot transition from {current} to {target}")
            now = audit_now().isoformat()
            connection.execute(
                "UPDATE collection_attempts SET status=?, finished_at=?, error=? WHERE attempt_id=?",
                (target.value, now, error, attempt_id),
            )
            self._audit(connection, "collection_attempt", attempt_id, current.value, target.value, error)
            return self._attempt(self._required(connection, "collection_attempts", "attempt_id", attempt_id))

    def list_attempts(self, partition_id: str) -> tuple[CollectionAttemptRecord, ...]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM collection_attempts WHERE partition_id=? ORDER BY attempt_number", (partition_id,)
            ).fetchall()
            return tuple(self._attempt(row) for row in rows)

    def record_throttle_event(self, record: ThrottleEventRecord) -> ThrottleEventRecord:
        if (not all((record.event_id, record.run_id, record.speed_level, record.reason)) or record.workers < 1
                or record.request_interval_seconds < 0 or record.cooldown_every_symbols < 1
                or record.cooldown_seconds < 0):
            raise ValueError("Throttle event fields must describe a valid positive profile")
        with self._transaction() as connection:
            self._required(connection, "collection_runs", "run_id", record.run_id)
            connection.execute(
                "INSERT INTO throttle_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (record.event_id, record.run_id, record.speed_level, record.workers,
                 record.request_interval_seconds, record.cooldown_every_symbols, record.cooldown_seconds,
                 record.reason, record.observed_at.isoformat()),
            )
        return record

    def list_throttle_events(self, run_id: str) -> tuple[ThrottleEventRecord, ...]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM throttle_events WHERE run_id=? ORDER BY observed_at, event_id", (run_id,)
            ).fetchall()
            return tuple(self._throttle_event(row) for row in rows)

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        try:
            yield connection
            connection.commit()
        finally:
            connection.close()

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                yield connection
            except Exception:
                connection.rollback()
                raise

    @staticmethod
    def _required(connection: sqlite3.Connection, table: str, key: str, value: str) -> sqlite3.Row:
        row = connection.execute(f"SELECT * FROM {table} WHERE {key}=?", (value,)).fetchone()
        if row is None:
            raise CatalogError(f"Unknown {table} record: {value}")
        return row

    @staticmethod
    def _audit(connection: sqlite3.Connection, entity_type: str, entity_id: str, before: str | None, after: str, detail: str | None) -> None:
        connection.execute(
            "INSERT INTO catalog_transitions(entity_type, entity_id, from_status, to_status, occurred_at, detail) VALUES (?, ?, ?, ?, ?, ?)",
            (entity_type, entity_id, before, after, audit_now().isoformat(), detail),
        )

    @staticmethod
    def _batch(row: sqlite3.Row) -> BatchRecord:
        return BatchRecord(row["batch_id"], row["provider"], date.fromisoformat(row["start_date"]),
                           date.fromisoformat(row["end_date"]), tuple(json.loads(row["symbols_json"])),
                           row["universe_version"], row["config_hash"], row["request_fingerprint"],
                           BatchStatus(row["status"]), row["row_count"], row["error"])

    @staticmethod
    def _version(row: sqlite3.Row) -> VersionRecord:
        published_at = datetime.fromisoformat(row["published_at"]) if row["published_at"] else None
        return VersionRecord(row["version_id"], row["batch_id"], row["manifest_fingerprint"],
                             row["content_fingerprint"], VersionStatus(row["status"]),
                             row["previous_version_id"], published_at, row["error"])

    @staticmethod
    def _run(row: sqlite3.Row) -> CollectionRunRecord:
        return CollectionRunRecord(
            row["run_id"], row["provider"], CollectionPhase(row["phase"]), date.fromisoformat(row["target_date"]),
            row["config_hash"], row["approved_speed_level"], CollectionRunStatus(row["status"]), row["error"],
            datetime.fromisoformat(row["started_at"]) if row["started_at"] else None,
            datetime.fromisoformat(row["finished_at"]) if row["finished_at"] else None,
        )

    @staticmethod
    def _partition(row: sqlite3.Row) -> CollectionPartitionRecord:
        identity = PartitionIdentity(
            row["provider"], row["symbol"], PriceMode(row["price_mode"]), date.fromisoformat(row["start_date"]),
            date.fromisoformat(row["end_date"]), row["source_identity"],
        )
        return CollectionPartitionRecord(
            row["partition_id"], row["run_id"], identity, PartitionStatus(row["status"]), row["row_count"],
            row["checksum"], row["storage_path"], row["retry_count"], row["error"],
        )

    @staticmethod
    def _attempt(row: sqlite3.Row) -> CollectionAttemptRecord:
        return CollectionAttemptRecord(
            row["attempt_id"], row["partition_id"], row["attempt_number"], AttemptStatus(row["status"]),
            datetime.fromisoformat(row["started_at"]),
            datetime.fromisoformat(row["finished_at"]) if row["finished_at"] else None, row["error"],
        )

    @staticmethod
    def _throttle_event(row: sqlite3.Row) -> ThrottleEventRecord:
        return ThrottleEventRecord(
            row["event_id"], row["run_id"], row["speed_level"], row["workers"],
            row["request_interval_seconds"], row["cooldown_every_symbols"], row["cooldown_seconds"],
            row["reason"], datetime.fromisoformat(row["observed_at"]),
        )
