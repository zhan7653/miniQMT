from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime
import json
from pathlib import Path
import sqlite3
from typing import Iterator

from fundlab.common.dates import audit_now

from .types import BatchStatus, VersionStatus


CATALOG_SCHEMA_VERSION = 1


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
            connection.execute(
                "INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES (?, ?)",
                (CATALOG_SCHEMA_VERSION, audit_now().isoformat()),
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
