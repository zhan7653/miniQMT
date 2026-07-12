from datetime import date, datetime, timezone
import sqlite3

import pytest

from fundlab.data.platform import (
    CATALOG_SCHEMA_VERSION,
    BatchRecord,
    BatchStatus,
    CollectionPartitionRecord,
    CollectionPhase,
    CollectionRunRecord,
    CollectionRunStatus,
    DataCatalog,
    InvalidTransition,
    PartitionIdentity,
    PartitionStatus,
    PriceMode,
    AttemptStatus,
    ThrottleEventRecord,
    VersionRecord,
    VersionStatus,
)


def batch(batch_id="batch-1", fingerprint="request-1"):
    return BatchRecord(batch_id, "xtquant", date(2026, 5, 8), date(2026, 5, 8),
                       ("510300.SH",), "reviewed-v1", "config-hash", fingerprint,
                       BatchStatus.PENDING, 0, None)


def version(version_id="version-1", batch_id="batch-1", content="content-1", previous=None):
    return VersionRecord(version_id, batch_id, f"manifest-{content}", content,
                         VersionStatus.BUILDING, previous, None, None)


def test_schema_version_and_append_only_audit(tmp_path):
    catalog = DataCatalog(tmp_path / "catalog.sqlite3")
    catalog.initialize()
    catalog.create_batch(batch())
    catalog.transition_batch("batch-1", BatchStatus.RUNNING)
    catalog.transition_batch("batch-1", BatchStatus.COMPLETE, row_count=3)

    with sqlite3.connect(catalog.path) as connection:
        assert connection.execute("SELECT MAX(version) FROM schema_migrations").fetchone()[0] == CATALOG_SCHEMA_VERSION
        assert connection.execute("SELECT COUNT(*) FROM catalog_transitions").fetchone()[0] == 3


def test_existing_v1_catalog_migrates_without_changing_complete_pointer(tmp_path):
    catalog = DataCatalog(tmp_path / "catalog.sqlite3")
    catalog.initialize()
    catalog.create_batch(batch())
    catalog.create_version(version())
    catalog.complete_version("version-1")
    with sqlite3.connect(catalog.path) as connection:
        connection.executescript(
            """
            DROP TABLE throttle_events;
            DROP TABLE collection_attempts;
            DROP TABLE collection_partitions;
            DROP TABLE collection_runs;
            DELETE FROM schema_migrations WHERE version=2;
            """
        )

    catalog.initialize()

    assert catalog.latest_complete().version_id == "version-1"
    with sqlite3.connect(catalog.path) as connection:
        assert connection.execute("SELECT version FROM schema_migrations ORDER BY version").fetchall() == [(1,), (2,)]
        assert connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='collection_partitions'"
        ).fetchone() == ("collection_partitions",)


def test_request_and_content_identities_are_idempotent(tmp_path):
    catalog = DataCatalog(tmp_path / "catalog.sqlite3")
    catalog.initialize()
    first = catalog.create_batch(batch())
    reused = catalog.create_batch(batch("different-id", "request-1"))
    assert reused.batch_id == first.batch_id
    catalog.create_version(version())
    reused_version = catalog.create_version(version("different-version", content="content-1"))
    assert reused_version.version_id == "version-1"


def test_complete_pointer_is_atomic_and_failed_candidate_is_invisible(tmp_path):
    catalog = DataCatalog(tmp_path / "catalog.sqlite3")
    catalog.initialize()
    catalog.create_batch(batch())
    catalog.create_version(version())
    assert catalog.latest_complete() is None
    catalog.fail_version("version-1", "manifest checksum mismatch")
    assert catalog.latest_complete() is None
    with pytest.raises(InvalidTransition):
        catalog.complete_version("version-1")


def test_revision_completes_then_supersedes_previous_in_one_transaction(tmp_path):
    catalog = DataCatalog(tmp_path / "catalog.sqlite3")
    catalog.initialize()
    catalog.create_batch(batch())
    catalog.create_version(version())
    catalog.complete_version("version-1")
    catalog.create_batch(batch("batch-2", "request-2"))
    catalog.create_version(version("version-2", "batch-2", "content-2", "version-1"))

    completed = catalog.complete_version("version-2")
    assert completed.status is VersionStatus.COMPLETE
    assert catalog.latest_complete().version_id == "version-2"
    with sqlite3.connect(catalog.path) as connection:
        assert connection.execute(
            "SELECT status FROM published_versions WHERE version_id='version-1'"
        ).fetchone()[0] == VersionStatus.SUPERSEDED.value


def test_invalid_transitions_and_failed_states_are_explicit(tmp_path):
    catalog = DataCatalog(tmp_path / "catalog.sqlite3")
    catalog.initialize()
    catalog.create_batch(batch())
    with pytest.raises(InvalidTransition):
        catalog.transition_batch("batch-1", BatchStatus.COMPLETE)
    with pytest.raises(ValueError, match="error"):
        catalog.transition_batch("batch-1", BatchStatus.FAILED)


def test_revision_must_link_current_complete_version(tmp_path):
    catalog = DataCatalog(tmp_path / "catalog.sqlite3")
    catalog.initialize()
    catalog.create_batch(batch())
    catalog.create_version(version())
    catalog.complete_version("version-1")
    catalog.create_batch(batch("batch-2", "request-2"))
    catalog.create_version(version("version-2", "batch-2", "content-2"))
    with pytest.raises(InvalidTransition, match="previous_version_id"):
        catalog.complete_version("version-2")
    assert catalog.latest_complete().version_id == "version-1"


def test_collection_run_partition_attempt_and_throttle_state_are_audited(tmp_path):
    catalog = DataCatalog(tmp_path / "catalog.sqlite3")
    catalog.initialize()
    run = CollectionRunRecord(
        "run-1", "xtquant", CollectionPhase.CANARY, date(2026, 5, 8), "config", "initial",
        CollectionRunStatus.PENDING,
    )
    catalog.create_collection_run(run)
    catalog.transition_collection_run("run-1", CollectionRunStatus.RUNNING)
    identity = PartitionIdentity(
        "xtquant", "510300.SH", PriceMode.RAW, date(2025, 1, 1), date(2025, 12, 31), "xtquant-v1"
    )
    created = catalog.create_partition(CollectionPartitionRecord(
        identity.fingerprint, "run-1", identity, PartitionStatus.PENDING,
    ))
    assert catalog.create_partition(CollectionPartitionRecord(
        identity.fingerprint, "run-1", identity, PartitionStatus.PENDING,
    )) == created
    catalog.transition_partition(identity.fingerprint, PartitionStatus.RUNNING)
    attempt = catalog.start_attempt(identity.fingerprint, "attempt-1")
    assert attempt.attempt_number == 1
    catalog.finish_attempt("attempt-1", AttemptStatus.SUCCEEDED)
    completed = catalog.transition_partition(
        identity.fingerprint, PartitionStatus.COMPLETE, row_count=242,
        checksum="abc", storage_path="raw/partitions/abc",
    )
    assert completed.status is PartitionStatus.COMPLETE
    assert completed.retry_count == 0
    catalog.record_throttle_event(ThrottleEventRecord(
        "throttle-1", "run-1", "initial", 1, 2.0, 20, 60.0, "run_started",
        datetime(2026, 5, 8, tzinfo=timezone.utc),
    ))
    catalog.transition_collection_run("run-1", CollectionRunStatus.PAUSED)

    with sqlite3.connect(catalog.path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM collection_attempts").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM throttle_events").fetchone()[0] == 1
        audited = connection.execute(
            "SELECT COUNT(*) FROM catalog_transitions WHERE entity_type LIKE 'collection_%'"
        ).fetchone()[0]
        assert audited == 8


def test_collection_illegal_transitions_and_retry_count_are_explicit(tmp_path):
    catalog = DataCatalog(tmp_path / "catalog.sqlite3")
    catalog.initialize()
    catalog.create_collection_run(CollectionRunRecord(
        "run-1", "xtquant", CollectionPhase.COLLECT, date(2026, 5, 8), "config", "initial",
        CollectionRunStatus.PENDING,
    ))
    identity = PartitionIdentity(
        "xtquant", "510300.SH", PriceMode.ADJUSTED, date(2025, 1, 1), date(2025, 12, 31), "xtquant-v1"
    )
    catalog.create_partition(CollectionPartitionRecord(
        identity.fingerprint, "run-1", identity, PartitionStatus.PENDING,
    ))
    with pytest.raises(InvalidTransition):
        catalog.transition_partition(identity.fingerprint, PartitionStatus.COMPLETE, checksum="abc", storage_path="x")
    catalog.transition_collection_run("run-1", CollectionRunStatus.RUNNING)
    catalog.transition_partition(identity.fingerprint, PartitionStatus.RUNNING)
    catalog.start_attempt(identity.fingerprint, "attempt-1")
    catalog.finish_attempt("attempt-1", AttemptStatus.FAILED, error="temporary")
    catalog.transition_partition(identity.fingerprint, PartitionStatus.FAILED, error="temporary")
    catalog.transition_partition(identity.fingerprint, PartitionStatus.RUNNING)
    catalog.start_attempt(identity.fingerprint, "attempt-2")
    assert catalog.get_partition(identity.fingerprint).retry_count == 1
    with pytest.raises(InvalidTransition):
        catalog.transition_collection_run("run-1", CollectionRunStatus.PENDING)
