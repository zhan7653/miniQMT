from datetime import date
import sqlite3

import pytest

from fundlab.data.platform import (
    CATALOG_SCHEMA_VERSION,
    BatchRecord,
    BatchStatus,
    DataCatalog,
    InvalidTransition,
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
        assert connection.execute("SELECT version FROM schema_migrations").fetchone()[0] == CATALOG_SCHEMA_VERSION
        assert connection.execute("SELECT COUNT(*) FROM catalog_transitions").fetchone()[0] == 3


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
