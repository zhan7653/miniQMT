from __future__ import annotations

from datetime import date
import os
from pathlib import Path
import shutil
import sqlite3
from typing import Mapping, Sequence

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from fundlab.data.platform import DataCatalog, ManifestIdentity, VersionStatus

from .manifest import PublishedManifest, build_manifest, checksum_file


class StorageError(RuntimeError):
    pass


class ImmutableVersionError(StorageError):
    pass


class VersionNotVisibleError(StorageError):
    pass


class CorruptVersionError(StorageError):
    pass


class VersionedParquetStore:
    """Immutable same-volume staging and published Parquet storage."""

    def __init__(self, root: str | Path, catalog: DataCatalog) -> None:
        self.root = Path(root)
        self.catalog = catalog
        self.raw_root = self.root / "raw"
        self.staging_root = self.root / "staging"
        self.published_root = self.root / "published"

    def initialize(self) -> None:
        for directory in (self.raw_root, self.staging_root, self.published_root):
            directory.mkdir(parents=True, exist_ok=True)

    def staging_path(self, version_id: str) -> Path:
        return self.staging_root / version_id

    def published_path(self, version_id: str) -> Path:
        return self.published_root / version_id

    def begin_version(self, version_id: str) -> Path:
        self.initialize()
        target = self.staging_path(version_id)
        if target.exists() or self.published_path(version_id).exists():
            raise ImmutableVersionError(f"Version storage already exists: {version_id}")
        target.mkdir()
        return target

    def write_table(self, version_id: str, table_name: str, table: pa.Table, *, partitioning: Sequence[str] = ()) -> None:
        target = self.staging_path(version_id) / table_name
        if not self.staging_path(version_id).is_dir():
            raise StorageError(f"Version is not staged: {version_id}")
        target.mkdir(parents=True, exist_ok=False)
        if partitioning:
            ds.write_dataset(table, target, format="parquet", partitioning=list(partitioning),
                             existing_data_behavior="error")
        else:
            pq.write_table(table, target / "part-0.parquet")

    def write_raw(self, batch_id: str, table_name: str, table: pa.Table) -> Path:
        target = self.raw_root / batch_id / table_name
        if target.exists():
            raise ImmutableVersionError(f"Raw batch table already exists: {batch_id}/{table_name}")
        target.mkdir(parents=True, exist_ok=False)
        path = target / "part-0.parquet"
        pq.write_table(table, path)
        return path

    def finalize(self, identity: ManifestIdentity, row_counts: Mapping[str, int]) -> PublishedManifest:
        source = self.staging_path(identity.version_id)
        target = self.published_path(identity.version_id)
        if not source.is_dir():
            raise StorageError(f"Version is not staged: {identity.version_id}")
        if target.exists():
            raise ImmutableVersionError(f"Published version already exists: {identity.version_id}")
        manifest = build_manifest(identity, source, row_counts)
        manifest.write(source)
        os.replace(source, target)
        return manifest

    def prepare_manifest(self, identity: ManifestIdentity, row_counts: Mapping[str, int]) -> PublishedManifest:
        source = self.staging_path(identity.version_id)
        if not source.is_dir():
            raise StorageError(f"Version is not staged: {identity.version_id}")
        return build_manifest(identity, source, row_counts)

    def finalize_manifest(self, manifest: PublishedManifest) -> None:
        version_id = manifest.identity.version_id
        source, target = self.staging_path(version_id), self.published_path(version_id)
        if not source.is_dir():
            raise StorageError(f"Version is not staged: {version_id}")
        if target.exists():
            raise ImmutableVersionError(f"Published version already exists: {version_id}")
        manifest.write(source)
        os.replace(source, target)

    def discard_staging(self, version_id: str) -> None:
        target = self.staging_path(version_id)
        if target.exists():
            shutil.rmtree(target)

    def resolve_complete(self, version_id: str | None = None) -> tuple[str, PublishedManifest]:
        record = self.catalog.latest_complete() if version_id is None else self._version_record(version_id)
        if record is None:
            raise VersionNotVisibleError("No complete published data version is available")
        allowed = ({VersionStatus.COMPLETE} if version_id is None
                   else {VersionStatus.COMPLETE, VersionStatus.SUPERSEDED})
        if record.status not in allowed:
            raise VersionNotVisibleError(f"Version is not complete and visible: {record.version_id} ({record.status.value})")
        directory = self.published_path(record.version_id)
        if not directory.is_dir():
            raise CorruptVersionError(f"Complete catalog version has no published directory: {record.version_id}")
        manifest = PublishedManifest.read(directory)
        if manifest.identity.version_id != record.version_id:
            raise CorruptVersionError("Manifest version identity does not match catalog")
        if manifest.fingerprint != record.manifest_fingerprint:
            raise CorruptVersionError("Manifest fingerprint does not match catalog")
        if manifest.identity.content_fingerprint != record.content_fingerprint:
            raise CorruptVersionError("Content fingerprint does not match catalog")
        for item in manifest.files:
            path = directory / item.path
            if not path.is_file() or path.stat().st_size != item.size or checksum_file(path) != item.sha256:
                raise CorruptVersionError(f"Published file checksum mismatch: {item.path}")
        return record.version_id, manifest

    def read_table(self, version_id: str, table_name: str, *, columns: Sequence[str] | None = None,
                   filters: ds.Expression | None = None) -> pa.Table:
        self.resolve_complete(version_id)
        path = self.published_path(version_id) / table_name
        if not path.is_dir():
            raise StorageError(f"Published table not found: {table_name}")
        projected = list(columns) if columns is not None else None
        return ds.dataset(path, format="parquet", partitioning="hive").to_table(columns=projected, filter=filters)

    def _version_record(self, version_id: str):
        with sqlite3.connect(f"file:{self.catalog.path.as_posix()}?mode=ro", uri=True) as connection:
            connection.row_factory = sqlite3.Row
            row = connection.execute("SELECT * FROM published_versions WHERE version_id=?", (version_id,)).fetchone()
        if row is None:
            raise VersionNotVisibleError(f"Unknown published data version: {version_id}")
        return self.catalog._version(row)
