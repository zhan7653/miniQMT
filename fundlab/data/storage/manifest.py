from __future__ import annotations

from dataclasses import asdict, dataclass
from hashlib import sha256
import json
from pathlib import Path
from typing import Mapping

from fundlab.data.platform import ManifestIdentity, TrustState


MANIFEST_FILE = "manifest.json"


class ManifestError(RuntimeError):
    pass


@dataclass(frozen=True)
class ManifestFile:
    path: str
    sha256: str
    size: int


@dataclass(frozen=True)
class PublishedManifest:
    identity: ManifestIdentity
    files: tuple[ManifestFile, ...]

    @property
    def fingerprint(self) -> str:
        return sha256(self.to_json().encode("utf-8")).hexdigest()

    def to_json(self) -> str:
        value = {
            "identity": {
                **asdict(self.identity),
                "published_at": self.identity.published_at.isoformat(),
                "quality_state": self.identity.quality_state.value,
                "row_counts": dict(sorted(self.identity.row_counts.items())),
            },
            "files": [asdict(item) for item in sorted(self.files, key=lambda item: item.path)],
        }
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

    def write(self, directory: Path) -> Path:
        path = directory / MANIFEST_FILE
        path.write_text(self.to_json(), encoding="utf-8", newline="\n")
        return path

    @classmethod
    def read(cls, directory: Path) -> "PublishedManifest":
        path = directory / MANIFEST_FILE
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
            identity = value["identity"]
            from datetime import datetime
            return cls(
                identity=ManifestIdentity(
                    provider=identity["provider"], batch_id=identity["batch_id"],
                    version_id=identity["version_id"], published_at=datetime.fromisoformat(identity["published_at"]),
                    quality_state=TrustState(identity["quality_state"]),
                    content_fingerprint=identity["content_fingerprint"], schema_version=identity["schema_version"],
                    row_counts=identity.get("row_counts", {}),
                ),
                files=tuple(ManifestFile(**item) for item in value["files"]),
            )
        except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ManifestError(f"Invalid manifest at {path}: {exc}") from exc


def checksum_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def build_manifest(identity: ManifestIdentity, directory: Path, row_counts: Mapping[str, int]) -> PublishedManifest:
    normalized_identity = ManifestIdentity(
        provider=identity.provider, batch_id=identity.batch_id, version_id=identity.version_id,
        published_at=identity.published_at, quality_state=identity.quality_state,
        content_fingerprint=identity.content_fingerprint, schema_version=identity.schema_version,
        row_counts=dict(row_counts),
    )
    files = tuple(
        ManifestFile(path=file.relative_to(directory).as_posix(), sha256=checksum_file(file), size=file.stat().st_size)
        for file in sorted(directory.rglob("*.parquet"))
    )
    return PublishedManifest(normalized_identity, files)
