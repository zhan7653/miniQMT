"""Immutable opinion snapshot repository with separate detail objects."""

from __future__ import annotations

import json
import re
from datetime import date
from pathlib import Path
from typing import Any, Mapping

from fundlab.common.atomic_files import publish_immutable_bytes
from fundlab.common.canonical import canonical_json
from fundlab.agent.opinion.models import OpinionSnapshot


class OpinionRepository:
    def __init__(self, root: str | Path):
        self.root = Path(root).resolve()

    def snapshot_path(self, as_of: str, snapshot_id: str) -> Path:
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", str(as_of)):
            raise ValueError("invalid opinion snapshot date")
        if not re.fullmatch(r"[A-Za-z0-9_.-]+", str(snapshot_id)):
            raise ValueError("invalid opinion snapshot id")
        return self.root / str(as_of) / f"{snapshot_id}.json"

    def save(
        self,
        snapshot: OpinionSnapshot,
        details: Mapping[str, Mapping[str, Any]] = (),
        raw_items: list[Mapping[str, Any]] | tuple[Mapping[str, Any], ...] = (),
    ) -> Path:
        root = self.root / snapshot.as_of.isoformat()
        root.mkdir(parents=True, exist_ok=True)
        payload = canonical_json(snapshot.to_dict()) + "\n"
        path = root / f"{snapshot.snapshot_id}.json"
        payload_bytes = payload.encode("utf-8")
        if path.exists():
            try:
                existing = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ValueError(f"Existing opinion snapshot is unreadable: {path}") from exc
            # ``created_at`` is intentionally run metadata.  A retry of the
            # same content is idempotent and must reuse the immutable snapshot.
            candidate = json.loads(payload)
            existing.pop("created_at", None)
            candidate.pop("created_at", None)
            if existing != candidate:
                raise ValueError(f"Immutable opinion snapshot collision: {path}")
        else:
            publish_immutable_bytes(path, payload_bytes)
        if raw_items:
            raw_path = root / f"{snapshot.snapshot_id}.raw.json"
            publish_immutable_bytes(
                raw_path,
                (canonical_json({
                    "schema_version": "opinion_raw.v1",
                    "as_of": snapshot.as_of.isoformat(),
                    "snapshot_id": snapshot.snapshot_id,
                    "items": list(raw_items),
                }) + "\n").encode("utf-8"),
            )
        if details:
            detail_root = root / "details"
            detail_root.mkdir(parents=True, exist_ok=True)
            for detail_ref, value in details.items():
                detail_path = detail_root / f"{detail_ref}.json"
                detail_bytes = (canonical_json(value) + "\n").encode("utf-8")
                try:
                    publish_immutable_bytes(detail_path, detail_bytes)
                except ValueError:
                    # A repeated collection may have a different retrieval
                    # timestamp while referring to the same content hash.
                    existing_detail = json.loads(detail_path.read_text(encoding="utf-8"))
                    candidate_detail = dict(value)
                    existing_detail.pop("retrieved_at", None)
                    candidate_detail.pop("retrieved_at", None)
                    if existing_detail != candidate_detail:
                        raise
        if not path.exists():
            raise OSError(f"Opinion snapshot was not published: {path}")
        return path

    def list_snapshots(self, *, as_of: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
        if as_of:
            date.fromisoformat(as_of)
        roots = [self.root / as_of] if as_of else sorted(self.root.glob("*"), reverse=True)
        found = []
        for root in roots:
            if not root.is_dir():
                continue
            for path in sorted(root.glob("*.json"), reverse=True):
                if path.name.endswith(".raw.json"):
                    continue
                try:
                    value = json.loads(path.read_text(encoding="utf-8"))
                except (OSError, UnicodeDecodeError, json.JSONDecodeError):
                    continue
                if isinstance(value, Mapping) and value.get('schema_version') == 'opinion_snapshot.v1':
                    found.append({"as_of": value.get("as_of"), "snapshot_id": value.get("snapshot_id"), "path": str(path), "quality": value.get("quality"), "item_count": len(value.get("items", [])) if isinstance(value.get("items"), list) else 0,
                                  'created_at': value.get('created_at', ''), 'queries': value.get('queries', []), 'providers': value.get('providers', []), 'errors': value.get('errors', [])})
        latest = {}
        for row in sorted(found, key=lambda r: (r['as_of'], r['quality'] == 'ready', r['item_count'], r['created_at']), reverse=True):
            # One current snapshot card per provider set/date avoids showing
            # every retry as a separate piece of news in the Dashboard.
            key = (row['as_of'], tuple(row['providers']))
            latest.setdefault(key, row)
        return list(latest.values())[:limit]

    def load(self, *, as_of: str, snapshot_id: str | None = None) -> dict[str, Any] | None:
        date.fromisoformat(as_of)
        candidates = [self.snapshot_path(as_of, snapshot_id)] if snapshot_id else [
            path for path in sorted((self.root / as_of).glob("*.json"), reverse=True)
            if not path.name.endswith(".raw.json")
        ]
        values = []
        for path in candidates:
            if not path.exists():
                continue
            value = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(value, dict) or value.get("as_of") != as_of:
                raise ValueError(f"Invalid opinion snapshot: {path}")
            values.append(value)
        if not values:
            return None
        return max(
            values,
            key=lambda r: (
                r.get('quality') == 'ready',
                r.get('item_count', 0),
                r.get('created_at', ''),
            ),
        )

    def detail(self, *, as_of: str, detail_ref: str, snapshot_id: str | None = None) -> dict[str, Any] | None:
        if not re.fullmatch(r"[0-9a-f]{32}", str(detail_ref)):
            raise ValueError("invalid opinion detail reference")
        snapshot = self.load(as_of=as_of, snapshot_id=snapshot_id)
        if snapshot is None:
            return None
        if not any(item.get('detail_ref') == detail_ref for item in snapshot.get('items', [])):
            return None
        path = self.root / as_of / "details" / f"{detail_ref}.json"
        if not path.exists():
            return None
        value = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(value, dict):
            raise ValueError(f"Invalid opinion detail: {path}")
        return value
