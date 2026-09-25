"""Versioned, auditable opinion records.

The model deliberately keeps the raw provider payload out of the public
summary.  A detail record can be requested separately when the analyst needs
to inspect the text and the provider permits retaining it.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Mapping

from fundlab.common.canonical import stable_digest


@dataclass(frozen=True)
class OpinionItem:
    source: str
    source_item_id: str
    url: str
    title: str
    excerpt: str
    published_at: datetime | None
    retrieved_at: datetime
    query: str
    author_ref: str | None = None
    engagement: Mapping[str, Any] = field(default_factory=dict)
    instrument_ids: tuple[str, ...] = ()
    stance: str = "unknown"
    topics: tuple[str, ...] = ()
    source_quality: str = "unknown"
    content_hash: str = ""
    detail_ref: str | None = None
    detail_available: bool = False
    truncated: bool = False
    source_set: tuple[str, ...] = ()
    analysis: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.stance not in {"bullish", "bearish", "neutral", "unknown"}:
            raise ValueError(f"unknown opinion stance: {self.stance}")
        if not self.source.strip() or not self.source_item_id.strip():
            raise ValueError("opinion source and source_item_id are required")
        if not self.url.strip() or not self.title.strip():
            raise ValueError("opinion url and title are required")

    @property
    def dedupe_key(self) -> str:
        return self.content_hash or stable_digest({
            "title": self.title,
            "excerpt": self.excerpt,
            "url": self.url,
        })

    def summary(self, *, max_excerpt_chars: int = 500) -> dict[str, Any]:
        excerpt = self.excerpt[:max_excerpt_chars]
        return {
            "source": self.source,
            "source_set": list(self.source_set or (self.source,)),
            "source_item_id": self.source_item_id,
            "url": self.url,
            "title": self.title,
            "excerpt": excerpt,
            "published_at": None if self.published_at is None else self.published_at.isoformat(),
            "retrieved_at": self.retrieved_at.isoformat(),
            "query": self.query,
            "author_ref": self.author_ref,
            "engagement": dict(self.engagement),
            "instrument_ids": list(self.instrument_ids),
            "stance": self.stance,
            "topics": list(self.topics),
            "source_quality": self.source_quality,
            "content_hash": self.content_hash,
            "detail_ref": self.detail_ref,
            "detail_available": self.detail_available,
            "truncated": self.truncated or len(self.excerpt) > max_excerpt_chars,
            "analysis": dict(self.analysis),
        }


@dataclass(frozen=True)
class OpinionSnapshot:
    as_of: date
    snapshot_id: str
    created_at: datetime
    items: tuple[OpinionItem, ...]
    summaries: Mapping[str, Mapping[str, Any]]
    providers: tuple[str, ...]
    queries: tuple[str, ...]
    quality: str
    errors: tuple[Mapping[str, Any], ...] = ()
    schema_version: str = "opinion_snapshot.v1"

    def to_dict(self, *, include_details: bool = False) -> dict[str, Any]:
        payload = {
            "schema_version": self.schema_version,
            "as_of": self.as_of.isoformat(),
            "snapshot_id": self.snapshot_id,
            "created_at": self.created_at.isoformat(),
            "providers": list(self.providers),
            "queries": list(self.queries),
            "quality": self.quality,
            "errors": [dict(item) for item in self.errors],
            "items": [
                item.summary(max_excerpt_chars=4000 if include_details else 500)
                for item in self.items
            ],
            "summaries": {str(key): dict(value) for key, value in self.summaries.items()},
        }
        return payload
