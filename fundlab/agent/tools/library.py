"""Read-only, explicitly allow-listed investment research documents."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from fundlab.common.canonical import stable_digest
from fundlab.settings import AgentLibrarySettings

_ALLOWED_SUFFIXES = {".md", ".txt"}


@dataclass(frozen=True)
class LibraryDocument:
    name: str
    content: str
    content_hash: str
    truncated: bool

    def evidence(self) -> dict[str, object]:
        return {
            "name": self.name,
            "content": self.content,
            "content_hash": self.content_hash,
            "truncated": self.truncated,
        }


@dataclass(frozen=True)
class ReadingLibrary:
    settings: AgentLibrarySettings

    @property
    def config_hash(self) -> str:
        return stable_digest({
            "documents": self.settings.documents,
            "max_document_chars": self.settings.max_document_chars,
            "max_total_chars": self.settings.max_total_chars,
        })

    def context(self) -> tuple[LibraryDocument, ...]:
        """Read only configured files, enforcing per-file and aggregate caps."""
        root = self.settings.root.resolve()
        remaining = self.settings.max_total_chars
        found: list[LibraryDocument] = []
        seen: set[str] = set()
        for raw_name in self.settings.documents:
            name = str(raw_name).strip()
            if not name or name in seen:
                raise ValueError(f"Duplicate or empty agent library document: {name!r}")
            seen.add(name)
            if Path(name).name != name or Path(name).suffix.lower() not in _ALLOWED_SUFFIXES:
                raise ValueError(f"Illegal agent library document name: {name!r}")
            path = root / name
            if not path.is_file() or path.resolve().parent != root:
                raise FileNotFoundError(f"Allow-listed agent library document is missing: {name}")
            original = path.read_text(encoding="utf-8", errors="strict")
            allowed = min(self.settings.max_document_chars, remaining)
            content = original[:allowed]
            found.append(LibraryDocument(
                name=name,
                content=content,
                content_hash=stable_digest(original),
                truncated=len(content) < len(original),
            ))
            remaining -= len(content)
            if remaining <= 0:
                break
        return tuple(found)
