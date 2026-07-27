"""Per-account append-only factual memory for Agent reviews and delivery."""

from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from fundlab.common.canonical import stable_digest


class AgentMemoryError(ValueError):
    """The append-only memory cannot be trusted as model context."""


@dataclass(frozen=True)
class AgentMemory:
    root: Path
    account_id: str

    @property
    def path(self) -> Path:
        return Path(self.root) / f"{self.account_id}.jsonl"

    def append(self, entry: dict[str, Any]) -> None:
        if not isinstance(entry, dict) or not entry:
            raise AgentMemoryError("Agent memory entries must be non-empty objects")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps(entry, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        with self.path.open("a", encoding="utf-8", newline="\n") as handle:
            handle.write(line + "\n")
            handle.flush()

    def entries(self) -> list[dict[str, Any]]:
        if not self.path.exists():
            return []
        found: list[dict[str, Any]] = []
        for index, line in enumerate(self.path.read_text(encoding="utf-8").splitlines(), 1):
            if not line.strip():
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError as exc:
                raise AgentMemoryError(
                    f"Corrupt agent memory {self.path} line {index}: {exc}"
                ) from exc
            if not isinstance(entry, dict):
                raise AgentMemoryError(
                    f"Agent memory {self.path} line {index} is not an object"
                )
            found.append(entry)
        return found

    def recent(
        self,
        limit: int,
        *,
        max_entry_chars: int = 20_000,
        max_total_chars: int = 120_000,
    ) -> list[dict[str, Any]]:
        if limit <= 0:
            return []
        if max_entry_chars < 1 or max_total_chars < 1:
            raise ValueError("Agent memory context limits must be positive")
        selected: list[dict[str, Any]] = []
        used = 0
        for entry in reversed(self.entries()[-limit:]):
            encoded = json.dumps(entry, ensure_ascii=False, sort_keys=True)
            if len(encoded) > max_entry_chars:
                entry = {
                    "event": str(entry.get("event", "unknown")),
                    "content_hash": stable_digest(entry),
                    "truncated": True,
                }
                encoded = json.dumps(entry, ensure_ascii=False, sort_keys=True)
            if used + len(encoded) > max_total_chars:
                break
            selected.append(entry)
            used += len(encoded)
        return list(reversed(selected))

    def has_review(self, period: str, config_hash: str) -> bool:
        return any(
            entry.get("event") == "review"
            and entry.get("review_period") == period
            and entry.get("policy_config_hash") == config_hash
            for entry in self.entries()
        )

    def sent_evidence_hashes(self) -> set[str]:
        hashes: set[str] = set()
        for entry in self.entries():
            if entry.get("event") != "email" or not entry.get("sent"):
                continue
            raw = entry.get("evidence_hashes", ())
            if isinstance(raw, Iterable) and not isinstance(raw, (str, bytes, dict)):
                hashes.update(map(str, raw))
        return hashes
