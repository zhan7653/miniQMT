"""Versioned, hash-bound investment charters for LLM-backed policies."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from types import MappingProxyType
from typing import Mapping

import yaml

from fundlab.common.canonical import stable_digest


class CharterError(ValueError):
    """A charter is missing, malformed, or contradicted by policy tactics."""


@dataclass(frozen=True)
class Charter:
    charter_id: str
    version: str
    established: str
    philosophy: str
    hard_rules: Mapping[str, object]

    def __post_init__(self) -> None:
        if not self.charter_id.strip() or not self.version.strip() or not self.philosophy.strip():
            raise CharterError("Charter id, version, and philosophy cannot be empty")
        object.__setattr__(self, "hard_rules", MappingProxyType(dict(self.hard_rules)))

    @property
    def content_hash(self) -> str:
        return stable_digest({
            "charter_id": self.charter_id,
            "version": self.version,
            "established": self.established,
            "philosophy": self.philosophy,
            "hard_rules": self.hard_rules,
        })

    def rule(self, key: str, default: object = None) -> object:
        return self.hard_rules.get(key, default)

    def decimal_rule(self, key: str, default: str) -> Decimal:
        value = self.hard_rules.get(key, default)
        try:
            parsed = Decimal(str(value))
        except (InvalidOperation, ValueError, TypeError) as exc:
            raise CharterError(f"Charter rule {key!r} is not decimal: {value!r}") from exc
        if not parsed.is_finite():
            raise CharterError(f"Charter rule {key!r} must be finite")
        return parsed

    def int_rule(self, key: str, default: int) -> int:
        value = self.hard_rules.get(key, default)
        try:
            return int(str(value))
        except (ValueError, TypeError) as exc:
            raise CharterError(f"Charter rule {key!r} is not an integer: {value!r}") from exc


def load_charter(path: str | Path) -> Charter:
    charter_path = Path(path)
    try:
        payload = yaml.safe_load(charter_path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise CharterError(f"Cannot read charter file {charter_path}: {exc}") from exc
    except yaml.YAMLError as exc:
        raise CharterError(f"Cannot parse charter file {charter_path}: {exc}") from exc
    if not isinstance(payload, dict) or not isinstance(payload.get("charter"), dict):
        raise CharterError(f"Charter file needs a `charter` mapping: {charter_path}")
    raw = payload["charter"]
    hard_rules = raw.get("hard_rules")
    if not isinstance(hard_rules, dict) or not hard_rules:
        raise CharterError(f"Charter needs non-empty hard_rules: {charter_path}")
    try:
        return Charter(
            charter_id=str(raw["id"]).strip(),
            version=str(raw["version"]).strip(),
            established=str(raw.get("established", "")).strip(),
            philosophy=str(raw["philosophy"]).strip(),
            hard_rules={str(key): value for key, value in hard_rules.items()},
        )
    except KeyError as exc:
        raise CharterError(f"Charter is missing {exc}: {charter_path}") from exc
