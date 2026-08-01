from __future__ import annotations

from dataclasses import fields, is_dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from hashlib import sha256
import json
from math import isfinite
from pathlib import Path
from types import MappingProxyType
from typing import Any, Mapping


def to_primitive(value: Any) -> Any:
    """Convert domain values into a deterministic JSON-compatible structure."""
    if is_dataclass(value) and not isinstance(value, type):
        return {item.name: to_primitive(getattr(value, item.name)) for item in fields(value)}
    if isinstance(value, Enum):
        return to_primitive(value.value)
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Path):
        return value.as_posix()
    if isinstance(value, Mapping):
        return {str(key): to_primitive(item) for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))}
    if isinstance(value, (set, frozenset)):
        return sorted((to_primitive(item) for item in value), key=_sort_key)
    if isinstance(value, (tuple, list)):
        return [to_primitive(item) for item in value]
    if isinstance(value, float) and not isfinite(value):
        raise ValueError("Non-finite floats cannot be canonically serialized")
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    raise TypeError(f"Unsupported canonical value: {type(value).__name__}")


def deep_freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({str(key): deep_freeze(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(deep_freeze(item) for item in value)
    if isinstance(value, (set, frozenset)):
        return frozenset(deep_freeze(item) for item in value)
    return value


def canonical_json(value: Any) -> str:
    return json.dumps(
        to_primitive(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False,
    )


def stable_digest(value: Any) -> str:
    return sha256(canonical_json(value).encode("utf-8")).hexdigest()


def _sort_key(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
