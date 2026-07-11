from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

import yaml

from .types import stable_fingerprint


@dataclass(frozen=True)
class UniverseSnapshot:
    version: str
    effective_date: date
    symbols: tuple[str, ...]
    benchmarks: tuple[str, ...]
    config_hash: str

    def __post_init__(self) -> None:
        normalized_symbols = tuple(sorted(set(self.symbols)))
        normalized_benchmarks = tuple(sorted(set(self.benchmarks)))
        if not normalized_symbols:
            raise ValueError("Reviewed universe cannot be empty")
        if not normalized_benchmarks:
            raise ValueError("At least one benchmark is required")
        if not set(normalized_benchmarks).issubset(normalized_symbols):
            raise ValueError("Benchmarks must belong to the reviewed universe")
        object.__setattr__(self, "symbols", normalized_symbols)
        object.__setattr__(self, "benchmarks", normalized_benchmarks)

    @classmethod
    def create(
        cls,
        *,
        version: str,
        effective_date: date,
        symbols: tuple[str, ...],
        benchmarks: tuple[str, ...],
        configuration: dict,
    ) -> "UniverseSnapshot":
        return cls(
            version=version,
            effective_date=effective_date,
            symbols=symbols,
            benchmarks=benchmarks,
            config_hash=stable_fingerprint(configuration),
        )


def load_universe_snapshot(path: str | Path) -> UniverseSnapshot:
    universe_path = Path(path).expanduser().resolve()
    with universe_path.open("r", encoding="utf-8") as stream:
        data = yaml.safe_load(stream) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Universe config must be a mapping: {universe_path}")
    required = ("version", "effective_date", "symbols", "benchmarks")
    missing = [key for key in required if not data.get(key)]
    if missing:
        raise ValueError(f"Universe config missing required fields: {', '.join(missing)}")
    return UniverseSnapshot.create(
        version=str(data["version"]),
        effective_date=date.fromisoformat(str(data["effective_date"])),
        symbols=tuple(data["symbols"]),
        benchmarks=tuple(data["benchmarks"]),
        configuration=data,
    )
