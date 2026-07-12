from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from copy import deepcopy
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any

from fundlab.strategies.asset_allocation import AssetAllocationStrategy
from fundlab.strategies.equal_weight import EqualWeightStrategy
from fundlab.strategies.momentum_rotation import MomentumRotationStrategy
from fundlab.strategies.value_momentum import ValueMomentumStrategy


StrategyFactory = Callable[..., object]


@dataclass(frozen=True)
class UniverseMetadata:
    version: str
    symbols: tuple[str, ...]
    cross_border_symbols: frozenset[str] = frozenset()


class StrategyRegistry:
    """Rule-strategy-only registry with versioned, immutable configuration."""

    def __init__(self) -> None:
        self._factories: dict[str, StrategyFactory] = {
            "equal_weight": EqualWeightStrategy,
            "momentum_rotation": MomentumRotationStrategy,
            "asset_allocation": AssetAllocationStrategy,
            "value_momentum": ValueMomentumStrategy,
        }
        self._configs: dict[tuple[str, str], dict[str, Any]] = {}
        self._universes: dict[str, UniverseMetadata] = {}

    def register(self, strategy_id: str, factory: StrategyFactory) -> None:
        if not strategy_id or strategy_id in self._factories:
            raise ValueError(f"strategy already registered or invalid: {strategy_id}")
        self._factories[strategy_id] = factory

    def register_config(
        self, strategy_id: str, version: str, config: Mapping[str, Any]
    ) -> None:
        if strategy_id not in self._factories or not version:
            raise ValueError("known strategy_id and non-empty version are required")
        key = (strategy_id, version)
        value = deepcopy(dict(config))
        if key in self._configs and self._configs[key] != value:
            raise ValueError("strategy configuration versions are immutable")
        self._configs[key] = value

    def register_universe(
        self, version: str, symbols: Iterable[str], *, cross_border_symbols: Iterable[str] = (),
    ) -> None:
        if not version:
            raise ValueError("universe version is required")
        normalized = tuple(dict.fromkeys(str(symbol) for symbol in symbols if str(symbol)))
        if not normalized:
            raise ValueError("universe must contain at least one symbol")
        cross_border = frozenset(str(symbol) for symbol in cross_border_symbols)
        if not cross_border <= set(normalized):
            raise ValueError("cross-border symbols must belong to the universe")
        metadata = UniverseMetadata(version, normalized, cross_border)
        if version in self._universes and self._universes[version] != metadata:
            raise ValueError("universe versions are immutable")
        self._universes[version] = metadata

    def get_universe(self, version: str) -> tuple[str, ...]:
        try:
            return self._universes[version].symbols
        except KeyError as exc:
            raise ValueError(f"unknown bound universe version: {version}") from exc

    def get_universe_metadata(self, version: str) -> UniverseMetadata:
        self.get_universe(version)
        return self._universes[version]

    def get_config(self, strategy_id: str, config_version: str) -> Mapping[str, Any]:
        if strategy_id not in self._factories:
            raise ValueError(f"unknown rule strategy: {strategy_id}")
        try:
            config = self._configs[(strategy_id, config_version)]
        except KeyError as exc:
            raise ValueError(
                f"unknown config version {config_version!r} for strategy {strategy_id!r}"
            ) from exc
        return MappingProxyType(deepcopy(config))

    def create(self, strategy_id: str, config_version: str) -> object:
        try:
            factory = self._factories[strategy_id]
        except KeyError as exc:
            raise ValueError(f"unknown rule strategy: {strategy_id}") from exc
        config = dict(self.get_config(strategy_id, config_version))
        try:
            return factory(**config)
        except TypeError as exc:
            raise ValueError(
                f"invalid config {config_version!r} for strategy {strategy_id!r}: {exc}"
            ) from exc
