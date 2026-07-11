from __future__ import annotations

from collections.abc import Callable

from fundlab.data.platform import DataProvider, ProviderCapability
from fundlab.data.sources.base import UnsupportedCapabilityError


class ProviderRegistry:
    """Registry containing only explicitly enabled production providers."""

    def __init__(self) -> None:
        self._providers: dict[str, DataProvider] = {}

    def register(self, provider: DataProvider, *, enabled: bool = True) -> None:
        if enabled:
            self._providers[provider.name] = provider

    @property
    def names(self) -> tuple[str, ...]:
        return tuple(self._providers)

    def resolve(self, name: str, capability: ProviderCapability | None = None) -> DataProvider:
        try:
            provider = self._providers[name]
        except KeyError as exc:
            raise LookupError(f"Provider {name!r} is not enabled") from exc
        if capability is not None and capability not in provider.capabilities:
            raise UnsupportedCapabilityError(
                f"Provider {name!r} does not declare capability {capability.value!r}"
            )
        return provider

    def providers_for(self, capability: ProviderCapability) -> tuple[DataProvider, ...]:
        return tuple(provider for provider in self._providers.values() if capability in provider.capabilities)


def build_v1_registry(xtquant_factory: Callable[[], DataProvider]) -> ProviderRegistry:
    registry = ProviderRegistry()
    registry.register(xtquant_factory())
    return registry
