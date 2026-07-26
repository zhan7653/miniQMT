from __future__ import annotations

from fundlab.marketdata.contracts import (
    MarketDataProvider,
    ObservationPayload,
    ProviderCapability,
    ProviderRequest,
    ProviderSelectionError,
)


class ProviderRegistry:
    """Explicit provider routing. It never falls back or silently merges sources."""

    def __init__(self) -> None:
        self._providers: dict[str, MarketDataProvider] = {}

    @property
    def names(self) -> tuple[str, ...]:
        return tuple(sorted(self._providers))

    def provider(self, name: str) -> MarketDataProvider:
        try:
            return self._providers[name]
        except KeyError as exc:
            raise ProviderSelectionError(f"Provider is not registered: {name}") from exc

    def register(self, provider: MarketDataProvider) -> None:
        if not provider.name.strip():
            raise ValueError("Provider name cannot be empty")
        if provider.name in self._providers:
            raise ProviderSelectionError(f"Provider is already registered: {provider.name}")
        self._providers[provider.name] = provider

    def resolve(self, name: str, capability: ProviderCapability) -> MarketDataProvider:
        provider = self.provider(name)
        if capability not in provider.capabilities:
            raise ProviderSelectionError(
                f"Provider {name!r} does not declare capability {capability.value!r}"
            )
        return provider

    def observe(self, name: str, request: ProviderRequest) -> ObservationPayload:
        payload = self.resolve(name, request.capability).observe(request)
        if payload.provider != name:
            raise ProviderSelectionError(
                f"Provider returned mismatched identity: requested {name!r}, observed {payload.provider!r}"
            )
        if payload.request != request:
            raise ProviderSelectionError("Provider returned an observation for a different request")
        return payload
