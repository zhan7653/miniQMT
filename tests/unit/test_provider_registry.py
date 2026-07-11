import pytest

from fundlab.data.platform import ProviderCapability
from fundlab.data.sources import ProviderRegistry, UnsupportedCapabilityError


class FakeProvider:
    name = "configured"
    capabilities = frozenset({ProviderCapability.TRADING_CALENDAR})

    def preflight(self):
        raise AssertionError("resolution must not perform I/O")


def test_registry_resolves_only_explicitly_enabled_providers():
    registry = ProviderRegistry()
    registry.register(FakeProvider(), enabled=False)
    with pytest.raises(LookupError, match="not enabled"):
        registry.resolve("configured")


def test_registry_rejects_undeclared_capability_before_provider_io():
    registry = ProviderRegistry()
    registry.register(FakeProvider())
    with pytest.raises(UnsupportedCapabilityError, match="daily_bars_raw"):
        registry.resolve("configured", ProviderCapability.DAILY_BARS_RAW)


def test_registry_has_no_implicit_fallback():
    registry = ProviderRegistry()
    registry.register(FakeProvider())
    with pytest.raises(LookupError, match="not enabled"):
        registry.resolve("unconfigured", ProviderCapability.TRADING_CALENDAR)
