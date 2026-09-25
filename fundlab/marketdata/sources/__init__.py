from __future__ import annotations

from dataclasses import dataclass

from fundlab.marketdata.providers import ProviderRegistry
from fundlab.marketdata.sources.baostock import BaoStockProvider
from fundlab.marketdata.sources.cninfo import CninfoCorporateActionProvider
from fundlab.marketdata.sources.efinance import EfinanceProvider
from fundlab.marketdata.sources.eastmoney_fund import EastmoneyEtfActionProvider
from fundlab.marketdata.sources.exchange import ExchangePublicUniverseProvider
from fundlab.marketdata.sources.sina import SinaEtfProvider
from fundlab.marketdata.sources.sina_calendar import SinaCalendarProvider
from fundlab.marketdata.sources.tickflow import TickFlowProvider


@dataclass(frozen=True)
class SourceStatus:
    provider: str
    backend_group: str
    available: bool
    capabilities: tuple[str, ...]


def default_provider_registry() -> ProviderRegistry:
    registry = ProviderRegistry()
    registry.register(TickFlowProvider())
    registry.register(EfinanceProvider())
    registry.register(EastmoneyEtfActionProvider())
    registry.register(ExchangePublicUniverseProvider())
    registry.register(SinaEtfProvider())
    registry.register(SinaCalendarProvider())
    registry.register(BaoStockProvider())
    registry.register(CninfoCorporateActionProvider())
    return registry


def source_statuses(registry: ProviderRegistry | None = None) -> tuple[SourceStatus, ...]:
    selected = registry or default_provider_registry()
    statuses = []
    for name in selected.names:
        provider = selected.provider(name)
        statuses.append(SourceStatus(
            name,
            str(getattr(provider, "backend_group", name)),
            bool(getattr(provider, "available", True)),
            tuple(sorted(item.value for item in provider.capabilities)),
        ))
    return tuple(statuses)


__all__ = [
    "BaoStockProvider",
    "CninfoCorporateActionProvider",
    "EfinanceProvider",
    "EastmoneyEtfActionProvider",
    "ExchangePublicUniverseProvider",
    "SinaEtfProvider",
    "SinaCalendarProvider",
    "SourceStatus",
    "TickFlowProvider",
    "default_provider_registry",
    "source_statuses",
]
