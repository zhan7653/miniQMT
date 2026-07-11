from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Sequence

import pandas as pd

from fundlab.data.platform import ProviderCapability, ProviderRequest, ProviderResult


class ProviderError(RuntimeError):
    """Base error for provider contract failures."""


class UnsupportedCapabilityError(ProviderError):
    """Raised before provider I/O when a capability was not declared."""


class ProviderUnavailableError(ProviderError):
    """Raised when the configured production provider cannot be reached."""


class MarketDataSource(ABC):
    name: str
    capabilities: frozenset[ProviderCapability] = frozenset()

    def require_capability(self, capability: ProviderCapability) -> None:
        if capability not in self.capabilities:
            raise UnsupportedCapabilityError(
                f"Provider {self.name!r} does not declare capability {capability.value!r}"
            )

    def fetch(self, request: ProviderRequest) -> tuple[pd.DataFrame, ProviderResult]:
        self.require_capability(request.capability)
        raise NotImplementedError

    def _provider_result(self, request: ProviderRequest, symbols) -> ProviderResult:
        return ProviderResult(
            provider=self.name,
            capability=request.capability,
            symbols=tuple(symbols),
            observed_at=datetime.now().astimezone(),
        )

    @abstractmethod
    def get_instruments(self) -> list[dict]:
        raise NotImplementedError

    def download_daily_bar(self, symbols: Sequence[str], start_date: str, end_date: str) -> None:
        raise NotImplementedError

    def get_daily_bar(self, symbols: Sequence[str], start_date: str, end_date: str) -> pd.DataFrame:
        raise NotImplementedError

    def get_trading_calendar(self, start_date: str, end_date: str) -> pd.DataFrame:
        raise NotImplementedError
