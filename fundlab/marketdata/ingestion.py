from __future__ import annotations

from dataclasses import dataclass

from fundlab.marketdata.contracts import (
    MarketTable,
    ObservationManifest,
    ProviderCapability,
    ProviderRequest,
)
from fundlab.marketdata.providers import ProviderRegistry
from fundlab.marketdata.warehouse import MarketDataWarehouse


@dataclass(frozen=True)
class MarketIngestionService:
    registry: ProviderRegistry
    warehouse: MarketDataWarehouse

    def capture(self, provider_name: str, request: ProviderRequest) -> ObservationManifest:
        """Capture exactly one named provider observation; no fallback or implicit merge exists."""
        payload = self.registry.observe(provider_name, request)
        return self.warehouse.record_observation(payload)

    def capture_resumable(
        self,
        provider_name: str,
        request: ProviderRequest,
        *,
        refresh: bool = False,
    ) -> tuple[ObservationManifest, bool]:
        """Reuse a verified observation only when it covers the exact requested boundary."""

        if not refresh:
            reusable = [
                item for item in self.warehouse.matching_observations(
                    provider=provider_name, request=request,
                )
                if _covers_request(item, request)
            ]
            if reusable:
                return reusable[-1], True
        return self.capture(provider_name, request), False


_CAPABILITY_TABLE = {
    ProviderCapability.INSTRUMENTS: MarketTable.INSTRUMENTS,
    ProviderCapability.TRADING_CALENDAR: MarketTable.CALENDAR,
    ProviderCapability.DAILY_BARS_RAW: MarketTable.DAILY_BARS,
    ProviderCapability.DAILY_BARS_ADJUSTED: MarketTable.DAILY_BARS,
    ProviderCapability.DAILY_STATUS: MarketTable.DAILY_BARS,
    ProviderCapability.CORPORATE_ACTIONS: MarketTable.CORPORATE_ACTIONS,
    ProviderCapability.ADJUSTMENT_FACTORS: MarketTable.ADJUSTMENT_FACTORS,
}


def _covers_request(manifest: ObservationManifest, request: ProviderRequest) -> bool:
    table = _CAPABILITY_TABLE.get(request.capability)
    if table is None:
        return False
    claims = [claim for claim in manifest.coverage if claim.table is table and claim.complete]
    for claim in claims:
        if request.start_date is not None:
            if claim.start_date is None or claim.end_date is None:
                continue
            if claim.start_date > request.start_date or claim.end_date < request.end_date:
                continue
        if request.instrument_ids and claim.instrument_ids:
            if not set(request.instrument_ids) <= set(claim.instrument_ids):
                continue
        return True
    return False
