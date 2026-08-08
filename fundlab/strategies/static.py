from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from types import MappingProxyType
from typing import Mapping

from fundlab.common.canonical import stable_digest
from fundlab.marketdata.portal import PointInTimeMarketView
from fundlab.trading.intent import PortfolioIntent, decimal_value
from fundlab.trading.state import PortfolioState


@dataclass(frozen=True)
class StaticAllocationSource:
    target_weights: Mapping[str, Decimal]
    strategy_id: str = "static_allocation"
    strategy_version: str = "1"

    def __post_init__(self) -> None:
        object.__setattr__(self, "target_weights", {
            str(symbol): decimal_value(weight) for symbol, weight in self.target_weights.items()
        })
        object.__setattr__(self, "target_weights", MappingProxyType(dict(self.target_weights)))

    @property
    def config_hash(self) -> str:
        return stable_digest({
            "strategy_id": self.strategy_id,
            "strategy_version": self.strategy_version,
            "target_weights": self.target_weights,
        })

    @property
    def market_scope(self) -> tuple[str, ...]:
        return tuple(self.target_weights)

    def decide(
        self,
        *,
        account_id: str,
        market: PointInTimeMarketView,
        state: PortfolioState,
    ) -> PortfolioIntent:
        observation_hash = stable_digest({
            "snapshot_id": market.snapshot_id,
            "as_of": market.as_of,
            "state_hash": state.state_hash,
            "target_weights": self.target_weights,
        })
        return PortfolioIntent.create(
            account_id=account_id,
            decision_date=market.as_of,
            snapshot_id=market.snapshot_id,
            strategy_id=self.strategy_id,
            strategy_version=self.strategy_version,
            strategy_config_hash=self.config_hash,
            observation_hash=observation_hash,
            target_weights=self.target_weights,
            reason="static_allocation",
        )
