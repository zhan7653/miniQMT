from __future__ import annotations

from fundlab.data.portal import DataPortal
from fundlab.strategies.base import Strategy


class AssetAllocationStrategy(Strategy):
    def __init__(self, cash_weight: float = 0.10, strategy_id: str = "asset_allocation"):
        self.cash_weight = cash_weight
        self.strategy_id = strategy_id

    def on_rebalance(self, date: str, data_portal: DataPortal, context: dict) -> dict[str, float]:
        # Asset classification is not part of the trusted v2 consumer schema.
        return {"cash": 1.0}
