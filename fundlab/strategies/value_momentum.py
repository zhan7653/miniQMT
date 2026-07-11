from __future__ import annotations

from fundlab.data.portal import DataPortal
from fundlab.strategies.base import Strategy


class ValueMomentumStrategy(Strategy):
    def __init__(self, max_positions: int = 3, cash_weight: float = 0.05, strategy_id: str = "value_momentum"):
        self.max_positions = max_positions
        self.cash_weight = cash_weight
        self.strategy_id = strategy_id

    def on_rebalance(self, date: str, data_portal: DataPortal, context: dict) -> dict[str, float]:
        # Valuation inputs are not part of the trusted v2 feature schema.
        return {"cash": 1.0}
