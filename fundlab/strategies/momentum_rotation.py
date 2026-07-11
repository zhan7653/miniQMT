from __future__ import annotations

import pandas as pd

from fundlab.data.portal import DataPortal
from fundlab.strategies.base import Strategy


class MomentumRotationStrategy(Strategy):
    def __init__(self, max_positions: int = 3, cash_weight: float = 0.05, strategy_id: str = "momentum_rotation"):
        self.max_positions = max_positions
        self.cash_weight = cash_weight
        self.strategy_id = strategy_id

    def on_rebalance(self, date: str, data_portal: DataPortal, context: dict) -> dict[str, float]:
        symbols = data_portal.get_universe(date)
        features = data_portal.get_features(symbols, date)
        if features.empty:
            return {"cash": 1.0}
        candidates = features.dropna(subset=["ret_20d", "ret_60d", "amount_avg_20d"]).copy()
        candidates = candidates[candidates["ret_20d"] > 0]
        if candidates.empty:
            return {"cash": 1.0}
        candidates["score"] = candidates["ret_20d"].rank(pct=True) * 0.5 + candidates["ret_60d"].rank(pct=True) * 0.5
        selected = candidates.sort_values("score", ascending=False).head(self.max_positions)
        weight = (1 - self.cash_weight) / len(selected)
        targets = {symbol: weight for symbol in selected.index}
        targets["cash"] = self.cash_weight
        return targets
