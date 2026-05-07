from __future__ import annotations

from fundlab.data.portal import DataPortal
from fundlab.strategies.base import Strategy


class ValueMomentumStrategy(Strategy):
    def __init__(self, max_positions: int = 3, cash_weight: float = 0.05, strategy_id: str = "value_momentum"):
        self.max_positions = max_positions
        self.cash_weight = cash_weight
        self.strategy_id = strategy_id

    def on_rebalance(self, date: str, data_portal: DataPortal, context: dict) -> dict[str, float]:
        symbols = data_portal.get_universe(date)
        features = data_portal.get_features(symbols, date, asof=date)
        if features.empty:
            return {"cash": 1.0}
        candidates = features.dropna(subset=["valuation_score", "momentum_score", "liquidity_score"]).copy()
        candidates = candidates[candidates["momentum_score"] > 0.5]
        if candidates.empty:
            return {"cash": 1.0}
        candidates["score"] = (
            candidates["valuation_score"] * 0.55
            + candidates["momentum_score"] * 0.35
            + candidates["liquidity_score"] * 0.10
        )
        selected = candidates.sort_values("score", ascending=False).head(self.max_positions)
        weight = (1 - self.cash_weight) / len(selected)
        targets = {symbol: weight for symbol in selected.index}
        targets["cash"] = self.cash_weight
        return targets

