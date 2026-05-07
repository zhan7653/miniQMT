from __future__ import annotations

from fundlab.data.portal import DataPortal
from fundlab.strategies.base import Strategy


class AssetAllocationStrategy(Strategy):
    def __init__(self, cash_weight: float = 0.10, strategy_id: str = "asset_allocation"):
        self.cash_weight = cash_weight
        self.strategy_id = strategy_id

    def on_rebalance(self, date: str, data_portal: DataPortal, context: dict) -> dict[str, float]:
        master = data_portal.get_fund_master(date=date)
        if master.empty:
            return {"cash": 1.0}
        buckets = {
            "equity": 0.60,
            "commodity": 0.20,
            "bond": 0.10,
        }
        targets = {}
        for asset_class, bucket_weight in buckets.items():
            symbols = master[(master["include_in_universe"] == 1) & (master["asset_class"] == asset_class)]["symbol"].tolist()
            if not symbols:
                continue
            features = data_portal.get_features(symbols, date, asof=date)
            symbol = features.sort_values("liquidity_score", ascending=False).index[0] if not features.empty else symbols[0]
            targets[symbol] = bucket_weight
        invested = sum(targets.values())
        if invested > 1 - self.cash_weight:
            scale = (1 - self.cash_weight) / invested
            targets = {symbol: weight * scale for symbol, weight in targets.items()}
        targets["cash"] = 1 - sum(targets.values())
        return targets

