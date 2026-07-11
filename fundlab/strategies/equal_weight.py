from __future__ import annotations

from fundlab.data.portal import DataPortal
from fundlab.data.platform import PriceMode
from fundlab.strategies.base import Strategy


class EqualWeightStrategy(Strategy):
    def __init__(self, symbols: list[str], cash_weight: float = 0.02, strategy_id: str = "equal_weight"):
        self.symbols = symbols
        self.cash_weight = cash_weight
        self.strategy_id = strategy_id

    def on_rebalance(self, date: str, data_portal: DataPortal, context: dict) -> dict[str, float]:
        universe = set(data_portal.get_universe(date))
        tradable = []
        for symbol in self.symbols:
            if symbol not in universe:
                continue
            if data_portal.get_price(symbol, date, price_mode=PriceMode.RAW, field="close") is not None:
                tradable.append(symbol)

        if not tradable:
            return {"cash": 1.0}

        invest_weight = max(0.0, 1.0 - self.cash_weight)
        weight = invest_weight / len(tradable)
        targets = {symbol: weight for symbol in tradable}
        targets["cash"] = self.cash_weight
        return targets
