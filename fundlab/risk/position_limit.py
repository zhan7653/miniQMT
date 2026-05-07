from __future__ import annotations

from fundlab.backtest.models import Account, Order
from fundlab.data.portal import DataPortal
from fundlab.risk.base import RiskCheckResult


class PositionLimit:
    name = "position_limit"

    def __init__(self, max_weight_per_symbol: float = 0.25):
        self.max_weight_per_symbol = max_weight_per_symbol

    def check_order(self, order: Order, account: Account, date: str, data_portal: DataPortal) -> RiskCheckResult:
        return RiskCheckResult(True, order)

    def check_target_weights(self, target_weights: dict[str, float], account: Account, date: str, data_portal: DataPortal) -> dict[str, float]:
        adjusted = dict(target_weights)
        for symbol, weight in list(adjusted.items()):
            if symbol != "cash" and weight > self.max_weight_per_symbol:
                adjusted[symbol] = self.max_weight_per_symbol
        symbol_total = sum(weight for symbol, weight in adjusted.items() if symbol != "cash")
        adjusted["cash"] = max(adjusted.get("cash", 0.0), 1 - symbol_total)
        return adjusted

