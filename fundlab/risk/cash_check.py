from __future__ import annotations

from fundlab.backtest.models import Account, Order
from fundlab.data.portal import DataPortal
from fundlab.risk.base import RiskCheckResult


class CashCheck:
    name = "cash_check"

    def __init__(self, min_cash_weight: float = 0.02):
        self.min_cash_weight = min_cash_weight

    def check_order(self, order: Order, account: Account, date: str, data_portal: DataPortal) -> RiskCheckResult:
        return RiskCheckResult(True, order)

    def check_target_weights(self, target_weights: dict[str, float], account: Account, date: str, data_portal: DataPortal) -> dict[str, float]:
        adjusted = dict(target_weights)
        symbol_weights = {symbol: weight for symbol, weight in adjusted.items() if symbol != "cash"}
        symbol_total = sum(symbol_weights.values())
        max_symbol_total = max(0.0, 1 - self.min_cash_weight)
        if symbol_total > max_symbol_total and symbol_total > 0:
            scale = max_symbol_total / symbol_total
            for symbol in symbol_weights:
                adjusted[symbol] *= scale
        adjusted["cash"] = max(self.min_cash_weight, 1 - sum(weight for symbol, weight in adjusted.items() if symbol != "cash"))
        return adjusted

