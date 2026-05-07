from __future__ import annotations

from fundlab.backtest.models import Account, Order
from fundlab.data.portal import DataPortal
from fundlab.risk.base import RiskCheckResult


class UniverseCheck:
    name = "universe_check"

    def __init__(self, allow_universe_only: bool = True):
        self.allow_universe_only = allow_universe_only

    def check_order(self, order: Order, account: Account, date: str, data_portal: DataPortal) -> RiskCheckResult:
        if not self.allow_universe_only:
            return RiskCheckResult(True, order)
        if order.symbol not in data_portal.get_universe(date):
            return RiskCheckResult(False, order, "symbol_not_in_universe")
        return RiskCheckResult(True, order)

    def check_target_weights(self, target_weights: dict[str, float], account: Account, date: str, data_portal: DataPortal) -> dict[str, float]:
        if not self.allow_universe_only:
            return target_weights
        universe = set(data_portal.get_universe(date))
        return {symbol: weight for symbol, weight in target_weights.items() if symbol == "cash" or symbol in universe}

