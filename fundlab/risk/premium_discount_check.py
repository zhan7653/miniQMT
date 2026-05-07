from __future__ import annotations

from fundlab.backtest.models import Account, Order
from fundlab.data.portal import DataPortal
from fundlab.risk.base import RiskCheckResult


class PremiumDiscountCheck:
    name = "premium_discount_check"

    def __init__(self, max_premium_abs: float = 0.03):
        self.max_premium_abs = max_premium_abs

    def check_order(self, order: Order, account: Account, date: str, data_portal: DataPortal) -> RiskCheckResult:
        if order.side == "sell":
            return RiskCheckResult(True, order)
        master = data_portal.get_fund_master([order.symbol], date=date)
        is_cross_border = not master.empty and master.iloc[0].get("asset_class") == "cross_border"
        premium_discount = data_portal.get_premium_discount(order.symbol, date, asof=date)
        if premium_discount is None:
            return RiskCheckResult(False, order, "missing_premium_discount") if is_cross_border else RiskCheckResult(True, order)
        if abs(premium_discount) > self.max_premium_abs:
            return RiskCheckResult(False, order, "premium_discount_limit")
        return RiskCheckResult(True, order)

    def check_target_weights(self, target_weights: dict[str, float], account: Account, date: str, data_portal: DataPortal) -> dict[str, float]:
        return target_weights

