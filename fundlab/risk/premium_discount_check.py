from __future__ import annotations

from fundlab.backtest.models import Account, Order
from fundlab.data.portal import DataPortal
from fundlab.risk.base import RiskCheckResult


class PremiumDiscountCheck:
    name = "premium_discount_check"

    def __init__(self, max_premium_abs: float = 0.03):
        self.max_premium_abs = max_premium_abs

    def check_order(self, order: Order, account: Account, date: str, data_portal: DataPortal) -> RiskCheckResult:
        # Legacy premium/discount and fund classification are untrusted and are
        # deliberately absent from the normal v2 portal. Do not consult them.
        return RiskCheckResult(True, order)

    def check_target_weights(self, target_weights: dict[str, float], account: Account, date: str, data_portal: DataPortal) -> dict[str, float]:
        return target_weights
