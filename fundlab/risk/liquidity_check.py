from __future__ import annotations

from fundlab.backtest.models import Account, Order
from fundlab.data.portal import DataPortal
from fundlab.risk.base import RiskCheckResult


class LiquidityCheck:
    name = "liquidity_check"

    def __init__(self, min_avg_amount_20d: float = 0.0, max_single_order_participation: float = 0.05):
        self.min_avg_amount_20d = min_avg_amount_20d
        self.max_single_order_participation = max_single_order_participation

    def check_order(self, order: Order, account: Account, date: str, data_portal: DataPortal) -> RiskCheckResult:
        decision_date = order.signal_date
        features = data_portal.get_features([order.symbol], decision_date, fields=["amount_avg_20d"])
        if features.empty or "amount_avg_20d" not in features.columns:
            return RiskCheckResult(False, order, "missing_frozen_liquidity", order.quantity)
        frozen_amount = float(features.iloc[0]["amount_avg_20d"])
        if order.side == "buy" and self.min_avg_amount_20d > 0:
            if frozen_amount < self.min_avg_amount_20d:
                return RiskCheckResult(False, order, "insufficient_avg_amount_20d")

        price = data_portal.get_open_price_for_execution(order.symbol, date)
        if price is None:
            return RiskCheckResult(False, order, "missing_liquidity_data")
        max_amount = frozen_amount * self.max_single_order_participation
        requested_amount = order.quantity * price
        if requested_amount <= max_amount:
            return RiskCheckResult(True, order)
        adjusted_quantity = int(max_amount / price / 100) * 100
        if adjusted_quantity <= 0:
            return RiskCheckResult(False, order, "participation_limit")
        requested = order.quantity
        order.quantity = min(requested, adjusted_quantity)
        return RiskCheckResult(True, order, "participation_limit_scaled", requested)

    def check_target_weights(self, target_weights: dict[str, float], account: Account, date: str, data_portal: DataPortal) -> dict[str, float]:
        return target_weights
