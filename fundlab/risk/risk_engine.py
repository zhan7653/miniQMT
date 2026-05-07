from __future__ import annotations

from fundlab.backtest.models import Account, Order
from fundlab.data.portal import DataPortal


class RiskEngine:
    def __init__(self, rules: list):
        self.rules = rules

    def check_target_weights(self, target_weights: dict[str, float], account: Account, date: str, data_portal: DataPortal) -> dict[str, float]:
        checked = dict(target_weights)
        for rule in self.rules:
            checked = rule.check_target_weights(checked, account, date, data_portal)
        return checked

    def check_orders(self, orders: list[Order], account: Account, date: str, data_portal: DataPortal) -> list[Order]:
        checked_orders = []
        for order in orders:
            current = order
            for rule in self.rules:
                result = rule.check_order(current, account, date, data_portal)
                if not result.passed:
                    current.status = "rejected"
                    current.reject_reason = result.reason
                    break
                current = result.adjusted_order or current
                if result.reason and current.reason:
                    current.reason += f";{result.reason}"
                elif result.reason:
                    current.reason = result.reason
            checked_orders.append(current)
        return checked_orders

