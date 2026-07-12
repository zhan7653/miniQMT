from __future__ import annotations

from fundlab.backtest.models import Account, Order
from fundlab.data.portal import DataPortal
from fundlab.trading.decision import DecisionValidationContext, validate_target_weights
from fundlab.trading.models import ValidationResult
from fundlab.trading.profiles import ResearchRiskProfile


class RiskEngine:
    def __init__(self, rules: list):
        self.rules = rules

    def check_target_weights(self, target_weights: dict[str, float], account: Account, date: str, data_portal: DataPortal) -> dict[str, float]:
        # Compatibility adapter: validation is explicit and the original decision is never mutated.
        return dict(target_weights)

    def validate_target_weights(self, target_weights: dict[str, float], account: Account, date: str,
                                data_portal: DataPortal,
                                profile: ResearchRiskProfile | None = None) -> ValidationResult:
        universe = frozenset(data_portal.get_universe(date))
        context = DecisionValidationContext(universe=universe)
        result = validate_target_weights(target_weights, context, profile or ResearchRiskProfile("backtest", "v1"))
        extra_codes = list(result.codes)
        for rule in self.rules:
            name = getattr(rule, "name", "risk_rule")
            if name == "position_limit" and any(
                    symbol != "cash" and weight > rule.max_weight_per_symbol
                    for symbol, weight in target_weights.items()):
                extra_codes.append("position_limit")
            if name == "cash_check" and float(target_weights.get("cash", 0.0)) < rule.min_cash_weight:
                extra_codes.append("minimum_cash_not_met")
        if extra_codes != list(result.codes):
            from fundlab.trading.models import ValidationStatus
            unique = tuple(dict.fromkeys(extra_codes))
            return ValidationResult(ValidationStatus.REJECTED, unique, ";".join(unique))
        return result

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
                if current.requested_quantity is None:
                    current.requested_quantity = result.requested_quantity or current.quantity
                if result.reason and current.reason:
                    current.reason += f";{result.reason}"
                elif result.reason:
                    current.reason = result.reason
            checked_orders.append(current)
        return checked_orders
