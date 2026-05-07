from __future__ import annotations

from math import floor

from fundlab.backtest.models import Account, Order, OrderIntent
from fundlab.common.ids import new_id
from fundlab.data.portal import DataPortal


class ExecutionPlanner:
    def create_intents(
        self,
        account_id: str,
        strategy_id: str,
        signal_date: str,
        execution_date: str,
        target_weights: dict[str, float],
    ) -> list[OrderIntent]:
        return [
            OrderIntent(
                intent_id=new_id("intent"),
                account_id=account_id,
                strategy_id=strategy_id,
                signal_date=signal_date,
                execution_date=execution_date,
                symbol=symbol,
                target_weight=target_weight,
            )
            for symbol, target_weight in target_weights.items()
            if symbol != "cash"
        ]

    def create_orders(self, intents: list[OrderIntent], account: Account, data_portal: DataPortal) -> list[Order]:
        orders: list[Order] = []
        total_asset = account.total_asset
        for intent in intents:
            price = data_portal.get_open_price_for_execution(intent.symbol, intent.execution_date)
            if price is None or price <= 0:
                continue
            target_value = total_asset * intent.target_weight
            target_quantity = floor(target_value / price / 100) * 100
            current_quantity = account.positions.get(intent.symbol).quantity if intent.symbol in account.positions else 0
            delta = target_quantity - current_quantity
            if delta == 0:
                continue
            orders.append(
                Order(
                    order_id=new_id("order"),
                    account_id=account.account_id,
                    symbol=intent.symbol,
                    side="buy" if delta > 0 else "sell",
                    order_type="target_weight",
                    target_weight=intent.target_weight,
                    quantity=abs(delta),
                    signal_date=intent.signal_date,
                    execution_date=intent.execution_date,
                )
            )
        return sorted(orders, key=lambda order: 0 if order.side == "sell" else 1)

