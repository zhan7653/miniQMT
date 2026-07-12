from __future__ import annotations

from fundlab.backtest.models import Account, Order, OrderIntent
from fundlab.common.ids import new_id
from fundlab.data.portal import DataPortal
from fundlab.trading.execution import size_target_orders


class ExecutionPlanner:
    def create_intents(
        self,
        account_id: str,
        strategy_id: str,
        signal_date: str,
        execution_date: str,
        target_weights: dict[str, float],
        held_symbols: list[str] | tuple[str, ...] = (),
    ) -> list[OrderIntent]:
        normalized = dict(target_weights)
        for symbol in held_symbols:
            normalized.setdefault(symbol, 0.0)
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
            for symbol, target_weight in normalized.items()
            if symbol != "cash"
        ]

    def create_orders(self, intents: list[OrderIntent], account: Account, data_portal: DataPortal) -> list[Order]:
        if not intents:
            return []
        target_weights = {intent.symbol: intent.target_weight for intent in intents}
        positions = {symbol: position.quantity for symbol, position in account.positions.items()}
        prices = {intent.symbol: data_portal.get_open_price_for_execution(intent.symbol, intent.execution_date)
                  for intent in intents}
        sized = size_target_orders(target_weights=target_weights, positions=positions,
                                   total_asset=account.total_asset, raw_open_prices=prices)
        intent_by_symbol = {intent.symbol: intent for intent in intents}
        orders: list[Order] = []
        for item in sized:
            intent = intent_by_symbol[item.symbol]
            orders.append(
                Order(
                    order_id=new_id("order"),
                    account_id=account.account_id,
                    symbol=item.symbol,
                    side=item.side,
                    order_type="target_weight",
                    target_weight=intent.target_weight,
                    quantity=item.requested_quantity,
                    signal_date=intent.signal_date,
                    execution_date=intent.execution_date,
                    requested_quantity=item.requested_quantity,
                )
            )
        return orders
