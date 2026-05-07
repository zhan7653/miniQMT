from __future__ import annotations

from fundlab.backtest.cost import CostModel
from fundlab.backtest.models import Account, Order, Trade
from fundlab.backtest.slippage import SlippageModel
from fundlab.common.ids import new_id
from fundlab.data.portal import DataPortal


class BacktestBroker:
    def __init__(self, cost_model: CostModel | None = None, slippage_model: SlippageModel | None = None):
        self.cost_model = cost_model or CostModel()
        self.slippage_model = slippage_model or SlippageModel()

    def execute_order(self, order: Order, account: Account, data_portal: DataPortal) -> Trade | None:
        open_price = data_portal.get_open_price_for_execution(order.symbol, order.execution_date)
        if open_price is None or order.quantity <= 0:
            order.status = "rejected"
            order.reject_reason = "missing_open_price_or_zero_quantity"
            return None

        price, slippage = self.slippage_model.adjust_price(open_price, order.side)
        amount = price * order.quantity
        fee = self.cost_model.calculate(amount)
        if order.side == "buy" and amount + fee > account.cash:
            affordable_quantity = int(account.cash / (price * 100)) * 100
            if affordable_quantity <= 0:
                order.status = "rejected"
                order.reject_reason = "insufficient_cash"
                return None
            order.quantity = min(order.quantity, affordable_quantity)
            amount = price * order.quantity
            fee = self.cost_model.calculate(amount)

        order.status = "partial_filled" if order.reason and "scaled" in order.reason else "filled"
        return Trade(
            trade_id=new_id("trade"),
            order_id=order.order_id,
            account_id=order.account_id,
            symbol=order.symbol,
            side=order.side,
            date=order.execution_date,
            datetime=f"{order.execution_date}T09:30:00",
            price=price,
            quantity=order.quantity,
            amount=amount,
            fee=fee,
            slippage=slippage,
        )
