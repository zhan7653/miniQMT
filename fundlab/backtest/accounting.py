from __future__ import annotations

from fundlab.backtest.models import Account, Position, Trade
from fundlab.data.portal import DataPortal


class Accounting:
    def apply_trade(self, account: Account, trade: Trade) -> None:
        position = account.positions.get(trade.symbol, Position(symbol=trade.symbol))
        if trade.side == "buy":
            total_cost_before = position.avg_cost * position.quantity
            total_cost_after = total_cost_before + trade.amount + trade.fee
            position.quantity += trade.quantity
            position.avg_cost = total_cost_after / position.quantity if position.quantity else 0.0
            account.cash -= trade.amount + trade.fee
        else:
            sell_quantity = min(position.quantity, trade.quantity)
            position.quantity -= sell_quantity
            account.cash += trade.amount - trade.fee
            if position.quantity == 0:
                position.avg_cost = 0.0

        account.positions[trade.symbol] = position

    def accrue_dividends(self, account: Account, date: str, data_portal: DataPortal) -> list[dict]:
        events = []
        for symbol, position in account.positions.items():
            if position.quantity <= 0:
                continue
            dividends = data_portal.get_dividends_by_record_date(symbol, date, asof=date)
            for dividend in dividends.to_dict(orient="records"):
                amount = position.quantity * float(dividend["dividend_per_share"]) * (1 - float(dividend.get("tax_rate") or 0))
                receivable = {
                    "symbol": symbol,
                    "record_date": date,
                    "payment_date": dividend.get("payment_date") or dividend["ex_dividend_date"],
                    "quantity": position.quantity,
                    "dividend_per_share": float(dividend["dividend_per_share"]),
                    "amount": amount,
                    "status": "pending",
                }
                account.dividend_receivables.append(receivable)
                events.append({"date": date, "event_type": "dividend_receivable", **receivable})
        return events

    def pay_dividends(self, account: Account, date: str) -> list[dict]:
        events = []
        for receivable in account.dividend_receivables:
            if receivable["status"] != "pending" or receivable["payment_date"] != date:
                continue
            cash_before = account.cash
            account.cash += receivable["amount"]
            receivable["status"] = "paid"
            events.append(
                {
                    "date": date,
                    "event_type": "dividend_paid",
                    "symbol": receivable["symbol"],
                    "amount": receivable["amount"],
                    "quantity": receivable["quantity"],
                    "cash_before": cash_before,
                    "cash_after": account.cash,
                }
            )
        return events

    def mark_to_market(self, account: Account, date: str, data_portal: DataPortal) -> None:
        for symbol, position in list(account.positions.items()):
            if position.quantity <= 0:
                account.positions.pop(symbol, None)
                continue
            price = data_portal.get_price(symbol, date, field="close", allow_previous=True)
            if price is None:
                continue
            position.market_price = price
            position.market_value = position.quantity * price
            position.unrealized_pnl = position.market_value - position.quantity * position.avg_cost

        account.total_asset = account.cash + account.market_value()
        account.nav = account.total_asset / account.initial_cash
