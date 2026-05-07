from __future__ import annotations

import pandas as pd

from fundlab.backtest.models import Account, Order, OrderIntent, Trade


class BacktestRecorder:
    def __init__(self):
        self.intents: list[OrderIntent] = []
        self.orders: list[Order] = []
        self.trades: list[Trade] = []
        self.account_daily: list[dict] = []
        self.position_daily: list[dict] = []
        self.account_events: list[dict] = []

    def record_intents(self, intents: list[OrderIntent]) -> None:
        self.intents.extend(intents)

    def load_intents(self, execution_date: str) -> list[OrderIntent]:
        return [intent for intent in self.intents if intent.execution_date == execution_date]

    def record_orders(self, orders: list[Order]) -> None:
        self.orders.extend(orders)

    def record_trades(self, trades: list[Trade]) -> None:
        self.trades.extend(trades)

    def record_account_events(self, events: list[dict]) -> None:
        self.account_events.extend(events)

    def record_account_daily(self, date: str, account: Account) -> None:
        previous_total = self.account_daily[-1]["total_asset"] if self.account_daily else account.initial_cash
        daily_return = account.total_asset / previous_total - 1 if previous_total else 0.0
        self.account_daily.append(
            {
                "date": date,
                "cash": account.cash,
                "market_value": account.market_value(),
                "total_asset": account.total_asset,
                "nav": account.nav,
                "daily_return": daily_return,
            }
        )

    def record_position_daily(self, date: str, account: Account) -> None:
        for position in account.positions.values():
            self.position_daily.append(
                {
                    "date": date,
                    "symbol": position.symbol,
                    "quantity": position.quantity,
                    "avg_cost": position.avg_cost,
                    "market_price": position.market_price,
                    "market_value": position.market_value,
                }
            )

    def account_daily_frame(self) -> pd.DataFrame:
        return pd.DataFrame(self.account_daily)

    def orders_frame(self) -> pd.DataFrame:
        return pd.DataFrame([order.__dict__ for order in self.orders])

    def trades_frame(self) -> pd.DataFrame:
        return pd.DataFrame([trade.__dict__ for trade in self.trades])
