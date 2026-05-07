from __future__ import annotations

from fundlab.backtest.accounting import Accounting
from fundlab.backtest.broker import BacktestBroker
from fundlab.backtest.execution import ExecutionPlanner
from fundlab.backtest.models import Account
from fundlab.backtest.recorder import BacktestRecorder
from fundlab.data.portal import DataPortal
from fundlab.risk import CashCheck, PositionLimit, PremiumDiscountCheck, RiskEngine, UniverseCheck
from fundlab.strategies.base import Strategy


class BacktestEngine:
    def __init__(
        self,
        data_portal: DataPortal,
        strategy: Strategy,
        start_date: str,
        end_date: str,
        initial_cash: float = 1_000_000,
        rebalance_frequency: str = "monthly",
        risk_engine: RiskEngine | None = None,
    ):
        self.data_portal = data_portal
        self.strategy = strategy
        self.start_date = start_date
        self.end_date = end_date
        self.account = Account.create("backtest_account", initial_cash)
        self.rebalance_frequency = rebalance_frequency
        self.broker = BacktestBroker()
        self.accounting = Accounting()
        self.execution_planner = ExecutionPlanner()
        self.recorder = BacktestRecorder()
        self.risk_engine = risk_engine or RiskEngine(
            [UniverseCheck(), PositionLimit(max_weight_per_symbol=0.50), CashCheck(), PremiumDiscountCheck()]
        )

    def run(self) -> BacktestRecorder:
        trading_days = self.data_portal.get_trading_days(self.start_date, self.end_date)
        for index, current_date in enumerate(trading_days):
            todays_intents = self.recorder.load_intents(current_date)
            if todays_intents:
                orders = self.execution_planner.create_orders(todays_intents, self.account, self.data_portal)
                orders = self.risk_engine.check_orders(orders, self.account, current_date, self.data_portal)
                trades = []
                for order in orders:
                    if order.status == "rejected":
                        continue
                    trade = self.broker.execute_order(order, self.account, self.data_portal)
                    if trade is not None:
                        self.accounting.apply_trade(self.account, trade)
                        trades.append(trade)
                self.recorder.record_orders(orders)
                self.recorder.record_trades(trades)

            self.recorder.record_account_events(self.accounting.accrue_dividends(self.account, current_date, self.data_portal))
            self.recorder.record_account_events(self.accounting.pay_dividends(self.account, current_date))
            self.accounting.mark_to_market(self.account, current_date, self.data_portal)
            self.recorder.record_account_daily(current_date, self.account)
            self.recorder.record_position_daily(current_date, self.account)

            if self._is_rebalance_day(current_date, trading_days, index):
                execution_date = self.data_portal.next_trading_day(current_date)
                if execution_date is None:
                    continue
                target_weights = self.strategy.on_rebalance(current_date, self.data_portal, {"account": self.account})
                target_weights = self.risk_engine.check_target_weights(
                    target_weights, self.account, current_date, self.data_portal
                )
                intents = self.execution_planner.create_intents(
                    account_id=self.account.account_id,
                    strategy_id=self.strategy.strategy_id,
                    signal_date=current_date,
                    execution_date=execution_date,
                    target_weights=target_weights,
                )
                self.recorder.record_intents(intents)

        return self.recorder

    def _is_rebalance_day(self, current_date: str, trading_days: list[str], index: int) -> bool:
        if index == 0:
            return True
        if self.rebalance_frequency == "daily":
            return True
        if self.rebalance_frequency == "monthly":
            next_date = trading_days[index + 1] if index + 1 < len(trading_days) else None
            return next_date is None or next_date[:7] != current_date[:7]
        raise ValueError(f"Unsupported rebalance_frequency: {self.rebalance_frequency}")
