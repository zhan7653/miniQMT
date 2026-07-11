from __future__ import annotations

from datetime import date

import pandas as pd

from fundlab.backtest.accounting import Accounting
from fundlab.backtest.broker import BacktestBroker
from fundlab.backtest.execution import ExecutionPlanner
from fundlab.backtest.models import Account
from fundlab.backtest.recorder import BacktestRecorder
from fundlab.data.portal import DataPortal
from fundlab.data.platform import PriceMode
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
        run_portal = _RunDataPortal(self.data_portal, self.start_date, self.end_date)
        for index, current_date in enumerate(trading_days):
            todays_intents = self.recorder.load_intents(current_date)
            if todays_intents:
                orders = self.execution_planner.create_orders(todays_intents, self.account, run_portal)
                orders = self.risk_engine.check_orders(orders, self.account, current_date, run_portal)
                trades = []
                for order in orders:
                    if order.status == "rejected":
                        continue
                    trade = self.broker.execute_order(order, self.account, run_portal)
                    if trade is not None:
                        self.accounting.apply_trade(self.account, trade)
                        trades.append(trade)
                self.recorder.record_orders(orders)
                self.recorder.record_trades(trades)

            self.recorder.record_account_events(self.accounting.accrue_dividends(self.account, current_date, run_portal))
            self.recorder.record_account_events(self.accounting.pay_dividends(self.account, current_date))
            self.accounting.mark_to_market(self.account, current_date, run_portal)
            self.recorder.record_account_daily(current_date, self.account)
            self.recorder.record_position_daily(current_date, self.account)

            if self._is_rebalance_day(current_date, trading_days, index):
                execution_date = trading_days[index + 1] if index + 1 < len(trading_days) else None
                if execution_date is None:
                    continue
                target_weights = self.strategy.on_rebalance(current_date, run_portal, {"account": self.account})
                target_weights = self.risk_engine.check_target_weights(
                    target_weights, self.account, current_date, run_portal
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


class _RunDataPortal:
    """Backtest-local bulk cache layered over one immutable portal version."""

    def __init__(self, portal: DataPortal, start_date: str, end_date: str) -> None:
        self._portal = portal
        self._start_date = start_date
        self._end_date = end_date
        self.data_version = portal.data_version
        self._raw_by_symbol: dict[str, pd.DataFrame] = {}

    def __getattr__(self, name: str):
        return getattr(self._portal, name)

    def _raw(self, symbol: str) -> pd.DataFrame:
        if symbol not in self._raw_by_symbol:
            self._raw_by_symbol[symbol] = self._portal.get_daily_bar(
                [symbol], self._start_date, self._end_date, price_mode=PriceMode.RAW
            )
        return self._raw_by_symbol[symbol]

    def get_price(self, symbol: str, query_date: str | date, *, price_mode: PriceMode,
                  field: str = "close", allow_previous: bool = False) -> float | None:
        if price_mode is not PriceMode.RAW:
            return self._portal.get_price(symbol, query_date, price_mode=price_mode, field=field,
                                          allow_previous=allow_previous)
        frame = self._raw(symbol)
        if frame.empty or field not in frame.columns:
            return None
        date_text = query_date.isoformat() if isinstance(query_date, date) else str(query_date)
        dates = frame.index.get_level_values("date").astype(str)
        eligible = dates <= date_text if allow_previous else dates == date_text
        selected = frame.loc[eligible]
        if selected.empty:
            return None
        value = selected.iloc[-1][field]
        return None if pd.isna(value) else float(value)

    def get_open_price_for_execution(self, symbol: str, execution_date: str | date) -> float | None:
        return self.get_price(symbol, execution_date, price_mode=PriceMode.RAW, field="open")

    def get_close_price_for_valuation(self, symbol: str, valuation_date: str | date) -> float | None:
        return self.get_price(symbol, valuation_date, price_mode=PriceMode.RAW, field="close")
