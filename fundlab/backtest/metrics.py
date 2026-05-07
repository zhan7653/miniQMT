from __future__ import annotations

import math

import pandas as pd


class PerformanceAnalyzer:
    def analyze(self, account_daily: pd.DataFrame, trades: pd.DataFrame) -> dict:
        if account_daily.empty:
            return {
                "cumulative_return": None,
                "annualized_return": None,
                "annualized_volatility": None,
                "sharpe": None,
                "calmar": None,
                "max_drawdown": None,
                "win_rate": None,
                "turnover": None,
                "trade_count": 0,
                "total_cost": 0.0,
            }

        nav = account_daily["nav"].astype(float)
        daily_return = account_daily["daily_return"].astype(float).fillna(0)
        cumulative_return = nav.iloc[-1] - 1
        periods = len(account_daily)
        annualized_return = nav.iloc[-1] ** (252 / periods) - 1 if periods > 0 and nav.iloc[-1] > 0 else None
        annualized_volatility = daily_return.std(ddof=0) * math.sqrt(252) if periods > 1 else 0.0
        sharpe = None
        if annualized_volatility and annualized_volatility > 0 and annualized_return is not None:
            sharpe = annualized_return / annualized_volatility

        drawdown = nav / nav.cummax() - 1
        max_drawdown = float(drawdown.min())
        calmar = None
        if max_drawdown < 0 and annualized_return is not None:
            calmar = annualized_return / abs(max_drawdown)

        trade_count = len(trades)
        total_cost = float(trades["fee"].sum()) if not trades.empty and "fee" in trades.columns else 0.0
        total_trade_amount = float(trades["amount"].sum()) if not trades.empty and "amount" in trades.columns else 0.0
        average_asset = float(account_daily["total_asset"].astype(float).mean())
        turnover = total_trade_amount / average_asset if average_asset else None
        win_rate = float((daily_return > 0).mean()) if periods else None

        return {
            "cumulative_return": float(cumulative_return),
            "annualized_return": None if annualized_return is None else float(annualized_return),
            "annualized_volatility": float(annualized_volatility),
            "sharpe": None if sharpe is None else float(sharpe),
            "calmar": None if calmar is None else float(calmar),
            "max_drawdown": max_drawdown,
            "win_rate": win_rate,
            "turnover": None if turnover is None else float(turnover),
            "trade_count": trade_count,
            "total_cost": total_cost,
            "benchmark_return": None,
            "excess_return": None,
            "max_drawdown_recovery_days": None,
        }

