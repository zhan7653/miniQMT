from __future__ import annotations

import math

import pandas as pd

from fundlab.data.portal import DataPortal


class FeatureEngine:
    def __init__(self, data_portal: DataPortal, feature_version: str = "v1"):
        self.data_portal = data_portal
        self.feature_version = feature_version

    def compute(self, start_date: str, end_date: str, symbols: list[str] | None = None) -> pd.DataFrame:
        trading_days = self.data_portal.get_trading_days(start_date, end_date)
        if not trading_days:
            return pd.DataFrame()

        universe = symbols or self.data_portal.get_universe(trading_days[-1])
        bars = self.data_portal.get_daily_bar(universe, trading_days[0], trading_days[-1]).reset_index()
        if bars.empty:
            return pd.DataFrame()

        master = self.data_portal.get_fund_master(universe).set_index("symbol")
        rows = []
        for symbol, group in bars.groupby("symbol", sort=True):
            history = group.sort_values("date").reset_index(drop=True).copy()
            close = history["close"].astype(float)
            amount = history["amount"].astype(float)
            daily_return = close.pct_change()
            rolling_max_60d = close.rolling(60, min_periods=60).max()
            drawdown_60d = close / rolling_max_60d - 1

            history["ret_1d"] = close.pct_change(1)
            history["ret_5d"] = close.pct_change(5)
            history["ret_20d"] = close.pct_change(20)
            history["ret_60d"] = close.pct_change(60)
            history["ret_120d"] = close.pct_change(120)
            history["volatility_20d"] = daily_return.rolling(20, min_periods=20).std() * math.sqrt(252)
            history["volatility_60d"] = daily_return.rolling(60, min_periods=60).std() * math.sqrt(252)
            history["max_drawdown_60d"] = drawdown_60d.rolling(60, min_periods=60).min()
            history["amount_avg_20d"] = amount.rolling(20, min_periods=20).mean()
            history["amount_avg_60d"] = amount.rolling(60, min_periods=60).mean()

            tracking_index_code = master.loc[symbol, "tracking_index_code"] if symbol in master.index else None
            for record in history.to_dict(orient="records"):
                feature_date = record["date"]
                valuation = self.data_portal.get_index_valuation(tracking_index_code, feature_date, asof=feature_date) if tracking_index_code else None
                premium_discount = self.data_portal.get_premium_discount(symbol, feature_date, asof=feature_date)
                dividend_yield_12m = self._dividend_yield_12m(symbol, feature_date, record["close"])

                valuation_score = self._valuation_score(valuation)
                momentum_score = self._bounded_score(record.get("ret_20d"), low=-0.10, high=0.10)
                liquidity_score = self._bounded_score(record.get("amount_avg_20d"), low=0, high=50_000_000)
                premium_discount_score = self._premium_discount_score(premium_discount)
                dividend_score = self._bounded_score(dividend_yield_12m, low=0, high=0.05)
                risk_penalty_score = self._bounded_score(record.get("volatility_20d"), low=0.10, high=0.50)

                score_parts = [momentum_score, liquidity_score, valuation_score, dividend_score, premium_discount_score]
                total_score = self._mean_without_none(score_parts)
                if total_score is not None and risk_penalty_score is not None:
                    total_score -= 0.2 * risk_penalty_score

                rows.append(
                    {
                        "date": feature_date,
                        "symbol": symbol,
                        "feature_version": self.feature_version,
                        "ret_1d": record.get("ret_1d"),
                        "ret_5d": record.get("ret_5d"),
                        "ret_20d": record.get("ret_20d"),
                        "ret_60d": record.get("ret_60d"),
                        "ret_120d": record.get("ret_120d"),
                        "volatility_20d": record.get("volatility_20d"),
                        "volatility_60d": record.get("volatility_60d"),
                        "max_drawdown_60d": record.get("max_drawdown_60d"),
                        "amount_avg_20d": record.get("amount_avg_20d"),
                        "amount_avg_60d": record.get("amount_avg_60d"),
                        "turnover_score": None,
                        "momentum_score": momentum_score,
                        "valuation_score": valuation_score,
                        "dividend_score": dividend_score,
                        "liquidity_score": liquidity_score,
                        "premium_discount_score": premium_discount_score,
                        "risk_penalty_score": risk_penalty_score,
                        "total_score": total_score,
                        "premium_discount": premium_discount,
                        "dividend_yield_12m": dividend_yield_12m,
                        "tracking_index_code": tracking_index_code,
                        "available_date": feature_date,
                        "source_data_version": "fake-local",
                    }
                )

        return pd.DataFrame(rows).where(pd.notna(pd.DataFrame(rows)), None)

    def _dividend_yield_12m(self, symbol: str, date: str, close: float | None) -> float | None:
        if close is None or pd.isna(close) or close <= 0:
            return None
        dividends = self.data_portal.get_dividends(symbol, "1900-01-01", date, asof=date)
        if dividends.empty:
            return 0.0
        return float(dividends["dividend_per_share"].sum() / close)

    def _valuation_score(self, valuation: dict | None) -> float | None:
        if not valuation:
            return None
        parts = [
            self._invert_percentile(valuation.get("pe_percentile_5y")),
            self._invert_percentile(valuation.get("pb_percentile_5y")),
            self._as_float(valuation.get("dividend_yield_percentile_5y")),
        ]
        return self._mean_without_none(parts)

    def _premium_discount_score(self, premium_discount: float | None) -> float | None:
        value = self._as_float(premium_discount)
        if value is None:
            return None
        if value > 0.03:
            return 0.0
        if value < -0.03:
            return 1.0
        return 0.5 + (-value / 0.06)

    def _bounded_score(self, value: float | None, low: float, high: float) -> float | None:
        number = self._as_float(value)
        if number is None:
            return None
        if high == low:
            return None
        return max(0.0, min(1.0, (number - low) / (high - low)))

    def _invert_percentile(self, value: float | None) -> float | None:
        number = self._as_float(value)
        if number is None:
            return None
        return max(0.0, min(1.0, 1 - number))

    def _mean_without_none(self, values: list[float | None]) -> float | None:
        numbers = [self._as_float(value) for value in values]
        numbers = [value for value in numbers if value is not None]
        if not numbers:
            return None
        return sum(numbers) / len(numbers)

    def _as_float(self, value: float | None) -> float | None:
        if value is None or pd.isna(value):
            return None
        return float(value)

