from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import pandas as pd


@dataclass(frozen=True)
class QualityIssue:
    issue_type: str
    symbol: str | None
    date: str | None
    message: str


class DailyBarQualityChecker:
    required_columns = {"date", "symbol", "open", "high", "low", "close", "volume", "amount"}

    def check(
        self,
        data: pd.DataFrame,
        trading_days: Iterable[str] | None = None,
        listed_dates: dict[str, str | None] | None = None,
    ) -> list[QualityIssue]:
        issues: list[QualityIssue] = []
        missing_columns = sorted(self.required_columns - set(data.columns))
        if missing_columns:
            issues.append(QualityIssue("missing_columns", None, None, ",".join(missing_columns)))
            return issues

        duplicated = data[data.duplicated(["date", "symbol"], keep=False)]
        for row in duplicated[["date", "symbol"]].drop_duplicates().itertuples(index=False):
            issues.append(QualityIssue("duplicate_key", row.symbol, row.date, "duplicated date+symbol"))

        price_invalid = data[
            (data["high"] < data["low"])
            | (data["open"] > data["high"])
            | (data["open"] < data["low"])
            | (data["close"] > data["high"])
            | (data["close"] < data["low"])
        ]
        for row in price_invalid[["date", "symbol"]].itertuples(index=False):
            issues.append(QualityIssue("invalid_ohlc", row.symbol, row.date, "OHLC values are inconsistent"))

        negative_amount = data[(data["amount"] < 0) | (data["volume"] < 0)]
        for row in negative_amount[["date", "symbol"]].itertuples(index=False):
            issues.append(QualityIssue("negative_liquidity", row.symbol, row.date, "amount or volume is negative"))

        if trading_days is not None:
            valid_days = set(trading_days)
            invalid_dates = sorted(set(data["date"]) - valid_days)
            for invalid_date in invalid_dates:
                issues.append(QualityIssue("non_trading_date", None, invalid_date, "bar date is not a trading day"))

            expected_days = sorted(valid_days)
            listed_dates = listed_dates or {}
            for symbol in sorted(data["symbol"].dropna().unique()):
                symbol_data = data[data["symbol"] == symbol]
                present_days = set(symbol_data["date"])
                first_bar_date = min(present_days) if present_days else None
                listed_date = listed_dates.get(symbol)
                start_date = max([value for value in [first_bar_date, listed_date] if value], default=None)
                symbol_expected_days = [day for day in expected_days if start_date is None or day >= start_date]
                missing_days = [day for day in symbol_expected_days if day not in present_days]
                for missing_day in missing_days:
                    issues.append(QualityIssue("missing_bar", symbol, missing_day, "symbol has no bar on trading day"))

        zero_liquidity = data[(data["amount"] == 0) & (data["volume"] == 0)]
        for row in zero_liquidity[["date", "symbol"]].itertuples(index=False):
            issues.append(QualityIssue("zero_liquidity", row.symbol, row.date, "amount and volume are both zero"))

        no_price_move = data[
            (data["open"] == data["high"])
            & (data["high"] == data["low"])
            & (data["low"] == data["close"])
            & ((data["amount"] == 0) | (data["volume"] == 0))
        ]
        for row in no_price_move[["date", "symbol"]].itertuples(index=False):
            issues.append(QualityIssue("suspected_suspension", row.symbol, row.date, "flat OHLC with zero liquidity"))

        return issues

    def to_frame(self, issues: list[QualityIssue]) -> pd.DataFrame:
        return pd.DataFrame([issue.__dict__ for issue in issues])
