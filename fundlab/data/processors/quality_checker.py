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

    def check(self, data: pd.DataFrame, trading_days: Iterable[str] | None = None) -> list[QualityIssue]:
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

        return issues

    def to_frame(self, issues: list[QualityIssue]) -> pd.DataFrame:
        return pd.DataFrame([issue.__dict__ for issue in issues])

