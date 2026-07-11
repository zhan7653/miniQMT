from __future__ import annotations

from collections.abc import Iterable, Iterator, Sequence
from dataclasses import asdict, dataclass

import pandas as pd

from fundlab.data.platform import QualityDisposition


@dataclass(frozen=True)
class QualityIssue:
    issue_type: str
    symbol: str | None
    date: str | None
    message: str
    disposition: QualityDisposition
    scope: str
    severity: str
    publication_eligible: bool


@dataclass(frozen=True)
class QualityReport(Sequence[QualityIssue]):
    issues: tuple[QualityIssue, ...]
    disposition: QualityDisposition
    publication_eligible: bool
    blocked_symbols: tuple[str, ...]

    def __getitem__(self, index):
        return self.issues[index]

    def __len__(self) -> int:
        return len(self.issues)

    def __iter__(self) -> Iterator[QualityIssue]:
        return iter(self.issues)


class DailyBarQualityChecker:
    required_columns = frozenset({"date", "symbol", "open", "high", "low", "close", "volume", "amount"})

    def check(
        self,
        data: pd.DataFrame,
        trading_days: Iterable[str] | None = None,
        listed_dates: dict[str, str | None] | None = None,
        requested_symbols: Iterable[str] | None = None,
        suspended: Iterable[tuple[str, str]] | None = None,
    ) -> QualityReport:
        issues: list[QualityIssue] = []
        missing_columns = sorted(self.required_columns - set(data.columns))
        if missing_columns:
            issues.append(self._issue("missing_columns", None, None, ",".join(missing_columns), QualityDisposition.BLOCK_BATCH))
            return self._report(issues)

        duplicated = data[data.duplicated(["date", "symbol"], keep=False)]
        for row in duplicated[["date", "symbol"]].drop_duplicates().itertuples(index=False):
            issues.append(self._issue("duplicate_key", row.symbol, row.date, "duplicated date+symbol", QualityDisposition.BLOCK_SYMBOL))

        invalid = data[(data["high"] < data["low"]) | (data["open"] > data["high"]) | (data["open"] < data["low"]) | (data["close"] > data["high"]) | (data["close"] < data["low"])]
        for row in invalid[["date", "symbol"]].drop_duplicates().itertuples(index=False):
            issues.append(self._issue("invalid_ohlc", row.symbol, row.date, "OHLC values are inconsistent", QualityDisposition.BLOCK_SYMBOL))

        negative = data[(data["amount"] < 0) | (data["volume"] < 0)]
        for row in negative[["date", "symbol"]].drop_duplicates().itertuples(index=False):
            issues.append(self._issue("negative_liquidity", row.symbol, row.date, "amount or volume is negative", QualityDisposition.BLOCK_SYMBOL))

        confirmed_suspensions = set(suspended or ())
        zero = data[(data["amount"] == 0) & (data["volume"] == 0)]
        for row in zero[["date", "symbol", "open", "high", "low", "close"]].drop_duplicates().itertuples(index=False):
            confirmed = (row.symbol, row.date) in confirmed_suspensions
            flat = row.open == row.high == row.low == row.close
            disposition = QualityDisposition.VALID_SUSPENDED if confirmed and flat else QualityDisposition.WARNING
            kind = "valid_suspension" if disposition is QualityDisposition.VALID_SUSPENDED else "zero_liquidity"
            issues.append(self._issue(kind, row.symbol, row.date, "confirmed suspension" if confirmed and flat else "amount and volume are both zero", disposition))

        if trading_days is not None:
            valid_days = set(trading_days)
            for invalid_date in sorted(set(data["date"]) - valid_days):
                issues.append(self._issue("non_trading_date", None, invalid_date, "bar date is not a trading day", QualityDisposition.BLOCK_BATCH))

            listed_dates = listed_dates or {}
            symbols = set(requested_symbols or data["symbol"].dropna().unique())
            for symbol in sorted(symbols):
                present = set(data.loc[data["symbol"] == symbol, "date"])
                listed = listed_dates.get(symbol)
                for day in sorted(valid_days):
                    if listed and day < listed:
                        continue
                    if day not in present and (symbol, day) not in confirmed_suspensions:
                        issues.append(self._issue("missing_active_bar", symbol, day, "active symbol has no bar on trading day", QualityDisposition.BLOCK_SYMBOL))

        return self._report(issues)

    @staticmethod
    def _issue(issue_type: str, symbol: str | None, date: str | None, message: str,
               disposition: QualityDisposition) -> QualityIssue:
        scope = "batch" if disposition is QualityDisposition.BLOCK_BATCH else ("symbol" if symbol else "row")
        severity = "error" if disposition in {QualityDisposition.BLOCK_BATCH, QualityDisposition.BLOCK_SYMBOL} else ("warning" if disposition is QualityDisposition.WARNING else "info")
        return QualityIssue(issue_type, symbol, date, message, disposition, scope, severity,
                            disposition not in {QualityDisposition.BLOCK_BATCH, QualityDisposition.BLOCK_SYMBOL})

    @staticmethod
    def _report(issues: list[QualityIssue]) -> QualityReport:
        dispositions = {issue.disposition for issue in issues}
        if QualityDisposition.BLOCK_BATCH in dispositions:
            outcome = QualityDisposition.BLOCK_BATCH
        elif QualityDisposition.BLOCK_SYMBOL in dispositions:
            outcome = QualityDisposition.BLOCK_SYMBOL
        elif QualityDisposition.WARNING in dispositions:
            outcome = QualityDisposition.WARNING
        elif QualityDisposition.VALID_SUSPENDED in dispositions:
            outcome = QualityDisposition.VALID_SUSPENDED
        else:
            outcome = QualityDisposition.PASS
        blocked = tuple(sorted({issue.symbol for issue in issues if issue.disposition is QualityDisposition.BLOCK_SYMBOL and issue.symbol}))
        return QualityReport(tuple(issues), outcome, outcome is not QualityDisposition.BLOCK_BATCH, blocked)

    def to_frame(self, result: QualityReport | Sequence[QualityIssue]) -> pd.DataFrame:
        return pd.DataFrame([asdict(issue) for issue in result])
