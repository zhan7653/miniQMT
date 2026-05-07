from __future__ import annotations

from typing import Literal, Sequence

import pandas as pd

from fundlab.common.dates import normalize_date
from fundlab.data.storage.parquet_store import ParquetStore
from fundlab.data.storage.sqlite_store import SQLiteStore


class DataPortal:
    def __init__(self, sqlite_store: SQLiteStore, parquet_store: ParquetStore, config: dict | None = None):
        self.sqlite_store = sqlite_store
        self.parquet_store = parquet_store
        self.config = config or {}

    def get_universe(self, date: str, filters: dict | None = None, asof: str | None = None) -> list[str]:
        query_date = normalize_date(date)
        fund_master = self.get_fund_master(date=query_date, point_in_time=True)
        universe = fund_master[
            (fund_master["include_in_universe"] == 1)
            & (fund_master["listed_date"].isna() | (fund_master["listed_date"] <= query_date))
            & (fund_master["delisted_date"].isna() | (fund_master["delisted_date"] > query_date))
        ].copy()

        if filters:
            for field, expected in filters.items():
                if field not in universe.columns:
                    continue
                if isinstance(expected, (list, tuple, set)):
                    universe = universe[universe[field].isin(expected)]
                else:
                    universe = universe[universe[field] == expected]

        return universe.sort_values("symbol")["symbol"].tolist()

    def get_fund_master(
        self,
        symbols: Sequence[str] | None = None,
        date: str | None = None,
        point_in_time: bool = True,
    ) -> pd.DataFrame:
        fund_master = self.sqlite_store.get_fund_master(list(symbols) if symbols else None)
        if date and point_in_time and not fund_master.empty:
            query_date = normalize_date(date)
            fund_master = fund_master[
                (fund_master["listed_date"].isna() | (fund_master["listed_date"] <= query_date))
                & (fund_master["delisted_date"].isna() | (fund_master["delisted_date"] > query_date))
            ]
        return fund_master.reset_index(drop=True)

    def get_daily_bar(
        self,
        symbols: Sequence[str],
        start_date: str,
        end_date: str,
        fields: Sequence[str] | None = None,
        adjusted: Literal["none", "front", "back"] = "none",
        fill_method: Literal["none", "previous_close"] = "none",
    ) -> pd.DataFrame:
        if adjusted != "none":
            raise NotImplementedError("Adjusted daily bars are not implemented in Step 1")
        if fill_method != "none":
            raise NotImplementedError("Daily bar filling is not implemented in Step 1")

        return self.parquet_store.read_daily_bar(
            symbols=symbols,
            start_date=normalize_date(start_date),
            end_date=normalize_date(end_date),
            fields=fields,
        )

    def get_price(
        self,
        symbol: str,
        date: str,
        field: str = "close",
        adjusted: Literal["none", "front", "back"] = "none",
        asof: str | None = None,
        allow_previous: bool = False,
    ) -> float | None:
        query_date = normalize_date(date)
        bars = self.get_daily_bar([symbol], query_date, query_date, fields=[field], adjusted=adjusted)
        if not bars.empty:
            value = bars.iloc[0][field]
            return None if pd.isna(value) else float(value)
        if not allow_previous:
            return None
        previous = self.previous_trading_day(query_date)
        if previous is None:
            return None
        return self.get_price(symbol, previous, field=field, adjusted=adjusted, allow_previous=True)

    def get_open_price_for_execution(self, symbol: str, execution_date: str) -> float | None:
        return self.get_price(symbol, execution_date, field="open", allow_previous=False)

    def get_index_valuation(self, index_code: str, date: str, asof: str | None = None) -> dict | None:
        query_date = normalize_date(date)
        query_asof = normalize_date(asof or date)
        frame = self.sqlite_store.read_frame(
            """
            SELECT *
            FROM index_valuation
            WHERE index_code = ?
              AND date <= ?
              AND available_date <= ?
            ORDER BY date DESC
            LIMIT 1
            """,
            [index_code, query_date, query_asof],
        )
        return None if frame.empty else frame.iloc[0].to_dict()

    def get_nav(self, symbol: str, date: str, asof: str | None = None) -> dict | None:
        query_date = normalize_date(date)
        query_asof = normalize_date(asof or date)
        frame = self.sqlite_store.read_frame(
            """
            SELECT *
            FROM fund_nav
            WHERE symbol = ?
              AND date <= ?
              AND available_date <= ?
            ORDER BY date DESC
            LIMIT 1
            """,
            [symbol, query_date, query_asof],
        )
        return None if frame.empty else frame.iloc[0].to_dict()

    def get_premium_discount(self, symbol: str, date: str, asof: str | None = None) -> float | None:
        nav = self.get_nav(symbol=symbol, date=date, asof=asof)
        if nav is None or pd.isna(nav.get("premium_discount")):
            return None
        return float(nav["premium_discount"])

    def get_features(
        self,
        symbols: Sequence[str],
        date: str,
        feature_version: str = "v1",
        asof: str | None = None,
    ) -> pd.DataFrame:
        query_date = normalize_date(date)
        query_asof = normalize_date(asof or date)
        placeholders = ",".join("?" for _ in symbols)
        frame = self.sqlite_store.read_frame(
            f"""
            SELECT *
            FROM fund_features_daily
            WHERE symbol IN ({placeholders})
              AND date = ?
              AND feature_version = ?
              AND available_date <= ?
            ORDER BY symbol
            """,
            [*symbols, query_date, feature_version, query_asof],
        )
        if frame.empty:
            return frame
        return frame.set_index("symbol")

    def get_dividends(self, symbol: str, start_date: str, end_date: str, asof: str | None = None) -> pd.DataFrame:
        query_start = normalize_date(start_date)
        query_end = normalize_date(end_date)
        query_asof = normalize_date(asof or end_date)
        return self.sqlite_store.read_frame(
            """
            SELECT *
            FROM fund_dividend
            WHERE symbol = ?
              AND ex_dividend_date >= ?
              AND ex_dividend_date <= ?
              AND available_date <= ?
            ORDER BY ex_dividend_date
            """,
            [symbol, query_start, query_end, query_asof],
        )

    def get_dividends_by_record_date(self, symbol: str, record_date: str, asof: str | None = None) -> pd.DataFrame:
        query_date = normalize_date(record_date)
        query_asof = normalize_date(asof or record_date)
        return self.sqlite_store.read_frame(
            """
            SELECT *
            FROM fund_dividend
            WHERE symbol = ?
              AND record_date = ?
              AND available_date <= ?
            ORDER BY payment_date
            """,
            [symbol, query_date, query_asof],
        )

    def get_trading_days(self, start_date: str, end_date: str) -> list[str]:
        days = self.sqlite_store.get_trading_days(normalize_date(start_date), normalize_date(end_date))
        return days["date"].tolist()

    def next_trading_day(self, date: str) -> str | None:
        frame = self.sqlite_store.read_frame(
            """
            SELECT next_trading_day
            FROM trading_calendar
            WHERE date = ?
            """,
            [normalize_date(date)],
        )
        if frame.empty:
            return None
        return frame.iloc[0]["next_trading_day"]

    def previous_trading_day(self, date: str) -> str | None:
        frame = self.sqlite_store.read_frame(
            """
            SELECT previous_trading_day
            FROM trading_calendar
            WHERE date = ?
            """,
            [normalize_date(date)],
        )
        if frame.empty:
            return None
        return frame.iloc[0]["previous_trading_day"]

    def is_trading_day(self, date: str) -> bool:
        frame = self.sqlite_store.read_frame(
            """
            SELECT is_trading_day
            FROM trading_calendar
            WHERE date = ?
            """,
            [normalize_date(date)],
        )
        if frame.empty:
            return False
        return bool(frame.iloc[0]["is_trading_day"])
