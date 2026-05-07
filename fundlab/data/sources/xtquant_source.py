from __future__ import annotations

from typing import Sequence

import pandas as pd

from fundlab.data.sources.base import MarketDataSource


class XtQuantSource(MarketDataSource):
    name = "xtquant"

    def __init__(self, config: dict | None = None):
        self.config = config or {}
        self.connected = False
        self.xtdata = None

    def connect(self) -> None:
        try:
            from xtquant import xtdata
        except ImportError as exc:
            raise RuntimeError("xtquant is not installed or MiniQMT environment is unavailable") from exc

        self.xtdata = xtdata
        self.connected = True

    def _require_connection(self) -> None:
        if not self.connected or self.xtdata is None:
            raise RuntimeError("XtQuantSource is not connected. Call connect() first.")

    def get_instruments(self) -> list[dict]:
        raise NotImplementedError("xtquant instrument loading is deferred until MiniQMT integration")

    def download_daily_bar(self, symbols: Sequence[str], start_date: str, end_date: str) -> None:
        self._require_connection()
        for symbol in symbols:
            self.xtdata.download_history_data(
                stock_code=symbol,
                period="1d",
                start_time=start_date.replace("-", ""),
                end_time=end_date.replace("-", ""),
            )

    def get_daily_bar(self, symbols: Sequence[str], start_date: str, end_date: str) -> pd.DataFrame:
        self._require_connection()
        return self.xtdata.get_market_data_ex(
            field_list=[],
            stock_list=list(symbols),
            period="1d",
            start_time=start_date.replace("-", ""),
            end_time=end_date.replace("-", ""),
            count=-1,
            dividend_type="none",
            fill_data=False,
        )

    def get_trading_calendar(self, start_date: str, end_date: str) -> pd.DataFrame:
        self._require_connection()
        raise NotImplementedError("xtquant trading calendar loading is deferred until field mapping is verified")

