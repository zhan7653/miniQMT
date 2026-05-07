from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Sequence

import pandas as pd


class MarketDataSource(ABC):
    name: str

    @abstractmethod
    def get_instruments(self) -> list[dict]:
        raise NotImplementedError

    def download_daily_bar(self, symbols: Sequence[str], start_date: str, end_date: str) -> None:
        raise NotImplementedError

    def get_daily_bar(self, symbols: Sequence[str], start_date: str, end_date: str) -> pd.DataFrame:
        raise NotImplementedError

    def get_trading_calendar(self, start_date: str, end_date: str) -> pd.DataFrame:
        raise NotImplementedError

