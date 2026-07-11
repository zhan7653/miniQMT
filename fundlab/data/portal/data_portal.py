from __future__ import annotations

from datetime import date
from typing import Sequence

import pandas as pd
import pyarrow.dataset as ds

from fundlab.common.dates import normalize_date
from fundlab.data.platform import PriceMode
from fundlab.data.storage import VersionedParquetStore

from .exceptions import AdjustedDataUnavailable, InvalidPriceMode
from .legacy_data_portal import LegacyDataPortal
from .snapshot import DataSnapshot


class DataPortal:
    """Read-only portal pinned to one catalog-complete immutable version."""

    def __init__(self, store: VersionedParquetStore | None = None, version_id: str | None = None,
                 *, sqlite_store=None, parquet_store=None) -> None:
        # Temporary constructor compatibility is deliberately calendar-only.  It
        # cannot read legacy bars/features or masquerade as a published version.
        if sqlite_store is not None:
            if store is not None or version_id is not None:
                raise TypeError("Choose either a v2 versioned store or legacy calendar compatibility")
            self._legacy_calendar = LegacyDataPortal.calendar_only(sqlite_store)
            self._store = None
            self.data_version = None
            self._snapshot = None
            return
        if store is None or version_id is None:
            raise TypeError("v2 DataPortal requires store and a complete version_id")
        resolved, _ = store.resolve_complete(version_id)
        self._legacy_calendar = None
        self._store = store
        self.data_version = resolved
        self._snapshot: DataSnapshot | None = None

    @classmethod
    def open_latest_complete(cls, store: VersionedParquetStore) -> "DataPortal":
        version_id, _ = store.resolve_complete()
        return cls(store, version_id)

    @classmethod
    def open_version(cls, store: VersionedParquetStore, version_id: str) -> "DataPortal":
        return cls(store, version_id)

    def snapshot(self) -> DataSnapshot:
        if self._store is None:
            raise RuntimeError("Legacy compatibility portal exposes calendar queries only")
        if self._snapshot is None:
            self._snapshot = DataSnapshot(self._store, self.data_version)
        return self._snapshot

    def get_daily_bar(self, symbols: Sequence[str], start_date: str | date, end_date: str | date,
                      fields: Sequence[str] | None = None, *, price_mode: PriceMode) -> pd.DataFrame:
        return self.snapshot().daily_bars(symbols, start_date, end_date, fields=fields, price_mode=price_mode)

    def get_price(self, symbol: str, date: str | date, *, price_mode: PriceMode,
                  field: str = "close", allow_previous: bool = False) -> float | None:
        query_date = normalize_date(date)
        bars = self.get_daily_bar([symbol], query_date, query_date, fields=[field], price_mode=price_mode)
        if not bars.empty:
            value = bars.iloc[0][field]
            return None if pd.isna(value) else float(value)
        if not allow_previous:
            return None
        days = self.get_trading_days("1900-01-01", query_date)
        previous = [item for item in days if item < query_date]
        return None if not previous else self.get_price(symbol, previous[-1], price_mode=price_mode,
                                                         field=field, allow_previous=False)

    def get_open_price_for_execution(self, symbol: str, execution_date: str | date) -> float | None:
        return self.get_price(symbol, execution_date, price_mode=PriceMode.RAW, field="open")

    def get_close_price_for_valuation(self, symbol: str, valuation_date: str | date) -> float | None:
        return self.get_price(symbol, valuation_date, price_mode=PriceMode.RAW, field="close")

    def get_trading_days(self, start_date: str | date, end_date: str | date) -> list[str]:
        if self._legacy_calendar is not None:
            return self._legacy_calendar.get_trading_days(str(start_date), str(end_date))
        frame = self.snapshot().calendar(start_date, end_date)
        if "is_trading_day" in frame.columns:
            frame = frame[frame["is_trading_day"]]
        return frame["date"].astype(str).tolist()

    def is_trading_day(self, date: str | date) -> bool:
        if self._legacy_calendar is not None:
            return self._legacy_calendar.is_trading_day(str(date))
        query_date = normalize_date(date)
        frame = self.snapshot().calendar(query_date, query_date)
        return False if frame.empty else bool(frame.iloc[0].get("is_trading_day", False))

    def next_trading_day(self, date: str | date) -> str | None:
        if self._legacy_calendar is not None:
            return self._legacy_calendar.next_trading_day(str(date))
        query_date = normalize_date(date)
        days = self.get_trading_days(query_date, "9999-12-31")
        return next((item for item in days if item > query_date), None)

    def previous_trading_day(self, date: str | date) -> str | None:
        if self._legacy_calendar is not None:
            return self._legacy_calendar.previous_trading_day(str(date))
        query_date = normalize_date(date)
        days = self.get_trading_days("1900-01-01", query_date)
        previous = [item for item in days if item < query_date]
        return previous[-1] if previous else None

    def get_universe(self, date: str | date) -> list[str]:
        return self.snapshot().universe(date)

    def get_features(self, symbols: Sequence[str], date: str | date,
                     fields: Sequence[str] | None = None) -> pd.DataFrame:
        return self.snapshot().features(symbols, date, fields=fields)
