from __future__ import annotations

from datetime import date
from typing import Sequence

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds

from fundlab.common.dates import normalize_date
from fundlab.data.platform import PriceMode
from fundlab.data.storage import StorageError, VersionedParquetStore

from .exceptions import AdjustedDataUnavailable, InvalidPriceMode


class DataSnapshot:
    """Run-local, version-bound cache with projected and predicate-pushed reads."""

    def __init__(self, store: VersionedParquetStore, version_id: str) -> None:
        store.resolve_complete(version_id)
        self._store = store
        self.data_version = version_id
        self._cache: dict[tuple[object, ...], pd.DataFrame | tuple[str, ...]] = {}

    def daily_bars(self, symbols: Sequence[str], start_date: str | date, end_date: str | date,
                   fields: Sequence[str] | None = None, *, price_mode: PriceMode) -> pd.DataFrame:
        if not isinstance(price_mode, PriceMode):
            raise InvalidPriceMode("price_mode must be PriceMode.RAW or PriceMode.ADJUSTED")
        start, end = normalize_date(start_date), normalize_date(end_date)
        normalized_symbols = tuple(sorted(set(symbols)))
        projected = tuple(dict.fromkeys(["date", "symbol", *(fields or ())])) if fields else None
        key = ("bars", price_mode.value, normalized_symbols, start, end, projected)
        if key not in self._cache:
            table_name = f"daily_bars_{price_mode.value}"
            predicate = (ds.field("symbol").isin(normalized_symbols)
                         & (ds.field("date") >= start) & (ds.field("date") <= end))
            try:
                table = self._store.read_table(self.data_version, table_name, columns=projected, filters=predicate)
            except StorageError as exc:
                if price_mode is PriceMode.ADJUSTED:
                    raise AdjustedDataUnavailable("Adjusted prices are unavailable; raw fallback is forbidden") from exc
                raise
            self._cache[key] = self._frame(table, index=("date", "symbol"))
        return self._cache[key].copy()  # type: ignore[union-attr]

    def calendar(self, start_date: str | date, end_date: str | date) -> pd.DataFrame:
        start, end = normalize_date(start_date), normalize_date(end_date)
        key = ("calendar", start, end)
        if key not in self._cache:
            predicate = (ds.field("date") >= start) & (ds.field("date") <= end)
            self._cache[key] = self._frame(self._store.read_table(self.data_version, "calendar", filters=predicate))
        return self._cache[key].copy()  # type: ignore[union-attr]

    def universe(self, effective_date: str | date) -> list[str]:
        query_date = normalize_date(effective_date)
        key = ("universe", query_date)
        if key not in self._cache:
            try:
                master = self._store.read_table(self.data_version, "fund_master").to_pandas()
            except StorageError:
                master = None
            if master is not None:
                if master.empty:
                    value = ()
                else:
                    listed = master["listed_date"].astype("string").str[:10]
                    if "delisted_date" in master:
                        delisted = master["delisted_date"].astype("string").str[:10]
                        valid_end = master["delisted_date"].isna() | (delisted >= query_date)
                    else:
                        valid_end = pd.Series(True, index=master.index)
                    trusted = (
                        master["trust_state"].astype(str).eq("trusted")
                        if "trust_state" in master else pd.Series(True, index=master.index)
                    )
                    valid = listed.notna() & (listed <= query_date) & valid_end & trusted
                    value = tuple(sorted(master.loc[valid, "symbol"].astype(str).unique()))
            else:
                predicate = ds.field("effective_date") <= query_date
                table = self._store.read_table(
                    self.data_version, "universe", columns=["symbol", "effective_date"],
                    filters=predicate,
                )
                frame = table.to_pandas()
                if frame.empty:
                    value = ()
                else:
                    latest = frame["effective_date"].max()
                    value = tuple(sorted(frame.loc[frame["effective_date"] == latest, "symbol"].unique()))
            self._cache[key] = value
        return list(self._cache[key])  # type: ignore[arg-type]

    def features(self, symbols: Sequence[str], effective_date: str | date,
                 fields: Sequence[str] | None = None) -> pd.DataFrame:
        query_date = normalize_date(effective_date)
        normalized_symbols = tuple(sorted(set(symbols)))
        projected = tuple(dict.fromkeys(["date", "symbol", *(fields or ())])) if fields else None
        key = ("features", normalized_symbols, query_date, projected)
        if key not in self._cache:
            predicate = ds.field("symbol").isin(normalized_symbols) & (ds.field("date") == query_date)
            table = self._store.read_table(self.data_version, "features", columns=projected, filters=predicate)
            self._cache[key] = self._frame(table, index=("symbol",))
        return self._cache[key].copy()  # type: ignore[union-attr]

    @staticmethod
    def _frame(table: pa.Table, index: tuple[str, ...] = ()) -> pd.DataFrame:
        frame = table.to_pandas()
        if not frame.empty:
            order = [column for column in ("date", "symbol") if column in frame.columns]
            if order:
                frame = frame.sort_values(order)
            if index:
                frame = frame.set_index(list(index))
        return frame
