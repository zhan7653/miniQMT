from __future__ import annotations

from datetime import datetime

from fundlab.data.processors.symbol_normalizer import normalize_symbol
from fundlab.data.sources.base import MarketDataSource
from fundlab.data.storage.sqlite_store import SQLiteStore


class FundUniverseLoader:
    columns = [
        "symbol",
        "raw_symbol",
        "name",
        "exchange",
        "product_type",
        "management_type",
        "asset_class",
        "category",
        "tracking_index",
        "tracking_index_code",
        "fund_company",
        "listed_date",
        "delisted_date",
        "expense_ratio",
        "custody_fee",
        "lot_size",
        "price_tick",
        "is_active",
        "include_in_universe",
        "exclusion_reason",
        "source",
        "source_updated_at",
    ]

    def __init__(self, source: MarketDataSource, sqlite_store: SQLiteStore):
        self.source = source
        self.sqlite_store = sqlite_store

    def load(self) -> int:
        instruments = self.source.get_instruments()
        now = datetime.now().isoformat(timespec="seconds")
        rows = []
        for instrument in instruments:
            item = dict(instrument)
            item["symbol"] = normalize_symbol(item["symbol"])
            item.setdefault("source", self.source.name)
            item.setdefault("source_updated_at", now)
            rows.append([item.get(column) for column in self.columns])

        placeholders = ",".join("?" for _ in self.columns)
        update_columns = ",".join(f"{column}=excluded.{column}" for column in self.columns if column != "symbol")
        sql = f"""
            INSERT INTO fund_master ({','.join(self.columns)})
            VALUES ({placeholders})
            ON CONFLICT(symbol) DO UPDATE SET {update_columns}, updated_at=CURRENT_TIMESTAMP
        """
        self.sqlite_store.execute_many(sql, rows)
        return len(rows)

