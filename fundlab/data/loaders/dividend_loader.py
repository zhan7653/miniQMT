from __future__ import annotations

import pandas as pd

from fundlab.common.ids import new_id
from fundlab.data.processors.symbol_normalizer import normalize_symbol
from fundlab.data.storage.sqlite_store import SQLiteStore


class DividendLoader:
    columns = [
        "dividend_id",
        "symbol",
        "announcement_date",
        "ex_dividend_date",
        "record_date",
        "payment_date",
        "dividend_per_share",
        "dividend_type",
        "tax_rate",
        "available_date",
        "source",
    ]

    def __init__(self, sqlite_store: SQLiteStore):
        self.sqlite_store = sqlite_store

    def load_frame(self, data: pd.DataFrame) -> int:
        normalized = data.copy()
        normalized["symbol"] = normalized["symbol"].map(normalize_symbol)
        if "dividend_id" not in normalized.columns:
            normalized["dividend_id"] = [new_id("div") for _ in range(len(normalized))]
        if "payment_date" not in normalized.columns:
            normalized["payment_date"] = normalized["ex_dividend_date"]
        if "available_date" not in normalized.columns:
            normalized["available_date"] = normalized.get("announcement_date", normalized["ex_dividend_date"])
        if "dividend_type" not in normalized.columns:
            normalized["dividend_type"] = "cash"
        if "tax_rate" not in normalized.columns:
            normalized["tax_rate"] = 0
        if "source" not in normalized.columns:
            normalized["source"] = "local"

        for column in self.columns:
            if column not in normalized.columns:
                normalized[column] = None

        rows = normalized.loc[:, self.columns].where(pd.notna(normalized.loc[:, self.columns]), None).values.tolist()
        placeholders = ",".join("?" for _ in self.columns)
        update_columns = ",".join(f"{column}=excluded.{column}" for column in self.columns if column != "dividend_id")
        self.sqlite_store.execute_many(
            f"""
            INSERT INTO fund_dividend ({','.join(self.columns)})
            VALUES ({placeholders})
            ON CONFLICT(symbol, ex_dividend_date, dividend_per_share, dividend_type)
            DO UPDATE SET {update_columns}, updated_at=CURRENT_TIMESTAMP
            """,
            rows,
        )
        return len(rows)

