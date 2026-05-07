from __future__ import annotations

import pandas as pd

from fundlab.data.processors.symbol_normalizer import normalize_symbol
from fundlab.data.storage.sqlite_store import SQLiteStore


class NavLoader:
    columns = [
        "date",
        "symbol",
        "nav",
        "iopv",
        "close",
        "premium_discount",
        "estimate_nav",
        "available_date",
        "source",
    ]

    def __init__(self, sqlite_store: SQLiteStore):
        self.sqlite_store = sqlite_store

    def load_frame(self, data: pd.DataFrame) -> int:
        normalized = data.copy()
        normalized["symbol"] = normalized["symbol"].map(normalize_symbol)
        if "premium_discount" not in normalized.columns or normalized["premium_discount"].isna().any():
            normalized["premium_discount"] = normalized["close"] / normalized["nav"] - 1
        if "available_date" not in normalized.columns:
            normalized["available_date"] = normalized["date"]
        if "source" not in normalized.columns:
            normalized["source"] = "local"

        for column in self.columns:
            if column not in normalized.columns:
                normalized[column] = None

        rows = normalized.loc[:, self.columns].where(pd.notna(normalized.loc[:, self.columns]), None).values.tolist()
        placeholders = ",".join("?" for _ in self.columns)
        update_columns = ",".join(f"{column}=excluded.{column}" for column in self.columns if column not in {"date", "symbol"})
        self.sqlite_store.execute_many(
            f"""
            INSERT INTO fund_nav ({','.join(self.columns)})
            VALUES ({placeholders})
            ON CONFLICT(date, symbol) DO UPDATE SET {update_columns}, updated_at=CURRENT_TIMESTAMP
            """,
            rows,
        )
        return len(rows)

