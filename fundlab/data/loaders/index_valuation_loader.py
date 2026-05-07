from __future__ import annotations

import pandas as pd

from fundlab.data.storage.sqlite_store import SQLiteStore


class IndexValuationLoader:
    columns = [
        "date",
        "index_code",
        "index_name",
        "pe_ttm",
        "pb",
        "ps",
        "dividend_yield",
        "roe",
        "pe_percentile_3y",
        "pe_percentile_5y",
        "pb_percentile_3y",
        "pb_percentile_5y",
        "dividend_yield_percentile_3y",
        "dividend_yield_percentile_5y",
        "available_date",
        "source",
    ]

    def __init__(self, sqlite_store: SQLiteStore):
        self.sqlite_store = sqlite_store

    def load_frame(self, data: pd.DataFrame) -> int:
        normalized = data.copy()
        if "available_date" not in normalized.columns:
            normalized["available_date"] = normalized["date"]
        if "source" not in normalized.columns:
            normalized["source"] = "local"

        for column in self.columns:
            if column not in normalized.columns:
                normalized[column] = None

        rows = normalized.loc[:, self.columns].where(pd.notna(normalized.loc[:, self.columns]), None).values.tolist()
        placeholders = ",".join("?" for _ in self.columns)
        update_columns = ",".join(f"{column}=excluded.{column}" for column in self.columns if column not in {"date", "index_code"})
        self.sqlite_store.execute_many(
            f"""
            INSERT INTO index_valuation ({','.join(self.columns)})
            VALUES ({placeholders})
            ON CONFLICT(date, index_code) DO UPDATE SET {update_columns}, updated_at=CURRENT_TIMESTAMP
            """,
            rows,
        )
        return len(rows)

