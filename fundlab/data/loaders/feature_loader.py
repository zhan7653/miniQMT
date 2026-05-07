from __future__ import annotations

import pandas as pd

from fundlab.data.processors.symbol_normalizer import normalize_symbol
from fundlab.data.storage.sqlite_store import SQLiteStore


class FeatureLoader:
    columns = [
        "date",
        "symbol",
        "feature_version",
        "ret_1d",
        "ret_5d",
        "ret_20d",
        "ret_60d",
        "ret_120d",
        "volatility_20d",
        "volatility_60d",
        "max_drawdown_60d",
        "amount_avg_20d",
        "amount_avg_60d",
        "amount_percentile_60d",
        "turnover_score",
        "momentum_score",
        "valuation_score",
        "dividend_score",
        "liquidity_score",
        "premium_discount_score",
        "risk_penalty_score",
        "total_score",
        "premium_discount",
        "dividend_yield_12m",
        "tracking_index_code",
        "available_date",
        "source_data_version",
    ]

    def __init__(self, sqlite_store: SQLiteStore):
        self.sqlite_store = sqlite_store

    def load_frame(self, data: pd.DataFrame) -> int:
        if data.empty:
            return 0

        normalized = data.copy()
        normalized["symbol"] = normalized["symbol"].map(normalize_symbol)
        if "feature_version" not in normalized.columns:
            normalized["feature_version"] = "v1"
        if "available_date" not in normalized.columns:
            normalized["available_date"] = normalized["date"]

        for column in self.columns:
            if column not in normalized.columns:
                normalized[column] = None

        rows = normalized.loc[:, self.columns].where(pd.notna(normalized.loc[:, self.columns]), None).values.tolist()
        placeholders = ",".join("?" for _ in self.columns)
        update_columns = ",".join(
            f"{column}=excluded.{column}" for column in self.columns if column not in {"date", "symbol", "feature_version"}
        )
        self.sqlite_store.execute_many(
            f"""
            INSERT INTO fund_features_daily ({','.join(self.columns)})
            VALUES ({placeholders})
            ON CONFLICT(date, symbol, feature_version)
            DO UPDATE SET {update_columns}, updated_at=CURRENT_TIMESTAMP
            """,
            rows,
        )
        return len(rows)

