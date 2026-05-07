from __future__ import annotations

from pathlib import Path

import pandas as pd


class DailyBarLoader:
    expected_columns = [
        "date",
        "symbol",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
        "pre_close",
        "adj_factor",
        "suspended",
        "limit_up",
        "limit_down",
        "source",
        "updated_at",
    ]

    def __init__(self, parquet_root: str | Path):
        self.parquet_root = Path(parquet_root)

    def load_frame(self, data: pd.DataFrame, year: int | None = None) -> Path:
        normalized = data.copy()
        normalized["date"] = normalized["date"].astype(str)
        if year is None:
            year = int(normalized["date"].str.slice(0, 4).mode().iloc[0])

        for column in self.expected_columns:
            if column not in normalized.columns:
                normalized[column] = None

        normalized = normalized.loc[:, self.expected_columns]
        output_dir = self.parquet_root / "fund_daily_bar" / f"year={year}"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / "part-000.parquet"
        normalized.sort_values(["date", "symbol"]).to_parquet(output_path, index=False)
        return output_path

