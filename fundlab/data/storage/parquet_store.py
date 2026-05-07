from __future__ import annotations

from pathlib import Path
from typing import Sequence

import pandas as pd


class ParquetStore:
    def __init__(self, parquet_root: str | Path):
        self.parquet_root = Path(parquet_root)

    def read_daily_bar(
        self,
        symbols: Sequence[str],
        start_date: str,
        end_date: str,
        fields: Sequence[str] | None = None,
    ) -> pd.DataFrame:
        table_path = self.parquet_root / "fund_daily_bar"
        if not table_path.exists():
            raise FileNotFoundError(f"Daily bar parquet path not found: {table_path}")

        data = pd.read_parquet(table_path)
        selected = data[
            data["symbol"].isin(list(symbols))
            & (data["date"] >= start_date)
            & (data["date"] <= end_date)
        ].copy()

        base_fields = ["date", "symbol"]
        if fields:
            columns = base_fields + [field for field in fields if field not in base_fields]
            selected = selected.loc[:, [column for column in columns if column in selected.columns]]

        selected = selected.sort_values(["date", "symbol"])
        return selected.set_index(["date", "symbol"])

