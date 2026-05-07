from __future__ import annotations

from pathlib import Path

import pandas as pd

from fundlab.data.sources.base import MarketDataSource


class ManualSource(MarketDataSource):
    name = "manual"

    def __init__(self, raw_root: str | Path = "data/raw/manual"):
        self.raw_root = Path(raw_root)

    def get_instruments(self) -> list[dict]:
        path = self.raw_root / "universe_seed.csv"
        if not path.exists():
            raise FileNotFoundError(f"Manual universe seed not found: {path}")

        data = pd.read_csv(path, dtype={"raw_symbol": "string"})
        data = data.where(pd.notna(data), None)
        return data.to_dict(orient="records")

