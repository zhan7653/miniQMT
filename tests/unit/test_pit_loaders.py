import pandas as pd
import pytest

from fundlab.common.config import get_path, load_config
from fundlab.data.loaders import DividendLoader, IndexValuationLoader, NavLoader
from fundlab.data.storage import SQLiteStore
from scripts.init_db import init_db


def test_pit_loaders_insert_rows():
    init_db()
    config = load_config()
    sqlite_store = SQLiteStore(get_path(config, "sqlite_db"))

    nav_count = NavLoader(sqlite_store).load_frame(
        pd.DataFrame([{"date": "2026-01-02", "symbol": "510300.SH", "nav": 4.0, "close": 4.04}])
    )
    dividend_count = DividendLoader(sqlite_store).load_frame(
        pd.DataFrame(
            [
                {
                    "symbol": "510300.SH",
                    "ex_dividend_date": "2026-01-15",
                    "dividend_per_share": 0.01,
                }
            ]
        )
    )
    valuation_count = IndexValuationLoader(sqlite_store).load_frame(
        pd.DataFrame([{"date": "2026-01-02", "index_code": "000300.SH", "available_date": "2026-01-02"}])
    )

    assert nav_count == 1
    assert dividend_count == 1
    assert valuation_count == 1
    premium = sqlite_store.read_frame(
        "SELECT premium_discount FROM fund_nav WHERE date = ? AND symbol = ?",
        ["2026-01-02", "510300.SH"],
    ).iloc[0]["premium_discount"]
    assert premium == pytest.approx(0.01)
