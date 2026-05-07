import pandas as pd

from fundlab.data.portal import DataPortal
from fundlab.data.storage import ParquetStore, SQLiteStore
from scripts.create_fake_data import create_fake_data


def build_portal() -> DataPortal:
    create_fake_data()
    return DataPortal(
        sqlite_store=SQLiteStore("data/warehouse/sqlite/fundlab.db"),
        parquet_store=ParquetStore("data/warehouse/parquet"),
    )


def test_get_universe_from_fake_data():
    portal = build_portal()

    assert portal.get_universe("2026-01-15") == ["510300.SH", "510500.SH", "518880.SH"]
    assert portal.get_universe("2026-01-15", filters={"asset_class": "commodity"}) == ["518880.SH"]


def test_get_daily_bar_from_fake_data():
    portal = build_portal()
    bars = portal.get_daily_bar(
        ["510300.SH", "518880.SH"],
        "2026-01-02",
        "2026-01-09",
        fields=["open", "close", "amount"],
    )

    assert isinstance(bars.index, pd.MultiIndex)
    assert bars.index.names == ["date", "symbol"]
    assert set(bars.index.get_level_values("symbol")) == {"510300.SH", "518880.SH"}
    assert list(bars.columns) == ["open", "close", "amount"]
    assert len(bars) == 12


def test_daily_bar_does_not_fill_missing_symbols():
    portal = build_portal()
    bars = portal.get_daily_bar(["159915.SZ"], "2026-01-02", "2026-01-09")

    assert bars.empty

