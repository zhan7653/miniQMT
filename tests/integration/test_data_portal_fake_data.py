import pandas as pd

from fundlab.data.platform import PriceMode
from scripts.create_fake_data import create_fake_v2_portal


def test_get_universe_from_fake_data(tmp_path):
    portal = create_fake_v2_portal(tmp_path / "v2")

    assert portal.get_universe("2026-01-15") == ["510300.SH", "510500.SH", "518880.SH"]


def test_get_daily_bar_from_fake_data(tmp_path):
    portal = create_fake_v2_portal(tmp_path / "v2")
    bars = portal.get_daily_bar(
        ["510300.SH", "518880.SH"],
        "2026-01-02",
        "2026-01-09",
        fields=["open", "close", "amount"],
        price_mode=PriceMode.RAW,
    )

    assert isinstance(bars.index, pd.MultiIndex)
    assert bars.index.names == ["date", "symbol"]
    assert set(bars.index.get_level_values("symbol")) == {"510300.SH", "518880.SH"}
    assert list(bars.columns) == ["open", "close", "amount"]
    assert len(bars) == 12


def test_daily_bar_does_not_fill_missing_symbols(tmp_path):
    portal = create_fake_v2_portal(tmp_path / "v2")
    bars = portal.get_daily_bar(["159915.SZ"], "2026-01-02", "2026-01-09", price_mode=PriceMode.RAW)

    assert bars.empty
