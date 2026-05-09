import pytest

from fundlab.common.config import get_path, load_config
from fundlab.data.portal import DataPortal
from fundlab.data.storage import ParquetStore, SQLiteStore
from scripts.create_fake_data import create_fake_data


def build_portal() -> DataPortal:
    create_fake_data()
    config = load_config()
    return DataPortal(
        sqlite_store=SQLiteStore(get_path(config, "sqlite_db")),
        parquet_store=ParquetStore(get_path(config, "parquet_root")),
    )


def test_index_valuation_respects_available_date():
    portal = build_portal()

    valuation_on_signal_date = portal.get_index_valuation("000300.SH", "2026-01-09", asof="2026-01-09")
    valuation_after_available = portal.get_index_valuation("000300.SH", "2026-01-09", asof="2026-01-12")

    assert valuation_on_signal_date["date"] == "2026-01-08"
    assert valuation_on_signal_date["source"] == "fake"
    assert valuation_after_available["date"] == "2026-01-09"
    assert valuation_after_available["source"] == "fake_poison_late_available"


def test_nav_and_premium_discount_respect_available_date():
    portal = build_portal()

    nav_on_signal_date = portal.get_nav("510300.SH", "2026-01-09", asof="2026-01-09")
    nav_after_available = portal.get_nav("510300.SH", "2026-01-09", asof="2026-01-12")

    assert nav_on_signal_date["date"] == "2026-01-08"
    assert nav_after_available["date"] == "2026-01-09"
    assert portal.get_premium_discount("510300.SH", "2026-01-09", asof="2026-01-12") == pytest.approx(
        4.028 / 4.020 - 1
    )


def test_dividends_respect_available_date():
    portal = build_portal()

    known = portal.get_dividends("510300.SH", "2026-01-01", "2026-01-31", asof="2026-01-09")
    late = portal.get_dividends("510500.SH", "2026-01-01", "2026-01-31", asof="2026-01-20")
    after_available = portal.get_dividends("510500.SH", "2026-01-01", "2026-01-31", asof="2026-01-21")

    assert known.iloc[0]["dividend_per_share"] == pytest.approx(0.03)
    assert late.empty
    assert after_available.iloc[0]["dividend_per_share"] == pytest.approx(0.02)
