import pandas as pd

from fundlab.data.sources.xtquant_source import XtQuantSource


def test_xtquant_source_normalizes_daily_bar_dict_response():
    source = XtQuantSource()
    raw = {
        "510300.SH": pd.DataFrame(
            {
                "open": [4.0, 4.1],
                "high": [4.2, 4.3],
                "low": [3.9, 4.0],
                "close": [4.1, 4.2],
                "volume": [1000, 1100],
                "amount": [4100, 4620],
            },
            index=["20260102", "20260105"],
        )
    }

    data = source._normalize_daily_bar_response(raw)

    assert data["date"].tolist() == ["2026-01-02", "2026-01-05"]
    assert data["symbol"].tolist() == ["510300.SH", "510300.SH"]
    assert data.iloc[1]["pre_close"] == 4.1
    assert data.iloc[0]["source"] == "xtquant"


def test_xtquant_source_prefers_xtdata_index_date_over_time_column():
    source = XtQuantSource()
    raw = {
        "510300.SH": pd.DataFrame(
            {
                "time": [1777996800000],
                "open": [4.866],
                "high": [4.914],
                "low": [4.852],
                "close": [4.888],
                "volume": [18677828],
                "amount": [9131467000.0],
            },
            index=["20260506"],
        )
    }

    data = source._normalize_daily_bar_response(raw)

    assert data.iloc[0]["date"] == "2026-05-06"


def test_xtquant_source_filters_invalid_dates_before_string_cast():
    source = XtQuantSource()
    raw = {
        "510300.SH": pd.DataFrame(
            {
                "open": [4.0, 4.1],
                "high": [4.2, 4.3],
                "low": [3.9, 4.0],
                "close": [4.1, 4.2],
                "volume": [1000, 1100],
                "amount": [4100, 4620],
            },
            index=[None, "20260105"],
        )
    }

    data = source._normalize_daily_bar_response(raw)

    assert data["date"].tolist() == ["2026-01-05"]


def test_xtquant_source_classifies_common_etf_names():
    source = XtQuantSource()

    assert source._classify_fund("518880", "黄金ETF") == ("commodity", "gold", "passive_commodity")
    assert source._classify_fund("513100", "纳指ETF") == (
        "cross_border",
        "cross_border",
        "cross_border_index",
    )
    assert source._classify_fund("510880", "红利ETF") == ("equity", "smart_beta", "smart_beta")
