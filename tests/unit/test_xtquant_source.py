import pandas as pd
from datetime import date
import pytest

from fundlab.data.platform import ProviderCapability, ProviderHealth, ProviderRequest
from fundlab.data.sources import ProviderUnavailableError, UnsupportedCapabilityError
from fundlab.data.sources.xtquant_source import XtQuantSource


class FakeXtData:
    def __init__(self, responses=None, error=None):
        self.responses = responses or {}
        self.error = error
        self.calls = []

    def get_market_data_ex(self, **kwargs):
        self.calls.append(kwargs)
        if self.error:
            raise self.error
        symbol = kwargs["stock_list"][0]
        return self.responses.get(symbol, {})


def _request(capability, symbols=("510300.SH",)):
    return ProviderRequest(symbols, date(2026, 1, 2), date(2026, 1, 5), capability)


def test_xtquant_declares_v1_capabilities_and_rejects_unknown_before_sdk_access():
    source = XtQuantSource(xtdata=FakeXtData())
    source.capabilities = frozenset()
    with pytest.raises(UnsupportedCapabilityError):
        source.fetch(_request(ProviderCapability.DAILY_BARS_RAW))
    assert source.xtdata.calls == []


def test_preflight_probes_real_client_and_classifies_unavailable_service():
    source = XtQuantSource(xtdata=FakeXtData(error=RuntimeError("MiniQMT offline")))
    result = source.preflight()
    assert result.health is ProviderHealth.SERVICE_UNAVAILABLE
    assert "offline" in result.detail


def test_raw_and_front_adjusted_reads_use_exact_distinct_sdk_parameters():
    frame = pd.DataFrame({"open": [4], "high": [5], "low": [3], "close": [4.5], "volume": [1], "amount": [4.5]}, index=["20260102"])
    fake = FakeXtData({"510300.SH": {"510300.SH": frame}})
    source = XtQuantSource(xtdata=fake)
    raw, raw_result = source.fetch(_request(ProviderCapability.DAILY_BARS_RAW))
    adjusted, adjusted_result = source.fetch(_request(ProviderCapability.DAILY_BARS_ADJUSTED))
    assert fake.calls[0] == {"field_list": [], "stock_list": ["510300.SH"], "period": "1d", "start_time": "20260102", "end_time": "20260105", "count": -1, "dividend_type": "none", "fill_data": False}
    assert fake.calls[1]["dividend_type"] == "front"
    assert raw["price_mode"].unique().tolist() == ["raw"]
    assert adjusted["price_mode"].unique().tolist() == ["adjusted"]
    assert raw_result.row_count == adjusted_result.row_count == 1


def test_symbol_errors_are_isolated_and_adjusted_data_is_not_substituted():
    good = pd.DataFrame({"open": [4], "high": [5], "low": [3], "close": [4.5], "volume": [1], "amount": [4.5]}, index=["20260102"])
    class PartialFake(FakeXtData):
        def get_market_data_ex(self, **kwargs):
            self.calls.append(kwargs)
            symbol = kwargs["stock_list"][0]
            if symbol == "BAD.SH":
                raise ValueError("bad symbol")
            return {symbol: good} if kwargs["dividend_type"] == "front" else {}
    source = XtQuantSource(xtdata=PartialFake())
    data, result = source.fetch(_request(ProviderCapability.DAILY_BARS_ADJUSTED, ("GOOD.SH", "BAD.SH")))
    assert data["symbol"].tolist() == ["GOOD.SH"]
    assert result.symbols[1].error == "bad symbol"
    assert all(call["dividend_type"] == "front" for call in source.xtdata.calls)


def test_system_errors_block_the_batch_instead_of_becoming_symbol_errors():
    source = XtQuantSource(xtdata=FakeXtData(error=RuntimeError("MiniQMT disconnected")))
    with pytest.raises(ProviderUnavailableError, match="MiniQMT disconnected"):
        source.fetch(_request(ProviderCapability.DAILY_BARS_RAW, ("A.SH", "B.SH")))


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
