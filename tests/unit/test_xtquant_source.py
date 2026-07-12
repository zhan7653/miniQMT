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


class DiscoveryFakeXtData(FakeXtData):
    def __init__(self, sectors=None, details=None, sector_errors=None, detail_errors=None):
        super().__init__()
        self.sectors = sectors or {}
        self.details = details or {}
        self.sector_errors = sector_errors or {}
        self.detail_errors = detail_errors or {}
        self.sector_calls = []
        self.detail_calls = []
        self.download_calls = []

    def get_stock_list_in_sector(self, sector):
        self.sector_calls.append(sector)
        if sector in self.sector_errors:
            raise self.sector_errors[sector]
        return self.sectors.get(sector, [])

    def get_instrument_detail(self, symbol):
        self.detail_calls.append(symbol)
        if symbol in self.detail_errors:
            raise self.detail_errors[symbol]
        return self.details.get(symbol, {})

    def download_history_data(self, **kwargs):
        self.download_calls.append(kwargs)


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


def test_full_fund_discovery_normalizes_etf_lof_money_and_inactive_provenance():
    fake = DiscoveryFakeXtData(
        sectors={
            "沪深基金": ["510300.SH", "501001.SH", "511990.SH", "600000.SH"],
            "深证基金": ["161725.SZ", "180101.SZ"],
            "深证LOF": ["161725.SZ"],
        },
        details={
            "510300.SH": {"InstrumentName": "沪深300ETF", "OpenDate": 20120528},
            "501001.SH": {
                "InstrumentName": "财通精选混合LOF",
                "OpenDate": "20161230",
                "ExpireDate": "20250115",
            },
            "511990.SH": {"InstrumentName": "华宝添益货币ETF", "IsTrading": 1},
            "600000.SH": {"InstrumentName": "浦发银行"},
            "161725.SZ": {"InstrumentName": "招商中证白酒LOF", "InstrumentStatus": "上市"},
            "180101.SZ": {"InstrumentName": "成长上市型开放式基金"},
        },
    )
    source = XtQuantSource(xtdata=fake)

    rows = {row["symbol"]: row for row in source.get_instruments()}

    assert sorted(rows) == ["161725.SZ", "180101.SZ", "501001.SH", "510300.SH", "511990.SH"]
    assert rows["510300.SH"]["product_type"] == "ETF"
    assert rows["161725.SZ"]["product_type"] == "LOF"
    assert rows["180101.SZ"]["product_type"] == "LOF"
    assert rows["511990.SH"]["product_type"] == "MONEY_ETF"
    assert rows["501001.SH"]["listed_date"] == "2016-12-30"
    assert rows["501001.SH"]["delisted_date"] == "2025-01-15"
    assert rows["501001.SH"]["is_active"] == 0
    assert rows["501001.SH"]["listed_date_source"] == "xtquant.instrument_detail.OpenDate"
    assert rows["501001.SH"]["delisted_date_source"] == "xtquant.instrument_detail.ExpireDate"
    assert rows["501001.SH"]["active_state_source"] == "derived:delisted_date"
    assert rows["161725.SZ"]["discovery_source"] == "sector:深证LOF|sector:深证基金"
    assert rows["161725.SZ"]["instrument_detail_source"] == "xtquant.get_instrument_detail"


def test_numeric_open_ended_expire_date_is_not_a_delisting_date():
    source = XtQuantSource()
    row = source._instrument_to_master_row(
        "510300.SH",
        {"InstrumentName": "沪深300ETF", "ExpireDate": 99999999},
    )

    assert row["delisted_date"] is None
    assert row["delisted_date_source"] == (
        "xtquant.instrument_detail.ExpireDate:open_ended"
    )
    assert row["is_active"] == 1
    assert row["active_state_source"] == "derived:no_known_delisting_date"


def test_formatted_open_ended_expire_date_is_not_a_delisting_date():
    source = XtQuantSource()
    row = source._instrument_to_master_row(
        "510300.SH",
        {"InstrumentName": "沪深300ETF", "ExpireDate": "9999-99-99"},
    )

    assert row["delisted_date"] is None
    assert row["delisted_date_source"] == (
        "xtquant.instrument_detail.ExpireDate:open_ended"
    )
    assert row["is_active"] == 1
    assert row["active_state_source"] == "derived:no_known_delisting_date"


def test_real_expire_date_remains_a_delisting_date_and_makes_instrument_inactive():
    source = XtQuantSource()
    row = source._instrument_to_master_row(
        "501001.SH",
        {"InstrumentName": "财通精选混合LOF", "ExpireDate": "20250115"},
    )

    assert row["delisted_date"] == "2025-01-15"
    assert row["delisted_date_source"] == "xtquant.instrument_detail.ExpireDate"
    assert row["is_active"] == 0
    assert row["active_state_source"] == "derived:delisted_date"


def test_current_tradability_fields_do_not_make_a_listed_instrument_inactive():
    source = XtQuantSource()
    row = source._instrument_to_master_row(
        "510300.SH",
        {
            "InstrumentName": "沪深300ETF",
            "ExpireDate": "99999999.0",
            "IsTrading": False,
            "InstrumentStatus": 0,
        },
    )

    assert row["delisted_date"] is None
    assert row["is_active"] == 1
    assert row["active_state_source"] == "derived:no_known_delisting_date"


@pytest.mark.parametrize("listing_status", ["DELISTED", "EXPIRED"])
def test_explicit_textual_listing_status_makes_instrument_inactive(listing_status):
    source = XtQuantSource()
    row = source._instrument_to_master_row(
        "510300.SH",
        {
            "InstrumentName": "沪深300ETF",
            "ExpireDate": "99999999",
            "ListingStatus": listing_status,
            "IsTrading": False,
            "InstrumentStatus": 0,
        },
    )

    assert row["delisted_date"] is None
    assert row["is_active"] == 0
    assert row["active_state_source"] == "xtquant.instrument_detail.ListingStatus"


def test_code_discovery_survives_symbol_detail_failure_but_system_failure_stops():
    symbol_fake = DiscoveryFakeXtData(
        sectors={"沪深基金": ["159001.SZ"]},
        detail_errors={"159001.SZ": ValueError("unknown symbol detail")},
    )
    rows = XtQuantSource(xtdata=symbol_fake).get_instruments()
    assert rows[0]["symbol"] == "159001.SZ"
    assert rows[0]["product_type"] == "MONEY_ETF"
    assert rows[0]["instrument_detail_source"] is None

    system_fake = DiscoveryFakeXtData(
        sectors={"沪深基金": ["510300.SH"]},
        detail_errors={"510300.SH": RuntimeError("MiniQMT offline")},
    )
    with pytest.raises(ProviderUnavailableError, match="MiniQMT offline"):
        XtQuantSource(xtdata=system_fake).get_instruments()


def test_sector_system_failure_is_not_silently_reported_as_empty_discovery():
    fake = DiscoveryFakeXtData(sector_errors={"沪深基金": TimeoutError("timed out")})
    source = XtQuantSource({"fund_sectors": ["沪深基金"]}, xtdata=fake)
    with pytest.raises(ProviderUnavailableError, match="timed out"):
        source.get_instruments()


def test_download_uses_exact_unadjusted_cache_parameters_without_read_fallback():
    fake = DiscoveryFakeXtData()
    source = XtQuantSource(xtdata=fake)
    source.download_daily_bar(["510300.SH", "161725.SZ"], "2026-01-02", "2026-01-05")
    assert fake.download_calls == [
        {
            "stock_code": "510300.SH",
            "period": "1d",
            "start_time": "20260102",
            "end_time": "20260105",
        },
        {
            "stock_code": "161725.SZ",
            "period": "1d",
            "start_time": "20260102",
            "end_time": "20260105",
        },
    ]
    assert fake.calls == []


def test_suspension_fields_are_normalized_without_changing_price_mode():
    frame = pd.DataFrame(
        {
            "open": [1.0, 1.0],
            "high": [1.0, 1.0],
            "low": [1.0, 1.0],
            "close": [1.0, 1.0],
            "vol": [0, 100],
            "amount": [0, 100.0],
            "preClose": [1.0, 1.0],
            "suspendFlag": ["1", "交易"],
        },
        index=["20260102", "20260105"],
    )
    source = XtQuantSource()
    data = source._normalize_daily_bar_response({"511990.SH": frame}, price_mode="front")
    assert data["suspended"].tolist() == [True, False]
    assert data["volume"].tolist() == [0, 100]
    assert data["amount"].tolist() == [0.0, 100.0]
    assert data["price_mode"].unique().tolist() == ["adjusted"]
