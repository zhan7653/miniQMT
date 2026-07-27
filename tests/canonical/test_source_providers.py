from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from fundlab.marketdata import (
    BaoStockProvider,
    CninfoCorporateActionProvider,
    EfinanceProvider,
    ExchangePublicUniverseProvider,
    MarketTable,
    ProviderCapability,
    ProviderRequest,
    SinaEtfProvider,
    SinaCalendarProvider,
    TickFlowProvider,
    XtQuantProvider,
)
from fundlab.marketdata.sources.cninfo import CninfoPublicClient, _cninfo_token


START = date(2026, 7, 13)
END = date(2026, 7, 14)


class _ExchangeClient:
    __version__ = "test"

    def stock_info_sh_name_code(self, *, symbol):
        if symbol == "主板A股":
            return pd.DataFrame({
                "证券代码": ["600000"], "证券简称": ["浦发银行"],
                "上市日期": ["1999-11-10"],
            })
        assert symbol == "科创板"
        return pd.DataFrame({
            "证券代码": ["688001"], "证券简称": ["华兴源创"],
            "上市日期": ["2019-07-22"],
        })

    def stock_info_sz_name_code(self, *, symbol):
        assert symbol == "A股列表"
        return pd.DataFrame({
            "A股代码": ["000001", "300001"],
            "A股简称": ["平安银行", "特锐德"],
            "A股上市日期": ["1991-04-03", "2009-10-30"],
            "板块": ["主板", "创业板"],
        })

    def fund_etf_scale_sse(self, *, date):
        assert date == "20260714"
        return pd.DataFrame({"基金代码": ["510300"], "基金简称": ["沪深300ETF"]})

    def fund_etf_list_sse(self):
        return pd.DataFrame([
            {
                "fundCode": "510300", "secNameFull": "沪深300ETF",
                "fundAbbr": "300ETF", "listingDate": "2012-05-28", "subClass": "03",
            },
            {
                "fundCode": "511990", "secNameFull": "华宝添益ETF",
                "fundAbbr": "华宝添益", "listingDate": "2013-01-28", "subClass": "05",
            },
        ])

    def fund_scale_daily_szse(self, *, start_date, end_date, symbol):
        assert (start_date, end_date, symbol) == ("20260714", "20260714", "ETF")
        return pd.DataFrame({"基金代码": ["159919"], "基金简称": ["沪深300ETF"]})

    def fund_etf_scale_szse(self):
        return pd.DataFrame({
            "基金代码": ["159919"], "基金简称": ["沪深300ETF"],
            "上市日期": ["2012-05-28"],
            "基金类别": ["ETF"], "投资类别": ["跨市场"],
        })


def test_exchange_provider_pins_exact_current_sh_sz_stock_etf_membership():
    provider = ExchangePublicUniverseProvider(client=_ExchangeClient())
    observed = provider.observe(ProviderRequest(
        ProviderCapability.INSTRUMENTS,
        parameters={"as_of_date": "2026-07-14"},
    ))

    instruments = observed.tables[MarketTable.INSTRUMENTS].set_index("instrument_id")
    assert set(instruments.index) == {
        "000001.SZ", "159919.SZ", "300001.SZ", "510300.SH", "511990.SH",
        "600000.SH", "688001.SH",
    }
    assert instruments.loc["300001.SZ", "board"] == "chinext"
    assert instruments.loc["688001.SH", "board"] == "star"
    assert instruments.loc["159919.SZ", "listed_date"] == "2012-05-28"
    assert instruments.loc["510300.SH", "listed_date"] == "2012-05-28"
    assert instruments.loc["511990.SH", "exchange_product_class"] == "sse-fund-subclass-05"
    assert instruments.loc["159919.SZ", "exchange_product_class"] == "szse-ETF|跨市场"
    assert instruments.loc["510300.SH", "price_tick"] == 0.001
    assert instruments["sell_delay_sessions"].isna().all()
    assert observed.coverage[0].complete
    assert observed.coverage[0].instrument_ids == tuple(sorted(instruments.index))
    assert observed.source_metadata["upstream"] == (
        "Shanghai Stock Exchange / Shenzhen Stock Exchange"
    )
    assert observed.source_metadata["endpoint_counts"] == {
        "sse-main-stock-list": 1,
        "sse-star-stock-list": 1,
        "szse-a-stock-list": 2,
        "sse-etf-scale-list": 1,
        "sse-current-full-etf-list": 2,
        "szse-etf-scale-daily": 1,
        "szse-current-etf-list": 1,
    }


def test_exchange_provider_rejects_membership_without_required_szse_classification():
    class Client(_ExchangeClient):
        def fund_etf_scale_szse(self):
            raise TypeError("incompatible spreadsheet decoder")

    with pytest.raises(Exception, match="szse-current-etf-list"):
        ExchangePublicUniverseProvider(client=Client()).observe(ProviderRequest(
            ProviderCapability.INSTRUMENTS,
            parameters={"as_of_date": "2026-07-14", "asset_types": ("etf",)},
        ))


class _SinaClient:
    __version__ = "test"

    def __init__(self):
        self.calls = []

    def fund_etf_hist_sina(self, *, symbol):
        self.calls.append(symbol)
        return pd.DataFrame({
            "date": ["2026-07-10", "2026-07-13", "2026-07-14"],
            "open": [9.9, 10.0, 10.5], "high": [10.0, 10.8, 11.0],
            "low": [9.8, 9.9, 10.4], "close": [9.95, 10.5, 10.8],
            "volume": [900, 1000, 1200], "amount": [8955, 10500, 12960],
        })


def test_sina_etf_adapter_keeps_sina_identity_share_units_and_prior_close():
    client = _SinaClient()
    observed = SinaEtfProvider(client=client).observe(ProviderRequest(
        ProviderCapability.DAILY_BARS_RAW,
        START,
        END,
        ("551000.SH",),
        {"max_workers": 1, "retries": 1},
    ))

    bars = observed.tables[MarketTable.DAILY_BARS]
    assert client.calls == ["sh551000"]
    assert bars["session_date"].tolist() == ["2026-07-13", "2026-07-14"]
    assert bars["previous_close"].tolist() == [9.95, 10.5]
    assert bars["volume"].tolist() == [1000, 1200]
    assert observed.source_metadata["upstream"] == "Sina Finance"
    assert observed.source_metadata["backend_group"] == "sina"
    assert observed.coverage[0].complete


def test_sina_etf_isolates_one_failed_symbol_and_reports_incomplete_coverage():
    class PartialClient(_SinaClient):
        def fund_etf_hist_sina(self, *, symbol):
            if symbol == "sh551000":
                raise RuntimeError("temporary")
            return super().fund_etf_hist_sina(symbol=symbol)

    observed = SinaEtfProvider(client=PartialClient()).observe(ProviderRequest(
        ProviderCapability.DAILY_BARS_RAW,
        START,
        END,
        ("159919.SZ", "551000.SH"),
        {"max_workers": 2, "retries": 2, "retry_backoff_seconds": 0},
    ))

    assert set(observed.tables[MarketTable.DAILY_BARS]["instrument_id"]) == {"159919.SZ"}
    assert not observed.coverage[0].complete
    assert "551000.SH" in observed.source_metadata["request_errors"]


def test_sina_calendar_expands_open_dates_to_complete_civil_day_states():
    class Client:
        __version__ = "test"

        def tool_trade_date_hist_sina(self):
            return pd.DataFrame({
                "trade_date": ["2026-07-10", "2026-07-13", "2026-07-14", "2026-07-15"],
            })

    observed = SinaCalendarProvider(client=Client()).observe(ProviderRequest(
        ProviderCapability.TRADING_CALENDAR,
        date(2026, 7, 12),
        date(2026, 7, 14),
        parameters={"exchanges": ("SH", "SZ")},
    ))

    calendar = observed.tables[MarketTable.CALENDAR]
    assert len(calendar) == 6
    assert not calendar.loc[calendar["session_date"].eq("2026-07-12"), "is_open"].any()
    assert calendar.loc[calendar["session_date"].eq("2026-07-13"), "is_open"].all()
    assert observed.coverage[0].complete
    assert observed.source_metadata["upstream"] == "Sina Finance"


class _TickTransport:
    def __init__(self) -> None:
        self.calls = []

    def get_json(self, url, *, parameters, headers, timeout):
        self.calls.append((url, parameters, headers, timeout))
        return {"data": {
            "timestamp": [1783900800000, 1783987200000],
            "open": [10.0, 10.5],
            "high": [10.8, 11.0],
            "low": [9.9, 10.4],
            "close": [10.5, 10.8],
            "volume": [1000, 1200],
            "amount": [10500.0, 12960.0],
        }}


def test_tickflow_direct_provider_keeps_real_upstream_and_raw_response_hash():
    transport = _TickTransport()
    provider = TickFlowProvider(transport=transport, base_url="https://tickflow.test")
    request = ProviderRequest(
        ProviderCapability.DAILY_BARS_RAW, START, END, ("600000.SH",),
    )

    observed = provider.observe(request)

    bars = observed.tables[MarketTable.DAILY_BARS]
    assert observed.provider == "tickflow"
    assert observed.source_metadata["upstream"] == "TickFlow"
    assert observed.source_metadata["response_sha256"]["600000.SH"]
    assert transport.calls[0][1]["adjust"] == "none"
    assert bars["previous_close"].iloc[1] == 10.5
    assert bars["volume"].tolist() == [100000, 120000]


def test_tickflow_uses_one_batch_request_for_multiple_instruments():
    class BatchTransport(_TickTransport):
        def get_json(self, url, *, parameters, headers, timeout):
            self.calls.append((url, parameters, headers, timeout))
            compact = {
                "timestamp": [1783900800000],
                "open": [10.0], "high": [10.8], "low": [9.9], "close": [10.5],
                "volume": [1000], "amount": [1050000.0], "prev_close": [9.8],
            }
            return {"data": {symbol: compact for symbol in parameters["symbols"].split(",")}}

    transport = BatchTransport()
    provider = TickFlowProvider(transport=transport, base_url="https://tickflow.test")

    observed = provider.observe(ProviderRequest(
        ProviderCapability.DAILY_BARS_RAW,
        START,
        END,
        ("000001.SZ", "600000.SH"),
    ))

    assert len(transport.calls) == 1
    assert transport.calls[0][0].endswith("/v1/klines/batch")
    assert len(observed.tables[MarketTable.DAILY_BARS]) == 2


def test_tickflow_daily_timestamp_is_interpreted_as_china_session_date():
    class ChinaMidnightTransport(_TickTransport):
        def get_json(self, url, *, parameters, headers, timeout):
            self.calls.append((url, parameters, headers, timeout))
            return {"data": {
                # 2010-01-04 00:00 Asia/Shanghai == 2010-01-03 16:00 UTC.
                "timestamp": [1262534400000],
                "open": [10], "high": [11], "low": [9], "close": [10.5],
                "volume": [100], "amount": [105000],
            }}

    transport = ChinaMidnightTransport()
    observed = TickFlowProvider(
        transport=transport, base_url="https://tickflow.test",
    ).observe(ProviderRequest(
        ProviderCapability.DAILY_BARS_RAW,
        date(2010, 1, 4),
        date(2010, 1, 4),
        ("600000.SH",),
    ))

    bars = observed.tables[MarketTable.DAILY_BARS]
    assert bars["session_date"].tolist() == ["2010-01-04"]
    assert transport.calls[0][1]["start_time"] == 1262534400000
    assert transport.calls[0][1]["end_time"] == 1262620799999


class _EfinanceStock:
    def get_quote_history(self, **kwargs):
        assert kwargs["fqt"] == 0
        return pd.DataFrame({
            "股票代码": ["600000", "600000"],
            "日期": ["2026-07-13", "2026-07-14"],
            "开盘": [10.0, 10.5],
            "收盘": [10.5, 10.8],
            "最高": [10.8, 11.0],
            "最低": [9.9, 10.4],
            "成交量": [10.0, 12.0],
            "成交额": [10500.0, 12960.0],
        })


class _EfinanceClient:
    __version__ = "test"
    stock = _EfinanceStock()


def test_efinance_adapter_identifies_eastmoney_backend_and_normalizes_lots_to_shares():
    provider = EfinanceProvider(client=_EfinanceClient())
    observed = provider.observe(ProviderRequest(
        ProviderCapability.DAILY_BARS_RAW, START, END, ("600000.SH",),
    ))

    bars = observed.tables[MarketTable.DAILY_BARS]
    assert observed.provider == "eastmoney-efinance"
    assert observed.source_metadata["backend_group"] == "eastmoney"
    assert bars["volume"].tolist() == [1000.0, 1200.0]
    assert bars["suspended"].isna().all()


def test_efinance_fetches_batch_as_bounded_retryable_single_symbol_calls():
    class RetryStock:
        def __init__(self):
            self.calls = {}

        def get_quote_history(self, **kwargs):
            code = str(kwargs["stock_codes"])
            assert not isinstance(kwargs["stock_codes"], list)
            self.calls[code] = self.calls.get(code, 0) + 1
            if code == "000001" and self.calls[code] == 1:
                raise RuntimeError("transient")
            return pd.DataFrame({
                "股票代码": [code],
                "日期": ["2026-07-13"],
                "开盘": [10], "收盘": [10.5], "最高": [10.8], "最低": [9.9],
                "成交量": [10], "成交额": [10500],
            })

    stock = RetryStock()

    class Client:
        __version__ = "test"

    client = Client()
    client.stock = stock
    observed = EfinanceProvider(client=client).observe(ProviderRequest(
        ProviderCapability.DAILY_BARS_RAW,
        START,
        END,
        ("000001.SZ", "600000.SH"),
        {"max_workers": 2, "retries": 2, "retry_backoff_seconds": 0},
    ))

    bars = observed.tables[MarketTable.DAILY_BARS]
    assert set(bars["instrument_id"]) == {"000001.SZ", "600000.SH"}
    assert observed.coverage[0].complete
    assert stock.calls == {"000001": 2, "600000": 1}


def test_efinance_direct_limit_snapshot_preserves_upstream_date_and_values():
    class SnapshotTransport:
        def get_json(self, url, *, parameters, headers, timeout):
            assert parameters == {"id": "600000"}
            return {
                "code": "600000",
                "topprice": "11.00",
                "bottomprice": "9.00",
                "fivequote": {"yesClosePrice": "10.00"},
                "realtimequote": {"date": "20260714", "time": "15:00:00"},
            }

    observed = EfinanceProvider(
        client=_EfinanceClient(), snapshot_transport=SnapshotTransport(),
    ).observe(ProviderRequest(
        ProviderCapability.DAILY_STATUS,
        END,
        END,
        ("600000.SH",),
        {
            "instrument_limit_snapshot": True,
            "max_workers": 1,
            "retries": 1,
            "retry_backoff_seconds": 0,
        },
    ))

    row = observed.tables[MarketTable.DAILY_BARS].iloc[0]
    assert observed.coverage[0].complete
    assert row["session_date"] == END.isoformat()
    assert (row["previous_close"], row["limit_up"], row["limit_down"]) == (10, 11, 9)


class _Result:
    def __init__(self, fields, rows):
        self.error_code = "0"
        self.error_msg = "success"
        self.fields = fields
        self.rows = rows
        self.index = -1

    def next(self):
        self.index += 1
        return self.index < len(self.rows)

    def get_row_data(self):
        return self.rows[self.index]


class _BaoStockClient:
    __version__ = "test"

    def __init__(self):
        self.logged_out = False

    def login(self):
        return _Result((), ())

    def logout(self):
        self.logged_out = True

    def query_history_k_data_plus(self, code, fields, **kwargs):
        assert code == "sh.600000" and kwargs["adjustflag"] == "3"
        names = fields.split(",")
        return _Result(names, [[
            "2026-07-13", code, "10", "10.8", "9.9", "10.5", "9.8",
            "1000", "10500", "1", "0",
        ]])


def test_baostock_adapter_preserves_tradability_and_share_units():
    client = _BaoStockClient()
    provider = BaoStockProvider(client=client)
    observed = provider.observe(ProviderRequest(
        ProviderCapability.DAILY_BARS_RAW, START, END, ("600000.SH",),
    ))

    bars = observed.tables[MarketTable.DAILY_BARS]
    assert bars.iloc[0]["volume"] == 1000
    assert bars.iloc[0]["previous_close"] == 9.8
    assert bars.iloc[0]["suspended"] == False  # noqa: E712
    assert bars.iloc[0]["price_limit_state"] == "unknown"
    assert client.logged_out


def test_baostock_normalizes_blank_suspended_turnover_to_zero():
    class SuspendedClient(_BaoStockClient):
        def query_history_k_data_plus(self, code, fields, **kwargs):
            return _Result(fields.split(","), [[
                "2026-07-13", code, "", "", "", "", "9.8", "", "", "0", "0",
            ]])

    observed = BaoStockProvider(client=SuspendedClient()).observe(ProviderRequest(
        ProviderCapability.DAILY_BARS_RAW, START, END, ("600000.SH",),
    ))

    bars = observed.tables[MarketTable.DAILY_BARS]
    assert bars.iloc[0]["suspended"] == True  # noqa: E712
    assert bars.iloc[0]["volume"] == 0
    assert pd.isna(bars.iloc[0]["open"])


def test_baostock_status_capability_fetches_only_state_fields():
    class StatusClient(_BaoStockClient):
        def query_history_k_data_plus(self, code, fields, **kwargs):
            assert fields == "date,code,preclose,tradestatus,isST"
            return _Result(fields.split(","), [
                ["2026-07-13", code, "9.8", "1", "0"],
                ["2026-07-14", code, "10.5", "0", "1"],
            ])

    observed = BaoStockProvider(client=StatusClient()).observe(ProviderRequest(
        ProviderCapability.DAILY_STATUS,
        START,
        END,
        ("600000.SH",),
    ))

    status = observed.tables[MarketTable.DAILY_BARS]
    assert status["suspended"].tolist() == [False, True]
    assert status["is_st"].tolist() == [False, True]
    assert status["previous_close"].tolist() == [9.8, 10.5]
    assert status[["open", "high", "low", "close"]].isna().all().all()
    assert observed.coverage[0].complete


def test_baostock_isolates_invalid_active_instrument_without_losing_batch():
    class PartiallyInvalidClient(_BaoStockClient):
        def query_history_k_data_plus(self, code, fields, **kwargs):
            if code == "sh.600000":
                return super().query_history_k_data_plus(code, fields, **kwargs)
            return _Result(fields.split(","), [[
                "2026-07-13", code, "", "", "", "", "", "", "", "1", "0",
            ]])

    observed = BaoStockProvider(client=PartiallyInvalidClient()).observe(ProviderRequest(
        ProviderCapability.DAILY_BARS_RAW,
        START,
        END,
        ("000001.SZ", "600000.SH"),
    ))

    bars = observed.tables[MarketTable.DAILY_BARS]
    assert set(bars["instrument_id"]) == {"600000.SH"}
    assert not observed.coverage[0].complete
    assert "000001.SZ(active_critical_value_missing)" in observed.coverage[0].detail


def test_baostock_discovers_lifetime_stock_and_etf_master_without_point_in_time_universe():
    class UniverseClient(_BaoStockClient):
        def query_stock_basic(self, code="", code_name=""):
            if code == "sh.510050":
                return _Result(
                    ["code", "code_name", "ipoDate", "outDate", "type", "status"],
                    [["sh.510050", "上证50ETF", "2005-02-23", "", "5", "1"]],
                )
            assert code == "" and code_name == ""
            return _Result(
                ["code", "code_name", "ipoDate", "outDate", "type", "status"],
                [
                    ["sh.600000", "浦发银行", "1999-11-10", "", "1", "1"],
                    ["sz.000003", "退市样本", "1991-01-14", "2010-01-04", "1", "0"],
                    ["sh.510050", "上证50ETF", "2005-02-23", "", "5", "1"],
                    ["sh.000001", "上证指数", "1991-07-15", "", "2", "1"],
                ],
            )

    provider = BaoStockProvider(client=UniverseClient())
    observed = provider.observe(ProviderRequest(
        ProviderCapability.INSTRUMENTS,
        parameters={
            "exchanges": ("SH", "SZ"),
            "asset_types": ("stock", "etf"),
            "include_delisted": True,
        },
    ))

    instruments = observed.tables[MarketTable.INSTRUMENTS]
    assert set(instruments["instrument_id"]) == {"000003.SZ", "510050.SH", "600000.SH"}
    assert instruments.set_index("instrument_id").loc["510050.SH", "price_tick"] == 0.001
    assert observed.coverage[0].complete
    explicit = provider.observe(ProviderRequest(
        ProviderCapability.INSTRUMENTS,
        instrument_ids=("510050.SH",),
    ))
    assert explicit.tables[MarketTable.INSTRUMENTS].iloc[0]["price_tick"] == 0.001


def test_baostock_converts_cumulative_adjustment_series_to_event_ratios():
    class FactorClient(_BaoStockClient):
        def query_adjust_factor(self, code, start_date, end_date):
            assert code == "sh.600000"
            assert start_date == "1990-01-01" and end_date == "2026-07-14"
            return _Result(
                ["code", "dividOperateDate", "foreAdjustFactor", "backAdjustFactor", "adjustFactor"],
                [
                    [code, "2009-06-01", "0.2", "2", "2"],
                    [code, "2010-06-01", "0.4", "2.5", "2.5"],
                    [code, "2011-06-01", "1", "5", "5"],
                ],
            )

    observed = BaoStockProvider(client=FactorClient()).observe(ProviderRequest(
        ProviderCapability.ADJUSTMENT_FACTORS,
        date(2010, 1, 1),
        END,
        ("600000.SH",),
    ))

    factors = observed.tables[MarketTable.ADJUSTMENT_FACTORS]
    assert factors["effective_date"].tolist() == ["2010-06-01", "2011-06-01"]
    assert factors["price_multiplier"].tolist() == [0.8, 0.5]
    assert factors["known_date"].tolist() == ["2010-06-01", "2011-06-01"]


def test_baostock_complete_factor_claim_includes_instrument_with_no_events():
    class FactorClient(_BaoStockClient):
        def query_adjust_factor(self, code, start_date, end_date):
            return _Result(
                ["code", "dividOperateDate", "foreAdjustFactor", "backAdjustFactor", "adjustFactor"],
                [] if code == "sz.000001" else [
                    [code, "2011-06-01", "1", "1.2", "1.2"],
                ],
            )

    requested = ("000001.SZ", "600000.SH")
    observed = BaoStockProvider(client=FactorClient()).observe(ProviderRequest(
        ProviderCapability.ADJUSTMENT_FACTORS,
        date(2010, 1, 1),
        END,
        requested,
    ))

    assert observed.coverage[0].complete
    assert observed.coverage[0].instrument_ids == requested


def test_cninfo_actions_preserve_announcement_and_effect_lifecycle_with_per_share_units():
    class Client:
        __version__ = "test"

        def stock_dividend_cninfo(self, *, symbol):
            assert symbol == "600000"
            return pd.DataFrame([{
                "实施方案公告日期": "2021-05-20",
                "送股比例": 3,
                "转增比例": 2,
                "派息比例": 4,
                "股权登记日": "2021-06-01",
                "除权日": "2021-06-02",
                "派息日": "2021-06-04",
                "股份到账日": None,
            }])

        def stock_allotment_cninfo(self, *, symbol, start_date, end_date):
            assert (symbol, start_date, end_date) == ("600000", "20200101", "20221231")
            return pd.DataFrame([{
                "公告日期": "2022-01-10",
                "股权登记日": "2022-01-20",
                "除权基准日": "2022-02-01",
                "配股缴款截止日": "2022-01-28",
                "配股上市日": "2022-02-10",
                "配股比例": 3,
                "配股价格": 8.5,
            }])

    observed = CninfoCorporateActionProvider(client=Client()).observe(ProviderRequest(
        ProviderCapability.CORPORATE_ACTIONS,
        date(2020, 1, 1),
        date(2022, 12, 31),
        ("600000.SH",),
        {"max_workers": 1, "retries": 1},
    ))

    actions = observed.tables[MarketTable.CORPORATE_ACTIONS].set_index("action_type")
    assert set(actions.index) == {"cash_dividend", "stock_dividend", "rights_issue"}
    assert actions.loc["cash_dividend", "known_date"] == "2021-05-20"
    assert actions.loc["cash_dividend", "cash_per_share"] == 0.4
    assert actions.loc["stock_dividend", "share_ratio"] == 0.5
    assert pd.isna(actions.loc["stock_dividend", "listing_date"])
    assert actions.loc["rights_issue", "share_ratio"] == 0.3
    assert actions.loc["rights_issue", "rights_price"] == 8.5
    assert observed.coverage[0].complete
    assert observed.source_metadata["upstream"] == "CNInfo (巨潮资讯)"


def test_cninfo_direct_client_generates_token_and_normalizes_public_api_records():
    class Transport:
        def __init__(self):
            self.calls = []

        def post_json(self, url, *, parameters, headers, timeout):
            self.calls.append((url, dict(parameters), dict(headers), timeout))
            if url.endswith("p_sysapi1139"):
                return {"records": [{
                    "F006D": "2025-05-22",
                    "F044V": "年度分红",
                    "F010N": 0,
                    "F011N": 0,
                    "F012N": 3.3,
                    "F018D": "2025-05-27",
                    "F020D": "2025-05-28",
                    "F023D": "2025-05-28",
                    "F025D": None,
                    "F007V": "10派3.3元",
                    "F001V": "2024年报",
                }]}
            return {"records": [{
                "DECLAREDATE": "2023-11-27",
                "F011D": "2023-11-29",
                "F012D": "2023-12-08",
                "F014D": "2023-12-06",
                "F038D": "2023-12-25",
                "F004N": 3,
                "F005N": 21.16,
            }]}

    transport = Transport()
    client = CninfoPublicClient(transport=transport, timeout=7)

    dividend = client.stock_dividend_cninfo(symbol="000049")
    rights = client.stock_allotment_cninfo(
        symbol="000049", start_date="20100101", end_date="20260717",
    )

    assert _cninfo_token(epoch_seconds=1_700_000_000) == "WltfJaS1dRCcbgz+YSPgFg=="
    assert dividend.iloc[0]["派息比例"] == 3.3
    assert rights.iloc[0]["配股缴款截止日"] == "2023-12-06"
    assert transport.calls[1][1]["sdate"] == "2010-01-01"
    assert transport.calls[1][1]["edate"] == "2026-07-17"
    assert all(call[2]["Accept-Enckey"] for call in transport.calls)
    assert all(call[3] == 7 for call in transport.calls)


def test_xtquant_batch_download_normalizes_lots_and_local_dates():
    class XtClient:
        def __init__(self):
            self.downloads = []

        def download_history_data2(self, stock_list, period, start, end, incrementally):
            self.downloads.append((tuple(stock_list), period, start, end, incrementally))
            return {"ok": True}

        def get_market_data_ex(self, **kwargs):
            return {
                "600000.SH": pd.DataFrame([{
                    "time": 1783900800000,
                    "open": 10, "high": 10.8, "low": 9.9, "close": 10.5,
                    "volume": 10, "amount": 10500, "preClose": 9.8,
                    "suspendFlag": 0,
                }], index=["20260713"]),
            }

    client = XtClient()
    observed = XtQuantProvider(client=client).observe(ProviderRequest(
        ProviderCapability.DAILY_BARS_RAW,
        START,
        END,
        ("600000.SH",),
        {"download": True, "incrementally": False},
    ))

    bars = observed.tables[MarketTable.DAILY_BARS]
    assert bars["session_date"].tolist() == ["2026-07-13"]
    assert bars["volume"].tolist() == [1000]
    assert bars["previous_close"].tolist() == [9.8]
    assert client.downloads == [(("600000.SH",), "1d", "20260713", "20260714", False)]


def test_xtquant_converts_local_dividend_dr_to_replayable_event_ratio():
    class XtClient:
        def get_divid_factors(self, instrument_id, start, end):
            assert (instrument_id, start, end) == ("600000.SH", "20260713", "20260714")
            event = pd.Timestamp("2026-07-14", tz="Asia/Shanghai").timestamp() * 1000
            return pd.DataFrame([{
                "time": event, "interest": 0.4, "stockBonus": 0,
                "stockGift": 0, "allotNum": 0, "allotPrice": 0,
                "gugai": 0, "dr": 1.25,
            }])

    observed = XtQuantProvider(client=XtClient()).observe(ProviderRequest(
        ProviderCapability.ADJUSTMENT_FACTORS,
        START,
        END,
        ("600000.SH",),
    ))

    factors = observed.tables[MarketTable.ADJUSTMENT_FACTORS]
    assert factors["effective_date"].tolist() == ["2026-07-14"]
    assert factors["known_date"].tolist() == ["2026-07-14"]
    assert factors["price_multiplier"].tolist() == [0.8]
    assert observed.coverage[0].complete


def test_xtquant_status_reads_each_instrument_separately_with_dense_fill_enabled():
    class XtClient:
        def __init__(self):
            self.calls = []

        def get_market_data_ex(self, **kwargs):
            self.calls.append(kwargs)
            instrument_id = kwargs["stock_list"][0]
            return {instrument_id: pd.DataFrame([
                {
                    "time": pd.Timestamp("2026-07-13", tz="Asia/Shanghai").timestamp() * 1000,
                    "open": 10, "high": 10, "low": 10, "close": 10,
                    "volume": 10, "amount": 10000, "preClose": 9.8, "suspendFlag": 0,
                },
                {
                    "time": pd.Timestamp("2026-07-14", tz="Asia/Shanghai").timestamp() * 1000,
                    "open": None, "high": None, "low": None, "close": None,
                    "volume": 0, "amount": 0, "preClose": None, "suspendFlag": 1,
                },
            ], index=["20260713", "20260714"])}

    client = XtClient()
    observed = XtQuantProvider(client=client).observe(ProviderRequest(
        ProviderCapability.DAILY_STATUS,
        START,
        END,
        ("510300.SH", "600000.SH"),
    ))

    assert len(client.calls) == 2
    assert all(item["fill_data"] is True and len(item["stock_list"]) == 1 for item in client.calls)
    status = observed.tables[MarketTable.DAILY_BARS]
    assert len(status) == 4
    assert status.groupby("instrument_id")["suspended"].sum().eq(1).all()
    assert status["is_st"].isna().all()
    assert observed.coverage[0].complete
    assert observed.coverage[0].complete


def test_xtquant_direct_limit_snapshot_uses_dated_instrument_detail():
    class XtClient:
        def get_instrument_detail(self, instrument_id, complete):
            assert (instrument_id, complete) == ("600000.SH", True)
            return {
                "TradingDay": "20260714",
                "PreClose": 10.0,
                "UpStopPrice": 11.0,
                "DownStopPrice": 9.0,
                "PriceTick": 0.01,
            }

    observed = XtQuantProvider(client=XtClient()).observe(ProviderRequest(
        ProviderCapability.DAILY_STATUS,
        END,
        END,
        ("600000.SH",),
        {"instrument_limit_snapshot": True},
    ))

    row = observed.tables[MarketTable.DAILY_BARS].iloc[0]
    assert observed.coverage[0].complete
    assert row["session_date"] == END.isoformat()
    assert (row["previous_close"], row["limit_up"], row["limit_down"]) == (10, 11, 9)
