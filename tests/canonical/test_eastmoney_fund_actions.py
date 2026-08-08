from __future__ import annotations

from dataclasses import replace
from datetime import date
import json

import pytest

from fundlab.marketdata import (
    MarketTable,
    ObservationError,
    ProviderCapability,
    ProviderRequest,
)
from fundlab.marketdata.sources import eastmoney_fund
from fundlab.marketdata.sources.eastmoney_fund import EastmoneyEtfActionProvider


_PAGE = """
<html><head><title>测试ETF(159999)基金分红送配</title></head><body>
<table>
  <tr><th>年份</th><th>权益登记日</th><th>除息日</th><th>每份分红</th><th>分红发放日</th></tr>
  <tr><td>2021年</td><td>2021-05-10</td><td>2021-05-11</td><td>每份派现金0.0800元</td><td>2021-05-14</td></tr>
</table>
<table>
  <tr><th>年份</th><th>拆分折算日</th><th>拆分类型</th><th>拆分折算比例</th></tr>
  <tr><td>2021年</td><td>2021-06-02</td><td>份额折算</td><td>1:0.5000</td></tr>
</table>
</body></html>
"""


def test_etf_action_summary_normalizes_header_declared_distribution_units():
    page = """
    <table>
      <tr><th>年份</th><th>权益登记日</th><th>除息日</th><th>每10份分红</th><th>分红发放日</th></tr>
      <tr><td>2026年</td><td>2026-05-12</td><td>2026-05-13</td><td>0.0310元</td><td>2026-05-15</td></tr>
    </table>
    <table><tr><th>年份</th><th>拆分折算日</th><th>拆分类型</th><th>拆分折算比例</th></tr></table>
    """

    events = eastmoney_fund._summary_events(
        page, start=date(2026, 1, 1), end=date(2026, 12, 31),
    )

    assert len(events) == 1
    assert events[0].cash_per_share == pytest.approx(0.0031)


def test_etf_action_summary_rejects_unknown_cash_schema_even_with_split_table():
    page = """
    <table>
      <tr><th>年份</th><th>权益登记日</th><th>除息日</th><th>现金分配</th><th>分红发放日</th></tr>
    </table>
    <table><tr><th>年份</th><th>拆分折算日</th><th>拆分类型</th><th>拆分折算比例</th></tr></table>
    """

    with pytest.raises(ObservationError, match="cash distribution column"):
        eastmoney_fund._summary_events(
            page, start=date(2026, 1, 1), end=date(2026, 12, 31),
        )


class _Transport:
    def get_text(self, url, *, parameters, headers, timeout):
        assert "fhsp_159999" in url
        return _PAGE

    def get_json(self, url, *, parameters, headers, timeout):
        if url.endswith("/JJGG"):
            return {"Data": [
                {
                    "ID": "cash-report", "TITLE": "测试ETF分红公告",
                    "PUBLISHDATEDesc": "2021-05-06",
                },
                {
                    "ID": "split-report", "TITLE": "测试ETF实施基金份额合并的公告",
                    "PUBLISHDATEDesc": "2021-05-25",
                },
            ]}
        report_id = parameters["art_code"]
        contents = {
            "cash-report": (
                "权益登记日：2021年5月10日。除息日：2021年5月11日。"
                "现金红利发放日：2021年5月14日。"
            ),
            "split-report": (
                "份额合并权益登记日：2021年6月1日。份额合并日：2021年6月2日。"
                "本基金将于2021年6月3日起恢复交易。"
                "本次基金份额合并比例为0.500000123。"
            ),
        }
        return {
            "success": 1,
            "data": {
                "art_code": report_id,
                "notice_date": (
                    "2021-05-06" if report_id == "cash-report" else "2021-05-25"
                ),
                "notice_content": contents[report_id],
            },
        }


def test_etf_action_provider_preserves_known_lifecycle_and_reverse_split_multiplier():
    request = ProviderRequest(
        ProviderCapability.CORPORATE_ACTIONS,
        date(2020, 1, 1),
        date(2021, 12, 31),
        ("159999.SZ",),
        {"listed_dates": {"159999.SZ": "2020-01-02"}, "max_workers": 1},
    )

    payload = EastmoneyEtfActionProvider(transport=_Transport()).observe(request)

    assert payload.coverage[0].complete
    actions = payload.tables[MarketTable.CORPORATE_ACTIONS].set_index("action_type")
    assert actions.loc["cash_dividend", "known_date"] == date(2021, 5, 6)
    assert actions.loc["cash_dividend", "cash_per_share"] == 0.08
    assert actions.loc["split", "record_date"] == date(2021, 6, 1)
    assert actions.loc["split", "ex_date"] == date(2021, 6, 3)
    assert actions.loc["split", "listing_date"] == date(2021, 6, 3)
    assert actions.loc["split", "quantity_multiplier"] == 0.500000123


def test_etf_action_provider_recovers_cash_notice_when_archive_table_is_empty():
    page = """
    <html><head><title>测试ETF(159999)基金分红送配</title></head><body>
    <table><tr><th>年份</th><th>权益登记日</th><th>除息日</th><th>每份分红</th><th>分红发放日</th></tr></table>
    <table><tr><th>年份</th><th>拆分折算日</th><th>拆分类型</th><th>拆分折算比例</th></tr></table>
    </body></html>
    """

    class NoticeOnlyTransport:
        def get_text(self, url, *, parameters, headers, timeout):
            return page

        def get_json(self, url, *, parameters, headers, timeout):
            if url.endswith("/JJGG"):
                return {"Data": [{
                    "ID": "cash-only", "TITLE": "测试ETF收益分配公告",
                    "PUBLISHDATEDesc": "2024-08-09",
                }], "TotalCount": 1}
            return {
                "success": 1,
                "data": {
                    "art_code": "cash-only",
                    "notice_date": "2024-08-09",
                    "notice_content": (
                        "每10份基金份额派发现金红利0.020元。"
                        "权益登记日：2024年8月13日。"
                        "除息日：2024年8月14日。"
                        "现金红利发放日：2024年8月16日。"
                    ),
                },
            }

    payload = EastmoneyEtfActionProvider(
        transport=NoticeOnlyTransport()
    ).observe(ProviderRequest(
        ProviderCapability.CORPORATE_ACTIONS,
        date(2020, 1, 1),
        date(2026, 7, 17),
        ("159999.SZ",),
        {
            "listed_dates": {"159999.SZ": "2020-01-02"},
            "max_workers": 1,
            "retries": 1,
        },
    ))

    assert payload.coverage[0].complete
    action = payload.tables[MarketTable.CORPORATE_ACTIONS].iloc[0]
    assert action["known_date"] == date(2024, 8, 9)
    assert action["record_date"] == date(2024, 8, 13)
    assert action["ex_date"] == date(2024, 8, 14)
    assert action["pay_date"] == date(2024, 8, 16)
    assert action["cash_per_share"] == pytest.approx(0.002)
    assert json.loads(action["source_payload"])["economics_source"] == (
        "implementation_notice"
    )


def test_etf_action_provider_marks_in_scope_unreadable_notice_incomplete():
    page = """
    <html><head><title>测试ETF(159999)基金分红送配</title></head><body>
    <table><tr><th>年份</th><th>权益登记日</th><th>除息日</th><th>每份分红</th><th>分红发放日</th></tr></table>
    <table><tr><th>年份</th><th>拆分折算日</th><th>拆分类型</th><th>拆分折算比例</th></tr></table>
    </body></html>
    """

    class UnreadableNoticeTransport:
        def get_text(self, url, *, parameters, headers, timeout):
            return page

        def get_json(self, url, *, parameters, headers, timeout):
            if url.endswith("/JJGG"):
                return {
                    "Data": [{
                        "ID": "cash-unreadable",
                        "TITLE": "测试ETF收益分配公告",
                        "PUBLISHDATEDesc": "2024-08-09",
                    }],
                    "TotalCount": 1,
                }
            return {
                "success": 1,
                "data": {
                    "art_code": "cash-unreadable",
                    "notice_date": "2024-08-09",
                    "notice_content": "本公告正文没有可解析的行动生命周期字段。",
                },
            }

        def get_bytes(self, url, *, parameters, headers, timeout):
            raise RuntimeError("announcement PDF unavailable")

    payload = EastmoneyEtfActionProvider(
        transport=UnreadableNoticeTransport()
    ).observe(ProviderRequest(
        ProviderCapability.CORPORATE_ACTIONS,
        date(2024, 8, 1),
        date(2024, 8, 31),
        ("159999.SZ",),
        {
            "listed_dates": {"159999.SZ": "2020-01-02"},
            "max_workers": 1,
            "retries": 1,
        },
    ))

    assert not payload.coverage[0].complete
    assert payload.tables[MarketTable.CORPORATE_ACTIONS].empty
    assert payload.source_metadata["invalid_lifecycle"]["159999.SZ"] == (
        "announcement_content:cash-unreadable",
    )


def test_etf_action_provider_recovers_split_notices_when_archive_table_is_empty():
    page = """
    <html><head><title>测试ETF(159999)基金分红送配</title></head><body>
    <table><tr><th>年份</th><th>权益登记日</th><th>除息日</th><th>每份分红</th><th>分红发放日</th></tr></table>
    <table><tr><th>年份</th><th>拆分折算日</th><th>拆分类型</th><th>拆分折算比例</th></tr></table>
    </body></html>
    """

    class SplitNoticeTransport:
        def get_text(self, url, *, parameters, headers, timeout):
            return page

        def get_json(self, url, *, parameters, headers, timeout):
            if url.endswith("/JJGG"):
                return {"Data": [
                    {
                        "ID": "arrangement", "TITLE": "测试ETF基金份额折算公告",
                        "PUBLISHDATEDesc": "2022-08-10",
                    },
                    {
                        "ID": "result", "TITLE": "测试ETF基金份额折算结果公告",
                        "PUBLISHDATEDesc": "2022-08-17",
                    },
                ], "TotalCount": 2}
            report_id = parameters["art_code"]
            content = (
                "份额折算权益登记日：2022年8月16日。"
                "份额折算日：2022年8月16日。"
                "本次基金份额折算比例为0.01。"
                if report_id == "arrangement" else
                "本基金将于2022年8月17日起恢复交易。"
            )
            return {
                "success": 1,
                "data": {
                    "art_code": report_id,
                    "notice_date": (
                        "2022-08-10" if report_id == "arrangement" else "2022-08-17"
                    ),
                    "notice_content": content,
                },
            }

    payload = EastmoneyEtfActionProvider(
        transport=SplitNoticeTransport()
    ).observe(ProviderRequest(
        ProviderCapability.CORPORATE_ACTIONS,
        date(2020, 1, 1),
        date(2026, 7, 17),
        ("159999.SZ",),
        {
            "listed_dates": {"159999.SZ": "2020-01-02"},
            "max_workers": 1,
            "retries": 1,
        },
    ))

    assert payload.coverage[0].complete
    action = payload.tables[MarketTable.CORPORATE_ACTIONS].iloc[0]
    assert action["known_date"] == date(2022, 8, 10)
    assert action["record_date"] == date(2022, 8, 16)
    assert action["ex_date"] == action["listing_date"] == date(2022, 8, 17)
    assert action["quantity_multiplier"] == pytest.approx(0.01)
    assert json.loads(action["source_payload"])["economics_source"] == (
        "implementation_notice"
    )


def test_etf_action_provider_uses_hashed_public_pdf_when_content_api_disconnects(
    monkeypatch,
):
    class PdfFallbackTransport(_Transport):
        def get_json(self, url, *, parameters, headers, timeout):
            if url.endswith("/JJGG"):
                return super().get_json(
                    url, parameters=parameters, headers=headers, timeout=timeout,
                )
            raise ConnectionError("content endpoint disconnected")

        def get_bytes(self, url, *, parameters, headers, timeout):
            assert url.startswith("https://pdf.dfcfw.com/pdf/H2_")
            if "Cookie" not in headers:
                return (
                    b'<script>var e={aa:11,bb:22,cc:function(a,n){return a+n},dd:33};'
                    b't=a[x](t,44);var x="EO_Bot_Ssid= __tst_status=";</script>'
                )
            assert headers["Cookie"] == "__tst_status=66; EO_Bot_Ssid=44"
            return b"%PDF-" + url.encode("ascii")

    monkeypatch.setattr(
        eastmoney_fund,
        "_pdf_text",
        lambda raw: (
            (
                "权益登记日：2021年5月10日。除息日：2021年5月11日。"
                "现金红利发放日：2021年5月14日。"
            ) if b"cash-report" in raw else (
                "份额合并权益登记日：2021年6月1日。份额合并日：2021年6月2日。"
                "本基金将于2021年6月3日起恢复交易。"
                "本次基金份额合并比例为0.500000123。"
            )
        ),
    )
    request = ProviderRequest(
        ProviderCapability.CORPORATE_ACTIONS,
        date(2020, 1, 1),
        date(2021, 12, 31),
        ("159999.SZ",),
        {
            "listed_dates": {"159999.SZ": "2020-01-02"},
            "max_workers": 1,
            "retries": 1,
        },
    )

    payload = EastmoneyEtfActionProvider(transport=PdfFallbackTransport()).observe(request)

    assert payload.coverage[0].complete
    transports = payload.source_metadata["response_sha256"]["159999.SZ"][
        "announcement_transport"
    ]
    assert set(transports.values()) == {"eastmoney-public-pdf-challenge-cookie"}
    provenance = json.loads(
        payload.tables[MarketTable.CORPORATE_ACTIONS].iloc[0]["source_payload"]
    )
    assert provenance["announcement_transport"] == "eastmoney-public-pdf-challenge-cookie"
    assert provenance["announcement_url"].startswith("https://pdf.dfcfw.com/pdf/H2_")


def test_etf_action_provider_verifies_narrow_official_archive_gap_supplement(
    monkeypatch,
):
    page = """
    <html><head><title>中证1000ETF易方达(159633)基金分红送配</title></head><body>
    <table><tr><th>年份</th><th>权益登记日</th><th>除息日</th><th>每份分红</th><th>分红发放日</th></tr></table>
    <table>
      <tr><th>年份</th><th>拆分折算日</th><th>拆分类型</th><th>拆分折算比例</th></tr>
      <tr><td>2022年</td><td>2022-10-28</td><td>份额合并</td><td>1:0.4716</td></tr>
    </table></body></html>
    """

    class SupplementTransport:
        def get_text(self, url, *, parameters, headers, timeout):
            return page

        def get_json(self, url, *, parameters, headers, timeout):
            assert url.endswith("/JJGG")
            return {"Data": []}

        def get_bytes(self, url, *, parameters, headers, timeout):
            return b"%PDF-" + url.encode("utf-8")

    def supplement_text(raw):
        if b"AN202210311579645114" in raw:
            return "159633 2022年10月28日 2022年10月31日 0.471642898"
        return "159633 实施基金份额合并业务 2022-10-25"

    monkeypatch.setattr(eastmoney_fund, "_pdf_text", supplement_text)
    request = ProviderRequest(
        ProviderCapability.CORPORATE_ACTIONS,
        date(2010, 1, 1),
        date(2026, 7, 17),
        ("159633.SZ",),
        {
            "listed_dates": {"159633.SZ": "2022-08-04"},
            "max_workers": 1,
            "retries": 1,
        },
    )

    payload = EastmoneyEtfActionProvider(transport=SupplementTransport()).observe(request)

    assert payload.coverage[0].complete
    action = payload.tables[MarketTable.CORPORATE_ACTIONS].iloc[0]
    assert action["known_date"] == date(2022, 10, 25)
    assert action["record_date"] == date(2022, 10, 28)
    assert action["ex_date"] == action["listing_date"] == date(2022, 10, 31)
    assert action["quantity_multiplier"] == pytest.approx(0.471642898)
    documents = payload.source_metadata["response_sha256"]["159633.SZ"][
        "official_supplement_documents"
    ]
    assert len(documents["2022-10-28"]) == 2


def test_etf_action_provider_paginates_general_notices_for_split_arrangement():
    page = """
    <html><head><title>测试ETF(159999)基金分红送配</title></head><body>
    <table><tr><th>年份</th><th>权益登记日</th><th>除息日</th><th>每份分红</th><th>分红发放日</th></tr></table>
    <table>
      <tr><th>年份</th><th>拆分折算日</th><th>拆分类型</th><th>拆分折算比例</th></tr>
      <tr><td>2024年</td><td>2024-01-26</td><td>份额合并</td><td>1:0.503546</td></tr>
    </table></body></html>
    """

    class PaginatedTransport:
        def get_text(self, url, *, parameters, headers, timeout):
            return page

        def get_json(self, url, *, parameters, headers, timeout):
            if not url.endswith("/JJGG"):
                return {
                    "success": 1,
                    "data": {
                        "art_code": "arrangement",
                        "notice_date": "2024-01-23",
                        "notice_content": (
                            "份额合并权益登记日：2024年1月26日。"
                            "份额合并日：2024年1月26日。"
                            "本基金将于2024年1月29日起恢复交易。"
                            "本次基金份额合并比例为0.503546。"
                        ),
                    },
                }
            if parameters["type"] == "2":
                return {"Data": [], "TotalCount": 0, "PageSize": 100, "PageIndex": 1}
            if parameters["pageIndex"] == "1":
                return {
                    "Data": [{
                        "ID": "unrelated", "TITLE": "测试ETF季度报告",
                        "PUBLISHDATEDesc": "2024-02-01",
                    }],
                    "TotalCount": 2,
                    "PageSize": 1,
                    "PageIndex": 1,
                }
            return {
                "Data": [{
                    "ID": "arrangement", "TITLE": "测试ETF实施基金份额合并业务公告",
                    "PUBLISHDATEDesc": "2024-01-23",
                }],
                "TotalCount": 2,
                "PageSize": 1,
                "PageIndex": 2,
            }

    payload = EastmoneyEtfActionProvider(transport=PaginatedTransport()).observe(
        ProviderRequest(
            ProviderCapability.CORPORATE_ACTIONS,
            date(2020, 1, 1),
            date(2026, 7, 17),
            ("159999.SZ",),
            {
                "listed_dates": {"159999.SZ": "2020-01-02"},
                "max_workers": 1,
                "retries": 1,
            },
        )
    )

    assert payload.coverage[0].complete
    action = payload.tables[MarketTable.CORPORATE_ACTIONS].iloc[0]
    assert action["known_date"] == date(2024, 1, 23)
    assert action["record_date"] == date(2024, 1, 26)
    assert action["ex_date"] == date(2024, 1, 29)
    assert action["quantity_multiplier"] == pytest.approx(0.503546)
    list_hashes = payload.source_metadata["response_sha256"]["159999.SZ"][
        "announcement_list"
    ]
    assert set(list_hashes) == {"type=2;page=1", "type=6;page=1", "type=6;page=2"}


def test_etf_action_provider_recovers_pdf_table_ordered_cash_dates():
    page = """
    <html><head><title>测试ETF(159999)基金分红送配</title></head><body>
    <table>
      <tr><th>年份</th><th>权益登记日</th><th>除息日</th><th>每份分红</th><th>分红发放日</th></tr>
      <tr><td>2023年</td><td>2023-06-19</td><td>2023-06-20</td><td>每份派现金0.076元</td><td>2023-06-27</td></tr>
    </table>
    <table><tr><th>年份</th><th>拆分折算日</th><th>拆分类型</th><th>拆分折算比例</th></tr></table>
    </body></html>
    """

    class TableOrderedTransport:
        def get_text(self, url, *, parameters, headers, timeout):
            return page

        def get_json(self, url, *, parameters, headers, timeout):
            if url.endswith("/JJGG"):
                return {"Data": [{
                    "ID": "cash-report", "TITLE": "测试ETF收益分配公告",
                    "PUBLISHDATEDesc": "2023-06-15",
                }]}
            return {
                "success": 1,
                "data": {
                    "art_code": "cash-report",
                    "notice_date": "2023-06-15",
                    "notice_content": (
                        "权益登记日除息日现金红利发放日"
                        "2023年06月19日2023年06月20日2023年06月27日"
                        "分红对象为全体基金份额持有人"
                    ),
                },
            }

    payload = EastmoneyEtfActionProvider(transport=TableOrderedTransport()).observe(
        ProviderRequest(
            ProviderCapability.CORPORATE_ACTIONS,
            date(2020, 1, 1),
            date(2026, 7, 17),
            ("159999.SZ",),
            {
                "listed_dates": {"159999.SZ": "2020-01-02"},
                "max_workers": 1,
                "retries": 1,
            },
        )
    )

    assert payload.coverage[0].complete
    action = payload.tables[MarketTable.CORPORATE_ACTIONS].iloc[0]
    assert action["record_date"] == date(2023, 6, 19)
    assert action["ex_date"] == date(2023, 6, 20)
    assert action["pay_date"] == date(2023, 6, 27)


def test_etf_action_provider_verifies_cash_archive_gap_supplement(monkeypatch):
    page = """
    <html><head><title>中小100ETF华夏(159902)基金分红送配</title></head><body>
    <table>
      <tr><th>年份</th><th>权益登记日</th><th>除息日</th><th>每份分红</th><th>分红发放日</th></tr>
      <tr><td>2014年</td><td>2014-03-13</td><td>2014-03-13</td><td>每份派现金0.0200元</td><td>2014-03-17</td></tr>
    </table>
    <table><tr><th>年份</th><th>拆分折算日</th><th>拆分类型</th><th>拆分折算比例</th></tr></table>
    </body></html>
    """

    class CashSupplementTransport:
        def get_text(self, url, *, parameters, headers, timeout):
            return page

        def get_json(self, url, *, parameters, headers, timeout):
            return {"Data": []}

        def get_bytes(self, url, *, parameters, headers, timeout):
            return b"%PDF-cash-supplement"

    monkeypatch.setattr(
        eastmoney_fund,
        "_pdf_text",
        lambda raw: (
            "159902 2014年3月6日 2014年3月13日 2014年3月17日 0.20"
        ),
    )
    payload = EastmoneyEtfActionProvider(
        transport=CashSupplementTransport()
    ).observe(ProviderRequest(
        ProviderCapability.CORPORATE_ACTIONS,
        date(2010, 1, 1),
        date(2026, 7, 17),
        ("159902.SZ",),
        {
            "listed_dates": {"159902.SZ": "2006-09-05"},
            "max_workers": 1,
            "retries": 1,
        },
    ))

    assert payload.coverage[0].complete
    action = payload.tables[MarketTable.CORPORATE_ACTIONS].iloc[0]
    assert action["known_date"] == date(2014, 3, 6)
    assert action["record_date"] == action["ex_date"] == date(2014, 3, 13)
    assert action["pay_date"] == date(2014, 3, 17)
    assert action["cash_per_share"] == pytest.approx(0.02)


def test_etf_notice_prefers_labeled_on_exchange_dates_over_off_exchange_dates():
    notice = eastmoney_fund._notice(
        {
            "ID": "dual-venue", "TITLE": "测试ETF分红公告",
            "PUBLISHDATEDesc": "2025-07-30",
        },
        {
            "success": 1,
            "data": {
                "art_code": "dual-venue",
                "notice_date": "2025-07-30",
                "notice_content": (
                    "权益登记日2025年7月31日（场内）2025年8月1日（场外）"
                    "除息日2025年8月1日"
                    "现金红利发放日2025年8月6日（场内）2025年8月4日（场外）"
                ),
            },
        },
        "digest",
        document_transport="fixture",
        document_url="fixture://notice",
    )

    assert notice is not None
    assert notice.record_date == date(2025, 7, 31)
    assert notice.ex_date == date(2025, 8, 1)
    assert notice.pay_date == date(2025, 8, 6)
    row = eastmoney_fund._cash_row(
        "159999.SZ",
        eastmoney_fund._SummaryEvent(
            "cash",
            date(2025, 8, 1),
            cash_per_share=0.041,
            record_date=date(2025, 8, 1),
            pay_date=date(2025, 8, 4),
        ),
        notice,
        "archive-hash",
    )
    assert row["record_date"] == date(2025, 7, 31)
    assert row["pay_date"] == date(2025, 8, 6)
    provenance = json.loads(row["source_payload"])
    assert provenance["archive_record_date"] == "2025-08-01"
    assert provenance["archive_pay_date"] == "2025-08-04"
    fallback = eastmoney_fund._cash_row(
        "159999.SZ",
        eastmoney_fund._SummaryEvent(
            "cash",
            date(2025, 8, 1),
            cash_per_share=0.041,
            record_date=date(2025, 7, 31),
            pay_date=date(2025, 8, 6),
        ),
        replace(notice, ex_date=None),
        "archive-hash",
    )
    assert fallback["ex_date"] == date(2025, 8, 1)

    iso_notice = eastmoney_fund._notice(
        {"ID": "iso", "TITLE": "测试ETF分红公告", "PUBLISHDATEDesc": "2018-12-24"},
        {
            "success": 1,
            "data": {
                "notice_date": "2018-12-24",
                "notice_content": (
                    "权益登记日2018-12-27除息日2018-12-28（场内）"
                    "现金红利发放日：2019年1月4日"
                ),
            },
        },
        "digest",
        document_transport="fixture",
        document_url="fixture://notice",
    )
    assert iso_notice is not None
    assert iso_notice.record_date == date(2018, 12, 27)
    assert iso_notice.ex_date == date(2018, 12, 28)
    assert iso_notice.pay_date == date(2019, 1, 4)


def test_etf_notice_recovers_combined_split_headers_and_resume_phrases():
    def parsed(report_id, known_date, content):
        return eastmoney_fund._notice(
            {"ID": report_id, "TITLE": "测试ETF份额拆分结果", "PUBLISHDATEDesc": known_date},
            {
                "success": 1,
                "data": {
                    "art_code": report_id,
                    "notice_date": known_date,
                    "notice_content": content,
                },
            },
            "digest",
            document_transport="fixture",
            document_url="fixture://notice",
        )

    combined = parsed(
        "combined",
        "2012-11-27",
        (
            "权益登记日、除权日、折算处理日2012年11月30日。"
            "折算处理日后的第一个工作日（2012年12月3日）复牌。"
        ),
    )
    assert combined is not None
    assert combined.record_date == combined.split_date == date(2012, 11, 30)
    assert combined.resume_date == date(2012, 12, 3)

    arrangement = parsed(
        "arrangement",
        "2022-08-23",
        "份额拆分日：2022年8月26日。拆分对象为份额拆分日登记在册的基金份额。",
    )
    result = parsed(
        "result",
        "2022-08-29",
        "自基金份额拆分日的下一工作日（2022年8月29日）起调整申购赎回单位。",
    )
    secondary_market = parsed(
        "secondary-market",
        "2021-07-19",
        "本基金场内份额将于2021年7月19日起恢复二级市场交易。",
    )
    assert arrangement is not None and result is not None and secondary_market is not None
    assert arrangement.record_date == arrangement.split_date == date(2022, 8, 26)
    assert result.resume_date == date(2022, 8, 29)
    assert secondary_market.resume_date == date(2021, 7, 19)
