from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, timedelta
from hashlib import sha256
from io import BytesIO, StringIO
import re
from time import sleep
from typing import Any, Mapping
import warnings

import pandas as pd

from fundlab.common.canonical import stable_digest
from fundlab.marketdata.contracts import (
    CorporateActionType,
    CoverageClaim,
    MarketTable,
    ObservationError,
    ObservationPayload,
    ProviderCapability,
    ProviderRequest,
)
from fundlab.marketdata.sources.base import (
    JsonTransport,
    UrllibJsonTransport,
    now_utc,
    payload_hash,
    require_daily_scope,
    source_payload,
    split_instrument_id,
)


_FUND_PAGE = "https://fundf10.eastmoney.com/fhsp_{code}.html"
_ANNOUNCEMENT_LIST = "https://api.fund.eastmoney.com/f10/JJGG"
_ANNOUNCEMENT_CONTENT = "https://np-cnotice-fund.eastmoney.com/api/content/ann"
_ANNOUNCEMENT_PDF = "https://pdf.dfcfw.com/pdf/H2_{report_id}_1.pdf"
_ACTION_TITLE = re.compile(r"(?:分红|收益分配|利润分配|份额拆|份额折|份额合)")
_DATE_TOKEN = r"(\d{4})年(\d{1,2})月(\d{1,2})日"
_ISO_DATE_TOKEN = r"(\d{4})-(\d{1,2})-(\d{1,2})"
EASTMONEY_ETF_ACTION_POLICY = "eastmoney-etf-actions-r2-v6"


@dataclass(frozen=True)
class _SummaryEvent:
    kind: str
    event_date: date
    cash_per_share: float | None = None
    quantity_multiplier: float | None = None
    record_date: date | None = None
    pay_date: date | None = None


@dataclass(frozen=True)
class _Notice:
    report_id: str
    title: str
    known_date: date
    content_hash: str
    document_transport: str
    document_url: str
    record_date: date | None
    ex_date: date | None
    pay_date: date | None
    split_date: date | None
    resume_date: date | None
    quantity_multiplier: float | None
    cash_per_share: float | None


@dataclass(frozen=True)
class _SupplementDocument:
    url: str
    markers: tuple[str, ...]


@dataclass(frozen=True)
class _OfficialSplitSupplement:
    known_date: date
    record_date: date
    raw_split_date: date
    effect_date: date
    quantity_multiplier: float
    documents: tuple[_SupplementDocument, ...]


@dataclass(frozen=True)
class _OfficialCashSupplement:
    known_date: date
    record_date: date
    ex_date: date
    pay_date: date
    cash_per_share: float
    documents: tuple[_SupplementDocument, ...]


_OFFICIAL_CASH_SUPPLEMENTS: Mapping[tuple[str, date], _OfficialCashSupplement] = {
    ("159902.SZ", date(2014, 3, 13)): _OfficialCashSupplement(
        date(2014, 3, 6),
        date(2014, 3, 13),
        date(2014, 3, 13),
        date(2014, 3, 17),
        0.02,
        (_SupplementDocument(
            (
                "https://epaper.stcn.com/paper/zqsb/page/1/"
                "2014-03/06/B012/20140306B012_pdf.pdf"
            ),
            ("159902", "2014年3月6日", "2014年3月13日", "2014年3月17日", "0.20"),
        ),),
    ),
}


_OFFICIAL_SPLIT_SUPPLEMENTS: Mapping[tuple[str, date], _OfficialSplitSupplement] = {
    ("159901.SZ", date(2010, 11, 19)): _OfficialSplitSupplement(
        date(2010, 11, 16),
        date(2010, 11, 19),
        date(2010, 11, 19),
        date(2010, 11, 22),
        5.0,
        (_SupplementDocument(
            "https://static.cninfo.com.cn/finalpage/2010-11-16/58667046.PDF",
            ("159901", "2010年11月19日", "2010年11月22日", "5:1"),
        ),),
    ),
    ("159901.SZ", date(2014, 8, 29)): _OfficialSplitSupplement(
        # The accessible prompt is dated on the entitlement day.  This is
        # deliberately later than the original arrangement notice and thus a
        # conservative, no-lookahead known date for the 2014-09-01 resume.
        date(2014, 8, 29),
        date(2014, 8, 29),
        date(2014, 8, 29),
        date(2014, 9, 1),
        0.2,
        (
            _SupplementDocument(
                "https://www.95579.com/upload/20140828/20140828200928114.pdf",
                ("159901", "2014年8月29日", "2014年9月1日", "0.2:1"),
            ),
            _SupplementDocument(
                "https://static.cninfo.com.cn/finalpage/2019-03-28/1205946287.PDF",
                ("159901", "2014年8月29日", "0.2:1"),
            ),
        ),
    ),
    ("159633.SZ", date(2022, 10, 28)): _OfficialSplitSupplement(
        date(2022, 10, 25),
        date(2022, 10, 28),
        date(2022, 10, 28),
        date(2022, 10, 31),
        0.471642898,
        (
            _SupplementDocument(
                "https://pdf.dfcfw.com/pdf/H2_AN202210311579645114_1.pdf",
                ("159633", "2022年10月28日", "2022年10月31日", "0.471642898"),
            ),
            _SupplementDocument(
                "https://pdf.dfcfw.com/pdf/H2_AN202403261628225110_1.pdf?1711497821000.pdf=",
                ("159633", "实施基金份额合并业务", "2022-10-25"),
            ),
        ),
    ),
    ("510020.SH", date(2012, 12, 13)): _OfficialSplitSupplement(
        date(2012, 11, 27),
        date(2012, 12, 12),
        date(2012, 12, 13),
        date(2012, 12, 14),
        0.1,
        (_SupplementDocument(
            "https://pdf.dfcfw.com/pdf/H2_AN201212110005640992_1.pdf",
            (
                "510020", "2012年11月27日", "2012年12月12日",
                "2012年12月14日", "每10份基金份额合并成1份",
            ),
        ),),
    ),
    ("560010.SH", date(2022, 9, 13)): _OfficialSplitSupplement(
        date(2022, 9, 1),
        date(2022, 9, 9),
        date(2022, 9, 13),
        date(2022, 9, 14),
        0.35295,
        (_SupplementDocument(
            (
                "https://www.sse.com.cn/disclosure/fund/announcement/c/new/"
                "2022-09-05/560010_20220905_1_3fn9DXfz.pdf"
            ),
            ("560010", "2022年9月1日", "2022年9月9日", "2022年9月14日", "0.35295"),
        ),),
    ),
}


class EastmoneyEtfActionProvider:
    """ETF cash distributions and unit split/consolidation lifecycle evidence.

    The per-fund distribution page is the exhaustive event index.  Announcement
    list/content endpoints supply the public-known date and record/effect lifecycle;
    Narrow keyed archive gaps are verified against hashed public copies of
    issuer/exchange disclosure documents.
    Canonical reconciliation still requires an independent MiniQMT factor event.
    """

    name = "eastmoney-fund-public"
    backend_group = "eastmoney"
    capabilities = frozenset({ProviderCapability.CORPORATE_ACTIONS})

    def __init__(self, *, transport: JsonTransport | None = None) -> None:
        self._transport = transport or UrllibJsonTransport()

    @property
    def available(self) -> bool:
        return True

    def observe(self, request: ProviderRequest) -> ObservationPayload:
        require_daily_scope(request)
        if request.capability is not ProviderCapability.CORPORATE_ACTIONS:
            raise ValueError(f"Eastmoney fund actions do not support {request.capability.value}")
        workers = int(request.parameters.get("max_workers", 6))
        retries = int(request.parameters.get("retries", 3))
        backoff = float(request.parameters.get("retry_backoff_seconds", 0.5))
        timeout = float(request.parameters.get("timeout_seconds", 15))
        if workers < 1 or workers > 8 or retries < 1 or backoff < 0 or timeout <= 0:
            raise ValueError("Invalid Eastmoney ETF action collection parameters")
        listed_dates = request.parameters.get("listed_dates")
        if not isinstance(listed_dates, Mapping):
            raise ValueError("ETF action requests require an exact listed_dates mapping")
        expected = set(request.instrument_ids)
        if set(map(str, listed_dates)) != expected:
            raise ValueError("ETF action listed_dates must match the requested instruments")

        rows: list[dict[str, Any]] = []
        hashes: dict[str, Mapping[str, Any]] = {}
        invalid: dict[str, tuple[str, ...]] = {}
        errors: dict[str, str] = {}
        completed: set[str] = set()
        pending: dict[str, tuple[Mapping[str, Any], ...]] = {}

        def fetch(instrument_id: str):
            listed = date.fromisoformat(str(listed_dates[instrument_id])[:10])
            return self._fetch_one(
                instrument_id,
                start=max(request.start_date, listed),
                end=request.end_date,
                retries=retries,
                backoff=backoff,
                timeout=timeout,
            )

        with ThreadPoolExecutor(
            max_workers=min(workers, len(request.instrument_ids)),
            thread_name_prefix="eastmoney-etf-actions",
        ) as executor:
            futures = {
                executor.submit(fetch, instrument_id): instrument_id
                for instrument_id in request.instrument_ids
            }
            for future in as_completed(futures):
                instrument_id = futures[future]
                try:
                    found_rows, found_hashes, issues, future_events = future.result()
                except Exception as exc:
                    errors[instrument_id] = f"{type(exc).__name__}: {str(exc)[:500]}"
                    continue
                rows.extend(found_rows)
                hashes[instrument_id] = found_hashes
                if issues:
                    invalid[instrument_id] = tuple(issues)
                else:
                    completed.add(instrument_id)
                if future_events:
                    pending[instrument_id] = tuple(future_events)

        if errors and len(errors) == len(request.instrument_ids):
            raise ObservationError(
                "Eastmoney ETF actions failed for every requested fund: "
                + "; ".join(f"{key}={value}" for key, value in sorted(errors.items()))
            )
        frame = pd.DataFrame(rows, columns=_action_columns())
        complete = completed == expected
        return ObservationPayload(
            self.name,
            now_utc(),
            request,
            {MarketTable.CORPORATE_ACTIONS: frame},
            (CoverageClaim(
                MarketTable.CORPORATE_ACTIONS,
                complete,
                request.start_date,
                request.end_date,
                request.instrument_ids,
                (
                    "Eastmoney per-fund index plus announcement lifecycle for every ETF"
                    if complete else
                    f"ETF action lifecycle complete for {len(completed)}/{len(expected)} funds"
                ),
            ),),
            {
                "upstream": (
                    "Eastmoney/Tiantian Fund public archive and announcement APIs; "
                    "hashed public copies of issuer/exchange disclosure documents "
                    "for audited archive gaps"
                ),
                "backend_group": self.backend_group,
                "transport": (
                    "direct HTTPS with hashed PDF verification; "
                    "no open-stock-data runtime dependency"
                ),
                "response_sha256": hashes,
                "request_errors": errors,
                "invalid_lifecycle": invalid,
                "known_pending_after_cutoff": pending,
                "requested_scope": {
                    "start_date": request.start_date,
                    "end_date": request.end_date,
                    "instrument_ids": request.instrument_ids,
                    "asset_type": "etf",
                },
                "split_semantics": (
                    "quantity_multiplier is total post-event units / pre-event units; "
                    "ex_date is the parsed resume/effect date"
                ),
                "parser_policy": EASTMONEY_ETF_ACTION_POLICY,
            },
        )

    def _fetch_one(
        self,
        instrument_id: str,
        *,
        start: date,
        end: date,
        retries: int,
        backoff: float,
        timeout: float,
    ) -> tuple[list[dict[str, Any]], Mapping[str, Any], list[str], list[Mapping[str, Any]]]:
        local, _ = split_instrument_id(instrument_id)
        page_url = _FUND_PAGE.format(code=local)
        text = self._retry_text(
            page_url, {}, {"User-Agent": "Mozilla/5.0"}, timeout, retries, backoff,
        )
        _require_fund_page_identity(text, local)
        page_hash = sha256(text.encode("utf-8")).hexdigest()
        summaries = _summary_events(text, start=start, end=end)

        # Some listed ETFs have complete implementation notices while the older
        # per-fund archive table is empty.  Distribution notices (category 2)
        # therefore remain a required source even when the archive has no rows.
        categories = ("2", "6") if any(item.kind == "split" for item in summaries) else ("2",)
        raw_announcements: list[Mapping[str, Any]] = []
        list_hashes: dict[str, str] = {}
        for category in categories:
            page_index = 1
            seen = 0
            while True:
                list_payload = self._retry_json(
                    _ANNOUNCEMENT_LIST,
                    {
                        "fundcode": local,
                        "pageIndex": str(page_index),
                        "pageSize": "100",
                        "type": category,
                    },
                    {
                        "User-Agent": "Mozilla/5.0",
                        "Referer": f"https://fundf10.eastmoney.com/jjgg_{local}_{category}.html",
                    },
                    timeout,
                    retries,
                    backoff,
                )
                page = list_payload.get("Data")
                if not isinstance(page, list):
                    raise ObservationError(
                        f"Eastmoney announcement list is invalid: {instrument_id}"
                    )
                list_hashes[f"type={category};page={page_index}"] = payload_hash(list_payload)
                raw_announcements.extend(
                    item for item in page if isinstance(item, Mapping)
                )
                seen += len(page)
                total = int(list_payload.get("TotalCount") or seen)
                if not page or seen >= total:
                    break
                page_index += 1
                if page_index > 100:
                    raise ObservationError(
                        f"Eastmoney announcement pagination is unbounded: {instrument_id}"
                    )
        candidates_by_id: dict[str, Mapping[str, Any]] = {}
        for item in raw_announcements:
            report_id = str(item.get("ID") or "")
            if report_id and _ACTION_TITLE.search(str(item.get("TITLE", ""))):
                candidates_by_id[report_id] = item
        candidates = list(candidates_by_id.values())
        notices: list[_Notice] = []
        content_hashes: dict[str, str] = {}
        content_transports: dict[str, str] = {}
        content_errors: dict[str, str] = {}
        supplement_hashes: dict[str, Mapping[str, Any]] = {}
        for item in candidates:
            report_id = str(item.get("ID") or "")
            if not report_id:
                continue
            pdf_url = _ANNOUNCEMENT_PDF.format(report_id=report_id)
            try:
                raw_pdf = self._retry_bytes(
                    pdf_url,
                    {},
                    {"User-Agent": "Mozilla/5.0"},
                    timeout,
                    retries,
                    backoff,
                )
                pdf_transport = "eastmoney-public-pdf"
                if not raw_pdf.startswith(b"%PDF"):
                    challenge_cookie = _pdf_challenge_cookie(raw_pdf)
                    if challenge_cookie is not None:
                        raw_pdf = self._retry_bytes(
                            pdf_url,
                            {},
                            {
                                "User-Agent": "Mozilla/5.0",
                                "Cookie": challenge_cookie,
                            },
                            timeout,
                            retries,
                            backoff,
                        )
                        pdf_transport += "-challenge-cookie"
                pdf_text = _pdf_text(raw_pdf)
                content_hashes[report_id] = sha256(raw_pdf).hexdigest()
                content_transports[report_id] = pdf_transport
                notice = _notice(
                    item,
                    {
                        "success": 1,
                        "data": {
                            "art_code": report_id,
                            "notice_content": pdf_text,
                            "notice_date": item.get("PUBLISHDATEDesc"),
                            "notice_title": item.get("TITLE"),
                        },
                    },
                    content_hashes[report_id],
                    document_transport=content_transports[report_id],
                    document_url=pdf_url,
                )
            except Exception as pdf_error:
                try:
                    payload = self._retry_json(
                        _ANNOUNCEMENT_CONTENT,
                        {"client_source": "web_fund", "show_all": "1", "art_code": report_id},
                        {"User-Agent": "Mozilla/5.0"},
                        timeout,
                        retries,
                        backoff,
                    )
                    content_hashes[report_id] = payload_hash(payload)
                    content_transports[report_id] = "eastmoney-announcement-json"
                    notice = _notice(
                        item,
                        payload,
                        content_hashes[report_id],
                        document_transport=content_transports[report_id],
                        document_url=_ANNOUNCEMENT_CONTENT,
                    )
                except Exception as content_error:
                    content_errors[report_id] = (
                        f"PDF:{type(pdf_error).__name__}:{str(pdf_error)[:160]};"
                        f"JSON:{type(content_error).__name__}:{str(content_error)[:160]}"
                    )
                    continue
            if notice is not None:
                notices.append(notice)

        notice_only_events: set[_SummaryEvent] = set()
        notice_only_split_events: set[_SummaryEvent] = set()
        for notice in notices:
            if (
                notice.cash_per_share is None
                or notice.cash_per_share <= 0
                or notice.record_date is None
                or notice.ex_date is None
                or notice.pay_date is None
                or not start <= notice.ex_date <= end
                or notice.known_date > notice.record_date
            ):
                continue
            already_indexed = any(
                event.kind == "cash" and (
                    event.event_date == notice.ex_date
                    or (
                        event.record_date == notice.record_date
                        and event.pay_date == notice.pay_date
                    )
                )
                for event in summaries
            )
            if already_indexed:
                continue
            event = _SummaryEvent(
                "cash",
                notice.ex_date,
                notice.cash_per_share,
                None,
                notice.record_date,
                notice.pay_date,
            )
            summaries.append(event)
            notice_only_events.add(event)
        indexed_splits = {
            event.event_date for event in summaries if event.kind == "split"
        }
        for notice in sorted(notices, key=lambda item: (item.known_date, item.report_id)):
            split_date = notice.split_date or notice.record_date
            if (
                notice.quantity_multiplier is None
                or notice.quantity_multiplier <= 0
                or split_date is None
                or split_date in indexed_splits
                or not start - timedelta(days=31) <= split_date <= end
                or notice.known_date > split_date
            ):
                continue
            event = _SummaryEvent(
                "split",
                split_date,
                quantity_multiplier=notice.quantity_multiplier,
            )
            summaries.append(event)
            notice_only_split_events.add(event)
            indexed_splits.add(split_date)
        summaries.sort(key=lambda item: (item.event_date, item.kind))

        rows: list[dict[str, Any]] = []
        issues: list[str] = []
        pending: list[Mapping[str, Any]] = []
        for event in summaries:
            if event.kind == "cash":
                matched = [
                    item for item in notices
                    if item.record_date is not None
                    and item.pay_date is not None
                    and item.known_date <= item.record_date
                    and (
                        item.ex_date == event.event_date
                        or (
                            item.record_date == event.record_date
                            and item.pay_date == event.pay_date
                        )
                    )
                ]
                if not matched:
                    supplement = _OFFICIAL_CASH_SUPPLEMENTS.get(
                        (instrument_id, event.event_date)
                    )
                    if supplement is not None:
                        row, documents = self._official_cash_supplement(
                            instrument_id,
                            event,
                            supplement,
                            page_hash=page_hash,
                            timeout=timeout,
                            retries=retries,
                            backoff=backoff,
                        )
                        rows.append(row)
                        supplement_hashes[f"cash:{event.event_date.isoformat()}"] = documents
                        continue
                    issues.append(f"cash_announcement_lifecycle:{event.event_date.isoformat()}")
                    continue
                notice = min(matched, key=lambda item: (item.known_date, item.report_id))
                rows.append(_cash_row(
                    instrument_id,
                    event,
                    notice,
                    page_hash,
                    economics_source=(
                        "implementation_notice"
                        if event in notice_only_events else "fund_archive"
                    ),
                ))
                continue

            arrangements = [
                item for item in notices
                if event.event_date in {item.split_date, item.record_date}
                and item.record_date is not None
                and item.known_date <= item.record_date
            ]
            if not arrangements:
                supplement = _OFFICIAL_SPLIT_SUPPLEMENTS.get(
                    (instrument_id, event.event_date)
                )
                if supplement is not None:
                    row, documents = self._official_split_supplement(
                        instrument_id,
                        event,
                        supplement,
                        page_hash=page_hash,
                        timeout=timeout,
                        retries=retries,
                        backoff=backoff,
                    )
                    rows.append(row)
                    supplement_hashes[event.event_date.isoformat()] = documents
                    continue
                issues.append(f"split_announcement_lifecycle:{event.event_date.isoformat()}")
                continue
            arrangement = min(arrangements, key=lambda item: (item.known_date, item.report_id))
            resume_dates = [
                item.resume_date for item in notices
                if item.resume_date is not None and (
                    event.event_date in {item.split_date, item.record_date}
                    or (
                        "结果" in item.title
                        and event.event_date <= item.resume_date
                        <= event.event_date + timedelta(days=7)
                    )
                )
            ]
            ex_dates = [
                item.ex_date for item in notices
                if item.ex_date is not None
                and event.event_date in {item.split_date, item.record_date}
            ]
            resume = (
                min(resume_dates)
                if resume_dates else min(ex_dates) if ex_dates else arrangement.resume_date
            )
            if resume is None:
                issues.append(f"split_resume_date:{event.event_date.isoformat()}")
                continue
            if resume < start:
                continue
            multiplier = arrangement.quantity_multiplier or event.quantity_multiplier
            if multiplier is None or event.quantity_multiplier is None:
                issues.append(f"split_multiplier:{event.event_date.isoformat()}")
                continue
            relative_difference = abs(multiplier - event.quantity_multiplier) / event.quantity_multiplier
            if relative_difference > 0.001:
                issues.append(f"split_multiplier_conflict:{event.event_date.isoformat()}")
                continue
            if resume > end:
                pending.append({
                    "kind": "split",
                    "known_date": arrangement.known_date,
                    "record_date": arrangement.record_date,
                    "split_date": event.event_date,
                    "resume_date": resume,
                    "quantity_multiplier": multiplier,
                    "report_id": arrangement.report_id,
                })
                continue
            rows.append(_split_row(
                instrument_id,
                event,
                arrangement,
                resume,
                multiplier,
                page_hash,
                economics_source=(
                    "implementation_notice"
                    if event in notice_only_split_events else "fund_archive"
                ),
            ))
        hashes = {
            "fund_archive_page": page_hash,
            "announcement_list": dict(sorted(list_hashes.items())),
            "announcement_content": dict(sorted(content_hashes.items())),
            "announcement_transport": dict(sorted(content_transports.items())),
            "announcement_errors": dict(sorted(content_errors.items())),
            "official_supplement_documents": dict(sorted(supplement_hashes.items())),
        }
        return rows, hashes, issues, pending

    def _official_split_supplement(
        self,
        instrument_id: str,
        event: _SummaryEvent,
        supplement: _OfficialSplitSupplement,
        *,
        page_hash: str,
        timeout: float,
        retries: int,
        backoff: float,
    ) -> tuple[dict[str, Any], Mapping[str, Any]]:
        relative_difference = abs(
            supplement.quantity_multiplier - float(event.quantity_multiplier or 0)
        ) / supplement.quantity_multiplier
        if relative_difference > 0.001:
            raise ObservationError(
                f"Official ETF supplement conflicts with the fund archive: {instrument_id}"
            )
        documents = self._verified_supplement_documents(
            instrument_id,
            supplement.documents,
            timeout=timeout,
            retries=retries,
            backoff=backoff,
        )
        identity = {
            "source": "audited-public-disclosure-fund-action-supplement",
            "instrument_id": instrument_id,
            "type": "split",
            "effect_date": supplement.effect_date,
            "quantity_multiplier": supplement.quantity_multiplier,
            "document_hashes": {
                url: item["sha256"] for url, item in sorted(documents.items())
            },
        }
        return ({
            "action_id": f"act-{stable_digest(identity)[:24]}",
            "instrument_id": instrument_id,
            "action_type": CorporateActionType.SPLIT.value,
            "known_date": supplement.known_date,
            "record_date": supplement.record_date,
            "ex_date": supplement.effect_date,
            "pay_date": pd.NA,
            "listing_date": supplement.effect_date,
            "cash_per_share": pd.NA,
            "share_ratio": pd.NA,
            "rights_price": pd.NA,
            "quantity_multiplier": supplement.quantity_multiplier,
            "source_payload": source_payload(
                upstream=(
                    "issuer/exchange disclosure documents in official or public archive copies"
                ),
                fund_archive_sha256=page_hash,
                raw_split_date=supplement.raw_split_date,
                canonical_effect_date=supplement.effect_date,
                evidence_policy="audited-public-disclosure-archive-gap-r2-v2",
                documents=documents,
            ),
        }, documents)

    def _official_cash_supplement(
        self,
        instrument_id: str,
        event: _SummaryEvent,
        supplement: _OfficialCashSupplement,
        *,
        page_hash: str,
        timeout: float,
        retries: int,
        backoff: float,
    ) -> tuple[dict[str, Any], Mapping[str, Any]]:
        if (
            supplement.ex_date != event.event_date
            or abs(supplement.cash_per_share - float(event.cash_per_share or 0)) > 1e-10
        ):
            raise ObservationError(
                f"Official ETF cash supplement conflicts with the fund archive: {instrument_id}"
            )
        documents = self._verified_supplement_documents(
            instrument_id,
            supplement.documents,
            timeout=timeout,
            retries=retries,
            backoff=backoff,
        )
        identity = {
            "source": "audited-public-disclosure-fund-action-supplement",
            "instrument_id": instrument_id,
            "type": "cash",
            "ex_date": supplement.ex_date,
            "cash_per_share": supplement.cash_per_share,
            "document_hashes": {
                url: item["sha256"] for url, item in sorted(documents.items())
            },
        }
        return ({
            "action_id": f"act-{stable_digest(identity)[:24]}",
            "instrument_id": instrument_id,
            "action_type": CorporateActionType.CASH_DIVIDEND.value,
            "known_date": supplement.known_date,
            "record_date": supplement.record_date,
            "ex_date": supplement.ex_date,
            "pay_date": supplement.pay_date,
            "listing_date": pd.NA,
            "cash_per_share": supplement.cash_per_share,
            "share_ratio": pd.NA,
            "rights_price": pd.NA,
            "quantity_multiplier": pd.NA,
            "source_payload": source_payload(
                upstream=(
                    "issuer/exchange disclosure documents in official or public archive copies"
                ),
                fund_archive_sha256=page_hash,
                archive_record_date=event.record_date,
                archive_pay_date=event.pay_date,
                evidence_policy="audited-public-disclosure-archive-gap-r2-v2",
                documents=documents,
            ),
        }, documents)

    def _verified_supplement_documents(
        self,
        instrument_id: str,
        source_documents: tuple[_SupplementDocument, ...],
        *,
        timeout: float,
        retries: int,
        backoff: float,
    ) -> dict[str, Any]:
        documents: dict[str, Any] = {}
        for document in source_documents:
            raw = self._retry_bytes(
                document.url,
                {},
                {"User-Agent": "Mozilla/5.0"},
                timeout,
                retries,
                backoff,
            )
            transport = "official-public-pdf"
            if not raw.startswith(b"%PDF"):
                challenge_cookie = _pdf_challenge_cookie(raw)
                if challenge_cookie is not None:
                    raw = self._retry_bytes(
                        document.url,
                        {},
                        {
                            "User-Agent": "Mozilla/5.0",
                            "Cookie": challenge_cookie,
                        },
                        timeout,
                        retries,
                        backoff,
                    )
                    transport += "-challenge-cookie"
            text = re.sub(r"\s+", "", _pdf_text(raw))
            missing = tuple(marker for marker in document.markers if marker not in text)
            if missing:
                raise ObservationError(
                    f"Official ETF supplement markers are missing: {instrument_id}/"
                    f"{','.join(missing)}"
                )
            documents[document.url] = {
                "sha256": sha256(raw).hexdigest(),
                "transport": transport,
                "verified_markers": document.markers,
            }
        return documents

    def _retry_text(self, url, parameters, headers, timeout, retries, backoff) -> str:
        return self._retry("get_text", url, parameters, headers, timeout, retries, backoff)

    def _retry_json(self, url, parameters, headers, timeout, retries, backoff) -> Mapping[str, Any]:
        return self._retry("get_json", url, parameters, headers, timeout, retries, backoff)

    def _retry_bytes(self, url, parameters, headers, timeout, retries, backoff) -> bytes:
        return self._retry("get_bytes", url, parameters, headers, timeout, retries, backoff)

    def _retry(self, method, url, parameters, headers, timeout, retries, backoff):
        last: Exception | None = None
        for attempt in range(retries):
            try:
                return getattr(self._transport, method)(
                    url, parameters=parameters, headers=headers, timeout=timeout,
                )
            except Exception as exc:
                last = exc
                if attempt + 1 < retries and backoff:
                    sleep(backoff * (2 ** attempt))
        assert last is not None
        raise last


def _summary_events(text: str, *, start: date, end: date) -> list[_SummaryEvent]:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", FutureWarning)
        try:
            tables = pd.read_html(StringIO(text), displayed_only=False)
        except ValueError as exc:
            raise ObservationError("Eastmoney fund archive contains no readable tables") from exc
    events: list[_SummaryEvent] = []
    found_action_table = False
    for frame in tables:
        columns = set(map(str, frame.columns))
        if {"权益登记日", "除息日", "每份分红", "分红发放日"} <= columns:
            found_action_table = True
            for item in frame.to_dict("records"):
                record = _date_value(item.get("权益登记日"))
                ex_date = _date_value(item.get("除息日"))
                pay = _date_value(item.get("分红发放日"))
                cash = _cash_value(item.get("每份分红"))
                if ex_date is None:
                    continue
                if start <= ex_date <= end and record is not None and pay is not None and cash is not None:
                    events.append(_SummaryEvent("cash", ex_date, cash, None, record, pay))
        if {"拆分折算日", "拆分类型", "拆分折算比例"} <= columns:
            found_action_table = True
            for item in frame.to_dict("records"):
                split_date = _date_value(item.get("拆分折算日"))
                multiplier = _multiplier(item.get("拆分折算比例"))
                if split_date is None:
                    continue
                if start <= split_date <= end and multiplier is not None and multiplier > 0:
                    events.append(_SummaryEvent("split", split_date, quantity_multiplier=multiplier))
    if not found_action_table:
        raise ObservationError("Eastmoney fund archive is missing action tables")
    return sorted(events, key=lambda item: (item.event_date, item.kind))


def _notice(
    item: Mapping[str, Any],
    payload: Mapping[str, Any],
    digest: str,
    *,
    document_transport: str,
    document_url: str,
) -> _Notice | None:
    if payload.get("success") != 1 or not isinstance(payload.get("data"), Mapping):
        return None
    data = payload["data"]
    content = str(data.get("notice_content") or "")
    compact = re.sub(r"\s+", "", content)
    known = _date_value(data.get("notice_date")) or _date_value(item.get("PUBLISHDATEDesc"))
    if known is None:
        return None
    record = _labeled_date(compact, (
        "份额拆分权益登记日", "份额折算权益登记日", "份额合并权益登记日",
        "合并权益登记日", "权益登记日", "股权登记日",
    ))
    ex_date = _labeled_date(compact, (
        "份额拆分除权日",
        "基金份额拆分除权日",
        "份额折算除权日",
        "份额合并除权日",
        "除权除息日",
        "除息日",
        "除权日",
    ))
    pay = _labeled_date(compact, ("现金红利发放日", "红利发放日", "分红发放日"))
    ordered_cash_dates = _ordered_cash_lifecycle(compact)
    if ordered_cash_dates is not None and (
        record is None or ex_date is None or pay is None
    ):
        record, ex_date, pay = ordered_cash_dates
    split_date = _labeled_date(
        compact, (
            "份额拆分日", "份额折算日", "份额折算处理日",
            "折算处理日", "拆分处理日", "合并处理日",
            "基金份额合并日", "份额合并日", "合并日",
        ),
    )
    combined = _regex_date(
        compact,
        rf"权益登记日(?:、除权日)?、(?:份额)?(?:拆分|折算|合并)(?:处理)?日"
        rf"[^0-9]{{0,12}}{_DATE_TOKEN}",
    )
    if combined is not None:
        record = record or combined
        split_date = split_date or combined
    if record is None and split_date is not None and any(
        phrase in compact for phrase in (
            "份额拆分日登记在册", "份额合并日登记在册",
            "折算处理日登记在册", "拆分处理日登记在册",
        )
    ):
        record = split_date
    resume = _labeled_date(compact, (
        "恢复交易日", "恢复上市交易日", "复牌日",
    ))
    if resume is None:
        resume = _regex_date(
            compact,
            rf"份额(?:拆分|合并|折算)(?:处理)?日的下一工作日（{_DATE_TOKEN}）",
        )
    if resume is None:
        resume = _regex_date(
            compact,
            rf"{_DATE_TOKEN}[^0-9]{{0,20}}恢复(?:二级市场|上市)?交易",
        )
    if resume is None:
        resume = _regex_date(compact, rf"{_DATE_TOKEN}[^0-9]{{0,8}}复牌")
    multiplier = _notice_multiplier(compact)
    cash_per_share = _notice_cash_per_share(compact)
    return _Notice(
        str(item.get("ID") or data.get("art_code") or ""),
        str(item.get("TITLE") or data.get("notice_title") or ""),
        known,
        digest,
        document_transport,
        document_url,
        record,
        ex_date,
        pay,
        split_date,
        resume,
        multiplier,
        cash_per_share,
    )


def _pdf_text(raw: bytes) -> str:
    if not raw.startswith(b"%PDF"):
        raise ObservationError("Eastmoney announcement fallback is not a PDF")
    try:
        from pypdf import PdfReader

        reader = PdfReader(BytesIO(raw))
        text = "\n".join(page.extract_text() or "" for page in reader.pages)
    except Exception as exc:
        raise ObservationError("Eastmoney announcement PDF cannot be parsed") from exc
    if not text.strip():
        raise ObservationError("Eastmoney announcement PDF contains no extractable text")
    return text


def _pdf_challenge_cookie(raw: bytes) -> str | None:
    """Decode Eastmoney's deterministic two-cookie PDF transport challenge."""

    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        return None
    if "EO_Bot_Ssid=" not in text or "__tst_status=" not in text:
        return None
    ssid = re.search(r"t=a\[[^]]+\]\(t,(\d+)\)", text)
    if ssid is None:
        return None
    status_parts = [
        int(value) for value in re.findall(r"\b[A-Za-z_$][\w$]*:(\d+)\b", text)
    ]
    if not status_parts:
        return None
    return (
        f"__tst_status={sum(status_parts)}; "
        f"EO_Bot_Ssid={int(ssid.group(1))}"
    )


def _cash_row(
    instrument_id: str, event: _SummaryEvent, notice: _Notice, page_hash: str,
    *,
    economics_source: str = "fund_archive",
) -> dict[str, Any]:
    canonical_ex_date = notice.ex_date or event.event_date
    identity = {
        "source": "eastmoney-fund-public",
        "instrument_id": instrument_id,
        "type": "cash",
        "ex_date": canonical_ex_date,
    }
    return {
        "action_id": f"act-{stable_digest(identity)[:24]}",
        "instrument_id": instrument_id,
        "action_type": CorporateActionType.CASH_DIVIDEND.value,
        "known_date": notice.known_date,
        "record_date": notice.record_date,
        "ex_date": canonical_ex_date,
        "pay_date": notice.pay_date,
        "listing_date": pd.NA,
        "cash_per_share": event.cash_per_share,
        "share_ratio": pd.NA,
        "rights_price": pd.NA,
        "quantity_multiplier": pd.NA,
        "source_payload": source_payload(
            upstream="Eastmoney/Tiantian Fund",
            fund_archive_sha256=page_hash,
            report_id=notice.report_id,
            announcement_sha256=notice.content_hash,
            announcement_title=notice.title,
            announcement_transport=notice.document_transport,
            announcement_url=notice.document_url,
            economics_source=economics_source,
            lifecycle_semantics=(
                "listed/on-exchange lifecycle from the implementation notice; "
                + (
                    "the same hashed notice supplies per-share economics because the archive "
                    "table has no row"
                    if economics_source == "implementation_notice" else
                    "the fund archive supplies event economics and a missing listed ex-date"
                )
            ),
            archive_record_date=event.record_date,
            archive_ex_date=event.event_date,
            archive_pay_date=event.pay_date,
            canonical_ex_semantics=(
                "implementation_notice"
                if notice.ex_date is not None
                else "archive_fallback_when_notice_has_no_listed_ex_date"
            ),
        ),
    }


def _require_fund_page_identity(text: str, local_code: str) -> None:
    title = re.search(r"<title>(.*?)</title>", text, flags=re.IGNORECASE | re.DOTALL)
    normalized = "" if title is None else re.sub(r"\s+", "", title.group(1))
    if local_code not in normalized:
        raise ObservationError(
            f"Eastmoney fund archive identity mismatch: expected {local_code}"
        )


def _split_row(
    instrument_id: str,
    event: _SummaryEvent,
    notice: _Notice,
    resume: date,
    multiplier: float,
    page_hash: str,
    *,
    economics_source: str = "fund_archive",
) -> dict[str, Any]:
    identity = {
        "source": "eastmoney-fund-public",
        "instrument_id": instrument_id,
        "type": "split",
        "effect_date": resume,
        "quantity_multiplier": multiplier,
    }
    return {
        "action_id": f"act-{stable_digest(identity)[:24]}",
        "instrument_id": instrument_id,
        "action_type": CorporateActionType.SPLIT.value,
        "known_date": notice.known_date,
        "record_date": notice.record_date,
        "ex_date": resume,
        "pay_date": pd.NA,
        "listing_date": resume,
        "cash_per_share": pd.NA,
        "share_ratio": pd.NA,
        "rights_price": pd.NA,
        "quantity_multiplier": multiplier,
        "source_payload": source_payload(
            upstream="Eastmoney/Tiantian Fund",
            fund_archive_sha256=page_hash,
            report_id=notice.report_id,
            announcement_sha256=notice.content_hash,
            announcement_title=notice.title,
            announcement_transport=notice.document_transport,
            announcement_url=notice.document_url,
            economics_source=economics_source,
            raw_split_date=event.event_date,
            canonical_effect_date=resume,
        ),
    }


def _labeled_date(text: str, labels: tuple[str, ...]) -> date | None:
    for label in labels:
        prefix = rf"{re.escape(label)}(?:（[^）]{{0,16}}）|\([^)]{{0,16}}\))?[:：]?"
        for token in (_DATE_TOKEN, _ISO_DATE_TOKEN):
            found = _regex_date(text, prefix + token)
            if found is not None:
                return found
    return None


def _regex_date(text: str, pattern: str) -> date | None:
    found = re.search(pattern, text)
    if found is None:
        return None
    year, month, day = map(int, found.groups()[-3:])
    try:
        return date(year, month, day)
    except ValueError:
        return None


def _ordered_cash_lifecycle(text: str) -> tuple[date, date, date] | None:
    """Recover dates from PDFs that extract table labels before table values."""

    start = text.find("权益登记日")
    if start < 0:
        return None
    segment = text[start:start + 400]
    if not any(
        label in segment
        for label in ("现金红利发放日", "红利发放日", "分红发放日")
    ):
        return None
    found: list[date] = []
    for match in re.finditer(_DATE_TOKEN, segment):
        try:
            found.append(date(*map(int, match.groups())))
        except ValueError:
            continue
        if len(found) == 3:
            break
    if len(found) != 3:
        return None
    record, ex_date, pay = found
    if not record <= ex_date <= pay or (pay - record).days > 60:
        return None
    return record, ex_date, pay


def _date_value(value: Any) -> date | None:
    if value is None or value is pd.NA:
        return None
    parsed = pd.to_datetime(value, errors="coerce")
    return None if pd.isna(parsed) else parsed.date()


def _cash_value(value: Any) -> float | None:
    found = re.search(r"(?:派现金)?\s*([0-9]+(?:\.[0-9]+)?)\s*元", str(value))
    return None if found is None else float(found.group(1))


def _multiplier(value: Any) -> float | None:
    text = str(value).strip().replace("：", ":")
    ratio = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*:\s*([0-9]+(?:\.[0-9]+)?)", text)
    if ratio is not None:
        denominator = float(ratio.group(1))
        return None if denominator <= 0 else float(ratio.group(2)) / denominator
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    return None if pd.isna(numeric) else float(numeric)


def _notice_multiplier(text: str) -> float | None:
    colon = re.search(
        r"(?:拆分|折算|合并)比例[^0-9]{0,12}([0-9]+(?:\.[0-9]+)?)[：:]([0-9]+(?:\.[0-9]+)?)",
        text,
    )
    if colon is not None:
        denominator = float(colon.group(1))
        return None if denominator <= 0 else float(colon.group(2)) / denominator
    direct = re.search(
        r"本次(?:基金)?份额(?:拆分|折算|合并)比例(?:为|是|[:：])([0-9]+(?:\.[0-9]+)?)",
        text,
    )
    return None if direct is None else float(direct.group(1))


def _notice_cash_per_share(text: str) -> float | None:
    """Parse a per-unit cash distribution only from an explicit `每 N 份` clause."""

    patterns = (
        r"每([0-9]+(?:\.[0-9]+)?)份(?:基金)?份额[^。；;]{0,80}?"
        r"(?:现金红利|分红|红利)[^0-9]{0,16}([0-9]+(?:\.[0-9]+)?)元",
        r"按每([0-9]+(?:\.[0-9]+)?)份(?:基金)?份额[^。；;]{0,80}?"
        r"([0-9]+(?:\.[0-9]+)?)元",
    )
    for pattern in patterns:
        found = re.search(pattern, text)
        if found is None:
            continue
        units, cash = map(float, found.groups())
        if units > 0 and cash > 0:
            return cash / units
    return None


def _action_columns() -> tuple[str, ...]:
    return (
        "action_id", "instrument_id", "action_type", "known_date", "record_date", "ex_date",
        "pay_date", "listing_date", "cash_per_share", "share_ratio", "rights_price",
        "quantity_multiplier", "source_payload",
    )
