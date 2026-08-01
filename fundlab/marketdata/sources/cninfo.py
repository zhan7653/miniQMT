from __future__ import annotations

from base64 import b64encode
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timezone
import json
from math import ceil
from time import sleep
from time import time
from typing import Any, Mapping, Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pandas as pd

from fundlab.common.canonical import canonical_json, stable_digest
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
    frame_hash,
    now_utc,
    require_daily_scope,
    source_payload,
    split_instrument_id,
)


_DIVIDEND_ENDPOINT = "https://webapi.cninfo.com.cn/api/sysapi/p_sysapi1139"
_RIGHTS_ENDPOINT = "https://webapi.cninfo.com.cn/api/stock/p_stock2232"
_ANNOUNCEMENT_ENDPOINT = "https://www.cninfo.com.cn/new/hisAnnouncement/query"
_CNINFO_KEY = b"1234567887654321"
_ANNOUNCEMENT_POLICY_VERSION = "cninfo-corporate-actions-v1"
_DEFAULT_ANNOUNCEMENT_CATEGORIES = (
    "category_qyfpxzcs_szsh",
    "category_pg_szsh",
    "category_bcgz_szsh",
)


@dataclass(frozen=True)
class CninfoAnnouncementRecord:
    """A canonical, audit-safe pointer to one CNInfo disclosure."""

    announcement_id: str
    instrument_id: str
    announcement_time: str
    category: str
    title: str
    document_url: str


@dataclass(frozen=True)
class CninfoAnnouncementPageEvidence:
    category: str
    page_number: int
    reported_total: int
    record_count: int
    response_hash: str
    response_json: str
    check: str


@dataclass(frozen=True)
class CninfoAnnouncementScan:
    """Complete announcement-index scan, deliberately independent of observe()."""

    policy_version: str
    start_date: str
    end_date: str
    categories: tuple[str, ...]
    page_evidence: tuple[CninfoAnnouncementPageEvidence, ...]
    records: tuple[CninfoAnnouncementRecord, ...]
    affected_instrument_ids: tuple[str, ...]
    complete: bool


class CninfoPostTransport(Protocol):
    def post_json(
        self,
        url: str,
        *,
        parameters: Mapping[str, str],
        headers: Mapping[str, str],
        timeout: float,
    ) -> Mapping[str, Any]: ...


class UrllibCninfoTransport:
    def post_json(self, url, *, parameters, headers, timeout) -> Mapping[str, Any]:
        request = Request(
            f"{url}?{urlencode(parameters)}",
            data=b"",
            headers=dict(headers),
            method="POST",
        )
        try:
            with urlopen(request, timeout=timeout) as response:  # noqa: S310
                raw = response.read()
        except HTTPError as exc:
            raise ObservationError(f"CNInfo HTTP {exc.code}: {url}") from exc
        except URLError as exc:
            raise ObservationError(f"CNInfo request failed: {exc.reason}") from exc
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ObservationError("CNInfo returned invalid UTF-8 JSON") from exc
        if not isinstance(payload, Mapping):
            raise ObservationError("CNInfo response root is not an object")
        return payload

    def post_form_json(self, url, *, parameters, headers, timeout) -> Mapping[str, Any]:
        request = Request(
            url,
            data=urlencode(parameters).encode("utf-8"),
            headers={
                **dict(headers),
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            },
            method="POST",
        )
        try:
            with urlopen(request, timeout=timeout) as response:  # noqa: S310
                raw = response.read()
        except HTTPError as exc:
            raise ObservationError(f"CNInfo HTTP {exc.code}: {url}") from exc
        except URLError as exc:
            raise ObservationError(f"CNInfo request failed: {exc.reason}") from exc
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ObservationError("CNInfo returned invalid UTF-8 JSON") from exc
        if not isinstance(payload, Mapping):
            raise ObservationError("CNInfo response root is not an object")
        return payload


class CninfoPublicClient:
    """Minimal direct client for the two public CNInfo implementation APIs."""

    __version__ = "direct-v1"

    def __init__(
        self,
        *,
        transport: CninfoPostTransport | None = None,
        timeout: float = 20,
    ) -> None:
        self._transport = transport or UrllibCninfoTransport()
        self._timeout = timeout

    def stock_dividend_cninfo(self, *, symbol: str) -> pd.DataFrame:
        records = self._records(_DIVIDEND_ENDPOINT, {"scode": symbol})
        columns = {
            "F006D": "实施方案公告日期",
            "F044V": "分红类型",
            "F010N": "送股比例",
            "F011N": "转增比例",
            "F012N": "派息比例",
            "F018D": "股权登记日",
            "F020D": "除权日",
            "F023D": "派息日",
            "F025D": "股份到账日",
            "F007V": "实施方案分红说明",
            "F001V": "报告时间",
        }
        if not records:
            return pd.DataFrame(columns=tuple(columns.values()))
        if not all(isinstance(item, Mapping) for item in records):
            raise ObservationError("CNInfo dividend records have an unexpected shape")
        return (
            pd.DataFrame(records)
            .rename(columns=columns)
            .reindex(columns=columns.values())
            .sort_values("实施方案公告日期", kind="stable")
            .reset_index(drop=True)
        )

    def stock_allotment_cninfo(
        self,
        *,
        symbol: str,
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        records = self._records(_RIGHTS_ENDPOINT, {
            "scode": symbol,
            "sdate": _compact_request_date(start_date),
            "edate": _compact_request_date(end_date),
        })
        columns = (
            "公告日期", "股权登记日", "除权基准日", "配股缴款截止日",
            "配股上市日", "配股比例", "配股价格",
        )
        if not records:
            return pd.DataFrame(columns=columns)
        rows = []
        for item in records:
            if isinstance(item, Mapping):
                rows.append({
                    "公告日期": item.get("DECLAREDATE"),
                    "股权登记日": item.get("F011D"),
                    "除权基准日": item.get("F012D"),
                    "配股缴款截止日": item.get("F014D"),
                    "配股上市日": item.get("F038D"),
                    "配股比例": item.get("F004N"),
                    "配股价格": item.get("F005N"),
                })
                continue
            if not isinstance(item, (list, tuple)) or len(item) < 48:
                raise ObservationError("CNInfo rights records have an unexpected shape")
            rows.append({
                "公告日期": item[45],
                "股权登记日": item[32],
                "除权基准日": item[18],
                "配股缴款截止日": item[47],
                "配股上市日": item[46],
                "配股比例": item[9],
                "配股价格": item[8],
            })
        return pd.DataFrame(rows, columns=columns)

    def announcement_page_cninfo(
        self,
        *,
        category: str,
        start_date: str,
        end_date: str,
        page_number: int,
        page_size: int,
    ) -> Mapping[str, Any]:
        """Fetch one disclosure-index page using CNInfo's public search endpoint."""
        if page_number < 1 or page_size < 1 or page_size > 100:
            raise ValueError("Invalid CNInfo announcement page parameters")
        parameters = {
            "pageNum": str(page_number),
            "pageSize": str(page_size),
            "column": "szse",
            "tabName": "fulltext",
            "plate": "",
            "stock": "",
            "searchkey": "",
            "secid": "",
            "category": category,
            "trade": "",
            "seDate": f"{_compact_request_date(start_date)}~{_compact_request_date(end_date)}",
            "sortName": "",
            "sortType": "",
            "isHLtitle": "true",
        }
        headers = {
            "Accept": "application/json, text/plain, */*",
            "Origin": "https://www.cninfo.com.cn",
            "Referer": "https://www.cninfo.com.cn/new/commonUrl/pageOfSearch?url=disclosure/list/search",
            "User-Agent": "Mozilla/5.0",
            "X-Requested-With": "XMLHttpRequest",
        }
        post_form_json = getattr(self._transport, "post_form_json", None)
        payload = (
            post_form_json(
                _ANNOUNCEMENT_ENDPOINT,
                parameters=parameters,
                headers=headers,
                timeout=self._timeout,
            )
            if callable(post_form_json)
            else self._transport.post_json(
                _ANNOUNCEMENT_ENDPOINT,
                parameters=parameters,
                headers=headers,
                timeout=self._timeout,
            )
        )
        if not isinstance(payload, Mapping):
            raise ObservationError("CNInfo announcement response root is not an object")
        return payload

    def _records(
        self, endpoint: str, parameters: Mapping[str, str],
    ) -> list[Any]:
        payload = self._transport.post_json(
            endpoint,
            parameters=parameters,
            headers={
                "Accept": "*/*",
                "Accept-Enckey": _cninfo_token(),
                "Origin": "https://webapi.cninfo.com.cn",
                "Referer": "https://webapi.cninfo.com.cn/",
                "User-Agent": "Mozilla/5.0",
                "X-Requested-With": "XMLHttpRequest",
            },
            timeout=self._timeout,
        )
        records = payload.get("records")
        if not isinstance(records, list):
            raise ObservationError(
                f"CNInfo rejected or malformed the request: {payload.get('resultmsg', 'no records')}"
            )
        return records


def _cninfo_token(*, epoch_seconds: int | None = None) -> str:
    import pyaes

    plaintext = str(int(time()) if epoch_seconds is None else epoch_seconds).encode("ascii")
    padding = 16 - len(plaintext) % 16
    padded = plaintext + bytes([padding]) * padding
    mode = pyaes.AESModeOfOperationCBC(_CNINFO_KEY, iv=_CNINFO_KEY)
    encrypted = b"".join(
        mode.encrypt(padded[index:index + 16])
        for index in range(0, len(padded), 16)
    )
    return b64encode(encrypted).decode("ascii")


def _compact_request_date(value: str) -> str:
    text = str(value)
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:]}"
    return text


def _announcement_page_records(
    payload: Mapping[str, Any],
    *,
    category: str,
    page_number: int,
    page_size: int,
) -> tuple[int, tuple[CninfoAnnouncementRecord, ...]]:
    reported_total = payload.get("totalAnnouncement")
    if isinstance(reported_total, bool) or not isinstance(reported_total, int) or reported_total < 0:
        raise ObservationError("CNInfo announcement response has invalid totalAnnouncement")
    announcements = payload.get("announcements")
    # The live endpoint represents a valid zero-result page as JSON null.
    # Accept only that exact total=0 shape; null on a non-empty page remains a
    # systemic schema/completeness failure.
    if announcements is None and reported_total == 0:
        announcements = []
    if not isinstance(announcements, list):
        raise ObservationError("CNInfo announcement response has no announcements list")
    expected_count = min(page_size, max(0, reported_total - (page_number - 1) * page_size))
    if len(announcements) != expected_count:
        raise ObservationError(
            "CNInfo announcement page count does not match totalAnnouncement: "
            f"page={page_number}, expected={expected_count}, actual={len(announcements)}"
        )
    records: list[CninfoAnnouncementRecord] = []
    for index, announcement in enumerate(announcements):
        if not isinstance(announcement, Mapping):
            raise ObservationError(f"CNInfo announcement {index} is not an object")
        records.append(_announcement_record(announcement, category))
    return reported_total, tuple(records)


def _announcement_record(
    announcement: Mapping[str, Any], category: str,
) -> CninfoAnnouncementRecord:
    required = ("announcementId", "secCode", "announcementTime", "announcementTitle", "adjunctUrl")
    missing = [name for name in required if name not in announcement or announcement[name] is None]
    if missing:
        raise ObservationError("CNInfo announcement missing required fields: " + ", ".join(missing))
    announcement_id = str(announcement["announcementId"]).strip()
    title = str(announcement["announcementTitle"]).strip()
    adjunct_url = str(announcement["adjunctUrl"]).strip()
    if not announcement_id or not title or not adjunct_url:
        raise ObservationError("CNInfo announcement contains a blank required field")
    return CninfoAnnouncementRecord(
        announcement_id=announcement_id,
        instrument_id=_cninfo_stock_instrument_id(announcement["secCode"]),
        announcement_time=_cninfo_announcement_time(announcement["announcementTime"]),
        category=category,
        title=title,
        document_url=(
            adjunct_url
            if adjunct_url.startswith(("https://", "http://"))
            else "https://static.cninfo.com.cn/" + adjunct_url.lstrip("/")
        ),
    )


def _cninfo_stock_instrument_id(value: Any) -> str:
    if isinstance(value, bool):
        raise ObservationError("CNInfo announcement secCode is invalid")
    code = str(value).strip()
    if code.isdigit() and len(code) < 6:
        code = code.zfill(6)
    if len(code) != 6 or not code.isdigit():
        raise ObservationError("CNInfo announcement secCode is invalid")
    if code.startswith(("5", "6", "9")):
        return f"{code}.SH"
    if code.startswith(("0", "1", "2", "3")):
        return f"{code}.SZ"
    raise ObservationError(f"CNInfo announcement secCode is outside SH/SZ: {code}")


def _cninfo_announcement_time(value: Any) -> str:
    if isinstance(value, bool) or value is None:
        raise ObservationError("CNInfo announcementTime is invalid")
    try:
        if isinstance(value, (int, float)) or (isinstance(value, str) and value.strip().isdigit()):
            timestamp = float(value)
            if timestamp < 0:
                raise ValueError("negative timestamp")
            if timestamp > 100_000_000_000:
                timestamp /= 1000
            parsed = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        else:
            parsed_value = pd.Timestamp(value)
            if pd.isna(parsed_value):
                raise ValueError("not a timestamp")
            if parsed_value.tzinfo is None:
                parsed_value = parsed_value.tz_localize("UTC")
            else:
                parsed_value = parsed_value.tz_convert("UTC")
            parsed = parsed_value.to_pydatetime()
    except (TypeError, ValueError, OverflowError, OSError) as exc:
        raise ObservationError("CNInfo announcementTime is invalid") from exc
    return parsed.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _announcement_evidence(
    category: str,
    page_number: int,
    reported_total: int,
    records: tuple[CninfoAnnouncementRecord, ...],
    payload: Mapping[str, Any],
    check: str,
) -> CninfoAnnouncementPageEvidence:
    return CninfoAnnouncementPageEvidence(
        category=category,
        page_number=page_number,
        reported_total=reported_total,
        record_count=len(records),
        response_hash=stable_digest(_plain(payload)),
        response_json=canonical_json(_plain(payload)),
        check=check,
    )


def _merge_announcement_records(
    records: dict[str, CninfoAnnouncementRecord],
    additions: tuple[CninfoAnnouncementRecord, ...],
) -> None:
    for record in additions:
        existing = records.get(record.announcement_id)
        if existing is None:
            records[record.announcement_id] = record
            continue
        if (
            existing.announcement_id,
            existing.instrument_id,
            existing.announcement_time,
            existing.title,
            existing.document_url,
        ) != (
            record.announcement_id,
            record.instrument_id,
            record.announcement_time,
            record.title,
            record.document_url,
        ):
            raise ObservationError(
                "CNInfo announcement ID has conflicting payloads: " + record.announcement_id
            )
        # A disclosure can be returned by more than one category.  Preserve an
        # actionable distribution/rights classification over the generic
        # correction bucket so durable pending work is never lost after dedupe.
        category_priority = {
            "category_qyfpxzcs_szsh": 0,
            "category_pg_szsh": 1,
            "category_bcgz_szsh": 2,
        }
        if category_priority.get(record.category, 99) < category_priority.get(
            existing.category, 99,
        ):
            records[record.announcement_id] = record


class CninfoCorporateActionProvider:
    """Stock dividends, bonus shares and rights issues from CNInfo public APIs."""

    name = "cninfo-public"
    backend_group = "cninfo"
    capabilities = frozenset({ProviderCapability.CORPORATE_ACTIONS})

    def __init__(self, *, client: Any | None = None) -> None:
        self._client = client
        self._last_announcement_scan_audit: dict[str, Any] = {}

    @property
    def available(self) -> bool:
        return True

    @property
    def last_announcement_scan_audit(self) -> Mapping[str, Any]:
        """Raw successful responses and failures from the latest scan attempt."""
        return json.loads(canonical_json(self._last_announcement_scan_audit))

    def scan_announcements(
        self,
        start_date: date,
        end_date: date,
        *,
        categories: tuple[str, ...] = _DEFAULT_ANNOUNCEMENT_CATEGORIES,
        page_size: int = 30,
        retries: int = 3,
        retry_backoff_seconds: float = 0.5,
    ) -> CninfoAnnouncementScan:
        """Scan the narrow corporate-action disclosure index for a date window.

        This is intentionally not a Provider ``observe`` capability: it is an
        auditable index which identifies the small set of symbols needing the
        existing per-symbol lifecycle fetch.
        """
        if not isinstance(start_date, date) or not isinstance(end_date, date):
            raise TypeError("CNInfo announcement windows require date values")
        if start_date > end_date:
            raise ValueError("CNInfo announcement start_date must not exceed end_date")
        if not categories or len(set(categories)) != len(categories):
            raise ValueError("CNInfo announcement categories must be a non-empty unique tuple")
        if not set(_DEFAULT_ANNOUNCEMENT_CATEGORIES) <= set(categories):
            raise ValueError("CNInfo announcement scan must include all corporate-action categories")
        if page_size < 1 or page_size > 100 or retries < 1 or retry_backoff_seconds < 0:
            raise ValueError("Invalid CNInfo announcement paging/retry parameters")

        client = self._client or CninfoPublicClient()
        evidence: list[CninfoAnnouncementPageEvidence] = []
        unique: dict[str, CninfoAnnouncementRecord] = {}
        self._last_announcement_scan_audit = {
            "schema_version": 1,
            "policy_version": _ANNOUNCEMENT_POLICY_VERSION,
            "start_date": start_date,
            "end_date": end_date,
            "categories": categories,
            "page_size": page_size,
            "status": "running",
            "responses": [],
            "failures": [],
        }

        def fetch(category: str, page_number: int, check: str) -> Mapping[str, Any]:
            last: Exception | None = None
            for attempt in range(retries):
                try:
                    payload = client.announcement_page_cninfo(
                        category=category,
                        start_date=start_date.strftime("%Y%m%d"),
                        end_date=end_date.strftime("%Y%m%d"),
                        page_number=page_number,
                        page_size=page_size,
                    )
                    if not isinstance(payload, Mapping):
                        raise ObservationError("CNInfo announcement response root is not an object")
                    self._last_announcement_scan_audit["responses"].append({
                        "category": category,
                        "page_number": page_number,
                        "check": check,
                        "attempt": attempt + 1,
                        "response_hash": stable_digest(_plain(payload)),
                        "response_json": canonical_json(_plain(payload)),
                    })
                    return payload
                except Exception as exc:
                    last = exc
                    self._last_announcement_scan_audit["failures"].append({
                        "category": category,
                        "page_number": page_number,
                        "check": check,
                        "attempt": attempt + 1,
                        "error": f"{type(exc).__name__}:{str(exc)[:500]}",
                    })
                    if attempt + 1 < retries and retry_backoff_seconds:
                        sleep(retry_backoff_seconds * (2 ** attempt))
            assert last is not None
            raise ObservationError(
                f"CNInfo announcement page failed: category={category}, page={page_number}"
            ) from last

        for category in categories:
            category_records: list[CninfoAnnouncementRecord] = []
            first_payload = fetch(category, 1, "initial")
            first_total, first_records = _announcement_page_records(
                first_payload, category=category, page_number=1, page_size=page_size,
            )
            category_records.extend(first_records)
            evidence.append(_announcement_evidence(
                category, 1, first_total, first_records, first_payload, "initial",
            ))
            _merge_announcement_records(unique, first_records)

            for page_number in range(2, ceil(first_total / page_size) + 1):
                payload = fetch(category, page_number, "page")
                reported_total, records = _announcement_page_records(
                    payload, category=category, page_number=page_number, page_size=page_size,
                )
                if reported_total != first_total:
                    raise ObservationError(
                        "CNInfo announcement pagination drift: "
                        f"category={category}, page={page_number}, "
                        f"expected_total={first_total}, reported_total={reported_total}"
                    )
                category_records.extend(records)
                evidence.append(_announcement_evidence(
                    category, page_number, reported_total, records, payload, "page",
                ))
                _merge_announcement_records(unique, records)

            category_ids = {item.announcement_id for item in category_records}
            if len(category_ids) != first_total:
                raise ObservationError(
                    "CNInfo announcement pages do not contain the reported number "
                    f"of unique IDs: category={category}, expected={first_total}, "
                    f"unique={len(category_ids)}"
                )

            recheck_payload = fetch(category, 1, "recheck")
            recheck_total, recheck_records = _announcement_page_records(
                recheck_payload, category=category, page_number=1, page_size=page_size,
            )
            recheck = _announcement_evidence(
                category, 1, recheck_total, recheck_records, recheck_payload, "recheck",
            )
            evidence.append(recheck)
            initial = next(item for item in reversed(evidence[:-1])
                           if item.category == category and item.check == "initial")
            if (
                recheck_total != first_total
                or recheck.response_hash != initial.response_hash
            ):
                raise ObservationError(
                    "CNInfo announcement first-page drift: "
                    f"category={category}, initial_total={initial.reported_total}, "
                    f"recheck_total={recheck_total}"
                )

        records = tuple(sorted(
            unique.values(),
            key=lambda item: (item.announcement_time, item.announcement_id),
        ))
        self._last_announcement_scan_audit["status"] = "complete"
        self._last_announcement_scan_audit["record_count"] = len(records)
        return CninfoAnnouncementScan(
            policy_version=_ANNOUNCEMENT_POLICY_VERSION,
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            categories=tuple(categories),
            page_evidence=tuple(evidence),
            records=records,
            affected_instrument_ids=tuple(sorted({item.instrument_id for item in records})),
            complete=True,
        )

    def observe(self, request: ProviderRequest) -> ObservationPayload:
        require_daily_scope(request)
        if request.capability is not ProviderCapability.CORPORATE_ACTIONS:
            raise ValueError(f"CNInfo actions do not support {request.capability.value}")
        client = self._client or CninfoPublicClient()
        workers = int(request.parameters.get("max_workers", 4))
        retries = int(request.parameters.get("retries", 3))
        backoff = float(request.parameters.get("retry_backoff_seconds", 0.5))
        if workers < 1 or workers > 8 or retries < 1 or backoff < 0:
            raise ValueError("Invalid CNInfo worker/retry parameters")

        def fetch_with_retry(call):
            last: Exception | None = None
            for attempt in range(retries):
                try:
                    return pd.DataFrame(call()).copy()
                except Exception as exc:
                    last = exc
                    if attempt + 1 < retries and backoff:
                        sleep(backoff * (2 ** attempt))
            assert last is not None
            raise last

        def required_channels(instrument_id: str) -> tuple[bool, bool]:
            categories_by_instrument = request.parameters.get(
                "announcement_categories_by_instrument",
            )
            if (
                request.parameters.get("collection_mode") != "announcement-targeted-v2"
                or not isinstance(categories_by_instrument, Mapping)
            ):
                return True, True
            raw_categories = categories_by_instrument.get(instrument_id)
            if not isinstance(raw_categories, (list, tuple, set, frozenset)):
                return True, True
            categories = set(map(str, raw_categories))
            dividend = bool(categories & {
                "category_qyfpxzcs_szsh", "category_bcgz_szsh",
            })
            rights = bool(categories & {"category_pg_szsh", "category_bcgz_szsh"})
            return (dividend, rights) if dividend or rights else (True, True)

        def fetch(instrument_id: str):
            local, _ = split_instrument_id(instrument_id)
            fetch_dividend, fetch_rights = required_channels(instrument_id)
            dividends = (
                fetch_with_retry(lambda: client.stock_dividend_cninfo(symbol=local))
                if fetch_dividend else pd.DataFrame()
            )
            rights = (
                fetch_with_retry(lambda: client.stock_allotment_cninfo(
                    symbol=local,
                    start_date=request.start_date.strftime("%Y%m%d"),
                    end_date=request.end_date.strftime("%Y%m%d"),
                ))
                if fetch_rights else pd.DataFrame()
            )
            return instrument_id, dividends, rights

        responses: dict[str, tuple[pd.DataFrame, pd.DataFrame]] = {}
        errors: dict[str, str] = {}
        with ThreadPoolExecutor(
            max_workers=min(workers, len(request.instrument_ids)),
            thread_name_prefix="cninfo-actions",
        ) as executor:
            futures = {
                executor.submit(fetch, instrument_id): instrument_id
                for instrument_id in request.instrument_ids
            }
            for future in as_completed(futures):
                instrument_id = futures[future]
                try:
                    key, dividends, rights = future.result()
                    responses[key] = (dividends, rights)
                except Exception as exc:
                    errors[instrument_id] = f"{type(exc).__name__}: {str(exc)[:500]}"
        if errors and len(errors) == len(request.instrument_ids):
            raise ObservationError(
                "CNInfo actions failed for every requested stock: "
                + "; ".join(f"{key}={value}" for key, value in sorted(errors.items()))
            )

        rows: list[dict[str, Any]] = []
        hashes: dict[str, Mapping[str, str]] = {}
        invalid: dict[str, list[str]] = {}
        known_pending_after_cutoff: dict[str, tuple[Mapping[str, str], ...]] = {}
        completed: list[str] = []
        for instrument_id in request.instrument_ids:
            response = responses.get(instrument_id)
            if response is None:
                continue
            dividends, rights = response
            hashes[instrument_id] = {
                "dividend": frame_hash(dividends),
                "rights": frame_hash(rights),
            }
            issues: list[str] = []
            dividend_rows, dividend_issues = _dividend_actions(
                instrument_id, dividends, hashes[instrument_id]["dividend"], request,
            )
            rights_rows, rights_issues = _rights_actions(
                instrument_id, rights, hashes[instrument_id]["rights"], request,
            )
            rows.extend(dividend_rows)
            rows.extend(rights_rows)
            pending = _known_pending_after_cutoff(
                dividends, rights, hashes[instrument_id], request,
            )
            if pending:
                known_pending_after_cutoff[instrument_id] = pending
            issues.extend(dividend_issues)
            issues.extend(rights_issues)
            if issues:
                invalid[instrument_id] = issues
            else:
                completed.append(instrument_id)
        frame = pd.DataFrame(rows, columns=_action_columns())
        complete = set(completed) == set(request.instrument_ids)
        detail = (
            "CNInfo implementation announcements cover dividends/bonus shares/rights issues"
            if complete else
            f"CNInfo action lifecycle complete for {len(completed)}/{len(request.instrument_ids)} stocks"
        )
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
                detail,
            ),),
            {
                "upstream": "CNInfo (巨潮资讯)",
                "backend_group": self.backend_group,
                "transport": (
                    "direct HTTPS over CNInfo p_sysapi1139 and p_stock2232 public APIs"
                ),
                "client": "fundlab-cninfo-direct",
                "client_version": getattr(client, "__version__", None),
                "response_sha256": hashes,
                "known_pending_after_cutoff": known_pending_after_cutoff,
                "request_errors": errors,
                "invalid_lifecycle": invalid,
                "requested_scope": {
                    "start_date": request.start_date,
                    "end_date": request.end_date,
                    "instrument_ids": request.instrument_ids,
                    "asset_type": "stock",
                },
                "ratio_units": "CNInfo per-10-share values normalized to per-share",
            },
        )


def _known_pending_after_cutoff(
    dividends: pd.DataFrame,
    rights: pd.DataFrame,
    response_hashes: Mapping[str, str],
    request: ProviderRequest,
) -> tuple[Mapping[str, str], ...]:
    """Known valid lifecycle events that become effective after this observation."""
    pending: list[Mapping[str, str]] = []
    dividend_required = {
        "实施方案公告日期", "送股比例", "转增比例", "派息比例",
        "股权登记日", "除权日", "派息日", "股份到账日",
    }
    if dividend_required <= set(dividends):
        for item in dividends.to_dict("records"):
            known = _date_text(item.get("实施方案公告日期"))
            record = _date_text(item.get("股权登记日"))
            ex_date = _date_text(item.get("除权日"))
            if (
                known is None or record is None or ex_date is None
                or known > request.end_date.isoformat() or ex_date <= request.end_date.isoformat()
                or known > record or record > ex_date
            ):
                continue
            cash = _per_share(item.get("派息比例"))
            pay = _date_text(item.get("派息日"))
            if cash is not None and cash > 0 and (pay is None or pay >= ex_date):
                pending.append({
                    "kind": "cash_dividend",
                    "known_date": known,
                    "ex_date": ex_date,
                    "response_hash": response_hashes["dividend"],
                })
            shares = sum(filter(None, (
                _per_share(item.get("送股比例")), _per_share(item.get("转增比例")),
            )))
            if shares > 0:
                pending.append({
                    "kind": "stock_dividend",
                    "known_date": known,
                    "ex_date": ex_date,
                    "response_hash": response_hashes["dividend"],
                })

    rights_required = {
        "公告日期", "股权登记日", "除权基准日", "配股缴款截止日",
        "配股上市日", "配股比例", "配股价格",
    }
    if rights_required <= set(rights):
        for item in rights.to_dict("records"):
            known = _date_text(item.get("公告日期"))
            record = _date_text(item.get("股权登记日"))
            ex_date = _date_text(item.get("除权基准日"))
            pay = _date_text(item.get("配股缴款截止日"))
            listing = _date_text(item.get("配股上市日"))
            ratio = _per_share(item.get("配股比例"))
            price = _number(item.get("配股价格"))
            if (
                known is None or record is None or ex_date is None or pay is None
                or known > request.end_date.isoformat() or ex_date <= request.end_date.isoformat()
                or known > record or pay < record or ex_date < pay
                or (listing is not None and listing < ex_date)
                or ratio is None or ratio <= 0 or price is None or price <= 0
            ):
                continue
            pending.append({
                "kind": "rights_issue",
                "known_date": known,
                "ex_date": ex_date,
                "response_hash": response_hashes["rights"],
            })
    return tuple(sorted(
        pending,
        key=lambda item: (item["known_date"], item["ex_date"], item["kind"], item["response_hash"]),
    ))


def _dividend_actions(
    instrument_id: str,
    frame: pd.DataFrame,
    digest: str,
    request: ProviderRequest,
) -> tuple[list[dict[str, Any]], list[str]]:
    if frame.empty:
        return [], []
    required = {"实施方案公告日期", "送股比例", "转增比例", "派息比例", "股权登记日", "除权日", "派息日", "股份到账日"}
    if not required <= set(frame):
        return [], ["dividend_response_schema"]
    rows: list[dict[str, Any]] = []
    issues: list[str] = []
    for index, item in enumerate(frame.to_dict("records")):
        known = _date_text(item.get("实施方案公告日期"))
        record = _date_text(item.get("股权登记日"))
        ex_date = _date_text(item.get("除权日"))
        if ex_date is None or not _in_scope(ex_date, request):
            continue
        if known is None or record is None or known > record or record > ex_date:
            if _lifecycle_issue_relevant(
                instrument_id, known_date=known, ex_date=ex_date, request=request,
            ):
                issues.append(f"dividend_dates:{index}")
            continue
        cash = _per_share(item.get("派息比例"))
        shares = sum(filter(None, (
            _per_share(item.get("送股比例")),
            _per_share(item.get("转增比例")),
        )))
        base = {
            "instrument_id": instrument_id,
            "known_date": known,
            "record_date": record,
            "ex_date": ex_date,
            "rights_price": pd.NA,
            "source_payload": source_payload(
                upstream="CNInfo",
                endpoint="p_sysapi1139",
                response_sha256=digest,
                raw=_plain(item),
            ),
        }
        if cash is not None and cash > 0:
            pay = _date_text(item.get("派息日"))
            if pay is not None and pay < ex_date:
                if _lifecycle_issue_relevant(
                    instrument_id, known_date=known, ex_date=ex_date, request=request,
                ):
                    issues.append(f"cash_pay_date:{index}")
            else:
                rows.append({
                    **base,
                    "action_id": _action_id(instrument_id, "cash", ex_date),
                    "action_type": CorporateActionType.CASH_DIVIDEND.value,
                    "pay_date": pay,
                    "listing_date": pd.NA,
                    "cash_per_share": cash,
                    "share_ratio": pd.NA,
                })
        if shares > 0:
            listing = _date_text(item.get("股份到账日"))
            rows.append({
                **base,
                "action_id": _action_id(instrument_id, "stock", ex_date),
                "action_type": CorporateActionType.STOCK_DIVIDEND.value,
                "pay_date": pd.NA,
                # CNInfo may report the post-close credit date (often the record
                # date), not the first executable session.  Preserve it here;
                # action/factor reconciliation completes an execution-safe date.
                "listing_date": listing,
                "cash_per_share": pd.NA,
                "share_ratio": shares,
            })
    return rows, issues


def _rights_actions(
    instrument_id: str,
    frame: pd.DataFrame,
    digest: str,
    request: ProviderRequest,
) -> tuple[list[dict[str, Any]], list[str]]:
    if frame.empty:
        return [], []
    required = {"公告日期", "股权登记日", "除权基准日", "配股缴款截止日", "配股上市日", "配股比例", "配股价格"}
    if not required <= set(frame):
        return [], ["rights_response_schema"]
    rows: list[dict[str, Any]] = []
    issues: list[str] = []
    for index, item in enumerate(frame.to_dict("records")):
        known = _date_text(item.get("公告日期"))
        record = _date_text(item.get("股权登记日"))
        ex_date = _date_text(item.get("除权基准日"))
        pay = _date_text(item.get("配股缴款截止日"))
        listing = _date_text(item.get("配股上市日"))
        ratio = _per_share(item.get("配股比例"))
        price = _number(item.get("配股价格"))
        if ex_date is None or not _in_scope(ex_date, request):
            continue
        if (
            known is None or record is None or pay is None
            or known > record or pay < record or ex_date < pay
            or (listing is not None and listing < ex_date)
            or ratio is None or ratio <= 0 or price is None or price <= 0
        ):
            if _lifecycle_issue_relevant(
                instrument_id, known_date=known, ex_date=ex_date, request=request,
            ):
                issues.append(f"rights_lifecycle:{index}")
            continue
        rows.append({
            "action_id": _action_id(instrument_id, "rights", ex_date),
            "instrument_id": instrument_id,
            "action_type": CorporateActionType.RIGHTS_ISSUE.value,
            "known_date": known,
            "record_date": record,
            "ex_date": ex_date,
            "pay_date": pay,
            "listing_date": listing,
            "cash_per_share": pd.NA,
            "share_ratio": ratio,
            "rights_price": price,
            "source_payload": source_payload(
                upstream="CNInfo",
                endpoint="p_stock2232",
                response_sha256=digest,
                pay_date_semantics="rights subscription payment deadline",
                raw=_plain(item),
            ),
        })
    return rows, issues


def _in_scope(value: str, request: ProviderRequest) -> bool:
    return request.start_date.isoformat() <= value <= request.end_date.isoformat()


def _lifecycle_issue_relevant(
    instrument_id: str,
    *,
    known_date: str | None,
    ex_date: str | None,
    request: ProviderRequest,
) -> bool:
    validation_start = request.parameters.get("validation_start_date")
    if not isinstance(validation_start, str):
        return True
    if ex_date is not None and ex_date >= validation_start:
        return True
    dates_by_instrument = request.parameters.get("announcement_dates_by_instrument", {})
    dates = (
        dates_by_instrument.get(instrument_id, ())
        if isinstance(dates_by_instrument, Mapping) else ()
    )
    return known_date is not None and known_date in set(map(str, dates))


def _action_id(instrument_id: str, kind: str, ex_date: str) -> str:
    return "act-" + stable_digest({
        "source": "cninfo",
        "instrument_id": instrument_id,
        "kind": kind,
        "ex_date": ex_date,
    })[:24]


def _date_text(value: Any) -> str | None:
    if value is None or value is pd.NA or pd.isna(value):
        return None
    parsed = pd.to_datetime(value, errors="coerce")
    return None if pd.isna(parsed) else parsed.strftime("%Y-%m-%d")


def _number(value: Any) -> float | None:
    parsed = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    return None if pd.isna(parsed) else float(parsed)


def _per_share(value: Any) -> float | None:
    parsed = _number(value)
    return None if parsed is None else parsed / 10.0


def _plain(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _plain(item) for key, item in value.items()}
    if value is None or value is pd.NA:
        return None
    missing = pd.isna(value)
    if isinstance(missing, bool) and missing:
        return None
    if isinstance(value, (date, pd.Timestamp)):
        return str(value)[:10]
    if hasattr(value, "item"):
        return value.item()
    return value


def _action_columns() -> tuple[str, ...]:
    return (
        "action_id", "instrument_id", "action_type", "known_date", "record_date", "ex_date",
        "pay_date", "listing_date", "cash_per_share", "share_ratio", "rights_price",
        "source_payload",
    )
