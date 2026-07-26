from __future__ import annotations

from base64 import b64encode
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
import json
from time import sleep
from time import time
from typing import Any, Mapping, Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

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
    frame_hash,
    now_utc,
    require_daily_scope,
    source_payload,
    split_instrument_id,
)


_DIVIDEND_ENDPOINT = "https://webapi.cninfo.com.cn/api/sysapi/p_sysapi1139"
_RIGHTS_ENDPOINT = "https://webapi.cninfo.com.cn/api/stock/p_stock2232"
_CNINFO_KEY = b"1234567887654321"


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


class CninfoCorporateActionProvider:
    """Stock dividends, bonus shares and rights issues from CNInfo public APIs."""

    name = "cninfo-public"
    backend_group = "cninfo"
    capabilities = frozenset({ProviderCapability.CORPORATE_ACTIONS})

    def __init__(self, *, client: Any | None = None) -> None:
        self._client = client

    @property
    def available(self) -> bool:
        return True

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

        def fetch(instrument_id: str):
            local, _ = split_instrument_id(instrument_id)
            last: Exception | None = None
            for attempt in range(retries):
                try:
                    dividends = pd.DataFrame(client.stock_dividend_cninfo(symbol=local)).copy()
                    rights = pd.DataFrame(client.stock_allotment_cninfo(
                        symbol=local,
                        start_date=request.start_date.strftime("%Y%m%d"),
                        end_date=request.end_date.strftime("%Y%m%d"),
                    )).copy()
                    return instrument_id, dividends, rights
                except Exception as exc:
                    last = exc
                    if attempt + 1 < retries and backoff:
                        sleep(backoff * (2 ** attempt))
            assert last is not None
            raise last

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
