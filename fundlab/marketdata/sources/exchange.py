from __future__ import annotations

from datetime import date
from io import BytesIO
from importlib import import_module, util
import json
from typing import Any, Callable, Mapping
from urllib.parse import urlencode
from urllib.request import Request, urlopen
import warnings

import pandas as pd

from fundlab.marketdata.contracts import (
    CoverageClaim,
    MarketTable,
    ObservationError,
    ObservationPayload,
    ProviderCapability,
    ProviderRequest,
)
from fundlab.marketdata.sources.base import frame_hash, now_utc, source_payload


class ExchangePublicUniverseProvider:
    """Current SH/SZ stock and ETF membership from exchange-published lists."""

    name = "exchange-public"
    backend_group = "exchange-public"
    capabilities = frozenset({ProviderCapability.INSTRUMENTS})

    def __init__(self, *, client: Any | None = None) -> None:
        self._client = client

    @property
    def available(self) -> bool:
        return self._client is not None or util.find_spec("akshare") is not None

    def observe(self, request: ProviderRequest) -> ObservationPayload:
        if request.capability is not ProviderCapability.INSTRUMENTS:
            raise ValueError(f"Exchange universe does not support {request.capability.value}")
        if request.instrument_ids:
            raise ValueError("Exchange current-universe observation does not accept instrument_ids")
        exchanges = tuple(sorted({
            str(item).upper()
            for item in request.parameters.get("exchanges", ("SH", "SZ"))
        }))
        assets = tuple(sorted({
            str(item).lower()
            for item in request.parameters.get("asset_types", ("stock", "etf"))
        }))
        if not set(exchanges) <= {"SH", "SZ"} or not set(assets) <= {"stock", "etf"}:
            raise ValueError("Exchange universe supports only SH/SZ stock/ETF")
        raw_as_of = request.parameters.get("as_of_date")
        if raw_as_of is None:
            raise ValueError("Exchange universe requires parameters.as_of_date")
        as_of = date.fromisoformat(str(raw_as_of)[:10])
        client = self._client or import_module("akshare")

        rows: list[dict[str, Any]] = []
        hashes: dict[str, str] = {}
        endpoint_counts: dict[str, int] = {}
        if "stock" in assets and "SH" in exchanges:
            for endpoint, board, symbol in (
                ("sse-main-stock-list", "main", "主板A股"),
                ("sse-star-stock-list", "star", "科创板"),
            ):
                frame = _call(endpoint, lambda symbol=symbol: client.stock_info_sh_name_code(symbol=symbol))
                hashes[endpoint] = frame_hash(frame)
                endpoint_counts[endpoint] = len(frame)
                for item in frame.to_dict("records"):
                    rows.append(_row(
                        code=item.get("证券代码"),
                        name=item.get("证券简称"),
                        exchange="SH",
                        asset_type="stock",
                        listed_date=item.get("上市日期"),
                        board=board,
                        product_class=None,
                        digest=hashes[endpoint],
                        endpoint=endpoint,
                        raw=item,
                    ))
        if "stock" in assets and "SZ" in exchanges:
            endpoint = "szse-a-stock-list"
            frame = _call(endpoint, lambda: client.stock_info_sz_name_code(symbol="A股列表"))
            hashes[endpoint] = frame_hash(frame)
            endpoint_counts[endpoint] = len(frame)
            for item in frame.to_dict("records"):
                panel = str(item.get("板块") or "")
                rows.append(_row(
                    code=item.get("A股代码"),
                    name=item.get("A股简称"),
                    exchange="SZ",
                    asset_type="stock",
                    listed_date=item.get("A股上市日期"),
                    board="chinext" if "创业" in panel else "main",
                    product_class=None,
                    digest=hashes[endpoint],
                    endpoint=endpoint,
                    raw=item,
                ))
        if "etf" in assets and "SH" in exchanges:
            scale_endpoint = "sse-etf-scale-list"
            scale = _call(
                scale_endpoint,
                lambda: client.fund_etf_scale_sse(date=as_of.strftime("%Y%m%d")),
            )
            list_endpoint = "sse-current-full-etf-list"
            full = _call(
                list_endpoint,
                client.fund_etf_list_sse if self._client is not None else _fetch_sse_fund_list,
            )
            hashes[scale_endpoint] = frame_hash(scale)
            hashes[list_endpoint] = frame_hash(full)
            endpoint_counts[scale_endpoint] = len(scale)
            endpoint_counts[list_endpoint] = len(full)
            scale_ids = set(scale["基金代码"].astype(str).str.zfill(6))
            full_non_money = set(
                full.loc[
                    ~full["subClass"].astype(str).isin(("05", "07")), "fundCode",
                ].astype(str).str.zfill(6)
            )
            if scale_ids != full_non_money:
                raise ObservationError(
                    "SSE ETF scale membership disagrees with the official full ETF list: "
                    f"scale_only={len(scale_ids - full_non_money)} "
                    f"list_only={len(full_non_money - scale_ids)}"
                )
            for item in full.to_dict("records"):
                rows.append(_row(
                    code=item.get("fundCode"),
                    name=item.get("secNameFull") or item.get("fundAbbr"),
                    exchange="SH",
                    asset_type="etf",
                    listed_date=item.get("listingDate"),
                    board="main",
                    product_class=f"sse-fund-subclass-{item.get('subClass')}",
                    digest=hashes[list_endpoint],
                    endpoint=list_endpoint,
                    raw=item,
                ))
        if "etf" in assets and "SZ" in exchanges:
            daily_endpoint = "szse-etf-scale-daily"
            daily = _call(daily_endpoint, lambda: client.fund_scale_daily_szse(
                start_date=as_of.strftime("%Y%m%d"),
                end_date=as_of.strftime("%Y%m%d"),
                symbol="ETF",
            ))
            detail_endpoint = "szse-current-etf-list"
            detail = _call(
                detail_endpoint,
                client.fund_etf_scale_szse if self._client is not None else _fetch_szse_etf_list,
            )
            hashes[daily_endpoint] = frame_hash(daily)
            hashes[detail_endpoint] = frame_hash(detail)
            endpoint_counts[daily_endpoint] = len(daily)
            endpoint_counts[detail_endpoint] = len(detail)
            detail_by_code = {
                str(item.get("基金代码")).zfill(6): item
                for item in detail.loc[
                    detail["基金类别"].astype(str).eq("ETF")
                ].to_dict("records")
            }
            daily_ids = set(daily["基金代码"].astype(str).str.zfill(6))
            detail_ids = set(detail_by_code)
            if daily_ids != detail_ids:
                raise ObservationError(
                    "SZSE daily ETF membership disagrees with the official full ETF list: "
                    f"daily_only={len(daily_ids - detail_ids)} "
                    f"list_only={len(detail_ids - daily_ids)}"
                )
            for item in daily.to_dict("records"):
                code = str(item.get("基金代码")).zfill(6)
                metadata = detail_by_code.get(code, {})
                rows.append(_row(
                    code=code,
                    name=item.get("基金简称") or metadata.get("基金简称"),
                    exchange="SZ",
                    asset_type="etf",
                    listed_date=metadata.get("上市日期"),
                    board="main",
                    product_class=(
                        f"szse-{metadata.get('基金类别')}|{metadata.get('投资类别')}"
                    ),
                    digest=hashes[daily_endpoint],
                    endpoint=daily_endpoint,
                    raw={"daily": item, "detail": metadata},
                ))

        frame = pd.DataFrame(rows)
        if frame.empty:
            raise ObservationError("Exchange public universe returned no instruments")
        frame = frame.drop_duplicates("instrument_id", keep="last").sort_values(
            "instrument_id", kind="stable",
        ).reset_index(drop=True)
        complete = all(count > 0 for count in endpoint_counts.values())
        instrument_ids = tuple(map(str, frame["instrument_id"]))
        return ObservationPayload(
            self.name,
            now_utc(),
            request,
            {MarketTable.INSTRUMENTS: frame},
            (CoverageClaim(
                MarketTable.INSTRUMENTS,
                complete,
                instrument_ids=instrument_ids,
                detail=(
                    f"Exchange-published current universe as of {as_of.isoformat()}; "
                    f"endpoint_counts={endpoint_counts}"
                ),
            ),),
            {
                "upstream": "Shanghai Stock Exchange / Shenzhen Stock Exchange",
                "backend_group": self.backend_group,
                "transport": "AKShare clients over SSE/SZSE public HTTP endpoints",
                "client": "akshare",
                "client_version": getattr(client, "__version__", None),
                "as_of_date": as_of,
                "response_sha256": hashes,
                "endpoint_counts": endpoint_counts,
                "requested_scope": {"exchanges": exchanges, "asset_types": assets},
            },
        )


def _call(name: str, function: Callable[[], Any]) -> pd.DataFrame:
    try:
        value = function()
    except Exception as exc:
        raise ObservationError(f"Exchange endpoint failed ({name}): {exc}") from exc
    frame = pd.DataFrame(value)
    if frame.empty:
        raise ObservationError(f"Exchange endpoint returned no rows: {name}")
    return frame


def _fetch_sse_fund_list() -> pd.DataFrame:
    raw = _http_bytes(
        "https://query.sse.com.cn/commonSoaQuery.do",
        {
            "isPagination": "true",
            "pageHelp.pageSize": "10000",
            "pageHelp.pageNo": "1",
            "pageHelp.beginPage": "1",
            "pageHelp.cacheSize": "1",
            "pageHelp.endPage": "1",
            "pagecache": "false",
            "sqlId": "FUND_LIST",
            "fundType": "00",
            "subClass": "01,02,03,04,05,06,07,08,09,31,32,33,34,35,36,37,38",
            "order": "",
        },
        {"Referer": "https://www.sse.com.cn/", "User-Agent": "Mozilla/5.0"},
    )
    try:
        payload = json.loads(raw.decode("utf-8"))
        frame = pd.DataFrame(payload["result"])
    except (UnicodeDecodeError, json.JSONDecodeError, KeyError, TypeError) as exc:
        raise ObservationError("SSE full ETF list returned invalid UTF-8 JSON") from exc
    required = {"fundCode", "secNameFull", "listingDate", "subClass"}
    if not required <= set(frame):
        raise ObservationError("SSE full ETF list is missing required fields")
    return frame


def _fetch_szse_etf_list() -> pd.DataFrame:
    raw = _http_bytes(
        "https://fund.szse.cn/api/report/ShowReport",
        {
            "SHOWTYPE": "xlsx",
            "CATALOGID": "1000_lf",
            "TABKEY": "tab1",
            "random": "0.07610353191740105",
        },
        {
            "Referer": "https://fund.szse.cn/marketdata/fundslist/index.html",
            "User-Agent": "Mozilla/5.0",
        },
    )
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            frame = pd.read_excel(BytesIO(raw), engine="openpyxl", dtype={"基金代码": str})
    except Exception as exc:
        raise ObservationError(f"SZSE ETF list workbook is invalid: {exc}") from exc
    required = {"基金代码", "基金简称", "基金类别", "投资类别", "上市日期"}
    if not required <= set(frame):
        raise ObservationError("SZSE ETF list is missing required fields")
    return frame


def _http_bytes(url: str, parameters: Mapping[str, Any], headers: Mapping[str, str]) -> bytes:
    request = Request(
        f"{url}?{urlencode(parameters)}",
        headers=dict(headers),
        method="GET",
    )
    try:
        with urlopen(request, timeout=30) as response:  # noqa: S310 - fixed official URLs
            return response.read()
    except Exception as exc:
        raise ObservationError(f"Official exchange request failed: {url}: {exc}") from exc


def _row(
    *,
    code: Any,
    name: Any,
    exchange: str,
    asset_type: str,
    listed_date: Any,
    board: str,
    product_class: str | None,
    digest: str,
    endpoint: str,
    raw: Mapping[str, Any],
) -> dict[str, Any]:
    local = str(code).split(".", 1)[0].zfill(6)
    listed = None if listed_date is None or pd.isna(listed_date) else str(listed_date)[:10]
    return {
        "instrument_id": f"{local}.{exchange}",
        "exchange": exchange,
        "local_code": local,
        "asset_type": asset_type,
        "name": str(name or f"{local}.{exchange}"),
        "currency": "CNY",
        "listed_date": listed,
        "delisted_date": None,
        "board": board,
        "exchange_product_class": product_class,
        "buy_lot": 100,
        "price_tick": 0.001 if asset_type == "etf" else 0.01,
        "sell_delay_sessions": pd.NA,
        "price_limit_ratio": pd.NA,
        "field_lineage": source_payload(
            upstream="exchange-public",
            endpoint=endpoint,
            response_sha256=digest,
        ),
        "source_payload": source_payload(
            upstream="exchange-public",
            endpoint=endpoint,
            response_sha256=digest,
            raw=_plain(raw),
        ),
    }


def _plain(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(item) for item in value]
    if value is None or value is pd.NA:
        return None
    missing = pd.isna(value)
    if isinstance(missing, bool) and missing:
        return None
    if hasattr(value, "item"):
        return value.item()
    if isinstance(value, (date, pd.Timestamp)):
        return str(value)[:10]
    return value
