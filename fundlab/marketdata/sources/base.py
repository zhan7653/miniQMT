from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
import json
from typing import Any, Iterable, Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pandas as pd

from fundlab.common.canonical import canonical_json
from fundlab.marketdata.contracts import ObservationError, ProviderRequest


class JsonTransport(Protocol):
    def get_json(
        self,
        url: str,
        *,
        parameters: dict[str, Any],
        headers: dict[str, str],
        timeout: float,
    ) -> dict[str, Any]: ...

    def get_text(
        self,
        url: str,
        *,
        parameters: dict[str, Any],
        headers: dict[str, str],
        timeout: float,
    ) -> str: ...

    def get_bytes(
        self,
        url: str,
        *,
        parameters: dict[str, Any],
        headers: dict[str, str],
        timeout: float,
    ) -> bytes: ...


class UrllibJsonTransport:
    def get_json(
        self,
        url: str,
        *,
        parameters: dict[str, Any],
        headers: dict[str, str],
        timeout: float,
    ) -> dict[str, Any]:
        raw = self._get(url, parameters=parameters, headers=headers, timeout=timeout)
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ObservationError(f"Source returned invalid UTF-8 JSON: {url}") from exc
        if not isinstance(payload, dict):
            raise ObservationError(f"Source JSON root must be an object: {url}")
        return payload

    def get_text(
        self,
        url: str,
        *,
        parameters: dict[str, Any],
        headers: dict[str, str],
        timeout: float,
    ) -> str:
        raw = self._get(url, parameters=parameters, headers=headers, timeout=timeout)
        try:
            return raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ObservationError(f"Source returned invalid UTF-8 text: {url}") from exc

    def get_bytes(
        self,
        url: str,
        *,
        parameters: dict[str, Any],
        headers: dict[str, str],
        timeout: float,
    ) -> bytes:
        return self._get(url, parameters=parameters, headers=headers, timeout=timeout)

    @staticmethod
    def _get(
        url: str,
        *,
        parameters: dict[str, Any],
        headers: dict[str, str],
        timeout: float,
    ) -> bytes:
        query = urlencode(parameters)
        target = f"{url}?{query}" if query else url
        request = Request(target, headers=headers, method="GET")
        try:
            with urlopen(request, timeout=timeout) as response:  # noqa: S310 - explicit source URL
                raw = response.read()
        except HTTPError as exc:
            retry = exc.headers.get("Retry-After") if exc.headers else None
            detail = f" retry_after={retry}" if retry else ""
            raise ObservationError(f"Source HTTP {exc.code}:{detail} {url}") from exc
        except URLError as exc:
            raise ObservationError(f"Source request failed: {url}: {exc.reason}") from exc
        return raw


def require_daily_scope(request: ProviderRequest) -> None:
    if request.start_date is None or request.end_date is None:
        raise ValueError("Daily collection requires start_date and end_date")
    if not request.instrument_ids:
        raise ValueError("Daily collection requires explicit canonical instrument_ids")


def split_instrument_id(instrument_id: str) -> tuple[str, str]:
    local, separator, exchange = instrument_id.strip().upper().partition(".")
    if not separator or not local or exchange not in {"SH", "SZ", "BJ"}:
        raise ValueError(f"Unsupported canonical A-share instrument id: {instrument_id}")
    return local, exchange


def baostock_code(instrument_id: str) -> str:
    local, exchange = split_instrument_id(instrument_id)
    return f"{exchange.lower()}.{local}"


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def frame_hash(frame: pd.DataFrame) -> str:
    payload = frame.to_csv(index=False, lineterminator="\n").encode("utf-8")
    return sha256(payload).hexdigest()


def payload_hash(payload: Any) -> str:
    return sha256(canonical_json(_json_plain(payload)).encode("utf-8")).hexdigest()


def result_frame(result: Any) -> pd.DataFrame:
    if getattr(result, "error_code", "0") != "0":
        raise ObservationError(
            f"BaoStock query failed: {getattr(result, 'error_code', '?')} "
            f"{getattr(result, 'error_msg', '')}"
        )
    rows: list[list[Any]] = []
    while result.next():
        rows.append(result.get_row_data())
    return pd.DataFrame(rows, columns=result.fields)


def source_payload(**values: Any) -> str:
    return canonical_json(values)


def numeric(series: pd.Series | Iterable[Any]) -> pd.Series:
    return pd.to_numeric(pd.Series(series), errors="coerce").astype("Float64")


def _json_plain(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_plain(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_json_plain(item) for item in value]
    if hasattr(value, "item"):
        value = value.item()
    if value is pd.NA or (not isinstance(value, (list, dict)) and pd.isna(value)):
        return None
    return value
