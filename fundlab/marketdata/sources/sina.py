from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from importlib import import_module, util
from time import sleep
from typing import Any, Mapping

import pandas as pd

from fundlab.marketdata.contracts import (
    CoverageClaim,
    MarketTable,
    ObservationError,
    ObservationPayload,
    PriceMode,
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


class SinaEtfProvider:
    """Unadjusted ETF daily bars from Sina's public K-line history endpoint.

    AKShare is only the transport/decoder.  The immutable observation keeps Sina
    as the upstream identity and records the decoded frame hash per instrument.
    """

    name = "sina-etf"
    backend_group = "sina"
    capabilities = frozenset({ProviderCapability.DAILY_BARS_RAW})

    def __init__(self, *, client: Any | None = None) -> None:
        self._client = client

    @property
    def available(self) -> bool:
        return self._client is not None or util.find_spec("akshare") is not None

    def observe(self, request: ProviderRequest) -> ObservationPayload:
        require_daily_scope(request)
        if request.capability is not ProviderCapability.DAILY_BARS_RAW:
            raise ValueError(f"Sina ETF history does not support {request.capability.value}")
        client = self._client or import_module("akshare")
        max_workers = int(request.parameters.get("max_workers", 4))
        retries = int(request.parameters.get("retries", 3))
        backoff = float(request.parameters.get("retry_backoff_seconds", 0.5))
        if max_workers < 1 or max_workers > 8 or retries < 1 or backoff < 0:
            raise ValueError("Invalid Sina ETF worker/retry parameters")

        def fetch(instrument_id: str) -> tuple[str, pd.DataFrame]:
            local, exchange = split_instrument_id(instrument_id)
            symbol = f"{exchange.lower()}{local}"
            last: Exception | None = None
            for attempt in range(retries):
                try:
                    value = client.fund_etf_hist_sina(symbol=symbol)
                    return instrument_id, pd.DataFrame(value).copy()
                except Exception as exc:  # AKShare exposes requests/decoder failures directly.
                    last = exc
                    if attempt + 1 < retries and backoff:
                        sleep(backoff * (2 ** attempt))
            assert last is not None
            raise last

        responses: dict[str, pd.DataFrame] = {}
        errors: dict[str, str] = {}
        with ThreadPoolExecutor(
            max_workers=min(max_workers, len(request.instrument_ids)),
            thread_name_prefix="sina-etf",
        ) as executor:
            futures = {
                executor.submit(fetch, instrument_id): instrument_id
                for instrument_id in request.instrument_ids
            }
            for future in as_completed(futures):
                instrument_id = futures[future]
                try:
                    key, frame = future.result()
                    responses[key] = frame
                except Exception as exc:
                    errors[instrument_id] = f"{type(exc).__name__}: {str(exc)[:500]}"

        if errors and len(errors) == len(request.instrument_ids):
            raise ObservationError(
                "Sina ETF history failed for every requested instrument: "
                + "; ".join(f"{key}={value}" for key, value in sorted(errors.items()))
            )

        frames: list[pd.DataFrame] = []
        hashes: dict[str, str] = {}
        completed: list[str] = []
        empty: list[str] = []
        for instrument_id in request.instrument_ids:
            source = responses.get(instrument_id)
            if source is None or source.empty:
                empty.append(instrument_id)
                continue
            digest = frame_hash(source)
            normalized = _normalize(source, instrument_id, digest)
            normalized = normalized.loc[normalized["session_date"].between(
                request.start_date.isoformat(), request.end_date.isoformat(),
            )].reset_index(drop=True)
            if normalized.empty:
                empty.append(instrument_id)
                continue
            hashes[instrument_id] = digest
            frames.append(normalized)
            completed.append(instrument_id)
        table = pd.concat(frames, ignore_index=True) if frames else _empty_daily_bars()
        complete = set(completed) == set(request.instrument_ids)
        actual_start, actual_end = _observed_dates(table)
        endpoint = (
            "https://finance.sina.com.cn/realstock/company/"
            "{exchange}{code}/hisdata_klc2/klc_kl.js"
        )
        return ObservationPayload(
            self.name,
            now_utc(),
            request,
            {MarketTable.DAILY_BARS: table},
            (CoverageClaim(
                MarketTable.DAILY_BARS,
                complete,
                actual_start,
                actual_end,
                request.instrument_ids,
                (
                    "Sina returned raw ETF history for every requested instrument"
                    if complete else
                    f"Sina returned rows for {len(completed)}/{len(request.instrument_ids)} instruments"
                ),
            ),),
            {
                "upstream": "Sina Finance",
                "backend_group": self.backend_group,
                "transport": "AKShare fund_etf_hist_sina decoder over Sina public HTTP",
                "endpoint": endpoint,
                "client": "akshare",
                "client_version": getattr(client, "__version__", None),
                "response_sha256": hashes,
                "request_errors": errors,
                "empty_instrument_ids": tuple(sorted(empty)),
                "requested_scope": {
                    "start_date": request.start_date,
                    "end_date": request.end_date,
                    "instrument_ids": request.instrument_ids,
                },
                "source_units": {"volume": "share", "amount": "CNY"},
                "canonical_units": {"price": "CNY", "volume": "share", "amount": "CNY"},
                "provider_adjusted_usage": None,
            },
        )


def _normalize(source: pd.DataFrame, instrument_id: str, digest: str) -> pd.DataFrame:
    required = ("date", "open", "high", "low", "close", "volume")
    missing = [name for name in required if name not in source]
    if missing:
        raise ObservationError(
            f"Sina ETF response is missing columns for {instrument_id}: {', '.join(missing)}"
        )
    ordered = source.copy()
    ordered["date"] = pd.to_datetime(ordered["date"], errors="coerce")
    ordered = ordered.dropna(subset=["date"]).sort_values("date", kind="stable").reset_index(drop=True)
    result = pd.DataFrame({
        "instrument_id": instrument_id,
        "session_date": ordered["date"].dt.strftime("%Y-%m-%d"),
        "price_mode": PriceMode.RAW.value,
    })
    for name in ("open", "high", "low", "close", "volume"):
        result[name] = pd.to_numeric(ordered[name], errors="coerce")
    result["amount"] = (
        pd.to_numeric(ordered["amount"], errors="coerce")
        if "amount" in ordered else pd.NA
    )
    # Shift before the requested-date filter so an incremental observation retains
    # the true prior trading close when Sina returned the full history.
    result["previous_close"] = result["close"].shift(1)
    result["suspended"] = pd.NA
    result["price_limit_state"] = pd.NA
    result["limit_up"] = pd.NA
    result["limit_down"] = pd.NA
    records = ordered.to_dict("records")
    result["source_payload"] = [
        source_payload(
            transport="akshare.fund_etf_hist_sina",
            response_sha256=digest,
            raw=_plain_mapping(records[index]),
        )
        for index in range(len(ordered))
    ]
    invalid = result[list(required[1:])].isna().any(axis=1)
    return result.loc[~invalid].reset_index(drop=True)


def _plain_mapping(row: Mapping[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in row.items():
        if value is None or value is pd.NA:
            result[str(key)] = None
            continue
        missing = pd.isna(value)
        if isinstance(missing, bool) and missing:
            result[str(key)] = None
        elif hasattr(value, "item"):
            result[str(key)] = value.item()
        elif isinstance(value, (date, pd.Timestamp)):
            result[str(key)] = str(value)[:10]
        else:
            result[str(key)] = value
    return result


def _empty_daily_bars() -> pd.DataFrame:
    return pd.DataFrame(columns=(
        "instrument_id", "session_date", "price_mode", "open", "high", "low", "close",
        "volume", "amount", "suspended", "price_limit_state", "previous_close", "limit_up",
        "limit_down", "source_payload",
    ))


def _observed_dates(frame: pd.DataFrame):
    if frame.empty:
        return None, None
    return (
        date.fromisoformat(str(frame["session_date"].min())[:10]),
        date.fromisoformat(str(frame["session_date"].max())[:10]),
    )
