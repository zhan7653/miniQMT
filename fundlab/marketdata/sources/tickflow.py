from __future__ import annotations

from datetime import datetime, time
import os
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd

from fundlab.marketdata.contracts import (
    CoverageClaim,
    MarketTable,
    ObservationPayload,
    PriceMode,
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


class TickFlowProvider:
    name = "tickflow"
    backend_group = "tickflow-unverified"
    capabilities = frozenset({
        ProviderCapability.DAILY_BARS_RAW,
        ProviderCapability.DAILY_BARS_ADJUSTED,
    })

    def __init__(
        self,
        *,
        transport: JsonTransport | None = None,
        base_url: str | None = None,
        api_key: str | None = None,
        timeout: float = 15.0,
    ) -> None:
        self.transport = transport or UrllibJsonTransport()
        self.api_key = (os.getenv("TICKFLOW_API_KEY", "") if api_key is None else api_key).strip()
        configured = os.getenv("TICKFLOW_API_URL", "").strip()
        default = "https://api.tickflow.org" if self.api_key else "https://free-api.tickflow.org"
        self.base_url = (base_url or configured or default).rstrip("/")
        self.timeout = timeout

    @property
    def available(self) -> bool:
        return True

    def observe(self, request: ProviderRequest) -> ObservationPayload:
        require_daily_scope(request)
        if request.capability not in self.capabilities:
            raise ValueError(f"TickFlow does not support {request.capability.value}")
        mode = (
            PriceMode.RAW
            if request.capability is ProviderCapability.DAILY_BARS_RAW
            else PriceMode.ADJUSTED
        )
        adjust = "none" if mode is PriceMode.RAW else "forward"
        frames: list[pd.DataFrame] = []
        hashes: dict[str, str] = {}
        completed: list[str] = []
        batch_size = int(request.parameters.get("batch_size", 100))
        if batch_size < 1 or batch_size > 100:
            raise ValueError("TickFlow batch_size must be between 1 and 100")
        common_parameters = {
            "period": "1d",
            "count": int(request.parameters.get("count", 10000)),
            "start_time": _date_ms(request.start_date, end_of_day=False),
            "end_time": _date_ms(request.end_date, end_of_day=True),
            "adjust": adjust,
        }
        for chunk in _chunks(request.instrument_ids, batch_size):
            if len(chunk) == 1:
                endpoint = "/v1/klines"
                payload = self.transport.get_json(
                    f"{self.base_url}{endpoint}",
                    parameters={"symbol": chunk[0], **common_parameters},
                    headers=self._headers(),
                    timeout=self.timeout,
                )
                compact_by_instrument = {chunk[0]: payload.get("data")}
            else:
                endpoint = "/v1/klines/batch"
                payload = self.transport.get_json(
                    f"{self.base_url}{endpoint}",
                    parameters={
                        "symbols": ",".join(chunk),
                        **common_parameters,
                    },
                    headers=self._headers(),
                    timeout=self.timeout,
                )
                data = payload.get("data")
                compact_by_instrument = data if isinstance(data, dict) else {}
            for instrument_id in chunk:
                compact = compact_by_instrument.get(instrument_id)
                hashes[instrument_id] = payload_hash(compact or {})
                frame = _compact_frame(
                    compact,
                    instrument_id,
                    mode,
                    hashes[instrument_id],
                    endpoint,
                )
                if not frame.empty:
                    frames.append(frame)
                    completed.append(instrument_id)
        table = pd.concat(frames, ignore_index=True) if frames else _empty_daily_bars()
        complete = set(completed) == set(request.instrument_ids)
        actual_start, actual_end = _observed_dates(table)
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
                "TickFlow /v1/klines response for every requested instrument" if complete
                else f"TickFlow returned rows for {len(completed)}/{len(request.instrument_ids)} instruments",
            ),),
            {
                "upstream": "TickFlow",
                "backend_group": self.backend_group,
                "transport": "REST /v1/klines or /v1/klines/batch",
                "base_url": self.base_url,
                "authenticated": bool(self.api_key),
                "adjust": adjust,
                "response_sha256": hashes,
                "requested_scope": {
                    "start_date": request.start_date,
                    "end_date": request.end_date,
                    "instrument_ids": request.instrument_ids,
                },
                "source_units": {"volume": "lot_100_shares", "amount": "CNY"},
                "canonical_units": {"price": "CNY", "volume": "share", "amount": "CNY"},
                "provider_adjusted_usage": "audit_only" if mode is PriceMode.ADJUSTED else None,
            },
        )

    def _headers(self) -> dict[str, str]:
        headers = {"Accept": "application/json", "User-Agent": "FundLab/0.1"}
        if self.api_key:
            headers["x-api-key"] = self.api_key
        return headers


def _date_ms(value, *, end_of_day: bool) -> int:
    wall_time = time.max if end_of_day else time.min
    # TickFlow daily timestamps identify China exchange sessions at midnight in
    # Asia/Shanghai.  UTC civil-day boundaries would omit the requested first
    # session and can include the following local session.
    return int(
        datetime.combine(
            value, wall_time, tzinfo=ZoneInfo("Asia/Shanghai"),
        ).timestamp() * 1000
    )


def _compact_frame(
    data: Any,
    instrument_id: str,
    mode: PriceMode,
    response_hash: str,
    endpoint: str,
) -> pd.DataFrame:
    if not isinstance(data, dict):
        return _empty_daily_bars()
    timestamps = data.get("timestamp") or []
    length = len(timestamps)
    if not length:
        return _empty_daily_bars()
    raw_columns: dict[str, list[Any]] = {}
    for name in ("open", "high", "low", "close", "volume", "amount"):
        values = list(data.get(name) or ())
        raw_columns[name] = (values + [None] * length)[:length]
    frame = pd.DataFrame({
        "instrument_id": instrument_id,
        # TickFlow may encode a China trading session as midnight Asia/Shanghai
        # (16:00 UTC on the previous civil day).  Session identity is an exchange-local
        # date, so formatting the UTC timestamp directly shifts part of the history by
        # one day and can even create apparent Sunday bars.
        "session_date": (
            pd.to_datetime(timestamps, unit="ms", utc=True)
            .tz_convert("Asia/Shanghai")
            .strftime("%Y-%m-%d")
        ),
        "price_mode": mode.value,
        **raw_columns,
    })
    frame["volume"] = pd.to_numeric(frame["volume"], errors="coerce") * 100
    previous_values = list(data.get("prev_close") or ())
    if previous_values:
        previous_values = (previous_values + [None] * length)[:length]
        frame["previous_close"] = pd.to_numeric(previous_values, errors="coerce")
    else:
        frame["previous_close"] = pd.to_numeric(frame["close"], errors="coerce").shift(1)
    frame["suspended"] = pd.NA
    frame["price_limit_state"] = pd.NA
    frame["limit_up"] = pd.NA
    frame["limit_down"] = pd.NA
    frame["source_payload"] = [
        source_payload(
            endpoint=endpoint,
            response_sha256=response_hash,
            timestamp=int(timestamps[index]),
            raw={name: raw_columns[name][index] for name in raw_columns},
        )
        for index in range(length)
    ]
    return frame


def _empty_daily_bars() -> pd.DataFrame:
    return pd.DataFrame(columns=(
        "instrument_id", "session_date", "price_mode", "open", "high", "low", "close",
        "volume", "amount", "suspended", "price_limit_state", "previous_close", "limit_up",
        "limit_down", "source_payload",
    ))


def _observed_dates(frame: pd.DataFrame):
    if frame.empty:
        return None, None
    from datetime import date

    return (
        date.fromisoformat(str(frame["session_date"].min())[:10]),
        date.fromisoformat(str(frame["session_date"].max())[:10]),
    )


def _chunks(values: tuple[str, ...], size: int):
    for index in range(0, len(values), size):
        yield values[index:index + size]
