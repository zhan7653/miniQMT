from __future__ import annotations

from datetime import timedelta
from importlib import import_module, util
from typing import Any

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


class SinaCalendarProvider:
    """SH/SZ historical open sessions from Sina's public calendar dataset."""

    name = "sina-calendar"
    backend_group = "sina"
    capabilities = frozenset({ProviderCapability.TRADING_CALENDAR})

    def __init__(self, *, client: Any | None = None) -> None:
        self._client = client

    @property
    def available(self) -> bool:
        return self._client is not None or util.find_spec("akshare") is not None

    def observe(self, request: ProviderRequest) -> ObservationPayload:
        if request.capability is not ProviderCapability.TRADING_CALENDAR:
            raise ValueError(f"Sina calendar does not support {request.capability.value}")
        if request.start_date is None or request.end_date is None:
            raise ValueError("Sina calendar requires start_date and end_date")
        exchanges = tuple(sorted({
            str(item).upper()
            for item in request.parameters.get("exchanges", ("SH", "SZ"))
        }))
        if not exchanges or not set(exchanges) <= {"SH", "SZ"}:
            raise ValueError("Sina calendar supports only SH/SZ")
        client = self._client or import_module("akshare")
        try:
            raw = pd.DataFrame(client.tool_trade_date_hist_sina()).copy()
        except Exception as exc:
            raise ObservationError(f"Sina trading calendar failed: {exc}") from exc
        if raw.empty or "trade_date" not in raw:
            raise ObservationError("Sina trading calendar returned no trade_date rows")
        digest = frame_hash(raw)
        parsed = pd.to_datetime(raw["trade_date"], errors="coerce").dropna().dt.date
        if parsed.empty or min(parsed) > request.start_date or max(parsed) < request.end_date:
            raise ObservationError("Sina trading calendar does not cover the requested dates")
        open_dates = {
            value for value in parsed
            if request.start_date <= value <= request.end_date
        }
        rows = []
        current = request.start_date
        while current <= request.end_date:
            for exchange in exchanges:
                rows.append({
                    "exchange": exchange,
                    "session_date": current.isoformat(),
                    "is_open": current in open_dates,
                    "source_payload": source_payload(
                        endpoint=(
                            "https://finance.sina.com.cn/realstock/company/klc_td_sh.txt"
                        ),
                        response_sha256=digest,
                    ),
                })
            current += timedelta(days=1)
        frame = pd.DataFrame(rows)
        return ObservationPayload(
            self.name,
            now_utc(),
            request,
            {MarketTable.CALENDAR: frame},
            (CoverageClaim(
                MarketTable.CALENDAR,
                True,
                request.start_date,
                request.end_date,
                detail="Sina SH calendar expanded to exact SH/SZ civil-date states",
            ),),
            {
                "upstream": "Sina Finance",
                "backend_group": self.backend_group,
                "transport": "AKShare tool_trade_date_hist_sina decoder over Sina public HTTP",
                "client": "akshare",
                "client_version": getattr(client, "__version__", None),
                "endpoint": "https://finance.sina.com.cn/realstock/company/klc_td_sh.txt",
                "response_sha256": digest,
                "source_open_date_min": min(parsed),
                "source_open_date_max": max(parsed),
                "requested_scope": {
                    "start_date": request.start_date,
                    "end_date": request.end_date,
                    "exchanges": exchanges,
                },
            },
        )
