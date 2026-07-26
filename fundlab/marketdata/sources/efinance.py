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
    JsonTransport,
    UrllibJsonTransport,
    frame_hash,
    now_utc,
    payload_hash,
    require_daily_scope,
    source_payload,
    split_instrument_id,
)


class EfinanceProvider:
    """Eastmoney history transported through the locked efinance client."""

    name = "eastmoney-efinance"
    backend_group = "eastmoney"
    capabilities = frozenset({
        ProviderCapability.DAILY_BARS_RAW,
        ProviderCapability.DAILY_BARS_ADJUSTED,
        ProviderCapability.DAILY_STATUS,
    })

    def __init__(
        self,
        *,
        client: Any | None = None,
        snapshot_transport: JsonTransport | None = None,
    ) -> None:
        self._client = client
        self._snapshot_transport = snapshot_transport or UrllibJsonTransport()

    @property
    def available(self) -> bool:
        return self._client is not None or util.find_spec("efinance") is not None

    def observe(self, request: ProviderRequest) -> ObservationPayload:
        require_daily_scope(request)
        if request.capability not in self.capabilities:
            raise ValueError(f"Efinance does not support {request.capability.value}")
        client = self._client or import_module("efinance")
        if request.capability is ProviderCapability.DAILY_STATUS:
            return self._daily_limit_status(client, request)
        mode = (
            PriceMode.RAW
            if request.capability is ProviderCapability.DAILY_BARS_RAW
            else PriceMode.ADJUSTED
        )

        fqt = 0 if mode is PriceMode.RAW else 1
        local_to_instrument = {
            split_instrument_id(item)[0]: item for item in request.instrument_ids
        }
        codes = tuple(local_to_instrument)
        response_frames: dict[str, pd.DataFrame] = {}
        errors: dict[str, str] = {}
        max_workers = int(request.parameters.get("max_workers", 4))
        retries = int(request.parameters.get("retries", 3))
        backoff = float(request.parameters.get("retry_backoff_seconds", 0.5))
        if max_workers < 1 or max_workers > 16 or retries < 1 or backoff < 0:
            raise ValueError("Invalid Efinance worker/retry parameters")

        def fetch(local_code: str):
            last: Exception | None = None
            for attempt in range(retries):
                try:
                    response = client.stock.get_quote_history(
                        stock_codes=local_code,
                        beg=request.start_date.strftime("%Y%m%d"),
                        end=request.end_date.strftime("%Y%m%d"),
                        klt=101,
                        fqt=fqt,
                    )
                    instrument_id = local_to_instrument[local_code]
                    return instrument_id, _split_response(
                        response, {local_code: instrument_id},
                    ).get(instrument_id)
                except Exception as exc:  # the client exposes requests errors directly
                    last = exc
                    if attempt + 1 < retries and backoff:
                        sleep(backoff * (2 ** attempt))
            assert last is not None
            raise last

        with ThreadPoolExecutor(max_workers=min(max_workers, len(codes))) as executor:
            futures = {executor.submit(fetch, local): local for local in codes}
            for future in as_completed(futures):
                local = futures[future]
                try:
                    instrument_id, frame = future.result()
                    if isinstance(frame, pd.DataFrame):
                        response_frames[instrument_id] = frame
                except Exception as exc:
                    errors[local_to_instrument[local]] = f"{type(exc).__name__}: {str(exc)[:500]}"
        if errors and len(errors) == len(codes) and not response_frames:
            raise ObservationError(
                "Efinance failed for every requested instrument: "
                + "; ".join(f"{key}={value}" for key, value in sorted(errors.items()))
            )
        frames: list[pd.DataFrame] = []
        hashes: dict[str, str] = {}
        completed: list[str] = []
        for instrument_id in request.instrument_ids:
            source = response_frames.get(instrument_id)
            if source is None or source.empty:
                continue
            digest = frame_hash(source)
            hashes[instrument_id] = digest
            frames.append(_normalize(source, instrument_id, mode, digest))
            completed.append(instrument_id)
        table = pd.concat(frames, ignore_index=True) if frames else _empty_daily_bars()
        complete = set(completed) == set(request.instrument_ids)
        actual_start, actual_end = _observed_dates(table)
        package_version = getattr(client, "__version__", None)
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
                "Eastmoney history via efinance for every requested instrument" if complete
                else f"Eastmoney returned rows for {len(completed)}/{len(request.instrument_ids)} instruments",
            ),),
            {
                "upstream": "Eastmoney",
                "backend_group": self.backend_group,
                "transport": "efinance.stock.get_quote_history",
                "client_version": package_version,
                "klt": 101,
                "fqt": fqt,
                "response_sha256": hashes,
                "request_errors": errors,
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

    def _daily_limit_status(
        self, client: Any, request: ProviderRequest,
    ) -> ObservationPayload:
        if (
            request.start_date != request.end_date
            or not request.parameters.get("instrument_limit_snapshot")
        ):
            raise ValueError(
                "Efinance daily_status supports only one-day instrument limit snapshots"
            )
        local_to_instrument = {
            split_instrument_id(item)[0]: item for item in request.instrument_ids
        }
        codes = tuple(local_to_instrument)
        max_workers = int(request.parameters.get("max_workers", 16))
        retries = int(request.parameters.get("retries", 3))
        backoff = float(request.parameters.get("retry_backoff_seconds", 0.25))
        if max_workers < 1 or max_workers > 16 or retries < 1 or backoff < 0:
            raise ValueError("Invalid Efinance limit-snapshot worker/retry parameters")

        def fetch(local_code: str):
            last: Exception | None = None
            for attempt in range(retries):
                try:
                    return self._snapshot_transport.get_json(
                        "https://hsmarketwg.eastmoney.com/api/SHSZQuoteSnapshot",
                        parameters={"id": local_code},
                        headers={"User-Agent": "Mozilla/5.0"},
                        timeout=float(request.parameters.get("timeout_seconds", 20)),
                    )
                except Exception as exc:
                    last = exc
                    if attempt + 1 < retries and backoff:
                        sleep(backoff * (2 ** attempt))
            assert last is not None
            raise last

        snapshots: dict[str, Mapping[str, Any]] = {}
        errors: dict[str, str] = {}
        with ThreadPoolExecutor(max_workers=min(max_workers, len(codes))) as executor:
            futures = {executor.submit(fetch, code): code for code in codes}
            for future in as_completed(futures):
                code = futures[future]
                try:
                    value = future.result()
                    if not isinstance(value, Mapping):
                        raise ObservationError("Eastmoney snapshot is not an object")
                    snapshots[code] = value
                except Exception as exc:
                    errors[local_to_instrument[code]] = (
                        f"{type(exc).__name__}: {str(exc)[:500]}"
                    )

        rows: list[dict[str, Any]] = []
        hashes: dict[str, str] = {}
        expected_date = request.end_date.isoformat()
        for code, instrument_id in local_to_instrument.items():
            payload = snapshots.get(code)
            if payload is None:
                continue
            realtime = payload.get("realtimequote")
            fivequote = payload.get("fivequote")
            if not isinstance(realtime, Mapping) or not isinstance(fivequote, Mapping):
                errors[instrument_id] = "missing_quote_sections"
                continue
            observed_date = str(realtime.get("date", ""))[:8]
            observed_date = (
                f"{observed_date[:4]}-{observed_date[4:6]}-{observed_date[6:8]}"
                if len(observed_date) == 8 else observed_date
            )
            upper = pd.to_numeric(payload.get("topprice"), errors="coerce")
            lower = pd.to_numeric(payload.get("bottomprice"), errors="coerce")
            previous = pd.to_numeric(fivequote.get("yesClosePrice"), errors="coerce")
            if observed_date != expected_date:
                errors[instrument_id] = f"stale_latest_date:{observed_date}"
                continue
            if (
                pd.isna(previous) or pd.isna(upper) or pd.isna(lower)
                or previous <= 0 or upper <= 0 or lower <= 0
            ):
                errors[instrument_id] = "missing_limit_prices"
                continue
            digest = payload_hash(payload)
            hashes[instrument_id] = digest
            rows.append(_limit_status_row(
                instrument_id, expected_date, previous, upper, lower,
                source_payload(
                    transport="efinance.stock.get_quote_snapshot",
                    response_sha256=digest,
                    snapshot_time=realtime.get("time"),
                    session_date=observed_date,
                ),
            ))
        frame = pd.DataFrame(rows, columns=_empty_daily_bars().columns)
        complete = len(rows) == len(request.instrument_ids)
        return ObservationPayload(
            self.name, now_utc(), request, {MarketTable.DAILY_BARS: frame},
            (CoverageClaim(
                MarketTable.DAILY_BARS, complete, request.start_date, request.end_date,
                request.instrument_ids,
                f"Eastmoney quote snapshot returned direct limits for {len(rows)}/{len(request.instrument_ids)} instruments",
            ),),
            {
                "upstream": "Eastmoney",
                "backend_group": self.backend_group,
                "transport": (
                    "Eastmoney SHSZQuoteSnapshot endpoint using the efinance field mapping"
                ),
                "response_sha256": hashes,
                "request_errors": errors,
                "status_fields": ("previous_close", "limit_up", "limit_down"),
            },
        )


def _split_response(
    response: Any,
    local_to_instrument: Mapping[str, str],
) -> dict[str, pd.DataFrame]:
    if isinstance(response, dict):
        found: dict[str, pd.DataFrame] = {}
        for raw_code, frame in response.items():
            local = str(raw_code).split(".", 1)[0].zfill(6)
            instrument_id = local_to_instrument.get(local) or local_to_instrument.get(str(raw_code))
            if instrument_id is not None and isinstance(frame, pd.DataFrame):
                found[instrument_id] = frame.copy()
        return found
    if not isinstance(response, pd.DataFrame):
        raise ObservationError("efinance returned neither a DataFrame nor a code-to-DataFrame mapping")
    if len(local_to_instrument) == 1:
        return {next(iter(local_to_instrument.values())): response.copy()}
    code_column = _column(response, ("股票代码", "代码"), required=False)
    if code_column is None:
        raise ObservationError("Batch efinance response has no instrument-code column")
    found = {}
    for raw_code, group in response.groupby(response[code_column].astype(str)):
        local = str(raw_code).split(".", 1)[0].zfill(6)
        instrument_id = local_to_instrument.get(local)
        if instrument_id is not None:
            found[instrument_id] = group.copy()
    return found


def _normalize(
    source: pd.DataFrame,
    instrument_id: str,
    mode: PriceMode,
    digest: str,
) -> pd.DataFrame:
    date_column = _column(source, ("日期", "date"))
    mappings = {
        "open": ("开盘", "open"),
        "high": ("最高", "high"),
        "low": ("最低", "low"),
        "close": ("收盘", "close"),
        "volume": ("成交量", "volume"),
        "amount": ("成交额", "amount"),
    }
    result = pd.DataFrame({
        "instrument_id": instrument_id,
        "session_date": pd.to_datetime(source[date_column], errors="coerce").dt.strftime("%Y-%m-%d"),
        "price_mode": mode.value,
    })
    for target, candidates in mappings.items():
        name = _column(source, candidates, required=target != "amount")
        result[target] = pd.NA if name is None else pd.to_numeric(source[name], errors="coerce")
    # Eastmoney's A-share K-line ``vol`` is reported in hands (100 shares).
    result["volume"] = pd.to_numeric(result["volume"], errors="coerce") * 100
    previous = _column(source, ("昨日收盘", "昨收", "previous_close", "preclose"), required=False)
    result["previous_close"] = (
        pd.to_numeric(source[previous], errors="coerce")
        if previous is not None else pd.to_numeric(result["close"], errors="coerce").shift(1)
    )
    result["suspended"] = pd.NA
    result["price_limit_state"] = pd.NA
    result["limit_up"] = pd.NA
    result["limit_down"] = pd.NA
    raw_records = source.to_dict("records")
    result["source_payload"] = [
        source_payload(
            transport="efinance.stock.get_quote_history",
            response_sha256=digest,
            raw=_plain_mapping(raw_records[index]),
        )
        for index in range(len(source))
    ]
    return result


def _column(frame: pd.DataFrame, candidates: tuple[str, ...], *, required: bool = True) -> str | None:
    columns = {str(item): item for item in frame.columns}
    for candidate in candidates:
        if candidate in columns:
            return columns[candidate]
    for candidate in candidates:
        for text, original in columns.items():
            if candidate in text:
                return original
    if required:
        raise ObservationError(f"efinance response is missing column variants: {candidates}")
    return None


def _plain_mapping(row: Mapping[str, Any]) -> dict[str, Any]:
    found: dict[str, Any] = {}
    for key, value in row.items():
        if value is pd.NA or pd.isna(value):
            found[str(key)] = None
        elif hasattr(value, "item"):
            found[str(key)] = value.item()
        else:
            found[str(key)] = value
    return found


def _empty_daily_bars() -> pd.DataFrame:
    return pd.DataFrame(columns=(
        "instrument_id", "session_date", "price_mode", "open", "high", "low", "close",
        "volume", "amount", "suspended", "price_limit_state", "previous_close", "limit_up",
        "limit_down", "source_payload",
    ))


def _limit_status_row(
    instrument_id: str,
    session_date: str,
    previous_close: Any,
    limit_up: Any,
    limit_down: Any,
    payload: str,
) -> dict[str, Any]:
    return {
        "instrument_id": instrument_id,
        "session_date": session_date,
        "price_mode": PriceMode.RAW.value,
        "open": pd.NA,
        "high": pd.NA,
        "low": pd.NA,
        "close": pd.NA,
        "volume": 0.0,
        "amount": pd.NA,
        "suspended": pd.NA,
        "is_st": pd.NA,
        "trade_rule_id": pd.NA,
        "trade_rule_known_date": pd.NA,
        "buy_lot": pd.NA,
        "price_tick": pd.NA,
        "sell_delay_sessions": pd.NA,
        "price_limit_state": "bounded",
        "previous_close": float(previous_close),
        "price_limit_ratio": pd.NA,
        "limit_up": float(limit_up),
        "limit_down": float(limit_down),
        "field_lineage": pd.NA,
        "source_payload": payload,
    }


def _observed_dates(frame: pd.DataFrame):
    if frame.empty:
        return None, None
    return (
        date.fromisoformat(str(frame["session_date"].min())[:10]),
        date.fromisoformat(str(frame["session_date"].max())[:10]),
    )
