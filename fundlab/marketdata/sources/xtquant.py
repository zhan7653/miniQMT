from __future__ import annotations

from datetime import date
from importlib import import_module, util
from typing import Any, Mapping

import pandas as pd

from fundlab.common.canonical import stable_digest
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
)


class XtQuantProvider:
    """Canonical adapter for the explicitly installed local MiniQMT/xtquant service."""

    name = "xtquant"
    backend_group = "xtquant"
    capabilities = frozenset({
        ProviderCapability.DAILY_BARS_RAW,
        ProviderCapability.DAILY_BARS_ADJUSTED,
        ProviderCapability.ADJUSTMENT_FACTORS,
        ProviderCapability.DAILY_STATUS,
    })

    def __init__(self, *, client: Any | None = None) -> None:
        self._client = client

    @property
    def available(self) -> bool:
        return self._client is not None or util.find_spec("xtquant") is not None

    def observe(self, request: ProviderRequest) -> ObservationPayload:
        require_daily_scope(request)
        if request.capability not in self.capabilities:
            raise ValueError(f"xtquant does not support {request.capability.value}")
        xtdata = self._client or import_module("xtquant.xtdata")
        if hasattr(xtdata, "enable_hello"):
            xtdata.enable_hello = False
        if request.capability is ProviderCapability.ADJUSTMENT_FACTORS:
            return self._adjustment_factors(xtdata, request)
        if request.capability is ProviderCapability.DAILY_STATUS:
            return self._daily_status(xtdata, request)
        mode = (
            PriceMode.RAW
            if request.capability is ProviderCapability.DAILY_BARS_RAW
            else PriceMode.ADJUSTED
        )
        dividend_type = "none" if mode is PriceMode.RAW else "front"
        start = request.start_date.strftime("%Y%m%d")
        end = request.end_date.strftime("%Y%m%d")
        downloaded = bool(request.parameters.get("download", False))
        download_result: Any = None
        if downloaded:
            try:
                download_result = xtdata.download_history_data2(
                    list(request.instrument_ids),
                    "1d",
                    start,
                    end,
                    incrementally=bool(request.parameters.get("incrementally", False)),
                )
            except Exception as exc:
                raise ObservationError(f"xtquant history download failed: {exc}") from exc
        try:
            response = xtdata.get_market_data_ex(
                field_list=[],
                stock_list=list(request.instrument_ids),
                period="1d",
                start_time=start,
                end_time=end,
                count=-1,
                dividend_type=dividend_type,
                fill_data=False,
            )
        except Exception as exc:
            raise ObservationError(f"xtquant history read failed: {exc}") from exc
        if not isinstance(response, Mapping):
            raise ObservationError("xtquant returned no symbol-to-frame mapping")

        frames: list[pd.DataFrame] = []
        hashes: dict[str, str] = {}
        completed: list[str] = []
        for instrument_id in request.instrument_ids:
            raw = response.get(instrument_id)
            if raw is None:
                continue
            source = pd.DataFrame(raw).copy()
            if source.empty:
                continue
            digest = frame_hash(source.reset_index())
            normalized = _normalize(source, instrument_id, mode, digest)
            if normalized.empty:
                continue
            hashes[instrument_id] = digest
            frames.append(normalized)
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
                (
                    "Local MiniQMT cache returned every requested instrument"
                    if complete
                    else f"Local MiniQMT cache returned {len(completed)}/{len(request.instrument_ids)} instruments"
                ),
            ),),
            {
                "upstream": "MiniQMT/xtquant",
                "backend_group": self.backend_group,
                "transport": "xtquant.xtdata.download_history_data2/get_market_data_ex",
                "download_requested": downloaded,
                "download_result": download_result,
                "dividend_type": dividend_type,
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

    def _adjustment_factors(
        self, xtdata: Any, request: ProviderRequest,
    ) -> ObservationPayload:
        rows: list[dict[str, Any]] = []
        hashes: dict[str, str] = {}
        errors: dict[str, str] = {}
        completed: list[str] = []
        for instrument_id in request.instrument_ids:
            try:
                raw = pd.DataFrame(xtdata.get_divid_factors(
                    instrument_id,
                    request.start_date.strftime("%Y%m%d"),
                    request.end_date.strftime("%Y%m%d"),
                )).copy()
                hashes[instrument_id] = frame_hash(raw)
                invalid = False
                for item in raw.to_dict("records"):
                    effective = _china_date(item.get("time"))
                    dr = pd.to_numeric(pd.Series([item.get("dr")]), errors="coerce").iloc[0]
                    if effective is None or pd.isna(dr) or float(dr) <= 0:
                        invalid = True
                        continue
                    if not request.start_date <= effective <= request.end_date:
                        continue
                    multiplier = 1.0 / float(dr)
                    identity = {
                        "source": "xtquant",
                        "instrument_id": instrument_id,
                        "effective_date": effective,
                        "price_multiplier": multiplier,
                    }
                    rows.append({
                        "factor_id": f"factor-{stable_digest(identity)[:24]}",
                        "instrument_id": instrument_id,
                        "effective_date": effective.isoformat(),
                        # The local effect feed has no announcement timestamp.  This
                        # conservative bound prevents it from appearing before effect.
                        "known_date": effective.isoformat(),
                        "price_multiplier": multiplier,
                        "source_payload": source_payload(
                            transport="xtquant.xtdata.get_divid_factors",
                            response_sha256=hashes[instrument_id],
                            raw=_plain(item),
                        ),
                    })
                if invalid:
                    errors[instrument_id] = "invalid_dividend_factor_row"
                else:
                    completed.append(instrument_id)
            except Exception as exc:
                errors[instrument_id] = f"{type(exc).__name__}: {str(exc)[:500]}"
        if errors and len(errors) == len(request.instrument_ids) and not completed:
            raise ObservationError(
                "xtquant adjustment factors failed for every instrument: "
                + "; ".join(f"{key}={value}" for key, value in sorted(errors.items()))
            )
        frame = pd.DataFrame(rows, columns=(
            "factor_id", "instrument_id", "effective_date", "known_date",
            "price_multiplier", "source_payload",
        ))
        complete = set(completed) == set(request.instrument_ids)
        return ObservationPayload(
            self.name,
            now_utc(),
            request,
            {MarketTable.ADJUSTMENT_FACTORS: frame},
            (CoverageClaim(
                MarketTable.ADJUSTMENT_FACTORS,
                complete,
                request.start_date,
                request.end_date,
                request.instrument_ids,
                (
                    "Local MiniQMT corporate-action effects converted from dr to event ratios"
                    if complete else
                    f"MiniQMT factors complete for {len(completed)}/{len(request.instrument_ids)} instruments"
                ),
            ),),
            {
                "upstream": "MiniQMT/xtquant",
                "backend_group": self.backend_group,
                "transport": "xtquant.xtdata.get_divid_factors",
                "response_sha256": hashes,
                "request_errors": errors,
                "factor_derivation": "price_multiplier=1/dr",
                "known_date_semantics": "conservative effective-date bound",
                "requested_scope": {
                    "start_date": request.start_date,
                    "end_date": request.end_date,
                    "instrument_ids": request.instrument_ids,
                },
            },
        )

    def _daily_status(
        self, xtdata: Any, request: ProviderRequest,
    ) -> ObservationPayload:
        if request.parameters.get("instrument_limit_snapshot"):
            return self._instrument_limit_status(xtdata, request)
        frames: list[pd.DataFrame] = []
        hashes: dict[str, str] = {}
        errors: dict[str, str] = {}
        completed: list[str] = []
        start = request.start_date.strftime("%Y%m%d")
        end = request.end_date.strftime("%Y%m%d")
        for instrument_id in request.instrument_ids:
            try:
                # Query one instrument at a time.  In a mixed-listing-date batch,
                # xtquant fill_data otherwise creates pre-listing suspension rows
                # back to the earliest instrument in the batch.
                response = xtdata.get_market_data_ex(
                    field_list=[],
                    stock_list=[instrument_id],
                    period="1d",
                    start_time=start,
                    end_time=end,
                    count=-1,
                    dividend_type="none",
                    fill_data=True,
                )
                raw = None if not isinstance(response, Mapping) else response.get(instrument_id)
                source = pd.DataFrame(raw).copy()
                if source.empty:
                    errors[instrument_id] = "empty_local_status"
                    continue
                digest = frame_hash(source.reset_index())
                normalized = _normalize(source, instrument_id, PriceMode.RAW, digest)
                normalized["is_st"] = pd.NA
                hashes[instrument_id] = digest
                frames.append(normalized)
                completed.append(instrument_id)
            except Exception as exc:
                errors[instrument_id] = f"{type(exc).__name__}: {str(exc)[:500]}"
        hard_errors = {
            instrument_id: detail for instrument_id, detail in errors.items()
            if detail != "empty_local_status"
        }
        if hard_errors and len(hard_errors) == len(request.instrument_ids) and not frames:
            raise ObservationError(
                "xtquant daily status failed for every instrument: "
                + "; ".join(f"{key}={value}" for key, value in sorted(hard_errors.items()))
            )
        frame = pd.concat(frames, ignore_index=True) if frames else _empty_daily_bars()
        complete = set(completed) == set(request.instrument_ids)
        return ObservationPayload(
            self.name,
            now_utc(),
            request,
            {MarketTable.DAILY_BARS: frame},
            (CoverageClaim(
                MarketTable.DAILY_BARS,
                complete,
                request.start_date,
                request.end_date,
                request.instrument_ids,
                (
                    "Local MiniQMT fill_data status returned every requested instrument"
                    if complete else
                    f"MiniQMT status returned {len(completed)}/{len(request.instrument_ids)} instruments"
                ),
            ),),
            {
                "upstream": "MiniQMT/xtquant",
                "backend_group": self.backend_group,
                "transport": "xtquant.xtdata.get_market_data_ex",
                "fill_data": True,
                "per_instrument_reads": True,
                "response_sha256": hashes,
                "request_errors": errors,
                "status_fields": ("suspended", "previous_close"),
                "st_state": "not_provided",
                "requested_scope": {
                    "start_date": request.start_date,
                    "end_date": request.end_date,
                    "instrument_ids": request.instrument_ids,
                },
            },
        )

    def _instrument_limit_status(
        self, xtdata: Any, request: ProviderRequest,
    ) -> ObservationPayload:
        if request.start_date != request.end_date:
            raise ValueError("xtquant instrument limit snapshot must cover one date")
        expected = request.end_date.strftime("%Y%m%d")
        rows: list[dict[str, Any]] = []
        hashes: dict[str, str] = {}
        errors: dict[str, str] = {}
        for instrument_id in request.instrument_ids:
            try:
                detail = xtdata.get_instrument_detail(instrument_id, True)
                if not isinstance(detail, Mapping):
                    raise ObservationError("empty instrument detail")
                trading_day = str(detail.get("TradingDay", "")).replace("-", "")[:8]
                upper = pd.to_numeric(detail.get("UpStopPrice"), errors="coerce")
                lower = pd.to_numeric(detail.get("DownStopPrice"), errors="coerce")
                previous = pd.to_numeric(detail.get("PreClose"), errors="coerce")
                if trading_day != expected:
                    raise ObservationError(f"stale TradingDay {trading_day}")
                if pd.isna(upper) or pd.isna(lower) or upper <= 0 or lower <= 0:
                    raise ObservationError("missing limit prices")
                digest = stable_digest(_plain(detail))
                hashes[instrument_id] = digest
                rows.append({
                    "instrument_id": instrument_id,
                    "session_date": request.end_date.isoformat(),
                    "price_mode": PriceMode.RAW.value,
                    "open": pd.NA, "high": pd.NA, "low": pd.NA, "close": pd.NA,
                    "volume": 0.0, "amount": pd.NA, "suspended": pd.NA,
                    "is_st": pd.NA, "trade_rule_id": pd.NA,
                    "trade_rule_known_date": pd.NA, "buy_lot": pd.NA,
                    "price_tick": detail.get("PriceTick"),
                    "sell_delay_sessions": pd.NA,
                    "price_limit_state": "bounded",
                    "previous_close": float(previous), "price_limit_ratio": pd.NA,
                    "limit_up": float(upper), "limit_down": float(lower),
                    "field_lineage": pd.NA,
                    "source_payload": source_payload(
                        transport="xtquant.xtdata.get_instrument_detail",
                        response_sha256=digest,
                        trading_day=trading_day,
                    ),
                })
            except Exception as exc:
                errors[instrument_id] = f"{type(exc).__name__}: {str(exc)[:500]}"
        frame = pd.DataFrame(rows, columns=_empty_daily_bars().columns)
        complete = len(rows) == len(request.instrument_ids)
        return ObservationPayload(
            self.name, now_utc(), request, {MarketTable.DAILY_BARS: frame},
            (CoverageClaim(
                MarketTable.DAILY_BARS, complete, request.start_date, request.end_date,
                request.instrument_ids,
                f"MiniQMT instrument detail returned direct limits for {len(rows)}/{len(request.instrument_ids)} instruments",
            ),),
            {
                "upstream": "MiniQMT/xtquant",
                "backend_group": self.backend_group,
                "transport": "xtquant.xtdata.get_instrument_detail",
                "response_sha256": hashes,
                "request_errors": errors,
                "status_fields": ("previous_close", "limit_up", "limit_down"),
            },
        )


def _normalize(
    source: pd.DataFrame,
    instrument_id: str,
    mode: PriceMode,
    digest: str,
) -> pd.DataFrame:
    frame = source.copy()
    index_dates = pd.Series(frame.index, index=frame.index)
    if index_dates.astype(str).str.match(r"^\d{8}$").all():
        dates = pd.to_datetime(index_dates.astype(str), format="%Y%m%d", errors="coerce")
    elif "time" in frame:
        raw_time = pd.to_numeric(frame["time"], errors="coerce")
        dates = pd.to_datetime(raw_time, unit="ms", errors="coerce")
    else:
        dates = pd.to_datetime(index_dates, errors="coerce")
    suspended_column = next(
        (name for name in ("suspendFlag", "suspend_flag", "suspended") if name in frame),
        None,
    )
    if suspended_column is None:
        raise ObservationError("xtquant daily bars are missing suspension evidence")
    suspended = frame[suspended_column].map(_suspended)
    volume_name = "volume" if "volume" in frame else "vol" if "vol" in frame else None
    if volume_name is None:
        raise ObservationError("xtquant daily bars are missing volume")
    volume = pd.to_numeric(frame[volume_name], errors="coerce") * 100
    volume = volume.mask(suspended & volume.isna(), 0)
    result = pd.DataFrame({
        "instrument_id": instrument_id,
        "session_date": dates.dt.strftime("%Y-%m-%d").to_numpy(),
        "price_mode": mode.value,
        "open": pd.to_numeric(frame.get("open"), errors="coerce").to_numpy(),
        "high": pd.to_numeric(frame.get("high"), errors="coerce").to_numpy(),
        "low": pd.to_numeric(frame.get("low"), errors="coerce").to_numpy(),
        "close": pd.to_numeric(frame.get("close"), errors="coerce").to_numpy(),
        "volume": volume.to_numpy(),
        "amount": pd.to_numeric(frame.get("amount"), errors="coerce").to_numpy(),
        "suspended": suspended.to_numpy(),
        "price_limit_state": pd.NA,
        "previous_close": pd.to_numeric(
            frame.get("preClose", frame.get("pre_close")), errors="coerce",
        ).to_numpy(),
        "limit_up": pd.NA,
        "limit_down": pd.NA,
    })
    raw_records = frame.reset_index(drop=False).to_dict("records")
    result["source_payload"] = [
        source_payload(
            transport="xtquant.xtdata.get_market_data_ex",
            response_sha256=digest,
            raw=_plain(raw_records[index]),
        )
        for index in range(len(result))
    ]
    return result.loc[result["session_date"].notna()].reset_index(drop=True)


def _suspended(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None or pd.isna(value):
        raise ObservationError("xtquant suspension field contains a missing value")
    normalized = str(value).strip().lower()
    if normalized in {"0", "false", "normal", "交易"}:
        return False
    if normalized in {"1", "true", "suspended", "停牌"}:
        return True
    raise ObservationError(f"Unsupported xtquant suspension value: {value!r}")


def _plain(row: Mapping[str, Any]) -> dict[str, Any]:
    found = {}
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


def _observed_dates(frame: pd.DataFrame):
    if frame.empty:
        return None, None
    return (
        date.fromisoformat(str(frame["session_date"].min())[:10]),
        date.fromisoformat(str(frame["session_date"].max())[:10]),
    )


def _china_date(value: Any) -> date | None:
    parsed = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(parsed):
        return None
    timestamp = pd.to_datetime(float(parsed), unit="ms", utc=True, errors="coerce")
    if pd.isna(timestamp):
        return None
    return timestamp.tz_convert("Asia/Shanghai").date()
