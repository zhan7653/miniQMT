from __future__ import annotations

from contextlib import contextmanager
from datetime import date
from importlib import import_module, util
from typing import Any, Mapping

import pandas as pd

from fundlab.common.canonical import stable_digest
from fundlab.marketdata.contracts import (
    CorporateActionType,
    CoverageClaim,
    MarketTable,
    ObservationError,
    ObservationPayload,
    PriceLimitState,
    PriceMode,
    ProviderCapability,
    ProviderRequest,
)
from fundlab.marketdata.sources.base import (
    baostock_code,
    frame_hash,
    now_utc,
    require_daily_scope,
    result_frame,
    source_payload,
    split_instrument_id,
)


class BaoStockProvider:
    name = "baostock"
    backend_group = "baostock"
    capabilities = frozenset({
        ProviderCapability.INSTRUMENTS,
        ProviderCapability.TRADING_CALENDAR,
        ProviderCapability.DAILY_BARS_RAW,
        ProviderCapability.DAILY_BARS_ADJUSTED,
        ProviderCapability.DAILY_STATUS,
        ProviderCapability.CORPORATE_ACTIONS,
        ProviderCapability.ADJUSTMENT_FACTORS,
    })

    def __init__(self, *, client: Any | None = None) -> None:
        self._client = client
        self._shared_client: Any | None = None
        self._reuse_session = False

    @property
    def available(self) -> bool:
        return self._client is not None or util.find_spec("baostock") is not None

    def observe(self, request: ProviderRequest) -> ObservationPayload:
        if request.capability not in self.capabilities:
            raise ValueError(f"BaoStock does not support {request.capability.value}")
        if self._reuse_session and self._shared_client is None:
            client = self._client or import_module("baostock")
            self._login(client)
            self._shared_client = client
        if self._shared_client is not None:
            shared = self._shared_client
            try:
                return self._observe_authenticated(shared, request)
            except Exception:
                # The optimization must never make collection less available.
                # Retire the shared session and replay this request through the
                # original one-login-per-request path.
                self._close_shared_client(shared, suppress_logout_error=True)
        return self._observe_one_shot(request)

    @contextmanager
    def session(self):
        """Reuse one authenticated client while callers retain batch boundaries."""

        if self._reuse_session:
            raise RuntimeError("BaoStock session is already active")
        self._reuse_session = True
        try:
            yield self
        finally:
            self._reuse_session = False
            if self._shared_client is not None:
                # Every completed batch has already been validated, recorded,
                # and checkpointed.  A transport cleanup failure must not turn
                # those durable successes into an all-universe status failure.
                self._close_shared_client(
                    self._shared_client, suppress_logout_error=True,
                )

    def _observe_one_shot(self, request: ProviderRequest) -> ObservationPayload:
        client = self._client or import_module("baostock")
        self._login(client)
        try:
            return self._observe_authenticated(client, request)
        finally:
            client.logout()

    @staticmethod
    def _login(client: Any) -> None:
        login = client.login()
        if getattr(login, "error_code", "") != "0":
            raise ObservationError(
                f"BaoStock login failed: {getattr(login, 'error_code', '?')} "
                f"{getattr(login, 'error_msg', '')}"
            )

    def _close_shared_client(
        self, client: Any, *, suppress_logout_error: bool = False,
    ) -> None:
        if self._shared_client is not client:
            return
        self._shared_client = None
        try:
            client.logout()
        except Exception:
            if not suppress_logout_error:
                raise

    def _observe_authenticated(
        self, client: Any, request: ProviderRequest,
    ) -> ObservationPayload:
        table, frame, complete, hashes, detail = self._collect(client, request)
        start, end = _frame_dates(table, frame, request)
        instruments = (
            request.instrument_ids
            if complete and request.capability in {
                ProviderCapability.CORPORATE_ACTIONS,
                ProviderCapability.ADJUSTMENT_FACTORS,
            }
            else tuple(sorted(set(map(str, frame["instrument_id"]))))
            if "instrument_id" in frame else request.instrument_ids
        )
        return ObservationPayload(
            self.name,
            now_utc(),
            request,
            {table: frame},
            (CoverageClaim(table, complete, start, end, instruments, detail),),
            {
                "upstream": "BaoStock",
                "backend_group": self.backend_group,
                "transport": f"baostock.{request.capability.value}",
                "client_version": getattr(client, "__version__", None),
                "response_sha256": hashes,
                "requested_scope": {
                    "start_date": request.start_date,
                    "end_date": request.end_date,
                    "instrument_ids": request.instrument_ids,
                },
                "canonical_units": {"price": "CNY", "volume": "share", "amount": "CNY"},
                "provider_adjusted_usage": (
                    "audit_only"
                    if request.capability is ProviderCapability.DAILY_BARS_ADJUSTED else None
                ),
            },
        )

    def _collect(
        self, client: Any, request: ProviderRequest,
    ) -> tuple[MarketTable, pd.DataFrame, bool, Mapping[str, str], str]:
        if request.capability in {
            ProviderCapability.DAILY_BARS_RAW,
            ProviderCapability.DAILY_BARS_ADJUSTED,
        }:
            return self._daily_bars(client, request)
        if request.capability is ProviderCapability.DAILY_STATUS:
            return self._daily_status(client, request)
        if request.capability is ProviderCapability.TRADING_CALENDAR:
            return self._calendar(client, request)
        if request.capability is ProviderCapability.INSTRUMENTS:
            return self._instruments(client, request)
        if request.capability is ProviderCapability.CORPORATE_ACTIONS:
            return self._corporate_actions(client, request)
        if request.capability is ProviderCapability.ADJUSTMENT_FACTORS:
            return self._adjustment_factors(client, request)
        raise AssertionError(request.capability)

    def _daily_bars(self, client: Any, request: ProviderRequest):
        require_daily_scope(request)
        mode = (
            PriceMode.RAW
            if request.capability is ProviderCapability.DAILY_BARS_RAW
            else PriceMode.ADJUSTED
        )
        adjustflag = "3" if mode is PriceMode.RAW else "2"
        frames: list[pd.DataFrame] = []
        hashes: dict[str, str] = {}
        completed: list[str] = []
        invalid: dict[str, str] = {}
        fields = "date,code,open,high,low,close,preclose,volume,amount,tradestatus,isST"
        for instrument_id in request.instrument_ids:
            raw = result_frame(client.query_history_k_data_plus(
                baostock_code(instrument_id),
                fields,
                start_date=request.start_date.isoformat(),
                end_date=request.end_date.isoformat(),
                frequency="d",
                adjustflag=adjustflag,
            ))
            hashes[instrument_id] = frame_hash(raw)
            if raw.empty:
                continue
            normalized = _normalize_daily(raw, instrument_id, mode, hashes[instrument_id])
            invalid_reason = _invalid_daily_reason(normalized)
            if invalid_reason is not None:
                invalid[instrument_id] = invalid_reason
                continue
            frames.append(normalized)
            completed.append(instrument_id)
        frame = pd.concat(frames, ignore_index=True) if frames else _empty_daily_bars()
        complete = set(completed) == set(request.instrument_ids)
        detail = (
            f"BaoStock adjustflag={adjustflag} returned every requested instrument"
            if complete else f"BaoStock returned rows for {len(completed)}/{len(request.instrument_ids)} instruments"
        )
        if invalid:
            detail += "; invalid instruments=" + ",".join(
                f"{key}({value})" for key, value in sorted(invalid.items())
            )
        return MarketTable.DAILY_BARS, frame, complete, hashes, detail

    def _calendar(self, client: Any, request: ProviderRequest):
        if request.start_date is None or request.end_date is None:
            raise ValueError("Calendar collection requires start_date and end_date")
        raw = result_frame(client.query_trade_dates(
            start_date=request.start_date.isoformat(),
            end_date=request.end_date.isoformat(),
        ))
        exchanges = tuple(request.parameters.get("exchanges", ("SH", "SZ")))
        unsupported = sorted(set(exchanges) - {"SH", "SZ"})
        rows = []
        digest = frame_hash(raw)
        for item in raw.to_dict("records"):
            for exchange in exchanges:
                if exchange in {"SH", "SZ"}:
                    rows.append({
                        "exchange": exchange,
                        "session_date": item["calendar_date"],
                        "is_open": str(item["is_trading_day"]) == "1",
                        "source_payload": source_payload(response_sha256=digest, raw=_plain(item)),
                    })
        complete = not raw.empty and not unsupported
        detail = "BaoStock national SH/SZ trading calendar"
        if unsupported:
            detail += f"; unsupported exchanges={unsupported}"
        return MarketTable.CALENDAR, pd.DataFrame(rows), complete, {"calendar": digest}, detail

    def _daily_status(self, client: Any, request: ProviderRequest):
        """Fetch only simulation state fields, avoiding redundant historical OHLC transfer."""

        require_daily_scope(request)
        fields = "date,code,preclose,tradestatus,isST"
        frames: list[pd.DataFrame] = []
        hashes: dict[str, str] = {}
        completed: list[str] = []
        for instrument_id in request.instrument_ids:
            raw = result_frame(client.query_history_k_data_plus(
                baostock_code(instrument_id),
                fields,
                start_date=request.start_date.isoformat(),
                end_date=request.end_date.isoformat(),
                frequency="d",
                adjustflag="3",
            ))
            hashes[instrument_id] = frame_hash(raw)
            if raw.empty:
                continue
            suspended = raw["tradestatus"].astype(str).eq("0")
            previous = pd.to_numeric(raw["preclose"], errors="coerce")
            if previous.isna().any():
                continue
            normalized = pd.DataFrame({
                "instrument_id": instrument_id,
                "session_date": raw["date"].astype(str),
                "price_mode": PriceMode.RAW.value,
                "open": pd.NA,
                "high": pd.NA,
                "low": pd.NA,
                "close": pd.NA,
                # This is a status-only observation, not a price observation.
                "volume": 0,
                "amount": pd.NA,
                "suspended": suspended,
                "is_st": raw["isST"].astype(str).str.strip().str.lower().isin(
                    {"1", "true"},
                ),
                "price_limit_state": PriceLimitState.UNKNOWN.value,
                "previous_close": previous,
                "limit_up": pd.NA,
                "limit_down": pd.NA,
                "source_payload": [
                    source_payload(response_sha256=hashes[instrument_id], raw=_plain(item))
                    for item in raw.to_dict("records")
                ],
            })
            frames.append(normalized)
            completed.append(instrument_id)
        frame = pd.concat(frames, ignore_index=True) if frames else _empty_daily_bars()
        complete = set(completed) == set(request.instrument_ids)
        return (
            MarketTable.DAILY_BARS,
            frame,
            complete,
            hashes,
            (
                "BaoStock exhaustive daily suspension/ST/previous-close status"
                if complete else
                f"BaoStock status returned {len(completed)}/{len(request.instrument_ids)} instruments"
            ),
        )

    def _instruments(self, client: Any, request: ProviderRequest):
        if not request.instrument_ids:
            return self._discover_instruments(client, request)
        rows = []
        hashes: dict[str, str] = {}
        completed: list[str] = []
        for instrument_id in request.instrument_ids:
            raw = result_frame(client.query_stock_basic(code=baostock_code(instrument_id)))
            hashes[instrument_id] = frame_hash(raw)
            if raw.empty:
                continue
            item = raw.iloc[0].to_dict()
            security_type = str(item.get("type", ""))
            if security_type not in {"1", "5"}:
                continue
            local, exchange = split_instrument_id(instrument_id)
            rows.append({
                "instrument_id": instrument_id,
                "exchange": exchange,
                "local_code": local,
                "asset_type": "stock" if security_type == "1" else "etf",
                "name": str(item.get("code_name") or instrument_id),
                "currency": "CNY",
                "listed_date": _optional_text(item.get("ipoDate")),
                "delisted_date": _optional_text(item.get("outDate")),
                "board": _board(local, exchange),
                "buy_lot": 100,
                "price_tick": 0.001 if security_type == "5" else 0.01,
                "sell_delay_sessions": 1,
                "price_limit_ratio": pd.NA,
                "source_payload": source_payload(response_sha256=hashes[instrument_id], raw=_plain(item)),
            })
            completed.append(instrument_id)
        complete = set(completed) == set(request.instrument_ids)
        return (
            MarketTable.INSTRUMENTS,
            pd.DataFrame(rows),
            complete,
            hashes,
            f"BaoStock basic metadata for {len(completed)}/{len(request.instrument_ids)} requested instruments",
        )

    def _discover_instruments(self, client: Any, request: ProviderRequest):
        """Discover BaoStock's lifetime SH/SZ stock and ETF master in one query.

        ``query_stock_basic`` differs from the point-in-time ``query_all_stock`` endpoint:
        it returns lifecycle dates and inactive securities, which is the minimum usable
        universe evidence for a historical build.  BaoStock does not cover Beijing, so a
        request that includes BJ is explicitly incomplete instead of silently narrowing.
        """

        exchanges = tuple(sorted({
            str(item).upper()
            for item in request.parameters.get("exchanges", ("SH", "SZ"))
        }))
        asset_types = tuple(sorted({
            str(item).lower()
            for item in request.parameters.get("asset_types", ("stock", "etf"))
        }))
        include_delisted = bool(request.parameters.get("include_delisted", True))
        supported_exchanges = {"SH", "SZ"}
        supported_assets = {"stock", "etf"}
        unsupported_exchanges = sorted(set(exchanges) - supported_exchanges)
        unsupported_assets = sorted(set(asset_types) - supported_assets)

        raw = result_frame(client.query_stock_basic())
        digest = frame_hash(raw)
        rows = []
        type_to_asset = {"1": "stock", "5": "etf"}
        for item in raw.to_dict("records"):
            raw_code = str(item.get("code", "")).lower()
            prefix, separator, local = raw_code.partition(".")
            exchange = {"sh": "SH", "sz": "SZ"}.get(prefix)
            asset_type = type_to_asset.get(str(item.get("type", "")))
            if not separator or exchange not in exchanges or asset_type not in asset_types:
                continue
            if not include_delisted and str(item.get("status", "")) != "1":
                continue
            listed_date = _optional_text(item.get("ipoDate"))
            if listed_date is None:
                continue
            instrument_id = f"{local.zfill(6)}.{exchange}"
            rows.append({
                "instrument_id": instrument_id,
                "exchange": exchange,
                "local_code": local.zfill(6),
                "asset_type": asset_type,
                "name": str(item.get("code_name") or instrument_id),
                "currency": "CNY",
                "listed_date": listed_date,
                "delisted_date": _optional_text(item.get("outDate")),
                "board": _board(local.zfill(6), exchange),
                "buy_lot": 100,
                "price_tick": 0.001 if asset_type == "etf" else 0.01,
                # The first research build records a conservative common rule.  ETF
                # subclasses with T+0 settlement remain a simulation-readiness gap.
                "sell_delay_sessions": 1,
                "price_limit_ratio": pd.NA,
                "source_payload": source_payload(
                    transport="baostock.query_stock_basic",
                    response_sha256=digest,
                    raw=_plain(item),
                ),
            })
        frame = pd.DataFrame(rows, columns=(
            "instrument_id", "exchange", "local_code", "asset_type", "name",
            "currency", "listed_date", "delisted_date", "board", "buy_lot",
            "price_tick", "sell_delay_sessions", "price_limit_ratio", "source_payload",
        ))
        complete = (
            not raw.empty
            and not unsupported_exchanges
            and not unsupported_assets
            and bool(rows)
        )
        detail = (
            f"BaoStock lifetime basic master returned {len(rows)} SH/SZ stock/ETF instruments"
        )
        if unsupported_exchanges:
            detail += f"; unsupported exchanges={unsupported_exchanges}"
        if unsupported_assets:
            detail += f"; unsupported asset_types={unsupported_assets}"
        return (
            MarketTable.INSTRUMENTS,
            frame,
            complete,
            {"lifetime_master": digest},
            detail,
        )

    def _corporate_actions(self, client: Any, request: ProviderRequest):
        require_daily_scope(request)
        rows = []
        hashes: dict[str, str] = {}
        successful: set[str] = set()
        for instrument_id in request.instrument_ids:
            pieces = []
            for year in range(request.start_date.year, request.end_date.year + 1):
                pieces.append(result_frame(client.query_dividend_data(
                    code=baostock_code(instrument_id), year=str(year), yearType="operate",
                )))
            raw = pd.concat(pieces, ignore_index=True) if pieces else pd.DataFrame()
            hashes[instrument_id] = frame_hash(raw)
            successful.add(instrument_id)
            for item in raw.to_dict("records"):
                ex_date = _optional_text(item.get("dividOperateDate"))
                record_date = _optional_text(item.get("dividRegistDate"))
                if ex_date is None or record_date is None:
                    continue
                base = {
                    "instrument_id": instrument_id,
                    # BaoStock does not expose a reliable announcement timestamp in
                    # this endpoint.  The ex-date is a conservative known-at bound:
                    # it prevents point-in-time consumers from seeing the event early.
                    "known_date": ex_date,
                    "record_date": record_date,
                    "ex_date": ex_date,
                    "pay_date": _optional_text(item.get("dividPayDate")),
                    "listing_date": _optional_text(item.get("dividStockMarketDate")),
                    "rights_price": pd.NA,
                    "source_payload": source_payload(response_sha256=hashes[instrument_id], raw=_plain(item)),
                }
                cash = _number(item.get("dividCashPsBeforeTax"))
                if cash is not None and cash > 0:
                    identity = {"source": "baostock", "instrument_id": instrument_id, "ex_date": ex_date, "kind": "cash"}
                    rows.append({
                        **base,
                        "action_id": f"act-{stable_digest(identity)[:24]}",
                        "action_type": CorporateActionType.CASH_DIVIDEND.value,
                        "cash_per_share": cash,
                        "share_ratio": pd.NA,
                    })
                share = sum(filter(None, (
                    _number(item.get("dividStocksPs")),
                    _number(item.get("dividReserveToStockPs")),
                )))
                if share > 0:
                    identity = {"source": "baostock", "instrument_id": instrument_id, "ex_date": ex_date, "kind": "stock"}
                    rows.append({
                        **base,
                        "action_id": f"act-{stable_digest(identity)[:24]}",
                        "action_type": CorporateActionType.STOCK_DIVIDEND.value,
                        "cash_per_share": pd.NA,
                        "share_ratio": share,
                    })
        frame = pd.DataFrame(rows, columns=_action_columns())
        # This endpoint covers dividends/bonus shares, but not the complete rights-issue
        # lifecycle required by simulation readiness.  Preserve it as a useful source
        # observation without overstating canonical coverage.
        complete = False
        return (
            MarketTable.CORPORATE_ACTIONS,
            frame,
            complete,
            hashes,
            "BaoStock dividend actions; rights issues require an official-source supplement",
        )

    def _adjustment_factors(self, client: Any, request: ProviderRequest):
        require_daily_scope(request)
        rows = []
        hashes: dict[str, str] = {}
        completed: set[str] = set()
        for instrument_id in request.instrument_ids:
            raw = result_frame(client.query_adjust_factor(
                code=baostock_code(instrument_id),
                # Event ratios need the immediately preceding cumulative factor.
                # Query from before any supported A-share listing, then clip the
                # derived events to the requested immutable observation scope.
                start_date="1990-01-01",
                end_date=request.end_date.isoformat(),
            ))
            hashes[instrument_id] = frame_hash(raw)
            completed.add(instrument_id)
            prepared = raw.copy()
            if not prepared.empty:
                prepared["dividOperateDate"] = pd.to_datetime(
                    prepared["dividOperateDate"], errors="coerce",
                )
                prepared["backAdjustFactor"] = pd.to_numeric(
                    prepared["backAdjustFactor"], errors="coerce",
                )
                prepared = prepared.dropna(
                    subset=["dividOperateDate", "backAdjustFactor"],
                ).sort_values("dividOperateDate", kind="stable")
                prepared["previous_back_factor"] = prepared["backAdjustFactor"].shift(1).fillna(1.0)
                prepared["event_price_multiplier"] = (
                    prepared["previous_back_factor"] / prepared["backAdjustFactor"]
                )
            for item in prepared.to_dict("records"):
                effective = _optional_text(item.get("dividOperateDate"))
                multiplier = _number(item.get("event_price_multiplier"))
                if (
                    effective is None
                    or not request.start_date.isoformat() <= effective <= request.end_date.isoformat()
                    or multiplier is None
                    or multiplier <= 0
                ):
                    continue
                identity = {
                    "source": "baostock",
                    "instrument_id": instrument_id,
                    "effective_date": effective,
                    "price_multiplier": multiplier,
                }
                rows.append({
                    "factor_id": f"factor-{stable_digest(identity)[:24]}",
                    "instrument_id": instrument_id,
                    "effective_date": effective,
                    # BaoStock factors do not carry an announcement timestamp.  Effective date is
                    # deliberately conservative: a historical view never sees the factor earlier.
                    "known_date": effective,
                    "price_multiplier": multiplier,
                    "source_payload": source_payload(
                        response_sha256=hashes[instrument_id],
                        derivation="previous_backAdjustFactor/backAdjustFactor",
                        raw=_plain(item),
                    ),
                })
        frame = pd.DataFrame(rows, columns=(
            "factor_id", "instrument_id", "effective_date", "known_date",
            "price_multiplier", "source_payload",
        ))
        complete = completed == set(request.instrument_ids)
        return (
            MarketTable.ADJUSTMENT_FACTORS,
            frame,
            complete,
            hashes,
            (
                "BaoStock cumulative back-adjust factors converted to replayable event ratios; "
                "known_date conservatively equals effective_date"
            ),
        )


def _normalize_daily(raw: pd.DataFrame, instrument_id: str, mode: PriceMode, digest: str) -> pd.DataFrame:
    suspended = raw["tradestatus"].astype(str).eq("0")
    is_st = raw["isST"].astype(str).str.strip().str.lower().isin({"1", "true"})
    volume = pd.to_numeric(raw["volume"], errors="coerce")
    # BaoStock represents some suspended sessions with blank OHLCV cells.  A
    # suspended session has known zero turnover; an active session with a blank
    # volume remains invalid and is rejected by the canonical schema.
    volume = volume.mask(suspended & volume.isna(), 0)
    result = pd.DataFrame({
        "instrument_id": instrument_id,
        "session_date": raw["date"].astype(str),
        "price_mode": mode.value,
        "open": pd.to_numeric(raw["open"], errors="coerce"),
        "high": pd.to_numeric(raw["high"], errors="coerce"),
        "low": pd.to_numeric(raw["low"], errors="coerce"),
        "close": pd.to_numeric(raw["close"], errors="coerce"),
        "volume": volume,
        "amount": pd.to_numeric(raw["amount"], errors="coerce"),
        "suspended": suspended,
        "is_st": is_st,
        "trade_rule_id": pd.NA,
        "trade_rule_known_date": pd.NA,
        "buy_lot": pd.NA,
        "price_tick": pd.NA,
        "sell_delay_sessions": pd.NA,
        "price_limit_state": PriceLimitState.UNKNOWN.value,
        "previous_close": pd.to_numeric(raw["preclose"], errors="coerce"),
        "limit_up": pd.NA,
        "limit_down": pd.NA,
    })
    records = raw.to_dict("records")
    result["source_payload"] = [
        source_payload(response_sha256=digest, raw=_plain(item)) for item in records
    ]
    return result


def _invalid_daily_reason(frame: pd.DataFrame) -> str | None:
    """Reject one malformed instrument without poisoning its whole source batch."""

    active = ~frame["suspended"].fillna(False).astype(bool)
    critical = frame[["open", "high", "low", "close", "volume"]].apply(
        pd.to_numeric, errors="coerce",
    )
    if critical.loc[active].isna().any(axis=1).any():
        return "active_critical_value_missing"
    if critical["volume"].dropna().lt(0).any():
        return "negative_volume"
    prices = critical.loc[active, ["open", "high", "low", "close"]]
    invalid_ohlc = (
        prices.min(axis=1).le(0)
        | prices["high"].lt(prices[["open", "close", "low"]].max(axis=1))
        | prices["low"].gt(prices[["open", "close", "high"]].min(axis=1))
    )
    if invalid_ohlc.any():
        return "invalid_active_ohlc"
    return None


def _frame_dates(table: MarketTable, frame: pd.DataFrame, request: ProviderRequest):
    if table is MarketTable.INSTRUMENTS:
        return None, None
    if table is MarketTable.DAILY_BARS and not frame.empty:
        return (
            date.fromisoformat(str(frame["session_date"].min())[:10]),
            date.fromisoformat(str(frame["session_date"].max())[:10]),
        )
    return request.start_date, request.end_date


def _action_columns() -> tuple[str, ...]:
    return (
        "action_id", "instrument_id", "action_type", "known_date", "record_date", "ex_date", "pay_date",
        "listing_date", "cash_per_share", "share_ratio", "rights_price", "source_payload",
    )


def _empty_daily_bars() -> pd.DataFrame:
    return pd.DataFrame(columns=(
        "instrument_id", "session_date", "price_mode", "open", "high", "low", "close",
        "volume", "amount", "suspended", "is_st", "trade_rule_id",
        "trade_rule_known_date", "buy_lot", "price_tick", "sell_delay_sessions",
        "price_limit_state", "previous_close", "limit_up",
        "limit_down", "source_payload",
    ))


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


def _number(value: Any) -> float | None:
    parsed = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    return None if pd.isna(parsed) else float(parsed)


def _optional_text(value: Any) -> str | None:
    if value is None or pd.isna(value) or not str(value).strip():
        return None
    return str(value)[:10]


def _board(local_code: str, exchange: str) -> str:
    if exchange == "BJ":
        return "beijing"
    if local_code.startswith("688"):
        return "star"
    if local_code.startswith("30"):
        return "chinext"
    return "main"
