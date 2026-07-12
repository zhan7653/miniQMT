from __future__ import annotations

from datetime import datetime
from typing import Any, Sequence

import pandas as pd

from fundlab.data.sources.base import MarketDataSource
from fundlab.data.platform import (
    PreflightResult,
    ProviderCapability,
    ProviderHealth,
    ProviderRequest,
    SymbolResult,
)
from fundlab.data.sources.base import ProviderUnavailableError


class XtQuantSource(MarketDataSource):
    name = "xtquant"
    _MONEY_ETF_CODES = frozenset({"159001", "159003", "159005"})
    _SZ_LOF_PREFIXES = tuple(str(prefix) for prefix in range(160, 170))
    capabilities = frozenset(
        {
            ProviderCapability.TRADING_CALENDAR,
            ProviderCapability.INSTRUMENTS,
            ProviderCapability.DAILY_BARS_RAW,
            ProviderCapability.DAILY_BARS_ADJUSTED,
        }
    )

    def __init__(self, config: dict | None = None, *, xtdata: Any | None = None):
        self.config = config or {}
        self.xtdata = xtdata
        self.connected = xtdata is not None

    def connect(self) -> None:
        try:
            from xtquant import xtdata
        except ImportError as exc:
            raise RuntimeError("xtquant is not installed or MiniQMT environment is unavailable") from exc

        self.xtdata = xtdata
        self.connected = True

    def preflight(self) -> PreflightResult:
        observed_at = datetime.now().astimezone()
        if self.xtdata is None:
            try:
                self.connect()
            except (ImportError, RuntimeError) as exc:
                health = ProviderHealth.SDK_MISSING if isinstance(exc.__cause__, ImportError) else ProviderHealth.SERVICE_UNAVAILABLE
                return PreflightResult(self.name, health, observed_at, str(exc))
        probe_symbol = self.config.get("preflight_symbol", "510300.SH")
        try:
            raw = self.xtdata.get_market_data_ex(
                field_list=["close"], stock_list=[probe_symbol], period="1d", start_time="", end_time="",
                count=1, dividend_type="none", fill_data=False,
            )
        except Exception as exc:
            return PreflightResult(self.name, ProviderHealth.SERVICE_UNAVAILABLE, observed_at, str(exc))
        if raw is None:
            return PreflightResult(self.name, ProviderHealth.UNHEALTHY, observed_at, "MiniQMT probe returned no response")
        return PreflightResult(self.name, ProviderHealth.AVAILABLE, observed_at)

    def fetch(self, request: ProviderRequest):
        self.require_capability(request.capability)
        self._require_connection()
        start = request.start_date.strftime("%Y%m%d")
        end = request.end_date.strftime("%Y%m%d")
        if request.capability in {ProviderCapability.DAILY_BARS_RAW, ProviderCapability.DAILY_BARS_ADJUSTED}:
            dividend_type = "none" if request.capability is ProviderCapability.DAILY_BARS_RAW else "front"
            return self._fetch_bars(request, start, end, dividend_type)
        if request.capability is ProviderCapability.TRADING_CALENDAR:
            frame = self.get_trading_calendar(request.start_date.isoformat(), request.end_date.isoformat())
            results = [SymbolResult(symbol, len(frame)) for symbol in request.symbols]
            return frame, self._provider_result(request, results)
        rows = self.get_instruments()
        frame = pd.DataFrame(rows)
        results = [SymbolResult(symbol, int(not frame.empty and symbol in set(frame["symbol"]))) for symbol in request.symbols]
        return frame, self._provider_result(request, results)

    def _fetch_bars(self, request: ProviderRequest, start: str, end: str, dividend_type: str):
        frames: list[pd.DataFrame] = []
        statuses: list[SymbolResult] = []
        for symbol in request.symbols:
            try:
                raw = self.xtdata.get_market_data_ex(
                    field_list=[], stock_list=[symbol], period="1d", start_time=start, end_time=end,
                    count=-1, dividend_type=dividend_type, fill_data=False,
                )
                frame = self._normalize_daily_bar_response(raw, price_mode=dividend_type)
                symbol_frame = frame[frame["symbol"] == symbol] if not frame.empty else frame
                if symbol_frame.empty:
                    statuses.append(SymbolResult(symbol, 0, "no data returned"))
                else:
                    frames.append(symbol_frame)
                    statuses.append(SymbolResult(symbol, len(symbol_frame)))
            except Exception as exc:
                if self._is_system_error(exc):
                    raise ProviderUnavailableError(
                        f"MiniQMT failed while reading {request.capability.value}: {exc}"
                    ) from exc
                statuses.append(SymbolResult(symbol, 0, str(exc)))
        data = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        return data, self._provider_result(request, statuses)

    def _require_connection(self) -> None:
        if not self.connected or self.xtdata is None:
            raise ProviderUnavailableError("XtQuantSource is not connected. Call connect() first.")

    def get_instruments(self) -> list[dict]:
        self._require_connection()
        candidates = self._discover_candidate_fund_symbols()
        instruments = []
        for symbol, discovery_sources in candidates.items():
            detail, detail_available = self._get_instrument_detail(symbol)
            if not self._looks_like_supported_fund(symbol, detail):
                continue
            instruments.append(
                self._instrument_to_master_row(
                    symbol,
                    detail,
                    discovery_sources=discovery_sources,
                    detail_available=detail_available,
                )
            )
        return sorted(instruments, key=lambda item: item["symbol"])

    def download_daily_bar(self, symbols: Sequence[str], start_date: str, end_date: str) -> None:
        self._require_connection()
        for symbol in symbols:
            self.xtdata.download_history_data(
                stock_code=symbol,
                period="1d",
                start_time=start_date.replace("-", ""),
                end_time=end_date.replace("-", ""),
            )

    def get_daily_bar(self, symbols: Sequence[str], start_date: str, end_date: str) -> pd.DataFrame:
        self._require_connection()
        raw = self.xtdata.get_market_data_ex(
            field_list=[],
            stock_list=list(symbols),
            period="1d",
            start_time=start_date.replace("-", ""),
            end_time=end_date.replace("-", ""),
            count=-1,
            dividend_type="none",
            fill_data=False,
        )
        return self._normalize_daily_bar_response(raw)

    def get_trading_calendar(self, start_date: str, end_date: str) -> pd.DataFrame:
        self._require_connection()
        start_time = start_date.replace("-", "")
        end_time = end_date.replace("-", "")
        market = self.config.get("calendar_market", "SH")
        try:
            values = self.xtdata.get_trading_calendar(market, start_time=start_time, end_time=end_time)
        except TypeError:
            values = self.xtdata.get_trading_calendar(market, start_time, end_time)
        except RuntimeError:
            return self._get_trading_calendar_from_probe_bar(start_date, end_date)
        rows = [{"date": self._normalize_xt_date(value)} for value in values]
        return pd.DataFrame(rows).dropna().drop_duplicates().sort_values("date").reset_index(drop=True)

    def _get_trading_calendar_from_probe_bar(self, start_date: str, end_date: str) -> pd.DataFrame:
        symbol = self.config.get("calendar_probe_symbol", "510300.SH")
        self.download_daily_bar([symbol], start_date, end_date)
        bars = self.get_daily_bar([symbol], start_date, end_date)
        if bars.empty:
            return pd.DataFrame(columns=["date"])
        return bars[["date"]].drop_duplicates().sort_values("date").reset_index(drop=True)

    def get_nav(self, symbols: Sequence[str], start_date: str, end_date: str) -> pd.DataFrame:
        bars = self.get_daily_bar(symbols, start_date, end_date)
        if bars.empty:
            return pd.DataFrame()
        return pd.DataFrame(
            {
                "date": bars["date"],
                "symbol": bars["symbol"],
                "nav": bars["close"],
                "iopv": None,
                "close": bars["close"],
                "premium_discount": 0.0,
                "estimate_nav": None,
                "available_date": bars["date"],
                "source": self.name,
            }
        )

    def get_dividends(self, symbols: Sequence[str], start_date: str, end_date: str) -> pd.DataFrame:
        return pd.DataFrame(
            columns=[
                "symbol",
                "announcement_date",
                "ex_dividend_date",
                "record_date",
                "payment_date",
                "dividend_per_share",
                "dividend_type",
                "tax_rate",
                "available_date",
                "source",
            ]
        )

    def get_index_valuation(self, start_date: str, end_date: str) -> pd.DataFrame:
        return pd.DataFrame(
            columns=[
                "date",
                "index_code",
                "index_name",
                "pe_ttm",
                "pb",
                "ps",
                "dividend_yield",
                "roe",
                "pe_percentile_3y",
                "pe_percentile_5y",
                "pb_percentile_3y",
                "pb_percentile_5y",
                "dividend_yield_percentile_3y",
                "dividend_yield_percentile_5y",
                "available_date",
                "source",
            ]
        )

    def _candidate_fund_symbols(self) -> list[str]:
        return list(self._discover_candidate_fund_symbols())

    def _discover_candidate_fund_symbols(self) -> dict[str, tuple[str, ...]]:
        configured_symbols = self.config.get("symbols")
        if configured_symbols:
            return {str(symbol).upper(): ("config:symbols",) for symbol in configured_symbols}

        sectors = self.config.get(
            "fund_sectors",
            ["沪深基金", "上证基金", "深证基金", "上证LOF", "深证LOF"],
        )
        symbol_sources: dict[str, set[str]] = {}
        failures: list[tuple[str, Exception]] = []
        successful_queries = 0
        for sector in sectors:
            try:
                values = self.xtdata.get_stock_list_in_sector(sector) or []
                successful_queries += 1
            except Exception as exc:
                if self._is_system_error(exc):
                    raise ProviderUnavailableError(
                        f"MiniQMT failed while discovering fund sector {sector!r}: {exc}"
                    ) from exc
                failures.append((str(sector), exc))
                continue
            for symbol in values:
                normalized = str(symbol).upper()
                symbol_sources.setdefault(normalized, set()).add(f"sector:{sector}")
        if not successful_queries and failures:
            detail = "; ".join(f"{sector}: {error}" for sector, error in failures)
            raise ProviderUnavailableError(f"MiniQMT fund-sector discovery failed: {detail}")
        return {
            symbol: tuple(sorted(symbol_sources[symbol]))
            for symbol in sorted(symbol_sources)
        }

    def _get_instrument_detail(self, symbol: str) -> tuple[dict[str, Any], bool]:
        try:
            detail = self.xtdata.get_instrument_detail(symbol) or {}
        except Exception as exc:
            if self._is_system_error(exc):
                raise ProviderUnavailableError(
                    f"MiniQMT failed while reading instrument detail for {symbol}: {exc}"
                ) from exc
            return {}, False
        if not isinstance(detail, dict):
            return {}, False
        return detail, bool(detail)

    def _looks_like_supported_fund(self, symbol: str, detail: dict[str, Any]) -> bool:
        symbol_upper = symbol.upper()
        code, exchange = self._split_symbol(symbol_upper)
        name = self._instrument_name(detail, default="")
        return exchange in {"SH", "SZ"} and self._product_type(code, exchange, name) is not None

    def _instrument_to_master_row(
        self,
        symbol: str,
        detail: dict[str, Any],
        *,
        discovery_sources: Sequence[str] = (),
        detail_available: bool = True,
    ) -> dict[str, Any]:
        symbol = symbol.upper()
        code, exchange = self._split_symbol(symbol)
        name = self._instrument_name(detail, default=symbol)
        listed_date, listed_date_source = self._first_normalized_date(
            detail, ("OpenDate", "CreateDate", "listDate", "listed_date")
        )
        delisted_date, delisted_date_source = self._first_normalized_date(
            detail, ("ExpireDate", "EndDelivDate", "delistDate", "delisted_date")
        )
        asset_class, category, management_type = self._classify_fund(code, name)
        product_type = self._product_type(code, exchange, name)
        is_active, active_state_source = self._normalize_active_state(detail, delisted_date)
        return {
            "symbol": symbol,
            "raw_symbol": code,
            "name": name,
            "exchange": exchange,
            "product_type": product_type,
            "management_type": management_type,
            "asset_class": asset_class,
            "category": category,
            "tracking_index": None,
            "tracking_index_code": None,
            "fund_company": None,
            "listed_date": listed_date,
            "delisted_date": delisted_date,
            "expense_ratio": None,
            "custody_fee": None,
            "lot_size": int(detail.get("VolumeMultiple") or detail.get("lot_size") or 100),
            "price_tick": float(detail.get("PriceTick") or detail.get("price_tick") or 0.001),
            "is_active": int(is_active),
            "include_in_universe": 1,
            "exclusion_reason": None,
            "source": self.name,
            "source_updated_at": datetime.now().isoformat(timespec="seconds"),
            "discovery_source": "|".join(discovery_sources) if discovery_sources else None,
            "instrument_detail_source": "xtquant.get_instrument_detail" if detail_available else None,
            "listed_date_source": listed_date_source,
            "delisted_date_source": delisted_date_source,
            "active_state_source": active_state_source,
        }

    def _split_symbol(self, symbol: str) -> tuple[str, str]:
        if "." in symbol:
            code, exchange = symbol.rsplit(".", 1)
            return code, exchange.upper()
        return symbol, self._infer_exchange(symbol)

    def _instrument_name(self, detail: dict[str, Any], *, default: str) -> str:
        return str(
            detail.get("InstrumentName")
            or detail.get("instrument_name")
            or detail.get("name")
            or default
        )

    def _product_type(self, code: str, exchange: str, name: str) -> str | None:
        upper_name = name.upper()
        money_keywords = ["货币", "现金", "快线", "保证金", "添益", "收益快钱", "财富宝"]
        if (
            any(keyword in name for keyword in money_keywords)
            or code in self._MONEY_ETF_CODES
            or (exchange == "SH" and code.startswith(("5116", "5117", "5118", "5119")))
        ):
            return "MONEY_ETF"
        if "LOF" in upper_name or "上市型开放式" in name:
            return "LOF"
        if exchange == "SH" and code.startswith(("501", "502", "506")):
            return "LOF"
        if exchange == "SZ" and code.startswith(self._SZ_LOF_PREFIXES):
            return "LOF"
        if "ETF" in upper_name or "交易型开放式" in name:
            return "ETF"
        if exchange == "SH" and code.startswith(("51", "56", "58")):
            return "ETF"
        if exchange == "SZ" and code.startswith("159"):
            return "ETF"
        return None

    def _first_normalized_date(
        self, detail: dict[str, Any], keys: Sequence[str]
    ) -> tuple[str | None, str | None]:
        for key in keys:
            if key not in detail:
                continue
            raw_value = detail.get(key)
            is_delisting_field = key in {
                "ExpireDate", "EndDelivDate", "delistDate", "delisted_date"
            }
            if is_delisting_field and self._is_open_ended_date_sentinel(raw_value):
                return None, f"xtquant.instrument_detail.{key}:open_ended"
            value = self._normalize_xt_date(raw_value)
            if value is not None:
                return value, f"xtquant.instrument_detail.{key}"
        return None, None

    def _is_open_ended_date_sentinel(self, value: Any) -> bool:
        if value is None:
            return False
        text = str(value).strip()
        if text.endswith(".0"):
            text = text[:-2]
        digits = text.replace("-", "").replace("/", "")
        return digits == "99999999"

    def _normalize_active_state(
        self, detail: dict[str, Any], delisted_date: str | None
    ) -> tuple[bool, str]:
        if delisted_date is not None:
            return False, "derived:delisted_date"
        listing_status_keys = (
            "ListingStatus",
            "listing_status",
            "ListStatus",
            "list_status",
            "DelistingStatus",
            "delisting_status",
        )
        inactive_listing_statuses = {
            "DELISTED",
            "EXPIRED",
            "TERMINATED",
            "TERMINATED_LISTING",
            "退市",
            "已退市",
            "到期",
            "终止上市",
        }
        for key in listing_status_keys:
            if key not in detail or detail[key] is None:
                continue
            status = str(detail[key]).strip().upper()
            if status in inactive_listing_statuses:
                return False, f"xtquant.instrument_detail.{key}"
        return True, "derived:no_known_delisting_date"

    def _is_system_error(self, exc: Exception) -> bool:
        return isinstance(exc, (ConnectionError, TimeoutError, RuntimeError, OSError))

    def _classify_fund(self, code: str, name: str) -> tuple[str, str, str]:
        money_keywords = ["货币", "现金", "快线", "保证金", "添益", "收益快钱", "财富宝"]
        bond_keywords = ["债", "国债", "政金债", "地方债", "公司债", "信用债", "城投债", "短融", "可转债"]
        commodity_keywords = ["黄金", "有色", "豆粕", "粮食", "能源", "油气", "石油", "商品"]
        sector_keywords = [
            "医药",
            "消费",
            "芯片",
            "半导体",
            "新能源",
            "军工",
            "证券",
            "银行",
            "养殖",
            "软件",
            "互联网",
            "人工智能",
            "机器人",
            "汽车",
            "金融科技",
            "工业",
            "煤炭",
            "钢铁",
            "化工",
            "电力",
            "传媒",
            "游戏",
            "农业",
            "稀土",
            "酒",
            "旅游",
        ]
        if any(keyword in name for keyword in money_keywords):
            return "money_market", "money", "passive_money_market"
        if any(keyword in name for keyword in bond_keywords) or code.startswith("511"):
            return "bond", "bond", "passive_bond"
        if any(keyword in name for keyword in commodity_keywords) or code.startswith("518"):
            category = "gold" if "黄金" in name or code.startswith("518") else "commodity"
            return "commodity", category, "passive_commodity"
        if any(keyword in name for keyword in ["纳指", "标普", "恒生", "港股", "日经", "德国", "法国", "海外", "中概"]):
            return "cross_border", "cross_border", "cross_border_index"
        if any(keyword in name for keyword in ["红利", "低波", "价值", "质量", "Smart", "smart"]):
            return "equity", "smart_beta", "smart_beta"
        if any(keyword in name for keyword in sector_keywords):
            return "equity", "sector", "passive_index"
        return "equity", "broad_based", "passive_index"

    def _normalize_daily_bar_response(self, raw: Any, price_mode: str = "none") -> pd.DataFrame:
        frames = []
        if isinstance(raw, dict):
            for symbol, frame in raw.items():
                normalized = self._normalize_symbol_daily_bar(symbol, frame)
                if not normalized.empty:
                    frames.append(normalized)
        elif isinstance(raw, pd.DataFrame):
            frames.append(self._normalize_symbol_daily_bar(None, raw))
        if not frames:
            return pd.DataFrame()
        data = pd.concat(frames, ignore_index=True)
        data = data.sort_values(["symbol", "date"]).drop_duplicates(["date", "symbol"], keep="last")
        if "pre_close" not in data.columns:
            data["pre_close"] = data.groupby("symbol")["close"].shift(1)
        else:
            data["pre_close"] = data["pre_close"].fillna(data.groupby("symbol")["close"].shift(1))
        data["price_mode"] = "raw" if price_mode == "none" else "adjusted"
        if "suspended" not in data.columns:
            data["suspended"] = False
        data["limit_up"] = None
        data["limit_down"] = None
        data["source"] = self.name
        data["updated_at"] = datetime.now().isoformat(timespec="seconds")
        return data

    def _normalize_symbol_daily_bar(self, symbol: str | None, frame: Any) -> pd.DataFrame:
        if frame is None:
            return pd.DataFrame()
        data = pd.DataFrame(frame).copy()
        if data.empty:
            return pd.DataFrame()
        if "symbol" not in data.columns and symbol is not None:
            data["symbol"] = symbol
        if "date" not in data.columns:
            index_dates = pd.Series(data.index, index=data.index).map(self._normalize_xt_date)
            if not index_dates.isna().all():
                data["date"] = index_dates
            elif "time" in data.columns:
                data["date"] = data["time"].map(self._normalize_xt_date)
            else:
                data["date"] = index_dates
        rename_map = {
            "vol": "volume",
            "amount": "amount",
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "preClose": "pre_close",
            "suspendFlag": "suspended",
            "suspend_flag": "suspended",
        }
        data = data.rename(columns=rename_map)
        required = ["date", "symbol", "open", "high", "low", "close", "volume", "amount", "pre_close", "suspended"]
        for column in required:
            if column not in data.columns:
                data[column] = None
        data = data.loc[:, required]
        for column in ["open", "high", "low", "close", "volume", "amount"]:
            data[column] = pd.to_numeric(data[column], errors="coerce")
        data["pre_close"] = pd.to_numeric(data["pre_close"], errors="coerce")
        data["suspended"] = data["suspended"].map(self._normalize_suspended)
        data = data.dropna(subset=["date", "symbol", "open", "high", "low", "close"])
        data = data[data["date"].astype(str).str.match(r"^\d{4}-\d{2}-\d{2}$")]
        data["date"] = data["date"].astype(str)
        data["symbol"] = data["symbol"].astype(str)
        return data

    def _normalize_suspended(self, value: Any) -> bool:
        if value is None:
            return False
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"", "0", "false", "no", "n", "normal", "交易"}:
                return False
            if normalized in {"1", "true", "yes", "y", "suspended", "停牌"}:
                return True
        try:
            if pd.isna(value):
                return False
        except (TypeError, ValueError):
            pass
        return bool(value)

    def _normalize_xt_date(self, value: Any) -> str | None:
        if value is None:
            return None
        try:
            if pd.isna(value):
                return None
        except (TypeError, ValueError):
            pass
        text = str(value)
        if text.endswith(".0"):
            text = text[:-2]
        if text in {"", "0", "00000000"}:
            return None
        if text.isdigit() and len(text) >= 13:
            return pd.to_datetime(int(text), unit="ms").strftime("%Y-%m-%d")
        if text.isdigit() and len(text) == 8:
            return f"{text[:4]}-{text[4:6]}-{text[6:8]}"
        try:
            return pd.to_datetime(value).strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            return None

    def _infer_exchange(self, symbol: str) -> str:
        return "SH" if symbol.startswith(("5", "6")) else "SZ"
