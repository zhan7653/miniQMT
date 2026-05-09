from __future__ import annotations

from datetime import datetime
from typing import Any, Sequence

import pandas as pd

from fundlab.data.sources.base import MarketDataSource


class XtQuantSource(MarketDataSource):
    name = "xtquant"

    def __init__(self, config: dict | None = None):
        self.config = config or {}
        self.connected = False
        self.xtdata = None

    def connect(self) -> None:
        try:
            from xtquant import xtdata
        except ImportError as exc:
            raise RuntimeError("xtquant is not installed or MiniQMT environment is unavailable") from exc

        self.xtdata = xtdata
        self.connected = True

    def _require_connection(self) -> None:
        if not self.connected or self.xtdata is None:
            raise RuntimeError("XtQuantSource is not connected. Call connect() first.")

    def get_instruments(self) -> list[dict]:
        self._require_connection()
        symbols = self._candidate_fund_symbols()
        instruments = []
        for symbol in symbols:
            detail = self._get_instrument_detail(symbol)
            if not self._looks_like_supported_fund(symbol, detail):
                continue
            instruments.append(self._instrument_to_master_row(symbol, detail))
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
        configured_symbols = self.config.get("symbols")
        if configured_symbols:
            return list(configured_symbols)

        sectors = self.config.get("fund_sectors", ["沪深基金", "上证基金", "深证基金"])
        symbols: set[str] = set()
        for sector in sectors:
            try:
                symbols.update(self.xtdata.get_stock_list_in_sector(sector) or [])
            except Exception:
                continue
        return sorted(symbols)

    def _get_instrument_detail(self, symbol: str) -> dict[str, Any]:
        try:
            detail = self.xtdata.get_instrument_detail(symbol) or {}
        except Exception:
            detail = {}
        return detail if isinstance(detail, dict) else {}

    def _looks_like_supported_fund(self, symbol: str, detail: dict[str, Any]) -> bool:
        symbol_upper = symbol.upper()
        name = str(
            detail.get("InstrumentName")
            or detail.get("instrument_name")
            or detail.get("name")
            or ""
        )
        if "ETF" in name.upper() or "交易型开放式" in name or "货币" in name:
            return True
        code = symbol_upper.split(".")[0]
        return symbol_upper.endswith((".SH", ".SZ")) and code.startswith(("51", "56", "58", "15"))

    def _instrument_to_master_row(self, symbol: str, detail: dict[str, Any]) -> dict[str, Any]:
        code, exchange = symbol.split(".") if "." in symbol else (symbol, self._infer_exchange(symbol))
        name = str(
            detail.get("InstrumentName")
            or detail.get("instrument_name")
            or detail.get("name")
            or symbol
        )
        listed_date = self._normalize_xt_date(
            detail.get("OpenDate") or detail.get("CreateDate") or detail.get("listDate") or detail.get("listed_date")
        )
        delisted_date = self._normalize_xt_date(
            detail.get("ExpireDate") or detail.get("EndDelivDate") or detail.get("delisted_date")
        )
        asset_class, category, management_type = self._classify_fund(code, name)
        product_type = "MONEY_ETF" if asset_class == "money_market" else "ETF"
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
            "is_active": 0 if delisted_date else 1,
            "include_in_universe": 1,
            "exclusion_reason": None,
            "source": self.name,
            "source_updated_at": datetime.now().isoformat(timespec="seconds"),
        }

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

    def _normalize_daily_bar_response(self, raw: Any) -> pd.DataFrame:
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
        data["adj_factor"] = 1.0
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
        data["suspended"] = pd.to_numeric(data["suspended"], errors="coerce").fillna(0).astype(int).astype(bool)
        data = data.dropna(subset=["date", "symbol", "open", "high", "low", "close"])
        data = data[data["date"].astype(str).str.match(r"^\d{4}-\d{2}-\d{2}$")]
        data["date"] = data["date"].astype(str)
        data["symbol"] = data["symbol"].astype(str)
        return data

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
