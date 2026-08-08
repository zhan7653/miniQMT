"""Point-in-time dividend features over the canonical stock universe."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date, timedelta
from statistics import fmean, median, pstdev
from typing import Mapping

import pandas as pd

from fundlab.marketdata.contracts import PriceMode
from fundlab.marketdata.portal import CanonicalMarketData

_HISTORY_YEARS = 12


@dataclass(frozen=True)
class DividendCandidate:
    instrument_id: str
    name: str
    as_of: date
    last_close: float
    ttm_dividend: float
    ttm_yield: float
    ttm_special_dividend: float
    latest_fiscal_year: int
    latest_fiscal_dividend: float
    latest_fiscal_yield: float
    normalized_dividend: float
    normalized_yield: float
    sustainable_dividend: float
    sustainable_yield: float
    normalization_years: int
    dividend_years: int
    avg_amount: float
    annual_dividends: tuple[tuple[int, float], ...]
    payout_variability: float
    amount_observed_sessions: int = 20

    def evidence(self) -> dict[str, object]:
        """Bounded, JSON-ready facts supplied to the adviser and audit log."""
        return {
            "instrument_id": self.instrument_id,
            "name": self.name,
            "last_close": round(self.last_close, 4),
            "ttm_cash_dividend_per_share": round(self.ttm_dividend, 6),
            "ttm_cash_yield": round(self.ttm_yield, 8),
            "ttm_special_dividend_per_share": round(self.ttm_special_dividend, 6),
            "ttm_special_dividend_share": round(
                0.0 if self.ttm_dividend <= 0
                else self.ttm_special_dividend / self.ttm_dividend,
                8,
            ),
            "latest_completed_fiscal_year": self.latest_fiscal_year,
            "latest_completed_fiscal_dividend_per_share": round(
                self.latest_fiscal_dividend, 6,
            ),
            "latest_completed_fiscal_yield": round(self.latest_fiscal_yield, 8),
            "normalized_3y_fiscal_dividend_per_share": round(
                self.normalized_dividend, 6,
            ),
            "normalized_3y_fiscal_yield": round(self.normalized_yield, 8),
            "conservative_sustainable_dividend_per_share": round(
                self.sustainable_dividend, 6,
            ),
            "conservative_sustainable_yield": round(self.sustainable_yield, 8),
            "normalization_fiscal_years": self.normalization_years,
            "ttm_to_normalized_dividend_ratio": round(
                0.0 if self.normalized_dividend <= 0
                else self.ttm_dividend / self.normalized_dividend,
                8,
            ),
            "consecutive_completed_fiscal_dividend_years": self.dividend_years,
            "average_daily_amount_20_sessions": round(self.avg_amount, 2),
            "average_daily_amount_observed_sessions": self.amount_observed_sessions,
            "completed_fiscal_year_dividends_per_share": [
                {"year": year, "amount": round(amount, 6)}
                for year, amount in self.annual_dividends
            ],
            "payout_variability": round(self.payout_variability, 6),
        }


def _fiscal_attribution(row: Mapping[str, object]) -> tuple[int, bool, bool] | None:
    """Return fiscal year, completion marker, and special-dividend marker.

    CNInfo's implementation feed carries the accounting period in the preserved
    raw payload (for example ``2025年报`` or ``2025半年报``).  Do not infer it
    from the ex-date: annual distributions are commonly implemented next year.
    """

    report_time = _optional_text(row.get("report_time"))
    dividend_type = _optional_text(row.get("dividend_type"))
    description = _optional_text(row.get("dividend_description"))
    payload_text = _optional_text(row.get("source_payload"))
    if payload_text:
        try:
            payload = json.loads(payload_text)
        except json.JSONDecodeError:
            payload = None
        if isinstance(payload, Mapping):
            raw = payload.get("raw")
            if isinstance(raw, Mapping):
                report_time = report_time or _optional_text(raw.get("报告时间"))
                dividend_type = dividend_type or _optional_text(raw.get("分红类型"))
                description = description or _optional_text(raw.get("实施方案分红说明"))
    if not report_time:
        return None
    match = re.search(r"(?<!\d)((?:19|20)\d{2})(?!\d)", report_time)
    if match is None:
        return None
    fiscal_year = int(match.group(1))
    compact_period = re.sub(r"\s+", "", report_time)
    interim = any(token in compact_period for token in ("半年", "半年度", "中报"))
    complete = not interim and any(
        token in compact_period for token in ("年报", "年度报告")
    )
    special_text = f"{dividend_type or ''} {description or ''}"
    special = "特别" in special_text
    return fiscal_year, complete, special


def _optional_text(value: object) -> str | None:
    if value is None or value is pd.NA:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    text = str(value).strip()
    return text or None


def build_dividend_candidates(
    market: CanonicalMarketData,
    *,
    as_of: date,
    min_dividend_years: int,
    min_avg_amount: float,
    exclude_st: bool = True,
    amount_window_sessions: int = 20,
) -> tuple[DividendCandidate, ...]:
    """Measure reliable, liquid cash-dividend payers without lookahead.

    This is the deterministic safety screen. The LLM never sees an instrument
    that did not trade at ``as_of``, is suspended or ST, lacks the required
    consecutive payout history, or falls below the configured liquidity floor.
    """
    if min_dividend_years < 1:
        raise ValueError("min_dividend_years must be positive")
    if min_avg_amount < 0:
        raise ValueError("min_avg_amount cannot be negative")
    if amount_window_sessions < 1:
        raise ValueError("amount_window_sessions must be positive")

    stocks = market.instruments(as_of=as_of, asset_types=("stock",))
    if not stocks:
        return ()
    symbols = tuple(item.instrument_id for item in stocks)
    names = {item.instrument_id: item.name for item in stocks}

    sessions = market.trading_days(as_of - timedelta(days=120), as_of)
    if not sessions or sessions[-1] != as_of:
        raise ValueError(f"{as_of.isoformat()} is not an open session in the snapshot calendar")
    window = sessions[-amount_window_sessions:]
    if len(window) != amount_window_sessions:
        raise ValueError(
            f"Need {amount_window_sessions} open sessions through {as_of.isoformat()} "
            "to compute the liquidity gate"
        )
    bars = market.bars(
        symbols,
        window[0],
        as_of,
        price_mode=PriceMode.RAW,
        as_of=as_of,
    )
    if bars.empty:
        return ()

    latest = bars[bars["session_date"].astype(str) == as_of.isoformat()].set_index(
        "instrument_id",
    )
    closes = pd.to_numeric(latest["close"], errors="coerce")
    suspended = latest["suspended"].fillna(False).astype(bool)
    st_flags = latest["is_st"].fillna(False).astype(bool)
    liquidity = bars[["instrument_id", "session_date", "amount"]].copy()
    liquidity["session_date"] = liquidity["session_date"].astype(str)
    liquidity = liquidity[liquidity["session_date"].isin(
        {item.isoformat() for item in window}
    )]
    liquidity["amount"] = pd.to_numeric(liquidity["amount"], errors="coerce")
    measured = liquidity.groupby("instrument_id", sort=False).agg(
        row_count=("session_date", "size"),
        session_count=("session_date", "nunique"),
        amount_count=("amount", "count"),
        total_amount=("amount", "sum"),
    )
    complete = measured[
        (measured["row_count"] == amount_window_sessions)
        & (measured["session_count"] == amount_window_sessions)
        & (measured["amount_count"] == amount_window_sessions)
    ]
    amounts = complete["total_amount"] / amount_window_sessions

    history_start = date(as_of.year - _HISTORY_YEARS, 1, 1)
    actions = market.corporate_actions(symbols, history_start, as_of, as_of=as_of)
    if actions.empty:
        return ()
    cash = actions[actions["action_type"].astype(str) == "cash_dividend"].copy()
    if cash.empty:
        return ()
    cash["cash_per_share"] = pd.to_numeric(cash["cash_per_share"], errors="coerce")
    cash = cash.dropna(subset=["cash_per_share"])
    cash = cash[cash["cash_per_share"] > 0]
    if cash.empty:
        return ()
    attributions = [_fiscal_attribution(row) for row in cash.to_dict("records")]
    cash["_fiscal_year"] = pd.array(
        [None if item is None else item[0] for item in attributions],
        dtype="Int64",
    )
    cash["_fiscal_complete"] = [
        False if item is None else item[1] for item in attributions
    ]
    cash["_special_dividend"] = [
        False if item is None else item[2] for item in attributions
    ]
    unattributed_symbols = set(map(
        str,
        cash.loc[cash["_fiscal_year"].isna(), "instrument_id"],
    ))

    ttm_start = (as_of - timedelta(days=365)).isoformat()
    ttm_cash = cash[cash["ex_date"].astype(str) > ttm_start]
    ttm = ttm_cash.groupby(
        "instrument_id",
    )["cash_per_share"].sum().to_dict()
    ttm_special = ttm_cash.loc[ttm_cash["_special_dividend"]].groupby(
        "instrument_id",
    )["cash_per_share"].sum().to_dict()

    attributed = cash.loc[cash["_fiscal_year"].notna()].copy()
    complete_fiscal_years = {
        (str(row["instrument_id"]), int(row["_fiscal_year"]))
        for row in attributed.to_dict("records")
        if bool(row["_fiscal_complete"])
    }
    regular = attributed.loc[~attributed["_special_dividend"]]
    annual = regular.groupby(
        ["instrument_id", "_fiscal_year"],
    )["cash_per_share"].sum()
    annual_by_symbol: dict[str, dict[int, float]] = {}
    for (symbol, year), amount in annual.items():
        key = (str(symbol), int(year))
        if key in complete_fiscal_years:
            annual_by_symbol.setdefault(key[0], {})[key[1]] = float(amount)

    found: list[DividendCandidate] = []
    for symbol in symbols:
        if symbol in unattributed_symbols:
            continue
        if symbol not in latest.index:
            continue
        close = closes.get(symbol)
        if close is None or pd.isna(close) or float(close) <= 0:
            continue
        if bool(suspended.get(symbol, False)):
            continue
        if exclude_st and bool(st_flags.get(symbol, False)):
            continue
        avg_amount = float(amounts.get(symbol, 0.0) or 0.0)
        if avg_amount < min_avg_amount:
            continue
        yearly = annual_by_symbol.get(symbol, {})
        if not yearly:
            continue
        # The canonical action query already applies the point-in-time known-date
        # boundary.  Use the newest completed fiscal year actually visible for
        # this company instead of delaying every new annual report to a fixed
        # calendar month.
        probe = max(yearly)
        streak_values: list[tuple[int, float]] = []
        while probe in yearly:
            streak_values.append((probe, yearly[probe]))
            probe -= 1
        if len(streak_values) < min_dividend_years:
            continue
        ttm_dividend = float(ttm.get(symbol, 0.0) or 0.0)
        latest_fiscal_year, latest_fiscal_dividend = streak_values[0]
        normalization_values = [amount for _, amount in streak_values[:3]]
        normalized_dividend = float(median(normalization_values))
        sustainable_dividend = min(latest_fiscal_dividend, normalized_dividend)
        amounts_for_stability = [amount for _, amount in streak_values[:6]]
        mean_amount = fmean(amounts_for_stability)
        variability = (
            0.0 if len(amounts_for_stability) < 2 or mean_amount <= 0
            else pstdev(amounts_for_stability) / mean_amount
        )
        found.append(DividendCandidate(
            instrument_id=symbol,
            name=names.get(symbol, symbol),
            as_of=as_of,
            last_close=float(close),
            ttm_dividend=ttm_dividend,
            ttm_yield=ttm_dividend / float(close),
            ttm_special_dividend=float(ttm_special.get(symbol, 0.0) or 0.0),
            latest_fiscal_year=latest_fiscal_year,
            latest_fiscal_dividend=latest_fiscal_dividend,
            latest_fiscal_yield=latest_fiscal_dividend / float(close),
            normalized_dividend=normalized_dividend,
            normalized_yield=normalized_dividend / float(close),
            sustainable_dividend=sustainable_dividend,
            sustainable_yield=sustainable_dividend / float(close),
            normalization_years=len(normalization_values),
            dividend_years=len(streak_values),
            avg_amount=avg_amount,
            annual_dividends=tuple(sorted(streak_values[:6])),
            payout_variability=variability,
            amount_observed_sessions=amount_window_sessions,
        ))
    return tuple(sorted(
        found,
        key=lambda item: (
            -item.sustainable_yield,
            item.payout_variability,
            -item.normalized_yield,
            -item.latest_fiscal_yield,
            item.instrument_id,
        ),
    ))
