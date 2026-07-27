"""Point-in-time dividend features over the canonical stock universe."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from statistics import fmean, pstdev

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
            "ttm_dividend_per_share": round(self.ttm_dividend, 6),
            "ttm_yield": round(self.ttm_yield, 8),
            "consecutive_dividend_years": self.dividend_years,
            "average_daily_amount_20_sessions": round(self.avg_amount, 2),
            "average_daily_amount_observed_sessions": self.amount_observed_sessions,
            "annual_dividends_per_share": [
                {"year": year, "amount": round(amount, 6)}
                for year, amount in self.annual_dividends
            ],
            "payout_variability": round(self.payout_variability, 6),
        }


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
    cash["year"] = cash["ex_date"].astype(str).str.slice(0, 4).astype(int)

    ttm_start = (as_of - timedelta(days=365)).isoformat()
    ttm = cash[cash["ex_date"].astype(str) >= ttm_start].groupby(
        "instrument_id",
    )["cash_per_share"].sum().to_dict()
    annual = cash.groupby(["instrument_id", "year"])["cash_per_share"].sum()
    annual_by_symbol: dict[str, dict[int, float]] = {}
    for (symbol, year), amount in annual.items():
        annual_by_symbol.setdefault(str(symbol), {})[int(year)] = float(amount)

    found: list[DividendCandidate] = []
    for symbol in symbols:
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
        probe = as_of.year if as_of.year in yearly else as_of.year - 1
        streak_values: list[tuple[int, float]] = []
        while probe in yearly:
            streak_values.append((probe, yearly[probe]))
            probe -= 1
        if len(streak_values) < min_dividend_years:
            continue
        ttm_dividend = float(ttm.get(symbol, 0.0) or 0.0)
        if ttm_dividend <= 0:
            continue
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
            dividend_years=len(streak_values),
            avg_amount=avg_amount,
            annual_dividends=tuple(sorted(streak_values[:6])),
            payout_variability=variability,
            amount_observed_sessions=amount_window_sessions,
        ))
    return tuple(sorted(found, key=lambda item: (-item.ttm_yield, item.instrument_id)))
