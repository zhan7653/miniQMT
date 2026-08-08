"""Point-in-time price/liquidity candidates for prospective stock policies."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from math import sqrt
from statistics import pstdev
from types import MappingProxyType
from typing import Mapping

import pandas as pd

from fundlab.marketdata import PriceMode
from fundlab.marketdata.portal import CanonicalMarketData


@dataclass(frozen=True)
class PriceSignalCandidate:
    instrument_id: str
    name: str
    as_of: date
    listed_date: date | None
    current_is_st: bool
    last_close: float
    avg_amount: float
    momentum: Mapping[int, float]
    volatility: float | None
    beta: float | None
    above_long_average: bool
    recent_limit_hits: int
    st_removed_on: date | None = None

    def momentum_for(self, window: int) -> float | None:
        return self.momentum.get(window)


def build_price_signal_candidates(
    market: CanonicalMarketData,
    *,
    as_of: date,
    mode: str,
    momentum_windows: tuple[int, ...],
    volatility_days: int,
    amount_window: int,
    min_avg_amount: float,
    minimum_listing_days: int,
    beta_benchmark: str | None = None,
    beta_days: int | None = None,
    preselection_count: int | None = None,
    st_removal_lookback: int = 0,
    limit_lookback: int = 20,
) -> tuple[PriceSignalCandidate, ...]:
    """Build liquid stock candidates from data visible at ``as_of`` only.

    ``mode`` is one of ``non_st``, ``st`` or ``recently_removed``.  The latter
    requires an observed daily ``True -> False`` ST transition inside the
    declared lookback.  Missing history excludes one instrument rather than
    weakening the requested signal.
    """

    if mode not in {"non_st", "st", "recently_removed"}:
        raise ValueError(f"Unknown price-signal mode: {mode!r}")
    if not momentum_windows or any(window < 1 for window in momentum_windows):
        raise ValueError("momentum_windows must contain positive values")
    if volatility_days < 2 or amount_window < 1 or minimum_listing_days < 0:
        raise ValueError("price-signal windows are invalid")
    if min_avg_amount < 0 or limit_lookback < 1:
        raise ValueError("price-signal liquidity/limit parameters are invalid")
    if beta_benchmark is None and beta_days is not None:
        raise ValueError("beta_days requires beta_benchmark")
    if beta_benchmark is not None and (beta_days is None or beta_days < 2):
        raise ValueError("beta_benchmark requires beta_days >= 2")
    if preselection_count is not None and preselection_count < 1:
        raise ValueError("preselection_count must be positive")
    if mode == "recently_removed" and st_removal_lookback < 1:
        raise ValueError("recently_removed mode needs a positive ST lookback")

    stocks = market.instruments(as_of=as_of, asset_types=("stock",))
    eligible_master = {
        item.instrument_id: item
        for item in stocks
        if item.listed_date is not None
        and (as_of - item.listed_date).days >= minimum_listing_days
    }
    if not eligible_master:
        return ()
    signal_days = max(
        *momentum_windows,
        volatility_days,
        beta_days or 0,
    )
    screen_days = max(
        amount_window,
        limit_lookback if mode != "non_st" else 1,
        st_removal_lookback,
    )
    screen_start = as_of - timedelta(days=max(screen_days * 2 + 30, 60))
    signal_start = as_of - timedelta(days=max(signal_days * 2 + 30, 90))
    sessions = market.trading_days(screen_start, as_of)
    required_screen_sessions = screen_days + (1 if mode == "recently_removed" else 0)
    if not sessions or sessions[-1] != as_of:
        raise ValueError(f"{as_of.isoformat()} is not an open session")
    if len(sessions) < required_screen_sessions:
        raise ValueError(
            f"Need {required_screen_sessions} screen sessions through {as_of.isoformat()}"
        )
    screen_window = sessions[-required_screen_sessions:]
    symbols = tuple(sorted(eligible_master))
    raw = market.bars(
        symbols, screen_window[0], as_of, price_mode=PriceMode.RAW, as_of=as_of,
    )
    if raw.empty:
        return ()
    raw = raw.sort_values(["instrument_id", "session_date"], kind="stable")
    latest = raw.loc[
        raw["session_date"].astype(str).eq(as_of.isoformat())
    ].set_index("instrument_id")
    selected: list[str] = []
    removed_on: dict[str, date] = {}
    liquidity: dict[str, float] = {}
    limit_hits: dict[str, int] = {}
    raw_volatility: dict[str, float] = {}
    for instrument_id, rows in raw.groupby("instrument_id", sort=False):
        if instrument_id not in latest.index:
            continue
        current = latest.loc[instrument_id]
        if bool(current.get("suspended", False)) or pd.isna(current.get("is_st")):
            continue
        current_st = bool(current["is_st"])
        if mode == "st" and not current_st:
            continue
        if mode in {"non_st", "recently_removed"} and current_st:
            continue
        recent_amount = rows.tail(amount_window)
        amounts = pd.to_numeric(recent_amount["amount"], errors="coerce")
        if len(recent_amount) != amount_window or amounts.isna().any():
            continue
        average = float(amounts.mean())
        if average < min_avg_amount:
            continue
        if mode == "recently_removed":
            statuses = rows.tail(st_removal_lookback + 1)[
                ["session_date", "is_st"]
            ].dropna(subset=["is_st"])
            transitions = statuses.loc[
                ~statuses["is_st"].astype(bool)
                & statuses["is_st"].shift(1).fillna(False).astype(bool)
            ]
            if transitions.empty:
                continue
            removed_on[instrument_id] = date.fromisoformat(
                str(transitions.iloc[-1]["session_date"])
            )
        hits_count = 0
        if mode != "non_st":
            limits = rows.tail(limit_lookback)
            closes = pd.to_numeric(limits["close"], errors="coerce")
            uppers = pd.to_numeric(limits["limit_up"], errors="coerce")
            lowers = pd.to_numeric(limits["limit_down"], errors="coerce")
            ticks = pd.to_numeric(limits["price_tick"], errors="coerce").fillna(0.01)
            hits = ((closes - uppers).abs() <= ticks / 2) | (
                (closes - lowers).abs() <= ticks / 2
            )
            hits_count = int(hits.fillna(False).sum())
        selected.append(instrument_id)
        liquidity[instrument_id] = average
        limit_hits[instrument_id] = hits_count
        raw_closes = pd.to_numeric(rows.tail(amount_window)["close"], errors="coerce").dropna()
        raw_returns = raw_closes.pct_change(fill_method=None).dropna()
        raw_volatility[instrument_id] = (
            float(raw_returns.std(ddof=0))
            if len(raw_returns) >= max(5, amount_window // 2)
            else float("inf")
        )
    if not selected:
        return ()
    if preselection_count is not None and len(selected) > preselection_count:
        selected = sorted(
            selected,
            key=lambda instrument_id: (
                raw_volatility[instrument_id],
                -liquidity[instrument_id],
                instrument_id,
            ),
        )[:preselection_count]

    adjusted_symbols = tuple(sorted(set(selected) | ({beta_benchmark} if beta_benchmark else set())))
    adjusted = market.adjusted_history(
        adjusted_symbols, signal_start, as_of, as_of=as_of,
    )
    pivot = adjusted.pivot(
        index="session_date", columns="instrument_id", values="close",
    ).sort_index().apply(pd.to_numeric, errors="coerce")
    return_frame = pivot.pct_change(fill_method=None)
    benchmark_returns = (
        return_frame[beta_benchmark] if beta_benchmark is not None else None
    )
    candidates: list[PriceSignalCandidate] = []
    for instrument_id in sorted(selected):
        if instrument_id not in pivot:
            continue
        closes = pivot[instrument_id].dropna()
        required = max(*momentum_windows, volatility_days)
        if len(closes) <= required or closes.iloc[-1] <= 0:
            continue
        momentum: dict[int, float] = {}
        for window in momentum_windows:
            base = float(closes.iloc[-1 - window])
            if base <= 0:
                break
            momentum[window] = float(closes.iloc[-1]) / base - 1.0
        if len(momentum) != len(momentum_windows):
            continue
        sample = closes.tail(volatility_days + 1)
        returns = sample.pct_change(fill_method=None).dropna()
        if len(returns) != volatility_days:
            continue
        volatility = pstdev(map(float, returns)) * sqrt(252)
        beta = None
        if beta_benchmark is not None:
            stock_returns = return_frame[instrument_id]
            assert benchmark_returns is not None
            valid = stock_returns.notna() & benchmark_returns.notna()
            stock_values = tuple(map(float, stock_returns.loc[valid].tail(beta_days)))
            market_values = tuple(map(float, benchmark_returns.loc[valid].tail(beta_days)))
            if len(stock_values) != beta_days:
                continue
            stock_mean = sum(stock_values) / len(stock_values)
            market_mean = sum(market_values) / len(market_values)
            variance = sum((value - market_mean) ** 2 for value in market_values)
            if variance <= 0:
                continue
            beta = sum(
                (stock - stock_mean) * (benchmark - market_mean)
                for stock, benchmark in zip(stock_values, market_values, strict=True)
            ) / variance
        long_window = max(momentum_windows)
        long_average = float(closes.tail(long_window).mean())
        instrument = eligible_master[instrument_id]
        candidates.append(PriceSignalCandidate(
            instrument_id=instrument_id,
            name=instrument.name,
            as_of=as_of,
            listed_date=instrument.listed_date,
            current_is_st=bool(latest.loc[instrument_id, "is_st"]),
            last_close=float(closes.iloc[-1]),
            avg_amount=liquidity[instrument_id],
            momentum=MappingProxyType(momentum),
            volatility=volatility,
            beta=beta,
            above_long_average=float(closes.iloc[-1]) > long_average,
            recent_limit_hits=limit_hits[instrument_id],
            st_removed_on=removed_on.get(instrument_id),
        ))
    return tuple(candidates)
