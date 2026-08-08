"""Point-in-time feature extraction for the local decision agent.

Everything here reads the published snapshot exclusively through the
research-adjusted portal view, pinned to an explicit ``as_of``.  The portal
already refuses any query past that boundary, so a policy built on these
features cannot look ahead by construction.
"""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import date, timedelta
from math import sqrt
from statistics import pstdev
from types import MappingProxyType
from typing import Iterable, Mapping

import pandas as pd

from fundlab.marketdata.portal import CanonicalMarketData


@dataclass(frozen=True)
class InstrumentSnapshot:
    """Deterministic per-instrument features visible at ``as_of``."""

    instrument_id: str
    as_of: date
    sessions: int
    last_close: float | None
    momentum: Mapping[int, float]
    volatility: Mapping[int, float] = field(
        default_factory=lambda: MappingProxyType({})
    )

    def momentum_for(self, window: int) -> float | None:
        return self.momentum.get(window)

    def volatility_for(self, window: int) -> float | None:
        return self.volatility.get(window)


@dataclass(frozen=True)
class CrisisInstrumentFeatures:
    """One ETF's point-in-time drawdown, reversal, and volatility evidence."""

    instrument_id: str
    as_of: date
    sessions: int
    current_close: float | None
    current_drawdown: float | None
    event_drawdown: float | None
    rebound_from_low: float | None
    recovery_ratio: float | None
    above_confirmation_average: bool | None
    annualized_volatility: float | None
    event_low_close: float | None = None
    event_peak_close: float | None = None


_CRISIS_CLOSE_CACHE_LIMIT = 512
_CRISIS_CLOSE_CACHE: OrderedDict[
    tuple[str, date, date, str], tuple[float, ...]
] = OrderedDict()


def build_instrument_snapshots(
    market: CanonicalMarketData,
    instrument_ids: Iterable[str],
    *,
    as_of: date,
    windows: tuple[int, ...],
) -> dict[str, InstrumentSnapshot]:
    """Compute adjusted-close momentum and volatility as of one session.

    ``momentum[w]`` is ``close[t] / close[t - w] - 1`` over the last ``w``
    *traded* sessions (suspension rows carry null OHLC and are skipped).  A
    ``volatility[w]`` is the population standard deviation of the last ``w``
    simple close-to-close returns, annualized by ``sqrt(252)``. A window
    without enough history is simply absent — the policy decides whether that
    is fatal.
    """
    symbols = tuple(sorted(set(instrument_ids)))
    if not symbols:
        return {}
    max_window = max(windows) if windows else 0
    # Trading sessions, weekends and holidays: ~250 sessions/year means a
    # calendar span of 2x the window plus a buffer always covers it.
    start = as_of - timedelta(days=max(max_window * 2 + 30, 60))
    frame = market.adjusted_history(symbols, start, as_of, as_of=as_of)
    snapshots: dict[str, InstrumentSnapshot] = {}
    for symbol in symbols:
        closes: list[float] = []
        if not frame.empty:
            rows = frame[frame["instrument_id"].astype(str) == symbol]
            for value in rows.sort_values("session_date")["close"]:
                # Suspension rows carry nullable-Float64 pd.NA closes; a bare
                # `value == value` check raises on pd.NA instead of skipping.
                if value is not None and not pd.isna(value):
                    closes.append(float(value))
        momentum: dict[int, float] = {}
        volatility: dict[int, float] = {}
        for window in windows:
            if window > 0 and len(closes) > window and closes[-1 - window] > 0:
                momentum[window] = closes[-1] / closes[-1 - window] - 1.0
                sample = closes[-1 - window:]
                returns = [
                    sample[index] / sample[index - 1] - 1.0
                    for index in range(1, len(sample))
                    if sample[index - 1] > 0
                ]
                if len(returns) == window:
                    volatility[window] = pstdev(returns) * sqrt(252)
        snapshots[symbol] = InstrumentSnapshot(
            instrument_id=symbol,
            as_of=as_of,
            sessions=len(closes),
            last_close=closes[-1] if closes else None,
            momentum=MappingProxyType(momentum),
            volatility=MappingProxyType(volatility),
        )
    return snapshots


def build_crisis_features(
    market: CanonicalMarketData,
    instrument_ids: Iterable[str],
    *,
    as_of: date,
    drawdown_days: int,
    event_lookback_days: int,
    confirmation_days: int,
    volatility_days: int,
) -> dict[str, CrisisInstrumentFeatures]:
    """Build crisis-entry evidence from adjusted ETF closes only.

    The recent event is the worst peak-to-trough drawdown whose trough falls
    inside ``event_lookback_days``.  Its peak may be older, but never older
    than the declared ``drawdown_days`` window.  Missing history remains
    explicit as ``None`` so a policy can fail closed instead of shortening a
    window silently.
    """

    windows = (
        drawdown_days,
        event_lookback_days,
        confirmation_days,
        volatility_days,
    )
    if any(window < 1 for window in windows):
        raise ValueError("crisis feature windows must be positive")
    if event_lookback_days > drawdown_days:
        raise ValueError("event_lookback_days cannot exceed drawdown_days")
    symbols = tuple(sorted(set(instrument_ids)))
    if not symbols:
        return {}

    required_closes = max(
        drawdown_days,
        event_lookback_days,
        confirmation_days,
        volatility_days + 1,
    )
    start = as_of - timedelta(days=max(required_closes * 2 + 30, 60))
    close_history = _cached_crisis_closes(
        market, symbols, start=start, as_of=as_of,
    )
    features: dict[str, CrisisInstrumentFeatures] = {}
    for symbol in symbols:
        closes = pd.Series(close_history[symbol], dtype="float64")

        current_close = float(closes.iloc[-1]) if not closes.empty else None
        current_drawdown = None
        event_drawdown = None
        rebound_from_low = None
        recovery_ratio = None
        above_confirmation_average = None
        annualized_volatility = None
        event_low_close = None
        event_peak_close = None

        if len(closes) >= drawdown_days:
            drawdown_history = closes.tail(drawdown_days).reset_index(drop=True)
            running_peak = drawdown_history.cummax()
            drawdowns = drawdown_history / running_peak - 1.0
            recent_drawdowns = drawdowns.tail(event_lookback_days)
            trough_index = int(recent_drawdowns.idxmin())
            event_drawdown = float(drawdowns.loc[trough_index])
            event_low_close = float(drawdown_history.loc[trough_index])
            event_peak_close = float(running_peak.loc[trough_index])
            current_drawdown = float(drawdowns.iloc[-1])
            if event_low_close > 0 and event_peak_close > 0:
                rebound_from_low = float(current_close / event_low_close - 1.0)
                recovery_ratio = float(current_close / event_peak_close)

        if len(closes) >= confirmation_days:
            confirmation = closes.tail(confirmation_days)
            above_confirmation_average = bool(
                current_close > float(confirmation.mean())
            )

        if len(closes) >= volatility_days + 1:
            returns = closes.tail(volatility_days + 1).pct_change().dropna()
            if len(returns) == volatility_days:
                annualized_volatility = float(returns.std(ddof=0) * sqrt(252))

        features[symbol] = CrisisInstrumentFeatures(
            instrument_id=symbol,
            as_of=as_of,
            sessions=len(closes),
            current_close=current_close,
            current_drawdown=current_drawdown,
            event_drawdown=event_drawdown,
            rebound_from_low=rebound_from_low,
            recovery_ratio=recovery_ratio,
            above_confirmation_average=above_confirmation_average,
            annualized_volatility=annualized_volatility,
            event_low_close=event_low_close,
            event_peak_close=event_peak_close,
        )
    return features


def _cached_crisis_closes(
    market: CanonicalMarketData,
    symbols: tuple[str, ...],
    *,
    start: date,
    as_of: date,
) -> dict[str, tuple[float, ...]]:
    """Reuse immutable close series across crisis variants in one CLI process."""

    snapshot_id = market.snapshot_id
    result: dict[str, tuple[float, ...]] = {}
    missing: list[str] = []
    for symbol in symbols:
        key = (snapshot_id, start, as_of, symbol)
        cached = _CRISIS_CLOSE_CACHE.get(key)
        if cached is None:
            missing.append(symbol)
            continue
        _CRISIS_CLOSE_CACHE.move_to_end(key)
        result[symbol] = cached

    if missing:
        frame = market.adjusted_history(missing, start, as_of, as_of=as_of)
        for symbol in missing:
            values: tuple[float, ...] = ()
            if not frame.empty:
                rows = frame[frame["instrument_id"].astype(str) == symbol]
                closes = pd.to_numeric(
                    rows.sort_values("session_date")["close"], errors="coerce",
                ).dropna()
                values = tuple(float(value) for value in closes)
            key = (snapshot_id, start, as_of, symbol)
            _CRISIS_CLOSE_CACHE[key] = values
            _CRISIS_CLOSE_CACHE.move_to_end(key)
            result[symbol] = values
        while len(_CRISIS_CLOSE_CACHE) > _CRISIS_CLOSE_CACHE_LIMIT:
            _CRISIS_CLOSE_CACHE.popitem(last=False)
    return result


def build_aligned_return_window(
    market: CanonicalMarketData,
    instrument_ids: Iterable[str],
    *,
    as_of: date,
    sessions: int,
) -> pd.DataFrame:
    """Return one complete adjusted simple-return window for all instruments.

    Rows are aligned by trading date and any date with a missing close for one
    declared instrument is discarded.  Callers must still verify that exactly
    ``sessions`` observations remain; returning a shorter frame keeps feature
    extraction separate from each policy's fail-closed decision.
    """

    symbols = tuple(sorted(set(instrument_ids)))
    if not symbols:
        return pd.DataFrame()
    if sessions < 1:
        raise ValueError("sessions must be positive")
    start = as_of - timedelta(days=max(sessions * 2 + 30, 60))
    frame = market.adjusted_history(symbols, start, as_of, as_of=as_of)
    if frame.empty:
        return pd.DataFrame(columns=symbols)
    closes = frame.pivot(
        index="session_date", columns="instrument_id", values="close",
    ).sort_index()
    closes = closes.reindex(columns=symbols).apply(pd.to_numeric, errors="coerce")
    returns = closes.pct_change(fill_method=None).dropna(how="any")
    return returns.tail(sessions).reset_index(drop=True)
