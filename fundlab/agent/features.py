"""Point-in-time feature extraction for the local decision agent.

Everything here reads the published snapshot exclusively through the
research-adjusted portal view, pinned to an explicit ``as_of``.  The portal
already refuses any query past that boundary, so a policy built on these
features cannot look ahead by construction.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
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

    def momentum_for(self, window: int) -> float | None:
        return self.momentum.get(window)


def build_instrument_snapshots(
    market: CanonicalMarketData,
    instrument_ids: Iterable[str],
    *,
    as_of: date,
    windows: tuple[int, ...],
) -> dict[str, InstrumentSnapshot]:
    """Compute adjusted-close momentum features as of one session.

    ``momentum[w]`` is ``close[t] / close[t - w] - 1`` over the last ``w``
    *traded* sessions (suspension rows carry null OHLC and are skipped).  A
    window without enough history is simply absent — the policy decides
    whether that is fatal.
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
        for window in windows:
            if window > 0 and len(closes) > window and closes[-1 - window] > 0:
                momentum[window] = closes[-1] / closes[-1 - window] - 1.0
        snapshots[symbol] = InstrumentSnapshot(
            instrument_id=symbol,
            as_of=as_of,
            sessions=len(closes),
            last_close=closes[-1] if closes else None,
            momentum=MappingProxyType(momentum),
        )
    return snapshots
