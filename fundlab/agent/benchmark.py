"""Point-in-time benchmark evidence for the dividend-value review loop."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Mapping

import pandas as pd

from fundlab.marketdata import AssetType


RATIO = Decimal("0.00000001")


@dataclass(frozen=True)
class PortfolioPerformancePoint:
    """One promoted end-of-session account valuation used for comparison."""

    session_date: date
    nav: Decimal
    market_value: Decimal

    def __post_init__(self) -> None:
        nav = _decimal(self.nav, "portfolio nav")
        market_value = _decimal(self.market_value, "portfolio market value")
        if nav <= 0 or market_value < 0:
            raise ValueError("Portfolio performance points require positive NAV and value >= 0")
        object.__setattr__(self, "nav", nav)
        object.__setattr__(self, "market_value", market_value)


@dataclass(frozen=True)
class BenchmarkEvaluation:
    """Bounded facts comparing the live paper portfolio with one passive ETF."""

    instrument_id: str
    name: str
    as_of: date
    minimum_actionable_sessions: int
    status: str
    reason: str | None
    actionability: str
    comparison_start: date | None = None
    comparison_end: date | None = None
    common_sessions: int = 0
    portfolio_base_nav: Decimal | None = None
    portfolio_nav: Decimal | None = None
    portfolio_normalized_nav: Decimal | None = None
    portfolio_return: Decimal | None = None
    portfolio_max_drawdown: Decimal | None = None
    benchmark_start_price: Decimal | None = None
    benchmark_price: Decimal | None = None
    benchmark_nav: Decimal | None = None
    benchmark_normalized_nav: Decimal | None = None
    benchmark_total_return: Decimal | None = None
    benchmark_max_drawdown: Decimal | None = None
    excess_return: Decimal | None = None
    since_previous_review: Mapping[str, object] | None = None

    @property
    def can_support_rebalance(self) -> bool:
        return self.actionability == "supporting_evidence"

    def evidence(self) -> dict[str, object]:
        return {
            "schema_version": "dividend-benchmark-v1",
            "instrument_id": self.instrument_id,
            "name": self.name,
            "as_of": self.as_of.isoformat(),
            "status": self.status,
            "reason": self.reason,
            "comparison_start": _date_text(self.comparison_start),
            "comparison_end": _date_text(self.comparison_end),
            "common_trading_sessions": self.common_sessions,
            "minimum_actionable_common_sessions": self.minimum_actionable_sessions,
            "actionability": self.actionability,
            "can_support_rebalance": self.can_support_rebalance,
            "return_basis": (
                "ETF point-in-time adjusted total return including cash distributions; "
                "portfolio return is net of its actual simulated fees and slippage"
            ),
            "portfolio_base_nav": _decimal_text(self.portfolio_base_nav),
            "portfolio_nav": _decimal_text(self.portfolio_nav),
            "portfolio_normalized_nav": _decimal_text(self.portfolio_normalized_nav),
            "portfolio_return": _decimal_text(self.portfolio_return),
            "portfolio_max_drawdown": _decimal_text(self.portfolio_max_drawdown),
            "benchmark_start_adjusted_price": _decimal_text(self.benchmark_start_price),
            "benchmark_adjusted_price": _decimal_text(self.benchmark_price),
            "benchmark_nav_on_portfolio_scale": _decimal_text(self.benchmark_nav),
            "benchmark_normalized_nav": _decimal_text(self.benchmark_normalized_nav),
            "benchmark_total_return": _decimal_text(self.benchmark_total_return),
            "benchmark_max_drawdown": _decimal_text(self.benchmark_max_drawdown),
            "excess_return": _decimal_text(self.excess_return),
            "since_previous_review": self.since_previous_review,
        }


def unavailable_benchmark(
    instrument_id: str,
    *,
    name: str | None,
    as_of: date,
    minimum_actionable_sessions: int,
    status: str,
    reason: str,
) -> BenchmarkEvaluation:
    return BenchmarkEvaluation(
        instrument_id=instrument_id,
        name=name or instrument_id,
        as_of=as_of,
        minimum_actionable_sessions=minimum_actionable_sessions,
        status=status,
        reason=reason,
        actionability="unavailable",
    )


def evaluate_benchmark(
    market,
    *,
    instrument_id: str,
    as_of: date,
    portfolio_points: tuple[PortfolioPerformancePoint, ...],
    minimum_actionable_sessions: int,
    previous_review_as_of: date | None = None,
) -> BenchmarkEvaluation:
    """Compare from the opening of the portfolio's first invested session.

    The immediately preceding promoted portfolio checkpoint is the strategy
    baseline.  The ETF starts at its point-in-time adjusted opening price on
    the first session where the strategy actually has market exposure.  This
    includes first-day execution effects on both sides without backfilling the
    account's earlier cash-only period.
    """

    if minimum_actionable_sessions < 1:
        raise ValueError("minimum_actionable_sessions must be positive")
    instrument_id = instrument_id.strip()
    if not instrument_id:
        raise ValueError("benchmark instrument cannot be empty")
    try:
        instrument = market.instrument(instrument_id)
    except Exception as exc:  # noqa: BLE001 - evidence degrades without blocking the Agent
        return unavailable_benchmark(
            instrument_id,
            name=None,
            as_of=as_of,
            minimum_actionable_sessions=minimum_actionable_sessions,
            status="unavailable",
            reason=f"{type(exc).__name__}: {str(exc)[:500]}",
        )
    asset_type = getattr(instrument.asset_type, "value", instrument.asset_type)
    if asset_type != AssetType.ETF.value:
        return unavailable_benchmark(
            instrument_id,
            name=str(instrument.name),
            as_of=as_of,
            minimum_actionable_sessions=minimum_actionable_sessions,
            status="unavailable",
            reason=f"configured benchmark is not an ETF: {asset_type}",
        )

    points = tuple(sorted(
        (item for item in portfolio_points if item.session_date <= as_of),
        key=lambda item: item.session_date,
    ))
    if len({item.session_date for item in points}) != len(points):
        return unavailable_benchmark(
            instrument_id,
            name=str(instrument.name),
            as_of=as_of,
            minimum_actionable_sessions=minimum_actionable_sessions,
            status="unavailable",
            reason="portfolio performance history contains duplicate session dates",
        )
    first_invested = next(
        (index for index, item in enumerate(points) if item.market_value > 0),
        None,
    )
    if first_invested is None:
        return unavailable_benchmark(
            instrument_id,
            name=str(instrument.name),
            as_of=as_of,
            minimum_actionable_sessions=minimum_actionable_sessions,
            status="waiting_for_first_investment",
            reason="portfolio has no promoted checkpoint with market exposure yet",
        )
    if first_invested == 0:
        return unavailable_benchmark(
            instrument_id,
            name=str(instrument.name),
            as_of=as_of,
            minimum_actionable_sessions=minimum_actionable_sessions,
            status="unavailable",
            reason="portfolio lacks a promoted pre-investment NAV checkpoint",
        )

    start = points[first_invested].session_date
    end = points[-1].session_date
    base_nav = points[first_invested - 1].nav
    try:
        bars = market.adjusted_history(
            (instrument_id,), start, end, as_of=as_of,
        )
        bar_map = _adjusted_bar_map(bars)
    except Exception as exc:  # noqa: BLE001 - benchmark failure is observable degradation
        return unavailable_benchmark(
            instrument_id,
            name=str(instrument.name),
            as_of=as_of,
            minimum_actionable_sessions=minimum_actionable_sessions,
            status="unavailable",
            reason=f"{type(exc).__name__}: {str(exc)[:500]}",
        )
    start_bar = bar_map.get(start)
    end_bar = bar_map.get(end)
    if start_bar is None or start_bar[0] is None:
        return unavailable_benchmark(
            instrument_id,
            name=str(instrument.name),
            as_of=as_of,
            minimum_actionable_sessions=minimum_actionable_sessions,
            status="unavailable",
            reason=f"benchmark lacks an adjusted opening price on {start.isoformat()}",
        )
    if end_bar is None or end_bar[1] is None:
        return unavailable_benchmark(
            instrument_id,
            name=str(instrument.name),
            as_of=as_of,
            minimum_actionable_sessions=minimum_actionable_sessions,
            status="unavailable",
            reason=f"benchmark lacks an adjusted close on {end.isoformat()}",
        )

    portfolio_by_date = {item.session_date: item for item in points[first_invested:]}
    common_dates = tuple(
        day for day in sorted(set(portfolio_by_date) & set(bar_map))
        if bar_map[day][1] is not None
    )
    if not common_dates or common_dates[0] != start or common_dates[-1] != end:
        return unavailable_benchmark(
            instrument_id,
            name=str(instrument.name),
            as_of=as_of,
            minimum_actionable_sessions=minimum_actionable_sessions,
            status="unavailable",
            reason="portfolio and benchmark do not share the required start/end valuations",
        )

    start_price = start_bar[0]
    assert start_price is not None
    portfolio_path = [Decimal("1")]
    benchmark_path = [Decimal("1")]
    for day in common_dates:
        close = bar_map[day][1]
        assert close is not None
        portfolio_path.append(_ratio(portfolio_by_date[day].nav, base_nav))
        benchmark_path.append(_ratio(close, start_price))
    portfolio_normalized = portfolio_path[-1]
    benchmark_normalized = benchmark_path[-1]
    portfolio_return = _quantize(portfolio_normalized - Decimal("1"))
    benchmark_return = _quantize(benchmark_normalized - Decimal("1"))
    excess_return = _quantize(portfolio_return - benchmark_return)
    common_sessions = len(common_dates)
    actionability = (
        "supporting_evidence"
        if common_sessions >= minimum_actionable_sessions
        else "diagnostic_only"
    )
    previous = _previous_review_evidence(
        previous_review_as_of,
        common_dates=common_dates,
        portfolio_by_date=portfolio_by_date,
        bar_map=bar_map,
    )
    return BenchmarkEvaluation(
        instrument_id=instrument_id,
        name=str(instrument.name),
        as_of=as_of,
        minimum_actionable_sessions=minimum_actionable_sessions,
        status="ready",
        reason=None,
        actionability=actionability,
        comparison_start=start,
        comparison_end=end,
        common_sessions=common_sessions,
        portfolio_base_nav=base_nav,
        portfolio_nav=portfolio_by_date[end].nav,
        portfolio_normalized_nav=portfolio_normalized,
        portfolio_return=portfolio_return,
        portfolio_max_drawdown=_max_drawdown(portfolio_path),
        benchmark_start_price=start_price,
        benchmark_price=end_bar[1],
        benchmark_nav=_quantize(base_nav * benchmark_normalized),
        benchmark_normalized_nav=benchmark_normalized,
        benchmark_total_return=benchmark_return,
        benchmark_max_drawdown=_max_drawdown(benchmark_path),
        excess_return=excess_return,
        since_previous_review=previous,
    )


def _adjusted_bar_map(frame: pd.DataFrame) -> dict[date, tuple[Decimal | None, Decimal | None]]:
    required = {"session_date", "open", "close"}
    if not required <= set(frame.columns):
        raise ValueError(f"adjusted benchmark bars miss columns: {sorted(required - set(frame))}")
    found: dict[date, tuple[Decimal | None, Decimal | None]] = {}
    for row in frame.to_dict("records"):
        day = date.fromisoformat(str(row["session_date"])[:10])
        if day in found:
            raise ValueError(f"duplicate adjusted benchmark bar: {day.isoformat()}")
        found[day] = (_optional_positive(row.get("open")), _optional_positive(row.get("close")))
    return found


def _previous_review_evidence(
    previous_review_as_of: date | None,
    *,
    common_dates: tuple[date, ...],
    portfolio_by_date: Mapping[date, PortfolioPerformancePoint],
    bar_map: Mapping[date, tuple[Decimal | None, Decimal | None]],
) -> dict[str, object] | None:
    if (
        previous_review_as_of is None
        or previous_review_as_of not in portfolio_by_date
        or previous_review_as_of not in bar_map
        or previous_review_as_of >= common_dates[-1]
        or previous_review_as_of < common_dates[0]
    ):
        return None
    previous_close = bar_map[previous_review_as_of][1]
    end_close = bar_map[common_dates[-1]][1]
    if previous_close is None or end_close is None:
        return None
    portfolio_return = _quantize(
        _ratio(portfolio_by_date[common_dates[-1]].nav, portfolio_by_date[previous_review_as_of].nav)
        - Decimal("1")
    )
    benchmark_return = _quantize(_ratio(end_close, previous_close) - Decimal("1"))
    return {
        "start": previous_review_as_of.isoformat(),
        "end": common_dates[-1].isoformat(),
        "common_trading_sessions": sum(day > previous_review_as_of for day in common_dates),
        "portfolio_return": str(portfolio_return),
        "benchmark_total_return": str(benchmark_return),
        "excess_return": str(_quantize(portfolio_return - benchmark_return)),
        "role": "diagnostic review-to-review comparison",
    }


def _max_drawdown(values: list[Decimal]) -> Decimal:
    peak: Decimal | None = None
    maximum = Decimal("0")
    for value in values:
        peak = value if peak is None else max(peak, value)
        if peak > 0:
            maximum = max(maximum, (peak - value) / peak)
    return _quantize(maximum)


def _optional_positive(value: object) -> Decimal | None:
    if value is None or pd.isna(value):
        return None
    parsed = _decimal(value, "benchmark price")
    return parsed if parsed > 0 else None


def _decimal(value: object, label: str) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ArithmeticError, TypeError, ValueError) as exc:
        raise ValueError(f"{label} is not a decimal: {value!r}") from exc
    if not parsed.is_finite():
        raise ValueError(f"{label} must be finite: {value!r}")
    return parsed


def _ratio(numerator: Decimal, denominator: Decimal) -> Decimal:
    if denominator <= 0:
        raise ValueError("benchmark comparison denominator must be positive")
    return _quantize(numerator / denominator)


def _quantize(value: Decimal) -> Decimal:
    return value.quantize(RATIO, rounding=ROUND_HALF_UP)


def _decimal_text(value: Decimal | None) -> str | None:
    return None if value is None else str(value)


def _date_text(value: date | None) -> str | None:
    return None if value is None else value.isoformat()
