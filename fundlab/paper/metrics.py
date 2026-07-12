from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Mapping, Sequence


TRADING_DAYS = 252


@dataclass(frozen=True)
class MetricResult:
    """A metric value that never hides why it is unavailable."""

    value: float | int | None
    reason: str | None = None

    def __post_init__(self) -> None:
        if self.value is None and not self.reason:
            raise ValueError("an unavailable metric requires a reason")
        if self.value is not None and self.reason is not None:
            raise ValueError("an available metric cannot have an unavailable reason")

    def as_dict(self) -> dict[str, float | int | str | None]:
        return {"value": self.value, "reason": self.reason}


def available(value: float | int) -> MetricResult:
    return MetricResult(value=value)


def unavailable(reason: str) -> MetricResult:
    return MetricResult(value=None, reason=reason)


def performance_metrics(
    nav: Sequence[float], *, risk_free_rate: float = 0.0,
    rolling_windows: Sequence[int] = (5, 20, 60),
) -> dict[str, Any]:
    values = [float(value) for value in nav]
    returns = _returns(values)
    result: dict[str, Any] = {
        "risk_free_rate": available(float(risk_free_rate)),
        "total_return": _total_return(values),
        "annualized_return": _annualized_return(values),
        "annualized_volatility": _volatility(returns),
        "downside_volatility": _downside_volatility(returns),
        "max_drawdown": _max_drawdown(values),
    }
    result["sharpe"] = _ratio(result["annualized_return"], result["annualized_volatility"], risk_free_rate,
                              "annualized volatility is zero")
    result["sortino"] = _ratio(result["annualized_return"], result["downside_volatility"], risk_free_rate,
                               "downside volatility is zero")
    drawdown = result["max_drawdown"]
    result["calmar"] = (
        unavailable("maximum drawdown is unavailable") if drawdown.value is None
        else unavailable("maximum drawdown is zero") if drawdown.value == 0
        else _ratio(result["annualized_return"], available(abs(float(drawdown.value))), 0.0, "")
    )
    result["rolling_returns"] = {
        f"{window}d": _rolling_return(values, int(window)) for window in rolling_windows
    }
    return result


def benchmark_and_excess_metrics(
    account_nav: Sequence[float], benchmark_prices: Sequence[float] | None,
    *, risk_free_rate: float = 0.0, rolling_windows: Sequence[int] = (5, 20, 60),
) -> tuple[dict[str, Any], dict[str, Any]]:
    if benchmark_prices is None:
        reason = "benchmark raw-close history is unavailable"
        names = ("total_return", "annualized_return", "annualized_volatility", "downside_volatility",
                 "max_drawdown", "sharpe", "sortino", "calmar")
        missing = {name: unavailable(reason) for name in names}
        missing["risk_free_rate"] = available(float(risk_free_rate))
        missing["rolling_returns"] = {f"{w}d": unavailable(reason) for w in rolling_windows}
        return missing, {"total_return": unavailable(reason), "annualized_return": unavailable(reason)}
    benchmark = performance_metrics(benchmark_prices, risk_free_rate=risk_free_rate,
                                    rolling_windows=rolling_windows)
    account = performance_metrics(account_nav, risk_free_rate=risk_free_rate,
                                  rolling_windows=rolling_windows)
    excess: dict[str, Any] = {}
    for name in ("total_return", "annualized_return"):
        left, right = account[name], benchmark[name]
        excess[name] = (available(float(left.value) - float(right.value))
                        if left.value is not None and right.value is not None
                        else unavailable(f"account or benchmark {name} is unavailable"))
    return benchmark, excess


def _returns(values: Sequence[float]) -> list[float]:
    return [values[index] / values[index - 1] - 1.0 for index in range(1, len(values))
            if values[index - 1] != 0]


def _total_return(values: Sequence[float]) -> MetricResult:
    if len(values) < 2:
        return unavailable("at least two observations are required")
    if values[0] == 0:
        return unavailable("initial value is zero")
    return available(values[-1] / values[0] - 1.0)


def _annualized_return(values: Sequence[float]) -> MetricResult:
    total = _total_return(values)
    periods = len(values) - 1
    if total.value is None:
        return unavailable(total.reason or "return is unavailable")
    if 1.0 + float(total.value) < 0:
        return unavailable("negative terminal wealth cannot be annualized")
    return available((1.0 + float(total.value)) ** (TRADING_DAYS / periods) - 1.0)


def _volatility(returns: Sequence[float]) -> MetricResult:
    if len(returns) < 2:
        return unavailable("at least two return observations are required")
    mean = sum(returns) / len(returns)
    variance = sum((value - mean) ** 2 for value in returns) / len(returns)
    return available(math.sqrt(variance) * math.sqrt(TRADING_DAYS))


def _downside_volatility(returns: Sequence[float]) -> MetricResult:
    if len(returns) < 2:
        return unavailable("at least two return observations are required")
    downside = [min(value, 0.0) for value in returns]
    return available(math.sqrt(sum(value * value for value in downside) / len(downside)) * math.sqrt(TRADING_DAYS))


def _max_drawdown(values: Sequence[float]) -> MetricResult:
    if not values:
        return unavailable("NAV history is empty")
    peak = values[0]
    worst = 0.0
    for value in values:
        peak = max(peak, value)
        if peak != 0:
            worst = min(worst, value / peak - 1.0)
    return available(worst)


def _ratio(numerator: MetricResult, denominator: MetricResult, risk_free_rate: float,
           zero_reason: str) -> MetricResult:
    if numerator.value is None:
        return unavailable(numerator.reason or "numerator is unavailable")
    if denominator.value is None:
        return unavailable(denominator.reason or "denominator is unavailable")
    if float(denominator.value) == 0:
        return unavailable(zero_reason)
    return available((float(numerator.value) - risk_free_rate) / float(denominator.value))


def _rolling_return(values: Sequence[float], window: int) -> MetricResult:
    if window <= 0:
        return unavailable("rolling window must be positive")
    if len(values) <= window:
        return unavailable(f"at least {window + 1} observations are required")
    base = values[-window - 1]
    if base == 0:
        return unavailable("rolling-window initial value is zero")
    return available(values[-1] / base - 1.0)


def metric_dict(value: Any) -> Any:
    if isinstance(value, MetricResult):
        return value.as_dict()
    if isinstance(value, Mapping):
        return {str(key): metric_dict(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [metric_dict(item) for item in value]
    return value
