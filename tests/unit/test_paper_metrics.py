from __future__ import annotations

import pytest

from fundlab.paper.metrics import MetricResult, benchmark_and_excess_metrics, performance_metrics


def test_metric_requires_explicit_reason_for_null():
    with pytest.raises(ValueError, match="requires a reason"):
        MetricResult(None)


def test_performance_metrics_cover_risk_and_record_zero_risk_free_rate():
    metrics = performance_metrics([100.0, 102.0, 101.0, 104.0], rolling_windows=(2,))
    assert metrics["risk_free_rate"].value == 0.0
    assert metrics["total_return"].value == pytest.approx(0.04)
    assert metrics["annualized_volatility"].value is not None
    assert metrics["downside_volatility"].value is not None
    assert metrics["max_drawdown"].value == pytest.approx(101 / 102 - 1)
    assert metrics["sharpe"].value is not None
    assert metrics["sortino"].value is not None
    assert metrics["calmar"].value is not None
    assert metrics["rolling_returns"]["2d"].value == pytest.approx(104 / 102 - 1)


def test_short_history_and_missing_benchmark_are_explicitly_unavailable():
    metrics = performance_metrics([100.0])
    assert metrics["total_return"].value is None
    assert metrics["total_return"].reason
    benchmark, excess = benchmark_and_excess_metrics([100.0], None)
    assert benchmark["total_return"].value is None
    assert "raw-close" in benchmark["total_return"].reason
    assert excess["total_return"].value is None
