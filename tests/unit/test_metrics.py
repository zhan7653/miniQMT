import pandas as pd

from fundlab.backtest.metrics import PerformanceAnalyzer


def test_performance_analyzer_basic_metrics():
    account_daily = pd.DataFrame(
        [
            {"date": "2026-01-01", "nav": 1.0, "daily_return": 0.0, "total_asset": 100.0},
            {"date": "2026-01-02", "nav": 1.1, "daily_return": 0.1, "total_asset": 110.0},
            {"date": "2026-01-03", "nav": 1.05, "daily_return": -0.0454545, "total_asset": 105.0},
        ]
    )
    trades = pd.DataFrame([{"amount": 10.0, "fee": 0.1}, {"amount": 20.0, "fee": 0.2}])

    metrics = PerformanceAnalyzer().analyze(account_daily, trades)

    assert metrics["cumulative_return"] == 0.050000000000000044
    assert metrics["max_drawdown"] < 0
    assert metrics["trade_count"] == 2
    assert metrics["total_cost"] == 0.30000000000000004

