import pandas as pd

from fundlab.data.processors import DailyBarQualityChecker


def test_daily_bar_quality_checker_detects_invalid_ohlc():
    data = pd.DataFrame(
        [
            {
                "date": "2026-01-02",
                "symbol": "510300.SH",
                "open": 4.0,
                "high": 3.9,
                "low": 3.8,
                "close": 4.1,
                "volume": 1.0,
                "amount": 4.0,
            }
        ]
    )

    issues = DailyBarQualityChecker().check(data, trading_days=["2026-01-02"])

    assert any(issue.issue_type == "invalid_ohlc" for issue in issues)

