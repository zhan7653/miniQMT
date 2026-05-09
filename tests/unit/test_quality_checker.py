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


def test_daily_bar_quality_checker_starts_missing_check_from_listing_date():
    data = pd.DataFrame(
        [
            {
                "date": "2026-01-05",
                "symbol": "159207.SZ",
                "open": 1.0,
                "high": 1.1,
                "low": 0.9,
                "close": 1.05,
                "volume": 100.0,
                "amount": 105.0,
            }
        ]
    )

    issues = DailyBarQualityChecker().check(
        data,
        trading_days=["2026-01-02", "2026-01-05", "2026-01-06"],
        listed_dates={"159207.SZ": "2026-01-05"},
    )

    missing_dates = [issue.date for issue in issues if issue.issue_type == "missing_bar"]
    assert "2026-01-02" not in missing_dates
    assert "2026-01-06" in missing_dates
