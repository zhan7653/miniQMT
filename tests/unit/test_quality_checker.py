import pandas as pd

from fundlab.data.platform import QualityDisposition
from fundlab.data.processors import DailyBarQualityChecker


def _bar(symbol="A", date="2026-01-02", **changes):
    row = {"date": date, "symbol": symbol, "open": 10.0, "high": 11.0, "low": 9.0,
           "close": 10.5, "volume": 100.0, "amount": 1000.0}
    row.update(changes)
    return row


def test_system_error_blocks_batch_and_is_machine_readable():
    report = DailyBarQualityChecker().check(pd.DataFrame([{"date": "2026-01-02"}]))
    assert report.disposition is QualityDisposition.BLOCK_BATCH
    assert not report.publication_eligible
    assert report[0].scope == "batch"
    assert report[0].severity == "error"


def test_invalid_symbol_rows_isolate_only_affected_symbols():
    data = pd.DataFrame([_bar("A", high=8.0), _bar("B", amount=-1.0), _bar("C"), _bar("C")])
    report = DailyBarQualityChecker().check(data)
    assert report.disposition is QualityDisposition.BLOCK_SYMBOL
    assert report.publication_eligible
    assert report.blocked_symbols == ("A", "B", "C")
    assert {issue.issue_type for issue in report} >= {"invalid_ohlc", "negative_liquidity", "duplicate_key"}


def test_confirmed_suspension_is_valid_but_unexplained_missing_bar_blocks_symbol():
    checker = DailyBarQualityChecker()
    suspended = checker.check(pd.DataFrame([_bar(open=10, high=10, low=10, close=10, volume=0, amount=0)]),
                              trading_days=["2026-01-02"], suspended=[("A", "2026-01-02")])
    assert suspended.disposition is QualityDisposition.VALID_SUSPENDED
    assert suspended.publication_eligible

    missing = checker.check(pd.DataFrame([_bar(date="2026-01-02")]),
                            trading_days=["2026-01-02", "2026-01-05"], requested_symbols=["A"])
    assert missing.disposition is QualityDisposition.BLOCK_SYMBOL
    assert any(issue.issue_type == "missing_active_bar" and issue.date == "2026-01-05" for issue in missing)


def test_listing_date_excludes_prelisting_missing_days():
    report = DailyBarQualityChecker().check(pd.DataFrame([_bar("A", "2026-01-05")]),
                                            trading_days=["2026-01-02", "2026-01-05", "2026-01-06"],
                                            listed_dates={"A": "2026-01-05"})
    dates = [issue.date for issue in report if issue.issue_type == "missing_active_bar"]
    assert dates == ["2026-01-06"]
