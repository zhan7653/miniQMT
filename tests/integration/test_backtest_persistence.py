from fundlab.backtest.persistence import BacktestSQLiteWriter
from fundlab.common.config import get_path, load_config
from fundlab.data.storage import SQLiteStore
from scripts.run_backtest import run_equal_weight_backtest


def test_run_backtest_persists_run_orders_trades_and_metrics():
    run_id, recorder, metrics = run_equal_weight_backtest()
    config = load_config()
    sqlite_store = SQLiteStore(get_path(config, "sqlite_db"))

    run_count = sqlite_store.read_frame("SELECT COUNT(*) AS count FROM backtest_run WHERE run_id = ?", [run_id]).iloc[0][
        "count"
    ]
    account_count = sqlite_store.read_frame(
        "SELECT COUNT(*) AS count FROM backtest_account_daily WHERE run_id = ?", [run_id]
    ).iloc[0]["count"]
    order_count = sqlite_store.read_frame("SELECT COUNT(*) AS count FROM backtest_order WHERE run_id = ?", [run_id]).iloc[0][
        "count"
    ]
    trade_count = sqlite_store.read_frame("SELECT COUNT(*) AS count FROM backtest_trade WHERE run_id = ?", [run_id]).iloc[0][
        "count"
    ]
    persisted_metrics = sqlite_store.read_frame("SELECT * FROM backtest_metrics WHERE run_id = ?", [run_id])

    assert run_count == 1
    assert account_count == len(recorder.account_daily)
    assert order_count == len(recorder.orders)
    assert trade_count == len(recorder.trades)
    assert not persisted_metrics.empty
    assert persisted_metrics.iloc[0]["trade_count"] == len(recorder.trades)
    assert persisted_metrics.iloc[0]["cumulative_return"] == metrics["cumulative_return"]


def test_backtest_persistence_generates_distinct_run_ids():
    first_run_id, _, _ = run_equal_weight_backtest()
    second_run_id, _, _ = run_equal_weight_backtest()

    assert first_run_id != second_run_id

