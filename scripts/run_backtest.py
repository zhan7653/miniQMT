from __future__ import annotations

from fundlab.backtest import BacktestEngine
from fundlab.backtest.persistence import BacktestSQLiteWriter
from fundlab.common.config import get_path, load_config
from fundlab.data.portal import DataPortal
from fundlab.data.storage import ParquetStore, SQLiteStore
from fundlab.strategies import EqualWeightStrategy


def run_equal_weight_backtest(
    start_date: str | None = None,
    end_date: str | None = None,
    symbols: list[str] | None = None,
    prepare_fake_data: bool = True,
):
    if prepare_fake_data:
        from scripts.create_fake_data import create_fake_data

        create_fake_data()
    config = load_config()
    if prepare_fake_data:
        start_date = start_date or "2026-01-02"
        end_date = end_date or "2026-02-12"
        symbols = symbols or ["510300.SH", "510500.SH", "518880.SH"]
    else:
        start_date = start_date or config.get("data", {}).get("default_start_date", "2023-01-01")
        end_date = end_date or config.get("data", {}).get("default_end_date", "2026-05-07")
        symbols = symbols or config.get("data", {}).get("validation_symbols", ["510300.SH", "510500.SH", "518880.SH"])
    sqlite_store = SQLiteStore(get_path(config, "sqlite_db"))
    portal = DataPortal(
        sqlite_store=sqlite_store,
        parquet_store=ParquetStore(get_path(config, "parquet_root")),
        config=config,
    )
    strategy = EqualWeightStrategy(symbols, cash_weight=0.02)
    engine = BacktestEngine(
        data_portal=portal,
        strategy=strategy,
        start_date=start_date,
        end_date=end_date,
        initial_cash=1_000_000,
        rebalance_frequency="monthly",
    )
    recorder = engine.run()
    run_id, metrics = BacktestSQLiteWriter(sqlite_store).persist(
        recorder=recorder,
        strategy_id=strategy.strategy_id,
        start_date=start_date,
        end_date=end_date,
        initial_cash=1_000_000,
        config={"strategy": strategy.strategy_id, "rebalance_frequency": "monthly"},
    )
    return run_id, recorder, metrics


def main() -> None:
    run_id, recorder, metrics = run_equal_weight_backtest(prepare_fake_data=False)
    account_daily = recorder.account_daily_frame()
    print(f"run_id={run_id}")
    print(account_daily.tail(1).to_string(index=False))
    print(f"orders={len(recorder.orders)} trades={len(recorder.trades)}")
    print(
        "metrics="
        f"cum_return={metrics['cumulative_return']:.6f}, "
        f"max_drawdown={metrics['max_drawdown']:.6f}, "
        f"sharpe={metrics['sharpe']:.6f}"
    )


if __name__ == "__main__":
    main()
