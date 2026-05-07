from __future__ import annotations

import json
from datetime import datetime

import pandas as pd

from fundlab.backtest.metrics import PerformanceAnalyzer
from fundlab.backtest.recorder import BacktestRecorder
from fundlab.common.hashing import stable_hash
from fundlab.common.ids import new_id
from fundlab.data.storage import SQLiteStore


class BacktestSQLiteWriter:
    def __init__(self, sqlite_store: SQLiteStore):
        self.sqlite_store = sqlite_store

    def persist(
        self,
        recorder: BacktestRecorder,
        strategy_id: str,
        start_date: str,
        end_date: str,
        initial_cash: float,
        config: dict | None = None,
        data_version: str | None = "fake-local",
    ) -> tuple[str, dict]:
        run_id = new_id("bt")
        config_payload = config or {}
        config_json = json.dumps(config_payload, sort_keys=True, ensure_ascii=False, default=str)
        metrics = PerformanceAnalyzer().analyze(recorder.account_daily_frame(), recorder.trades_frame())
        now = datetime.now().isoformat(timespec="seconds")

        self.sqlite_store.execute_many(
            """
            INSERT INTO backtest_run (
                run_id, strategy_id, strategy_name, start_date, end_date, initial_cash,
                config_json, config_hash, data_version, status, created_at, finished_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                [
                    run_id,
                    strategy_id,
                    strategy_id,
                    start_date,
                    end_date,
                    initial_cash,
                    config_json,
                    stable_hash(config_payload),
                    data_version,
                    "success",
                    now,
                    now,
                ]
            ],
        )
        self._persist_account_daily(run_id, recorder.account_daily_frame())
        self._persist_orders(run_id, recorder.orders_frame())
        self._persist_trades(run_id, recorder.trades_frame())
        self._persist_metrics(run_id, metrics)
        return run_id, metrics

    def _persist_account_daily(self, run_id: str, frame: pd.DataFrame) -> None:
        if frame.empty:
            return
        enriched = frame.copy()
        enriched["run_id"] = run_id
        enriched["cumulative_return"] = enriched["nav"] - 1
        enriched["drawdown"] = enriched["nav"] / enriched["nav"].cummax() - 1
        enriched["turnover"] = None
        enriched["cost"] = None
        columns = [
            "run_id",
            "date",
            "cash",
            "market_value",
            "total_asset",
            "nav",
            "daily_return",
            "cumulative_return",
            "drawdown",
            "turnover",
            "cost",
        ]
        self.sqlite_store.execute_many(
            f"""
            INSERT INTO backtest_account_daily ({','.join(columns)})
            VALUES ({','.join('?' for _ in columns)})
            """,
            enriched.loc[:, columns].where(pd.notna(enriched.loc[:, columns]), None).values.tolist(),
        )

    def _persist_orders(self, run_id: str, frame: pd.DataFrame) -> None:
        if frame.empty:
            return
        enriched = frame.copy()
        enriched["run_id"] = run_id
        enriched["target_quantity"] = None
        columns = [
            "order_id",
            "run_id",
            "account_id",
            "signal_date",
            "execution_date",
            "symbol",
            "side",
            "order_type",
            "target_weight",
            "target_quantity",
            "quantity",
            "status",
            "reason",
            "reject_reason",
        ]
        self.sqlite_store.execute_many(
            f"""
            INSERT INTO backtest_order ({','.join(columns)})
            VALUES ({','.join('?' for _ in columns)})
            """,
            enriched.loc[:, columns].where(pd.notna(enriched.loc[:, columns]), None).values.tolist(),
        )

    def _persist_trades(self, run_id: str, frame: pd.DataFrame) -> None:
        if frame.empty:
            return
        enriched = frame.copy()
        enriched["run_id"] = run_id
        enriched["execution_model"] = "simple_next_open"
        columns = [
            "trade_id",
            "order_id",
            "run_id",
            "account_id",
            "datetime",
            "date",
            "symbol",
            "side",
            "price",
            "quantity",
            "amount",
            "fee",
            "slippage",
            "execution_model",
        ]
        self.sqlite_store.execute_many(
            f"""
            INSERT INTO backtest_trade ({','.join(columns)})
            VALUES ({','.join('?' for _ in columns)})
            """,
            enriched.loc[:, columns].where(pd.notna(enriched.loc[:, columns]), None).values.tolist(),
        )

    def _persist_metrics(self, run_id: str, metrics: dict) -> None:
        columns = [
            "run_id",
            "cumulative_return",
            "annualized_return",
            "annualized_volatility",
            "sharpe",
            "calmar",
            "max_drawdown",
            "win_rate",
            "turnover",
            "trade_count",
            "total_cost",
            "benchmark_return",
            "excess_return",
            "max_drawdown_recovery_days",
        ]
        row = [run_id] + [metrics.get(column) for column in columns if column != "run_id"]
        self.sqlite_store.execute_many(
            f"""
            INSERT INTO backtest_metrics ({','.join(columns)})
            VALUES ({','.join('?' for _ in columns)})
            """,
            [row],
        )

