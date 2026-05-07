from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Iterable

import pandas as pd


class SQLiteStore:
    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)

    def connect(self) -> sqlite3.Connection:
        if not self.db_path.exists():
            raise FileNotFoundError(f"SQLite database not found: {self.db_path}")
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        return connection

    def read_frame(self, sql: str, params: Iterable[Any] | dict[str, Any] = ()) -> pd.DataFrame:
        with self.connect() as connection:
            return pd.read_sql_query(sql, connection, params=params)

    def execute_many(self, sql: str, rows: Iterable[Iterable[Any]]) -> None:
        with self.connect() as connection:
            connection.executemany(sql, rows)

    def get_fund_master(self, symbols: list[str] | None = None) -> pd.DataFrame:
        sql = "SELECT * FROM fund_master"
        params: list[Any] = []
        if symbols:
            placeholders = ",".join("?" for _ in symbols)
            sql += f" WHERE symbol IN ({placeholders})"
            params.extend(symbols)
        sql += " ORDER BY symbol"
        return self.read_frame(sql, params)

    def get_trading_days(self, start_date: str, end_date: str) -> pd.DataFrame:
        return self.read_frame(
            """
            SELECT *
            FROM trading_calendar
            WHERE date >= ? AND date <= ? AND is_trading_day = 1
            ORDER BY date
            """,
            [start_date, end_date],
        )

    def get_calendar_frame(self, start_date: str, end_date: str) -> pd.DataFrame:
        return self.read_frame(
            """
            SELECT *
            FROM trading_calendar
            WHERE date >= ? AND date <= ?
            ORDER BY date
            """,
            [start_date, end_date],
        )
