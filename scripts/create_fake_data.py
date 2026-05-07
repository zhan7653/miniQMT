from __future__ import annotations

import sqlite3
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from fundlab.common.config import get_path, load_config
from fundlab.data.loaders import DividendLoader, IndexValuationLoader, NavLoader
from fundlab.data.storage import SQLiteStore
from scripts.init_db import init_db


FAKE_FUNDS = [
    {
        "symbol": "510300.SH",
        "raw_symbol": "510300",
        "name": "沪深300ETF",
        "exchange": "SH",
        "product_type": "ETF",
        "management_type": "passive_index",
        "asset_class": "equity",
        "category": "broad_based",
        "tracking_index": "沪深300",
        "tracking_index_code": "000300.SH",
        "fund_company": "fake",
        "listed_date": "2012-05-28",
        "delisted_date": None,
        "expense_ratio": 0.005,
        "custody_fee": 0.001,
        "lot_size": 100,
        "price_tick": 0.001,
        "is_active": 1,
        "include_in_universe": 1,
        "exclusion_reason": None,
        "source": "fake",
        "source_updated_at": "2026-01-01T00:00:00",
    },
    {
        "symbol": "510500.SH",
        "raw_symbol": "510500",
        "name": "中证500ETF",
        "exchange": "SH",
        "product_type": "ETF",
        "management_type": "passive_index",
        "asset_class": "equity",
        "category": "broad_based",
        "tracking_index": "中证500",
        "tracking_index_code": "000905.SH",
        "fund_company": "fake",
        "listed_date": "2013-03-15",
        "delisted_date": None,
        "expense_ratio": 0.005,
        "custody_fee": 0.001,
        "lot_size": 100,
        "price_tick": 0.001,
        "is_active": 1,
        "include_in_universe": 1,
        "exclusion_reason": None,
        "source": "fake",
        "source_updated_at": "2026-01-01T00:00:00",
    },
    {
        "symbol": "518880.SH",
        "raw_symbol": "518880",
        "name": "黄金ETF",
        "exchange": "SH",
        "product_type": "ETF",
        "management_type": "passive_commodity",
        "asset_class": "commodity",
        "category": "gold",
        "tracking_index": "黄金现货",
        "tracking_index_code": "AU9999.SH",
        "fund_company": "fake",
        "listed_date": "2013-07-29",
        "delisted_date": None,
        "expense_ratio": 0.006,
        "custody_fee": 0.001,
        "lot_size": 100,
        "price_tick": 0.001,
        "is_active": 1,
        "include_in_universe": 1,
        "exclusion_reason": None,
        "source": "fake",
        "source_updated_at": "2026-01-01T00:00:00",
    },
]


def business_days(start: date, count: int) -> list[date]:
    days: list[date] = []
    current = start
    while len(days) < count:
        if current.weekday() < 5:
            days.append(current)
        current += timedelta(days=1)
    return days


def write_fund_master(connection: sqlite3.Connection) -> None:
    columns = list(FAKE_FUNDS[0].keys())
    placeholders = ",".join("?" for _ in columns)
    update_columns = ",".join(f"{column}=excluded.{column}" for column in columns if column != "symbol")
    sql = f"""
        INSERT INTO fund_master ({','.join(columns)})
        VALUES ({placeholders})
        ON CONFLICT(symbol) DO UPDATE SET {update_columns}
    """
    connection.executemany(sql, [[fund[column] for column in columns] for fund in FAKE_FUNDS])


def write_calendar(connection: sqlite3.Connection, trading_days: list[date]) -> None:
    rows = []
    for index, trading_day in enumerate(trading_days):
        previous_day = trading_days[index - 1].isoformat() if index > 0 else None
        next_day = trading_days[index + 1].isoformat() if index + 1 < len(trading_days) else None
        is_week_end = index + 1 == len(trading_days) or trading_days[index + 1].weekday() < trading_day.weekday()
        is_month_end = index + 1 == len(trading_days) or trading_days[index + 1].month != trading_day.month
        is_quarter_end = is_month_end and trading_day.month in {3, 6, 9, 12}
        rows.append(
            (
                trading_day.isoformat(),
                "CN",
                1,
                previous_day,
                next_day,
                int(is_month_end),
                int(is_week_end),
                int(is_quarter_end),
            )
        )

    connection.executemany(
        """
        INSERT INTO trading_calendar (
            date, exchange, is_trading_day, previous_trading_day, next_trading_day,
            is_month_end, is_week_end, is_quarter_end
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(date) DO UPDATE SET
            exchange=excluded.exchange,
            is_trading_day=excluded.is_trading_day,
            previous_trading_day=excluded.previous_trading_day,
            next_trading_day=excluded.next_trading_day,
            is_month_end=excluded.is_month_end,
            is_week_end=excluded.is_week_end,
            is_quarter_end=excluded.is_quarter_end,
            updated_at=CURRENT_TIMESTAMP
        """,
        rows,
    )


def write_daily_bars(parquet_root: Path, trading_days: list[date]) -> Path:
    base_prices = {"510300.SH": 4.00, "510500.SH": 6.00, "518880.SH": 5.20}
    rows = []
    for day_index, trading_day in enumerate(trading_days):
        for symbol_index, (symbol, base_price) in enumerate(base_prices.items()):
            drift = day_index * (0.004 + symbol_index * 0.001)
            open_price = round(base_price + drift, 3)
            close_price = round(open_price * (1 + 0.001 * ((day_index + symbol_index) % 5 - 2)), 3)
            high_price = round(max(open_price, close_price) + 0.025, 3)
            low_price = round(min(open_price, close_price) - 0.025, 3)
            volume = float(8_000_000 + day_index * 10_000 + symbol_index * 100_000)
            amount = round(volume * close_price, 2)
            rows.append(
                {
                    "date": trading_day.isoformat(),
                    "symbol": symbol,
                    "open": open_price,
                    "high": high_price,
                    "low": low_price,
                    "close": close_price,
                    "volume": volume,
                    "amount": amount,
                    "pre_close": None if day_index == 0 else round(base_price + (day_index - 1) * 0.004, 3),
                    "adj_factor": 1.0,
                    "suspended": False,
                    "limit_up": None,
                    "limit_down": None,
                    "source": "fake",
                    "updated_at": "2026-01-01T00:00:00",
                }
            )

    output_dir = parquet_root / "fund_daily_bar" / "year=2026"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "part-000.parquet"
    pd.DataFrame(rows).to_parquet(output_path, index=False)
    return output_path


def fake_nav_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "date": "2026-01-08",
                "symbol": "510300.SH",
                "nav": 4.015,
                "iopv": 4.016,
                "close": 4.018,
                "premium_discount": None,
                "estimate_nav": None,
                "available_date": "2026-01-08",
                "source": "fake",
            },
            {
                "date": "2026-01-09",
                "symbol": "510300.SH",
                "nav": 4.020,
                "iopv": 4.021,
                "close": 4.028,
                "premium_discount": None,
                "estimate_nav": None,
                "available_date": "2026-01-12",
                "source": "fake",
            },
            {
                "date": "2026-01-08",
                "symbol": "518880.SH",
                "nav": 5.240,
                "iopv": 5.241,
                "close": 5.230,
                "premium_discount": None,
                "estimate_nav": 5.238,
                "available_date": "2026-01-08",
                "source": "fake",
            },
        ]
    )


def fake_dividend_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "dividend_id": "fake_div_510300_20260115",
                "symbol": "510300.SH",
                "announcement_date": "2026-01-09",
                "ex_dividend_date": "2026-01-15",
                "record_date": "2026-01-14",
                "payment_date": "2026-01-16",
                "dividend_per_share": 0.03,
                "dividend_type": "cash",
                "tax_rate": 0,
                "available_date": "2026-01-09",
                "source": "fake",
            },
            {
                "dividend_id": "fake_div_510500_20260120",
                "symbol": "510500.SH",
                "announcement_date": "2026-01-21",
                "ex_dividend_date": "2026-01-20",
                "record_date": "2026-01-19",
                "payment_date": "2026-01-21",
                "dividend_per_share": 0.02,
                "dividend_type": "cash",
                "tax_rate": 0,
                "available_date": "2026-01-21",
                "source": "fake",
            },
        ]
    )


def fake_index_valuation_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "date": "2026-01-08",
                "index_code": "000300.SH",
                "index_name": "沪深300",
                "pe_ttm": 11.5,
                "pb": 1.25,
                "ps": 1.1,
                "dividend_yield": 0.028,
                "roe": 0.11,
                "pe_percentile_3y": 0.35,
                "pe_percentile_5y": 0.32,
                "pb_percentile_3y": 0.40,
                "pb_percentile_5y": 0.38,
                "dividend_yield_percentile_3y": 0.65,
                "dividend_yield_percentile_5y": 0.68,
                "available_date": "2026-01-08",
                "source": "fake",
            },
            {
                "date": "2026-01-09",
                "index_code": "000300.SH",
                "index_name": "沪深300",
                "pe_ttm": 1.0,
                "pb": 0.1,
                "ps": 0.1,
                "dividend_yield": 0.20,
                "roe": 0.11,
                "pe_percentile_3y": 0.01,
                "pe_percentile_5y": 0.01,
                "pb_percentile_3y": 0.01,
                "pb_percentile_5y": 0.01,
                "dividend_yield_percentile_3y": 0.99,
                "dividend_yield_percentile_5y": 0.99,
                "available_date": "2026-01-12",
                "source": "fake_poison_late_available",
            },
            {
                "date": "2026-01-08",
                "index_code": "000905.SH",
                "index_name": "中证500",
                "pe_ttm": 18.0,
                "pb": 1.65,
                "ps": 1.4,
                "dividend_yield": 0.018,
                "roe": 0.09,
                "pe_percentile_3y": 0.55,
                "pe_percentile_5y": 0.52,
                "pb_percentile_3y": 0.57,
                "pb_percentile_5y": 0.53,
                "dividend_yield_percentile_3y": 0.45,
                "dividend_yield_percentile_5y": 0.48,
                "available_date": "2026-01-08",
                "source": "fake",
            },
        ]
    )


def create_fake_data(config_path: str | Path = "config/base.yaml") -> None:
    init_db(config_path)
    config = load_config(config_path)
    db_path = get_path(config, "sqlite_db")
    parquet_root = get_path(config, "parquet_root")
    trading_days = business_days(date(2026, 1, 2), 30)

    with sqlite3.connect(db_path) as connection:
        write_fund_master(connection)
        write_calendar(connection, trading_days)

    output_path = write_daily_bars(parquet_root, trading_days)
    sqlite_store = SQLiteStore(db_path)
    NavLoader(sqlite_store).load_frame(fake_nav_frame())
    DividendLoader(sqlite_store).load_frame(fake_dividend_frame())
    IndexValuationLoader(sqlite_store).load_frame(fake_index_valuation_frame())
    print(f"Created fake data in {db_path} and {output_path}")


def main() -> None:
    create_fake_data()


if __name__ == "__main__":
    main()
