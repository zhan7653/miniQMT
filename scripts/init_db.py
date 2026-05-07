from __future__ import annotations

import sqlite3
from pathlib import Path

from fundlab.common.config import get_path, load_config


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS fund_master (
    symbol                  TEXT PRIMARY KEY,
    raw_symbol              TEXT,
    name                    TEXT NOT NULL,
    exchange                TEXT NOT NULL,
    product_type            TEXT NOT NULL,
    management_type         TEXT NOT NULL,
    asset_class             TEXT NOT NULL,
    category                TEXT,
    tracking_index          TEXT,
    tracking_index_code     TEXT,
    fund_company            TEXT,
    listed_date             TEXT,
    delisted_date           TEXT,
    expense_ratio           REAL,
    custody_fee             REAL,
    lot_size                INTEGER DEFAULT 100,
    price_tick              REAL DEFAULT 0.001,
    is_active               INTEGER NOT NULL DEFAULT 1,
    include_in_universe     INTEGER NOT NULL DEFAULT 0,
    exclusion_reason        TEXT,
    source                  TEXT,
    source_updated_at       TEXT,
    created_at              TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at              TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_fund_master_universe
ON fund_master(include_in_universe, is_active, product_type, management_type);

CREATE INDEX IF NOT EXISTS idx_fund_master_asset_class
ON fund_master(asset_class, category);

CREATE INDEX IF NOT EXISTS idx_fund_master_tracking_index
ON fund_master(tracking_index_code);

CREATE TABLE IF NOT EXISTS trading_calendar (
    date                    TEXT PRIMARY KEY,
    exchange                TEXT NOT NULL DEFAULT 'CN',
    is_trading_day          INTEGER NOT NULL,
    previous_trading_day    TEXT,
    next_trading_day        TEXT,
    is_month_end            INTEGER DEFAULT 0,
    is_week_end             INTEGER DEFAULT 0,
    is_quarter_end          INTEGER DEFAULT 0,
    created_at              TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at              TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_trading_calendar_trading
ON trading_calendar(is_trading_day, date);

CREATE INDEX IF NOT EXISTS idx_trading_calendar_month_end
ON trading_calendar(is_month_end, date);
CREATE TABLE IF NOT EXISTS data_update_log (
    update_id           TEXT PRIMARY KEY,
    job_name            TEXT NOT NULL,
    source              TEXT NOT NULL,
    table_name          TEXT NOT NULL,
    start_date          TEXT,
    end_date            TEXT,
    row_count           INTEGER DEFAULT 0,
    status              TEXT NOT NULL,
    error_message       TEXT,
    data_version        TEXT,
    started_at          TEXT NOT NULL,
    finished_at         TEXT
);

CREATE INDEX IF NOT EXISTS idx_data_update_log_table_time
ON data_update_log(table_name, started_at);
CREATE TABLE IF NOT EXISTS fund_nav (
    date                    TEXT NOT NULL,
    symbol                  TEXT NOT NULL,
    nav                     REAL,
    iopv                    REAL,
    close                   REAL,
    premium_discount        REAL,
    estimate_nav            REAL,
    available_date          TEXT,
    source                  TEXT,
    updated_at              TEXT DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (date, symbol)
);

CREATE INDEX IF NOT EXISTS idx_fund_nav_symbol_date
ON fund_nav(symbol, date);
CREATE INDEX IF NOT EXISTS idx_fund_nav_available
ON fund_nav(symbol, available_date);
CREATE TABLE IF NOT EXISTS fund_dividend (
    dividend_id             TEXT PRIMARY KEY,
    symbol                  TEXT NOT NULL,
    announcement_date       TEXT,
    ex_dividend_date        TEXT NOT NULL,
    record_date             TEXT,
    payment_date            TEXT,
    dividend_per_share      REAL NOT NULL,
    dividend_type           TEXT,
    tax_rate                REAL DEFAULT 0,
    available_date          TEXT,
    source                  TEXT,
    updated_at              TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_fund_dividend_symbol_exdate
ON fund_dividend(symbol, ex_dividend_date);
CREATE INDEX IF NOT EXISTS idx_fund_dividend_payment
ON fund_dividend(payment_date);
CREATE UNIQUE INDEX IF NOT EXISTS uq_fund_dividend_event
ON fund_dividend(symbol, ex_dividend_date, dividend_per_share, dividend_type);
CREATE TABLE IF NOT EXISTS index_valuation (
    date                                TEXT NOT NULL,
    index_code                          TEXT NOT NULL,
    index_name                          TEXT,
    pe_ttm                              REAL,
    pb                                  REAL,
    ps                                  REAL,
    dividend_yield                      REAL,
    roe                                 REAL,
    pe_percentile_3y                    REAL,
    pe_percentile_5y                    REAL,
    pb_percentile_3y                    REAL,
    pb_percentile_5y                    REAL,
    dividend_yield_percentile_3y        REAL,
    dividend_yield_percentile_5y        REAL,
    available_date                      TEXT NOT NULL,
    source                              TEXT,
    updated_at                          TEXT DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (date, index_code)
);

CREATE INDEX IF NOT EXISTS idx_index_valuation_available
ON index_valuation(index_code, available_date, date);
CREATE INDEX IF NOT EXISTS idx_index_valuation_date
ON index_valuation(date);
CREATE TABLE IF NOT EXISTS fund_features_daily (
    date                        TEXT NOT NULL,
    symbol                      TEXT NOT NULL,
    feature_version             TEXT NOT NULL DEFAULT 'v1',
    ret_1d                      REAL,
    ret_5d                      REAL,
    ret_20d                     REAL,
    ret_60d                     REAL,
    ret_120d                    REAL,
    volatility_20d              REAL,
    volatility_60d              REAL,
    max_drawdown_60d            REAL,
    amount_avg_20d              REAL,
    amount_avg_60d              REAL,
    amount_percentile_60d       REAL,
    turnover_score              REAL,
    momentum_score              REAL,
    valuation_score             REAL,
    dividend_score              REAL,
    liquidity_score             REAL,
    premium_discount_score      REAL,
    risk_penalty_score          REAL,
    total_score                 REAL,
    premium_discount            REAL,
    dividend_yield_12m          REAL,
    tracking_index_code         TEXT,
    available_date              TEXT NOT NULL,
    source_data_version         TEXT,
    updated_at                  TEXT DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (date, symbol, feature_version)
);

CREATE INDEX IF NOT EXISTS idx_fund_features_symbol_date
ON fund_features_daily(symbol, date, feature_version);
CREATE INDEX IF NOT EXISTS idx_fund_features_date_score
ON fund_features_daily(date, total_score);
CREATE INDEX IF NOT EXISTS idx_fund_features_available
ON fund_features_daily(symbol, available_date);
CREATE TABLE IF NOT EXISTS backtest_run (
    run_id              TEXT PRIMARY KEY,
    strategy_id         TEXT NOT NULL,
    strategy_name       TEXT,
    start_date          TEXT NOT NULL,
    end_date            TEXT NOT NULL,
    initial_cash        REAL NOT NULL,
    benchmark_symbol    TEXT,
    config_json         TEXT,
    config_hash         TEXT,
    data_version        TEXT,
    code_version        TEXT,
    universe_version    TEXT,
    status              TEXT NOT NULL,
    warning_text        TEXT,
    created_at          TEXT DEFAULT CURRENT_TIMESTAMP,
    finished_at         TEXT
);

CREATE INDEX IF NOT EXISTS idx_backtest_run_strategy
ON backtest_run(strategy_id, start_date, end_date);
CREATE TABLE IF NOT EXISTS backtest_account_daily (
    run_id                  TEXT NOT NULL,
    date                    TEXT NOT NULL,
    cash                    REAL NOT NULL,
    market_value            REAL NOT NULL,
    total_asset             REAL NOT NULL,
    nav                     REAL NOT NULL,
    daily_return            REAL,
    cumulative_return       REAL,
    drawdown                REAL,
    turnover                REAL,
    cost                    REAL,
    PRIMARY KEY (run_id, date)
);

CREATE INDEX IF NOT EXISTS idx_backtest_account_daily_date
ON backtest_account_daily(date);
CREATE TABLE IF NOT EXISTS backtest_order (
    order_id            TEXT PRIMARY KEY,
    run_id              TEXT NOT NULL,
    account_id          TEXT NOT NULL,
    signal_date         TEXT NOT NULL,
    execution_date      TEXT NOT NULL,
    symbol              TEXT NOT NULL,
    side                TEXT NOT NULL,
    order_type          TEXT NOT NULL,
    target_weight       REAL,
    target_quantity     INTEGER,
    quantity            INTEGER,
    status              TEXT NOT NULL,
    reason              TEXT,
    reject_reason       TEXT,
    created_at          TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_backtest_order_run_date
ON backtest_order(run_id, execution_date);
CREATE INDEX IF NOT EXISTS idx_backtest_order_symbol
ON backtest_order(symbol);
CREATE TABLE IF NOT EXISTS backtest_trade (
    trade_id            TEXT PRIMARY KEY,
    order_id            TEXT NOT NULL,
    run_id              TEXT NOT NULL,
    account_id          TEXT NOT NULL,
    datetime            TEXT NOT NULL,
    date                TEXT NOT NULL,
    symbol              TEXT NOT NULL,
    side                TEXT NOT NULL,
    price               REAL NOT NULL,
    quantity            INTEGER NOT NULL,
    amount              REAL NOT NULL,
    fee                 REAL NOT NULL,
    slippage            REAL NOT NULL,
    execution_model     TEXT,
    created_at          TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_backtest_trade_run_date
ON backtest_trade(run_id, date);
CREATE INDEX IF NOT EXISTS idx_backtest_trade_symbol
ON backtest_trade(symbol);
CREATE TABLE IF NOT EXISTS backtest_metrics (
    run_id                      TEXT PRIMARY KEY,
    cumulative_return           REAL,
    annualized_return           REAL,
    annualized_volatility       REAL,
    sharpe                      REAL,
    calmar                      REAL,
    max_drawdown                REAL,
    win_rate                    REAL,
    turnover                    REAL,
    trade_count                 INTEGER,
    total_cost                  REAL,
    benchmark_return            REAL,
    excess_return               REAL,
    max_drawdown_recovery_days  INTEGER,
    created_at                  TEXT DEFAULT CURRENT_TIMESTAMP
);
"""


def init_db(config_path: str | Path = "config/base.yaml") -> Path:
    config = load_config(config_path)
    db_path = get_path(config, "sqlite_db")
    db_path.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(db_path) as connection:
        connection.executescript(SCHEMA_SQL)

    return db_path


def main() -> None:
    db_path = init_db()
    print(f"Initialized SQLite database: {db_path}")


if __name__ == "__main__":
    main()
