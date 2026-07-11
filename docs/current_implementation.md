# FundLab Current Implementation

This document describes what is implemented in the current FundLab codebase. It is intentionally scoped to the local, testable implementation in this repository.

## Overview

FundLab is a lightweight research and backtesting lab for exchange-traded rule-based funds. The current implementation centers on a local data warehouse, a point-in-time `DataPortal`, feature generation, rule strategies, risk checks, and a simple next-open backtest engine.

The repository also contains Data Platform v2: provider capability/configuration contracts, an
audited SQLite catalog, immutable raw/staging/published Parquet lifecycle, complete-only versioned
portal and run-local snapshot, trusted adjusted-price/raw-liquidity features, read-only v1 migration,
and a deterministic daily update/reporting path.

## Implemented Capabilities

- Local package skeleton with `fundlab` modules and script entry points under `scripts/`.
- SQLite metadata warehouse at `data/warehouse/sqlite/fundlab.db`.
- Parquet daily-bar store under `data/warehouse/parquet/fund_daily_bar/`.
- Fake local dataset covering fund master data, trading calendar, daily bars, NAV/premium-discount data, dividends, and index valuation data.
- Real `xtquant` ingestion scripts for trading calendars, ETF universe discovery, daily bars, close-derived NAV placeholders, dividend stubs, and index valuation stubs.
- Point-in-time reads through `DataPortal` using `date` plus `available_date` filters where applicable.
- Feature computation and persistence for momentum, volatility, drawdown, liquidity, valuation, dividend, premium/discount, and composite scores.
- Rule strategies that only consume data through `DataPortal`.
- Backtest loop with signal-date to next-trading-day execution, basic accounting, risk checks, metrics, and SQLite persistence.
- Unit, integration, and regression tests for data access, feature timing, strategies, risk checks, backtesting, and persistence.

## Repository Map

| Path | Purpose |
| --- | --- |
| `fundlab/common/` | Shared config loading, date normalization, logging, IDs, hashes, and base exceptions. |
| `fundlab/data/storage/` | `SQLiteStore` and `ParquetStore` wrappers for warehouse access. |
| `fundlab/data/platform/` | Provider, batch, version, trust, universe, and catalog contracts for v2. |
| `fundlab/data/migration/` | Read-only v1 bootstrap, quarantine, reconciliation, rollback manifest, and retry behavior. |
| `fundlab/data/pipeline/` | Deterministic provider preflight, quality gate, feature build, atomic publication, recovery, and reports. |
| `fundlab/data/loaders/` | Loaders for fund universe, calendar, daily bars, NAV, dividends, index valuations, and computed features. |
| `fundlab/data/portal/` | `DataPortal`, the read interface used by strategies, risk rules, feature generation, and backtests. |
| `fundlab/data/processors/` | Symbol normalization and daily-bar quality checks. |
| `fundlab/data/sources/` | Manual local source and placeholder `xtquant` source abstraction. |
| `fundlab/features/` | `FeatureEngine` for daily feature and score generation. |
| `fundlab/strategies/` | Rule strategy implementations. |
| `fundlab/risk/` | Target-weight and order-level risk checks. |
| `fundlab/backtest/` | Account/order/trade models, execution planner, broker, accounting, recorder, metrics, and persistence. |
| `scripts/` | Setup, fake-data creation, feature computation, quality checks, data update helpers, and sample backtest runner. |
| `tests/` | Unit, integration, and regression coverage for the implemented behavior. |

## Data Warehouse

`scripts.init_db` creates the SQLite schema. The implemented tables include:

- `fund_master` for normalized ETF/fund metadata and universe flags.
- `trading_calendar` for trading-day flags plus previous/next trading-day links.
- `data_update_log` for update job metadata.
- `fund_nav` for NAV, IOPV, close, and premium/discount values with `available_date`.
- `fund_dividend` for dividend events and payment timing with `available_date`.
- `index_valuation` for index valuation fields and percentile data with `available_date`.
- `fund_features_daily` for generated feature snapshots with `feature_version` and `available_date`.
- Backtest tables for runs, account daily snapshots, positions, orders, trades, and metrics.

Daily OHLCV bars are stored separately as Parquet datasets through `ParquetStore`. They are partitioned by date and read back by symbol/date range with optional field selection.

## Data Access

`DataPortal` is the main read boundary. Implemented methods cover:

- Universe and fund master queries with listing/delisting filters.
- Daily bar and single-price reads from Parquet.
- Execution open-price lookup for next-open backtests.
- Point-in-time NAV, premium/discount, dividend, index valuation, and feature reads using `available_date <= asof`.
- Trading-calendar helpers for trading-day lists, previous trading day, next trading day, and trading-day checks.

Adjusted daily bars and daily-bar fill methods are explicitly not implemented yet; calls that request them raise `NotImplementedError`.

## Data Loading And Sources

Implemented loaders normalize incoming frames and upsert into the warehouse:

- `FundUniverseLoader` loads normalized fund metadata from a `MarketDataSource`.
- `CalendarLoader` writes placeholder business-day calendars and previous/next links.
- `DailyBarLoader` writes normalized daily bars to Parquet.
- `NavLoader`, `DividendLoader`, `IndexValuationLoader`, and `FeatureLoader` write point-in-time SQLite datasets.

`ManualSource` reads local manual files from `data/raw/manual`. `XtQuantSource` connects to `xtquant`, reads trading calendars, discovers ETF-like instruments from configured sectors, downloads daily bars, and normalizes `get_market_data_ex` responses into the warehouse bar schema.

`scripts.update_real_data` runs the full real-data refresh path: calendar, universe, daily bars, NAV placeholders, dividends, index valuations, feature generation, and daily-bar quality reporting.

Real market data policy: MiniQMT / `xtquant` is the primary source for warehouse real data. If `xtquant` is unavailable, refresh scripts must fail clearly and the operator must restore the `xtquant` environment; the project must not silently switch to another market-data provider. Non-`xtquant` sources are allowed only as explicitly requested auxiliary supplements or comparison/reporting inputs for data that already exists locally. See `docs/data_source_policy.md`.

## Feature Generation

`FeatureEngine` computes daily features for a date range and optional symbol list. It uses only `DataPortal` reads, which keeps feature generation aligned with the same point-in-time access path used by strategies.

Generated fields include:

- Returns over 1, 5, 20, 60, and 120 trading days.
- Annualized volatility over 20 and 60 trading days.
- 60-day max drawdown.
- Average traded amount over 20 and 60 trading days.
- Momentum, valuation, dividend, liquidity, premium/discount, risk-penalty, and total scores.
- Tracking index code, premium/discount, 12-month dividend yield, feature version, and source data version.

Features are persisted by `FeatureLoader` into `fund_features_daily`.

## Strategies

All strategies implement `Strategy.on_rebalance(date, data_portal, context)` and return target weights keyed by symbol plus optional `cash`.

- `EqualWeightStrategy` equal-weights a configured symbol list that is in the current universe and has a close price.
- `MomentumRotationStrategy` selects positive-momentum funds using 20-day and 60-day returns.
- `ValueMomentumStrategy` combines valuation, momentum, and liquidity scores.
- `DividendValueStrategy` selects equity funds with dividend and value characteristics.
- `AssetAllocationStrategy` allocates across configured asset classes using top total-score candidates per class.

Tests assert that strategies run through the backtest path without directly depending on raw storage internals.

## Risk Engine

`RiskEngine` applies rules to both target weights and executable orders:

- `UniverseCheck` removes or rejects symbols outside the date-specific universe.
- `PositionLimit` caps per-symbol target weight.
- `CashCheck` scales buy orders when available cash is insufficient.
- `LiquidityCheck` scales orders relative to recent traded amount.
- `PremiumDiscountCheck` rejects buys with missing cross-border premium/discount data or excessive absolute premium/discount.

The default `BacktestEngine` risk stack uses `UniverseCheck`, `PositionLimit`, `CashCheck`, and `PremiumDiscountCheck`.

## Backtesting

`BacktestEngine` runs over trading days from `DataPortal`:

1. Load intents scheduled for the current execution date.
2. Convert intents to target-weight orders.
3. Apply order risk checks.
4. Execute accepted orders with `BacktestBroker` using the execution-day open price.
5. Apply trades, accrue/pay dividends, mark positions to market, and record daily account and position snapshots.
6. On rebalance days, call the strategy with the current signal date and create intents for the next trading day.

Monthly and daily rebalancing are implemented. Unsupported rebalance frequencies raise `ValueError`.

`BacktestSQLiteWriter` persists completed runs and uses `PerformanceAnalyzer` for cumulative return, annualized return, annualized volatility, Sharpe, Calmar, max drawdown, win rate, turnover, trade count, and total cost metrics.

## Scripts

Common local workflows:

```powershell
uv sync
uv run python -m scripts.init_db
uv run python -m scripts.create_fake_data
uv run python -m scripts.compute_features
uv run python -m scripts.run_backtest
uv run pytest
```

Additional scripts:

- `scripts.update_universe` loads `fund_master` from `xtquant` instrument discovery.
- `scripts.update_calendar` writes real `xtquant` trading-calendar rows.
- `scripts.update_daily_bars` downloads and stores real `xtquant` daily bars in Parquet.
- `scripts.update_nav`, `scripts.update_dividends`, and `scripts.update_index_valuation` use `xtquant` source methods; NAV currently uses close-derived neutral placeholders where true NAV is unavailable.
- `scripts.check_data_quality` writes a daily-bar quality report to `data/reports/quality/daily_bar_quality.csv`.
- `scripts.update_real_data` orchestrates the real-data refresh, feature generation, and quality report.

## Current Limitations

- MiniQMT/`xtquant` ingestion now has a first usable implementation, but exact sector names and instrument field mappings may need adjustment for the local QMT build.
- Daily-bar adjustment and fill modes are not implemented.
- True ETF NAV, dividend events, and index valuation data are still limited by available `xtquant` fields; NAV currently falls back to close-derived neutral premium/discount placeholders.
- Fake data is deterministic and test-oriented, not production market data.
- Strategy execution is a simple next-open simulation, not a live trading or paper-trading gateway.
- Risk checks are intentionally lightweight and should be extended before production trading.

V2 intentionally does not add paid/automatic fallback providers, live scheduling services, real
corporate-action completion, minute data, or a UI. The operator or Codex/cron invokes the CLI. The
legacy warehouse stays available only as a read-only rollback boundary; new consumers use a pinned
complete v2 version and explicit raw/adjusted modes.
