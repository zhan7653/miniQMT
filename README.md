# FundLab

FundLab is a lightweight research and backtesting lab for exchange-traded rule-based funds. The current codebase provides a local warehouse, point-in-time `DataPortal`, `xtquant` ingestion scripts, feature generation, rule strategies, risk checks, and a simple next-open backtest engine.

## Quick Start

```powershell
uv sync
uv run python -m scripts.init_db
uv run python -m scripts.create_fake_data
uv run python -m scripts.compute_features
uv run python -m scripts.run_backtest
uv run pytest
```

## Real Data Workflow

Real market data must come primarily from MiniQMT / `xtquant`. If `xtquant` is unavailable, stop and fix the MiniQMT environment instead of automatically falling back to another provider. Non-`xtquant` sources may only be used as explicitly requested auxiliary supplements or comparison reports for data that already exists locally.

Run these commands with MiniQMT logged in and `xtquant` available in the active Python environment:

```powershell
python -m scripts.update_calendar
python -m scripts.update_universe
python -m scripts.update_daily_bars
python -m scripts.update_nav
python -m scripts.update_dividends
python -m scripts.update_index_valuation
python -m scripts.compute_features
python -m scripts.check_data_quality
python -m scripts.run_backtest
```

Or run the full data refresh path:

```powershell
python -m scripts.update_real_data
```

## Current Scope

- Local Python package skeleton using `uv` and Python 3.11+.
- SQLite metadata and result database at `data/warehouse/sqlite/fundlab.db`.
- Parquet daily bars under `data/warehouse/parquet/fund_daily_bar/`.
- Fake local test data plus real `xtquant` update scripts for trading calendar, ETF universe, daily bars, NAV placeholders, dividends, index valuations, and generated features.
- Point-in-time `DataPortal` for universe, calendar, daily bars, NAV, dividends, index valuations, and features.
- Rule strategies, risk checks, feature generation, backtest execution, metrics, and backtest persistence.

See `docs/current_implementation.md` for a detailed implementation map, workflows, and known limitations.
See `docs/data_source_policy.md` for the project data-source policy.
