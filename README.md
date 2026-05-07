# FundLab

FundLab is a lightweight research and backtesting lab for exchange-traded rule-based funds. The current codebase provides a local warehouse, point-in-time `DataPortal`, feature generation, rule strategies, risk checks, and a simple next-open backtest engine that can run without MiniQMT or `xtquant`.

## Quick Start

```powershell
uv sync
uv run python -m scripts.init_db
uv run python -m scripts.create_fake_data
uv run python -m scripts.compute_features
uv run python -m scripts.run_backtest
uv run pytest
```

## Current Scope

- Local Python package skeleton using `uv` and Python 3.11+.
- SQLite metadata and result database at `data/warehouse/sqlite/fundlab.db`.
- Parquet daily bars under `data/warehouse/parquet/fund_daily_bar/`.
- Fake local ETF/fund universe, trading calendar, daily bars, NAV/premium-discount data, dividends, index valuations, and generated features.
- Point-in-time `DataPortal` for universe, calendar, daily bars, NAV, dividends, index valuations, and features.
- Rule strategies, risk checks, feature generation, backtest execution, metrics, and backtest persistence.

See `docs/current_implementation.md` for a detailed implementation map, workflows, and known limitations.
