# FundLab

FundLab is a lightweight research and backtesting lab for exchange-traded rule-based funds. The current codebase provides a local warehouse, point-in-time `DataPortal`, `xtquant` ingestion scripts, feature generation, rule strategies, risk checks, and a simple next-open backtest engine.

## Quick Start

```powershell
uv sync --dev --frozen --inexact
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

## Data Platform v2

The production v2 path uses an explicitly configured MiniQMT/`xtquant` provider, immutable ingestion
batches, atomic complete-only publication, raw/adjusted price separation, and version-pinned reads.
It does not fall back to another provider. Normal v2 writes stay under ignored
`data/warehouse/v2/` and `data/reports/data_v2/`; the v1 warehouse is read-only.

```powershell
.\.venv\Scripts\python.exe -m scripts.update_data_v2 --config config/base.yaml --target-date 2026-05-08 --json
.\.venv\Scripts\python.exe -m scripts.benchmark_data_v2 --config config/base.yaml --assert-thresholds
```

See `docs/data_platform_v2.md` for the operator, recovery, migration, report, and performance contract.

## ETF Paper Trading

The paper-trading core persists isolated rule-strategy accounts in its own ignored SQLite ledger.
Commands are non-interactive and emit one JSON result, so an operator, Codex, or cron can call the
same interface. Bootstrap is idempotent and creates the reviewed-universe equal-weight and momentum
accounts with CNY 1,000,000 each:

```powershell
uv run --project D:\Code\miniQMT python D:\Code\miniQMT\scripts\manage_paper_accounts.py bootstrap
uv run --project D:\Code\miniQMT python D:\Code\miniQMT\scripts\run_paper_daily.py --date 2026-05-07
uv run --project D:\Code\miniQMT python D:\Code\miniQMT\scripts\run_paper_daily.py --start-date 2026-05-01 --target-date 2026-05-07
```

These absolute examples are intentionally independent of the caller's working directory. No
production cron task is created by this repository. See `docs/paper_trading_core.md` for lifecycle,
timing, replay, report, and recovery details.

## Current Scope

- Local Python package skeleton using `uv` and Python 3.11+.
- SQLite metadata and result database at `data/warehouse/sqlite/fundlab.db`.
- Parquet daily bars under `data/warehouse/parquet/fund_daily_bar/`.
- Fake local test data plus real `xtquant` update scripts for trading calendar, ETF universe, daily bars, NAV placeholders, dividends, index valuations, and generated features.
- Point-in-time `DataPortal` for universe, calendar, daily bars, NAV, dividends, index valuations, and features.
- Rule strategies, risk checks, feature generation, backtest execution, metrics, and backtest persistence.
- Durable multi-account ETF paper trading, lifecycle management, backfill, replay versions, and
  canonical JSON/CSV/Markdown reports.

See `docs/current_implementation.md` for a detailed implementation map, workflows, and known limitations.
See `docs/data_source_policy.md` for the project data-source policy.
