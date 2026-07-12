# ETF Paper Trading Core

## Operating boundary

The paper core is a local, end-of-day ETF simulator. Its internal SQLite ledger is the sole
authority for account cash, positions, decisions, orders, fills, snapshots, and events. It does not
send MiniQMT orders, run intraday, create a web service, or create a production Codex cron task.
Published Data Platform v2 files and the legacy v1 warehouse remain read-only.

Default runtime state is `data/warehouse/v2/paper_trading.sqlite3`; reports are written below
`data/reports/data_v2/paper_trading/`. Both paths are resolved relative to the config file, not the
caller's current directory, and are ignored runtime locations.

## Bootstrap and immutable bindings

Run bootstrap before the first daily batch:

```powershell
uv run --project D:\Code\miniQMT python D:\Code\miniQMT\scripts\manage_paper_accounts.py bootstrap
```

The command registers versioned execution and research-risk profiles and idempotently creates two
active accounts:

- `equal_weight_reviewed_v1`: equal weight over `510300.SH`, `510500.SH`, and `518880.SH`.
- `momentum_reviewed_v1`: momentum rotation over the same reviewed universe.

Each starts with CNY 1,000,000 and benchmark `510300.SH`. Strategy/config version, universe version,
benchmark, execution profile/version, risk profile/version, initial cash, and schedule are immutable.
Create a new account to change a binding.

## Lifecycle and queries

Successful commands and operational failures emit one UTF-8 JSON object. They return zero on
success, one on an operational/preflight error, argparse's exit code two on invalid syntax, and
three when a daily account batch or replay rebuild completes with a structured failure result.

```powershell
python scripts/manage_paper_accounts.py list --status active
python scripts/manage_paper_accounts.py show equal_weight_reviewed_v1
python scripts/manage_paper_accounts.py pause equal_weight_reviewed_v1
python scripts/manage_paper_accounts.py resume equal_weight_reviewed_v1
python scripts/manage_paper_accounts.py close equal_weight_reviewed_v1
```

Paused accounts cancel pending orders, continue close valuation, and create no new decisions.
Resuming makes a paused account active. Closing is permanent: it stops future processing without
liquidating holdings, while all history remains queryable.

`create` accepts explicit strategy, config, universe, profile, benchmark, cash, and schedule options.
The profile keys refer to entries in `config/paper_trading.yaml`.

## Daily timing and backfill

```powershell
python scripts/run_paper_daily.py --date 2026-05-07
python scripts/run_paper_daily.py --start-date 2026-05-01 --target-date 2026-05-07
```

The runner opens one immutable complete v2 version before processing. On T it values at the raw
close and, when scheduled, records the original validated decision and pending target orders. On
T+1 it sizes and executes those orders at the raw open, sells before buys, applies the bound costs
and slippage, then values at the raw close. A repeated completed account/date is reused without new
decisions, orders, fills, or snapshots.

Backfill enumerates covered trading dates in ascending order. Account failures roll back only that
account savepoint and are represented in the result; a system/preflight failure rolls back or stops
the date batch. A result containing failed accounts returns exit code three. Missing/incomplete data
or a non-trading target is a preflight error and returns exit code one.

## Replay and history

Completed ledger rows are append-only and are never silently corrected. Start an explicit version:

```powershell
python scripts/manage_paper_accounts.py replay equal_weight_reviewed_v1 `
  --start-date 2026-05-01 --target-date 2026-05-07 --reason "corrected published input"
```

This creates a child ledger version, rebuilds every required account date from the earliest parent
history through the target date using pinned complete data, validates the rebuilt daily runs and
snapshots, and only then atomically activates it. The prior version remains queryable and unchanged.
Repeating the exact account/start/target/reason request reuses the validated replay. A failed replay
returns structured JSON with a nonzero exit code and leaves the prior selected version active.

## Reports

After a successful daily or backfill invocation, the runner writes `latest.json`, `latest.csv`, and
`latest.md` below each account's report directory. All formats derive from one canonical payload and
cover returns, rolling returns, volatility, downside volatility, drawdown, Sharpe, Sortino, Calmar,
benchmark and excess returns, costs, turnover, exposure, and data health. Unavailable values are
null with an explicit reason. Every report states that corporate-action/dividend data is incomplete
and that account and benchmark results are price-return estimates, not complete total returns.

Use `--no-reports` only when a caller deliberately wants ledger advancement without rendering.

## Configuration and limitations

`config/paper_trading.yaml` owns the database/report paths, default accounts, strategy configuration
versions, explicit immutable `universes` mappings, execution profiles, and research-risk profiles.
The daily runner registers each universe version before strategy configurations and resolves an
account's bound `universe_version` from that registry; it never substitutes the portal's current
universe. `data_platform_config` points to the v2
catalog configuration. Use `--config` and `--database` for isolated tests or recovery; do not point
them at the protected legacy database.

V1 supports long-only, unleveraged ETF rule strategies. It has no manual orders, deposits,
withdrawals, live execution, authentication, notification, automatic strategy iteration, complete
corporate actions, or production scheduler installation. Scheduling is an operator responsibility;
Codex/cron should call the same non-interactive runner command after v2 publication completes.
