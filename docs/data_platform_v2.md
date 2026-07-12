# Data Platform v2 Operations

Data Platform v2 is an immutable, version-pinned ETF data path. Normal runs write only below
`data/warehouse/v2/` and `data/reports/data_v2/`. The legacy SQLite database and legacy daily-bar
Parquet tree remain a read-only rollback boundary.

## Operating contract

- MiniQMT/`xtquant` is the only enabled V1 production provider. A failed preflight stops the run;
  there is no automatic provider fallback.
- Each ingestion attempt and published version is recorded in the v2 catalog. Consumers may open
  only a catalog `complete` version and remain pinned to that version for the run.
- Publication uses immutable staging plus an atomic directory rename. A `building` or `failed`
  version is not consumer-visible. A successful revision supersedes its predecessor without
  modifying the predecessor's files.
- Execution and valuation use raw prices. Research returns, momentum, and volatility use adjusted
  prices. Missing adjusted data suppresses the dependent trusted feature; raw substitution is
  forbidden.
- Legacy NAV, premium/discount, valuation, and dividend placeholders are untrusted and are absent
  from the normal v2 consumer surface.

## Non-interactive daily update

Run from the repository root with MiniQMT logged in:

```powershell
.\.venv\Scripts\python.exe -m scripts.update_data_v2 --config config/base.yaml --target-date 2026-05-08 --json
```

Use `--start-date YYYY-MM-DD` for an explicit missing-date backfill start. Exit code `0` means a
complete or safely reused version, `1` means the update ran but failed, and `2` means configuration
or startup failed. `--json` writes the result to stdout for Codex/cron. Every attempted run also
writes UTF-8 JSON and Markdown reports under the configured `v2_report_root`; the result records
status, batch/version IDs, predecessor, reuse state, missing dates, excluded symbols, row counts,
timestamps, and error details.

Repeated runs with identical content reuse the complete version. Changed provider content creates
a new immutable revision whose `previous_version_id` is the current complete version.

## Full-market history bootstrap: phase 1

The bootstrap command is non-interactive and writes runtime state only below the configured v2
warehouse and report roots. Run preflight and discovery first:

```powershell
.\.venv\Scripts\python.exe -m scripts.bootstrap_fund_history --config config/base.yaml --phase preflight --json
.\.venv\Scripts\python.exe -m scripts.bootstrap_fund_history --config config/base.yaml --phase discover --no-publish --json
```

Phase 1 canary collection is deliberately capped at 20 deterministic representatives and always
starts at one worker, a two-second request interval, and the confirmed 20-symbol cooldown. It
collects calendar-year partitions clipped to listing date, delisting date, and target date:

```powershell
.\.venv\Scripts\python.exe -m scripts.bootstrap_fund_history --config config/base.yaml --phase canary --sample-limit 20 --no-publish --json
```

The collector applies the shared aggregate throttle separately to every external history request:
the calendar read, a required history download, and each raw or front-adjusted read each receive
their own gate and durable `request_gate` event. A successful download is reused for the matching
symbol/date scope, so raw and adjusted reads do not trigger redundant downloads. The gate is locked
across its interval wait, preserving the aggregate two-requests-per-second maximum if configured
workers are later used. Cooldowns remain counted by completed symbol, not by provider-call count.

The collector persists run, partition, attempt, and throttle records in the v2 catalog. A restart
returns to the initial speed profile and reuses a partition only after identity, checksum, and row
count validation. Transient partition work has at most three attempts with configured backoff.
Provider-health failures pause collection; schema, raw/front-adjusted key, OHLC, volume, amount, and
trading-day coverage failures are recorded as explicit quarantine reasons. Reports contain the
fund-master discovery evidence, category gaps, exact missing dates, attempts, throttle changes,
partition checksums, legacy manifests, and publication decision.

`collect` and `publish` are present as stable command phases but are blocked in this implementation
until the phase-1 evidence has been reviewed and the user explicitly approves the remaining
full-market run. Phase 1 never changes `latest_complete`; `publish` returns a blocked result even if
`--no-publish` is omitted. Do not bypass this pause by editing catalog pointers or staging files.

To resume, rerun the exact canary command with the same target and configuration. To investigate a
pause, inspect the JSON report under `data/reports/data_v2/` and the collection records in the v2
catalog. Preserve completed immutable partitions. Stop and request direction for legacy hash drift,
insufficient disk, non-xtquant price requirements, material discovery conflicts, or any need to
exceed the confirmed throttle maximum.

## Initial migration and rollback

The migration reads legacy SQLite with read-only URI mode and reads the legacy Parquet tree without
writing it. It quarantines known contaminated rows, repairs active raw and adjusted history through
the explicitly configured MiniQMT provider, reconciles counts and values, then publishes an initial
complete v2 version. Preserve the pre-run legacy SHA-256 manifest and compare it after migration.

If migration fails, do not delete or rewrite v1. Inspect the JSON/Markdown migration report and the
failed catalog records, correct the external/provider condition, and rerun. The legacy warehouse and
historical backtest records remain the read-only rollback source.

## Recovery and failure isolation

- Provider/system errors block the batch.
- Symbol-level provider or quality errors exclude only affected symbols when eligible symbols remain.
- A crash before atomic rename leaves no visible version; staging is discarded on failure.
- A crash after atomic rename but before the catalog pointer update is recoverable from the immutable
  manifest. Retry never mutates the failed attempt.
- Consumers continue to see the previous complete version until the successor is fully complete.
- Never manually point the catalog at a staging, building, failed, or checksum-invalid directory.

## Deterministic validation and performance evidence

The performance harness creates fixed synthetic data in an isolated temporary directory. It makes
no network calls, excludes provider time, and never writes configured warehouse paths:

```powershell
.\.venv\Scripts\python.exe -m scripts.benchmark_data_v2 --config config/base.yaml --assert-thresholds
```

It prints JSON containing environment-independent scenario names, measured seconds, thresholds, and
the `provider_time_included: false` declaration. Thresholds are: three ETFs, about three years and
monthly rebalancing at most 10 seconds; a 30-ETF one-day snapshot cold at most 1 second and repeated
in-run at most 100 ms; and 30-ETF incremental feature computation plus local publication at most 60
seconds.

Run owned failure/performance tests with:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\system tests\performance -q -p no:cacheprovider
```

Markers `system`, `performance`, `real_data`, and `miniqmt` allow offline validation to exclude
external or long-running checks explicitly.

## Operator checks

Before a live run, verify the reviewed universe and benchmark configuration, MiniQMT login, writable
v2/report roots, free disk space, and the legacy hash manifest. Afterward, verify a complete status,
the expected version ID/reuse behavior, row counts, exclusions, report files, and unchanged legacy
hashes. Treat corrupt manifests, unexplained raw/adjusted divergence, or v1 hash changes as stop
conditions rather than repair-in-place events.
