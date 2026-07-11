# FundLab Data Source Policy

FundLab treats local warehouse data as research-critical infrastructure. Source provenance must be explicit and conservative.

## Primary Real-Data Source

`xtquant` / MiniQMT is the primary and authoritative source for real market data in this project.

Data Platform v2 enables providers only through explicit reviewed configuration. V1 enables exactly
`xtquant` and requires `providers.fallback: null`. A provider may be called only for a capability it
declares. Provider credentials, if future adapters require them, belong in environment variables or
local secure configuration and must not be committed.

Production or regression-grade real-data refreshes must use the existing `xtquant` path:

- `scripts.update_calendar`
- `scripts.update_universe`
- `scripts.update_daily_bars`
- `scripts.update_nav`
- `scripts.update_dividends`
- `scripts.update_index_valuation`
- `scripts.update_real_data`

If `xtquant` is unavailable, missing from the Python environment, not connected, or MiniQMT is not logged in, the refresh must fail clearly and ask the operator to restore the `xtquant` environment. It must not silently fall back to another provider.

The absence of adjusted prices is also not a fallback condition. Dependent research features remain
unpublished rather than silently using raw prices. Raw prices remain the sole legal execution,
cost-basis, and valuation source.

## Auxiliary Sources

Non-`xtquant` sources are allowed only as explicit auxiliary supplements to data that already exists in the local warehouse.

Auxiliary sources must not replace the primary `xtquant` refresh path. They may be used only when all of these are true:

- The operator explicitly requests the auxiliary source.
- The affected date range, symbols, fields, and source name are recorded.
- Existing `xtquant` data provenance is not overwritten silently.
- The output can be distinguished from `xtquant` data through `source`, logs, or a separate comparison/reporting artifact.
- Any discrepancy against existing `xtquant` data is reported instead of hidden.

Examples of acceptable auxiliary use:

- Checking whether a missing local date range is also missing from another provider.
- Comparing OHLCV values for already downloaded symbols to diagnose data quality issues.
- Producing a separate report that helps decide whether an `xtquant` rerun is needed.

Examples of unacceptable auxiliary use:

- Automatically downloading from another provider because `xtquant` is not installed.
- Filling historical warehouse gaps from another provider without explicit operator approval.
- Mixing auxiliary data into regression benchmarks as if it were `xtquant` data.

## Agent Rule

When working on this repository, an agent must not fetch real market data from non-`xtquant` providers by default.

If `xtquant` is unavailable, the agent must stop and tell the user that the real-data refresh requires `xtquant` / MiniQMT. It may suggest an auxiliary comparison workflow only after making clear that the result is not the primary real-data warehouse source.

The tracked v1 SQLite and daily-bar Parquet artifacts are read-only legacy evidence. V2 migration
must snapshot their hashes, use read-only access, publish only into v2 roots, and verify the hashes
again. Legacy NAV, premium/discount, valuation, and dividend placeholders are untrusted and must not
be exposed by default to v2 consumers.
