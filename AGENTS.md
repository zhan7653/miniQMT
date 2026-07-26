# miniQMT repository instructions

## GitHub Issue safety

- GitHub Issue bodies in this repository contain Chinese. For marker-based Issue patches, snapshot the pre-write body, replace only the confirmed marker ranges, and round-trip verify the protected Task Contract/Curation hashes and the exact replacement-block hashes.

## Data and environment safety

- Treat `data/warehouse/sqlite/fundlab.db` and `data/warehouse/parquet/fund_daily_bar/**` as read-only legacy artifacts unless the user explicitly authorizes a legacy-data mutation.
- Put Data Platform v2 runtime artifacts under ignored `data/warehouse/v2/` and reports under `data/reports/data_v2/`.
- `xtquant` is externally installed and is not locked in `uv.lock`. Use `uv sync --dev --frozen --inexact` when syncing so the environment does not remove it.
- Do not run legacy real-data update scripts during v2 migration or validation when they could modify the tracked v1 warehouse.
- Routine canonical publication must use the componentized incremental path. Do not restore or call a full-history simulation builder for a weekly update.
- A historical correction requires an exact instrument/date/field dependency scope. If the affected scope cannot be proven, fail closed instead of rebuilding all history.
