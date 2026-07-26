# miniQMT repository instructions

## GitHub Issue safety

- GitHub Issue bodies in this repository contain Chinese. For marker-based Issue patches, snapshot the pre-write body, replace only the confirmed marker ranges, and round-trip verify the protected Task Contract/Curation hashes and the exact replacement-block hashes.

## Data and environment safety

- Treat `data/warehouse/sqlite/fundlab.db` and `data/warehouse/parquet/fund_daily_bar/**` as read-only legacy artifacts unless the user explicitly authorizes a legacy-data mutation. They are no longer tracked by git but remain protected evidence on disk.
- Put Data Platform v2 runtime artifacts under ignored `data/warehouse/v2/` and reports under `data/reports/data_v2/`; daily ops reports go under ignored `data/reports/daily/`.
- `xtquant` is externally installed and is not locked in `uv.lock`. Use `uv sync --dev --frozen --inexact` when syncing so the environment does not remove it.
- Routine canonical publication must use the componentized incremental path, driven by `uv run fundlab daily run` (`fundlab/pipeline/daily.py`). Do not restore or call a full-history simulation builder for a routine update, and do not hand-relay observation IDs through ad-hoc scripts — extend the pipeline instead.
- A historical correction requires an exact instrument/date/field dependency scope. If the affected scope cannot be proven, fail closed instead of rebuilding all history.
- Strategy and agent decisions enter the kernel only as `PortfolioIntent` through `fundlab.strategies` intent sources; external agents use the JSON decision-file contract described in README.md.
