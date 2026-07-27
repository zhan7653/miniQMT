# miniQMT repository instructions

## GitHub Issue safety

- GitHub Issue bodies in this repository contain Chinese. For marker-based Issue patches, snapshot the pre-write body, replace only the confirmed marker ranges, and round-trip verify the protected Task Contract/Curation hashes and the exact replacement-block hashes.

## Data and environment safety

- The pre-v2 legacy warehouses (`data/warehouse/sqlite`, `data/warehouse/parquet`) and the `audit-legacy`/`import-legacy` channel were retired with user authorization on 2026-07-27; do not recreate them. One-time build/validation reports live in `data/archive/build-reports-2026-07.zip`.
- Put Data Platform v2 runtime artifacts under ignored `data/warehouse/v2/` and canonical reports under `data/reports/data_v2/canonical/`; daily ops reports go under ignored `data/reports/daily/`.
- `xtquant` is externally installed and is not locked in `uv.lock`. Use `uv sync --dev --frozen --inexact` when syncing so the environment does not remove it.
- Routine canonical publication must use the componentized incremental path, driven by `uv run fundlab daily run` (`fundlab/pipeline/daily.py`). Do not restore or call a full-history simulation builder for a routine update, and do not hand-relay observation IDs through ad-hoc scripts — extend the pipeline instead.
- A historical correction requires an exact instrument/date/field dependency scope. If the affected scope cannot be proven, fail closed instead of rebuilding all history.
- Strategy and agent decisions enter the kernel only as `PortfolioIntent` through `fundlab.strategies` intent sources; external agents use the JSON decision-file contract described in README.md.
