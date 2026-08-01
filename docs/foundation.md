# FundLab foundation

Decision sources: [Issue #6](https://github.com/zhan7653/miniQMT/issues/6), revision 1,
[Issue #7](https://github.com/zhan7653/miniQMT/issues/7), revision 2, and
[Issue #8](https://github.com/zhan7653/miniQMT/issues/8), revision 1.

## Product boundary

FundLab runs locally for one user. It is building reliable inputs and feedback for a future trading
Agent, not the Agent's learning algorithm. The first market scope is A-share ordinary stocks and ETFs
at daily frequency. Live trading, minute/tick/order-book simulation, UI, distributed services, and
other markets are outside the current boundary.

## Canonical data path

Each channel is a named `MarketDataProvider`. A request always names one provider and one declared
capability. The registry never falls back. The direct adapters are:

- `tickflow`: TickFlow REST daily K-lines; A-share volume is normalized from hands to shares, raw
  prices are eligible for reconciliation and provider forward-adjusted data is audit-only.
- `eastmoney-efinance`: Eastmoney history transported by the Efinance client. Its A-share
  volume is normalized from hands to shares. AkShare calls to the same Eastmoney backend would still
  count as this one backend.
- `baostock`: raw/audit bars plus exhaustive historical stock ST and suspension observations. Its
  dividend results remain audit-only because they do not prove complete rights/action lifecycle.
- `xtquant`: the explicitly installed local MiniQMT service. History download is opt-in per request;
  daily volume is normalized from hands to shares. It supplies the dense local daily-status baseline
  and independent event-ratio factors used to check action economics.
- `exchange-public` and `sina-calendar`: the fixed current SH/SZ stock/ETF universe and a separately
  reconciled full civil-date exchange calendar.
- `cninfo-public`: direct CNInfo public APIs for stock cash, share and rights-action lifecycle.
- `eastmoney-fund-public`: the exhaustive ETF distribution/split index plus paginated announcement
  categories and hashed public PDFs. Narrow public-archive gaps are explicit keyed supplements whose
  documents, hashes and required markers are verified at collection time.

`openstockdata/open-stock-data` is only an implementation and source-catalog reference. FundLab does
not import its code and does not record it as the upstream identity.

Provider output is normalized into five canonical tables:

- `instruments`: identity, lifecycle, exchange, asset type, lot, tick, settlement delay and board.
- `calendar`: explicit exchange sessions.
- `daily_bars`: raw OHLC, volume/amount, prior close, tradability and daily price-limit facts/rules.
- `corporate_actions`: record/ex/pay/listing dates and cash/share/rights terms.
- `adjustment_factors`: event ratio, effective date and conservative first-known date.

An immutable source observation stores the normalized raw facts, row-level original payload, true
upstream and transport, backend group, request scope, coverage claim, observation time, response hash,
Parquet SHA-256 and row counts. A correction or later source revision creates another observation.
An exact repeated `data collect` request reuses a locally verified observation only when its declared
coverage reaches the requested boundaries; a partial/stale response is fetched again. `--refresh`
forces a new observation. This is safe canary-level resume/deduplication, not yet the full partitioned
backfill scheduler.

Source observations cannot be published directly as trusted data. `ReconciliationService` creates a
derived canonical observation under a fixed policy version. It arbitrates each field independently,
records every candidate and selected source in `field_lineage`, and never averages values. Raw OHLCV
requires agreement from at least two independent backend groups. A two-versus-one cluster resolves the
outlier; equally supported clusters, missing independent coverage, ambiguous units or missing critical
values block that scope. TickFlow remains one `tickflow-unverified` backend until its upstream
independence is established. Efinance and any AkShare Eastmoney transport remain one `eastmoney`
backend. Runtime metrics count candidates and selections but cannot change policy priority silently.
The reconciliation-ready result is a non-optional trust gate: disabling ordinary source coverage
checks for diagnostics cannot publish a tied or insufficient-backend result.

Snapshots declare one use gate:

- `research_price`: trusted instrument/date, raw OHLC, volume and lineage. Missing amount, calendar,
  actions or execution rules do not block raw price research.
- `simulation`: everything above plus calendar completeness, prior close, explicit tradability,
  bounded/unbounded daily limit state, daily limit prices or a daily ratio, lot/tick/settlement rules,
  complete holding-affecting corporate actions and adjustment factors. A current static instrument
  limit ratio is not applied across historical ST/board/rule regimes.

Provider-adjusted rows are never the strategy view. `adjusted_history(..., as_of=...)` reads canonical
raw bars and multiplies only event factors whose effective and known dates are visible at that simulated
time. It leaves volume and amount raw and suppresses execution-only prior-close/limit fields. Thus a
future split or dividend cannot rewrite an older point-in-time research query.

Snapshots do not copy history. Externally, every consumer still binds one immutable snapshot ID.
Internally, that snapshot composes content-addressed market facts, effective-dated trading rules,
field adjudications and disposable simulation views. Component identity depends only on its actual
content and direct dependency versions, not on an unrelated source snapshot ID. A daily correction
creates an exact instrument/date/field overlay; an unknown impact scope fails closed instead of
triggering a full rebuild. Simulation binds the snapshot ID, never a mutable "latest" view.
Publication is atomic and compare-and-swap protected, so an incomplete successor or stale predecessor
leaves the pointer unchanged.
Schema-v1 snapshot identities still verify for audit compatibility, but their missing readiness field
loads as `legacy_unknown`; such a snapshot cannot be republished or opened by the canonical research or
simulation portal until it is rebuilt and revalidated under schema v2.

## One trading kernel

Both clock adapters invoke `TradingKernel`:

```text
T open:     settle lots -> apply payable/listed corporate actions -> execute prior orders
T close:    value with raw close -> capture record-date entitlements -> accept PortfolioIntent
after close: risk assessment -> size quantity at T raw close -> schedule DAY order for T+1 open
```

The strategy or future Agent can only emit an immutable `PortfolioIntent`. It includes the account,
decision date, snapshot, strategy/config hashes, observation hash, target weights, reason, and stable
content ID. Risk produces a separate assessment. Only the kernel creates orders, fills, fees,
positions, cash changes, and valuations.

The conservative daily execution model includes:

- suspension and zero-volume rejection;
- buy-at-limit-up and sell-at-limit-down blocking;
- instrument-specific buy lots, price ticks, and sellable-session delay (including T+1);
- volume participation caps, deterministic impact/slippage, partial fills, and DAY expiry;
- cash-constrained buys and settled-position-constrained sells;
- effective-dated broker commission, minimum commission, stamp duty, transfer, exchange, and regulatory
  fee components;
- record-date corporate-action entitlements, cash payment, share listing, and an explicit
  do-not-subscribe rights policy;
- ex-date cost-basis allocation across existing and pending distribution shares, preserving total
  basis before later partial sales;
- raw-close valuation with stale-price disclosure.

It does not claim to reproduce order-book queue position, intraday path, or the probability of a fill
while pinned at a price limit. Blocking at the adverse limit is intentionally conservative.

Cash dividends are booked gross. Because individual dividend tax can depend on holding period and
account circumstances, a cash-dividend run is marked incomplete until that treatment is configured.
Fractional share distributions are likewise disclosed as incomplete rather than silently discarded as
economically exact.

## Fees are versioned simulation assumptions

The model supports exact, effective-dated fee components. The committed schedule combines public
taxes with an explicit 0.03% commission and CNY 5 minimum as a reproducible simulation assumption;
it is not a claim about a real broker account. Public reference
points include the Ministry of Finance's [2023 stamp-duty reduction](https://www.mof.gov.cn/jrttts/202308/t20230828_3904235.htm),
the Shanghai Stock Exchange [current fee table](https://www.sse.com.cn/services/tradingservice/charge/ssecharge/),
and the [SSE 2026 trading rules](https://www.sse.com.cn/lawandrules/sselawsrules2025/trade/universal/c/c_20260424_10816492.shtml).
These do not determine a user's broker commission or minimum charge. Broker-parity or live work must
bind a separately verified account schedule and cannot reuse the simulation-trust declaration.
The committed amount-based schedule starts on 2015-08-01, when ChinaClear unified SH/SZ A-share
transfer fees at 0.02 per mille; it includes the 2022 reduction to 0.01 per mille and the 2023 stamp
duty reduction. Earlier Shanghai transfer fees were charged on face value, which the current
instrument contract does not carry, so pre-2015-08 stock simulations fail instead of approximating it.

## Persistent accounts and replay

Accounts are isolated. A completed daily run can advance one account head without affecting another.
Historical runs default to a non-promoted branch. A run binding fixes:

- account and optional parent run;
- canonical snapshot;
- strategy ID/version/config hash;
- execution, risk, and fee hashes;
- initial state hash, date range, clock mode, and seed.

Events form a SHA-256 chain starting at the initial-state hash. Session checkpoints and events are
append-only SQLite rows. Completed and failed runs cannot be updated or deleted. An exact completed
binding is idempotently reused; a retry after failure receives a new immutable attempt.

Promoted daily account history is monotonic. Repeating the current head with the same binding reuses
the completed run; a different binding for that finalized date or a date before the head is rejected.
A correction must replay from an earlier parent as a separate branch instead of applying an old date
to a newer portfolio state.

Each completed run produces deterministic feedback containing the pinned binding and result hashes,
event-chain head, quality/incomplete reasons, equity and drawdown, order/fill/expiry counts, fill ratio,
turnover, fees, modeled slippage, realized P&L, and dividend income. This is the future Agent's audit
surface; learning behavior is still out of scope.

## Real legacy-data audit on 2026-07-18

The read-only importer verifies the authoritative collect report, its evidence digest and catalog
anchor, every report-bound partition identity, Parquet checksum, and row count. It also hashes the
protected v1 SQLite database and Parquet files before and after the audit.

The final audit bound exactly 23,454 complete partitions (4,676,344 rows) from the authoritative
evidence, with 1,538 quarantined partitions, 11,727 raw/adjusted scope pairs, and no corrupt or
unpaired scope. Another 348 complete partitions from older, unbound source identities remain read-only
and are excluded. The audit also established
that the legacy collector is not canonical-ready:

- instrument coverage: `0.9009995240361732`;
- expected-day coverage: `0.9788350780482216`;
- ordinary stocks were not collected;
- corporate actions, price-limit rules, and settlement rules are absent;
- old duplicate source identities exist outside the report-bound partition set and are excluded.

The protected warehouse therefore remained read-only. No legacy data was copied and no current snapshot
was published from it.

Retirement note (2026-07-27): after the revision-2 canonical delivery fully superseded the legacy
collector, the protected v1 warehouses and the `audit-legacy`/`import-legacy` channel were deleted with
explicit user authorization. This section is preserved as the historical record of that audit.

## Daily pipeline

`fundlab/pipeline/daily.py` is the routine driver for everything below. `uv run fundlab daily run`
performs one idempotent cycle: capture and cross-check the BaoStock and Sina exchange calendars into
one validated canonical calendar observation, resolve the latest completed session against the
configured cutoff, and — when the published snapshot is behind — run the increment path end to end
(two-source history build with a BaoStock adjudicator, automatic no-trade consensus for full-window
suspensions, current-research derivation, xtquant/BaoStock status collection, action and factor
evidence collection, target-date xtquant/Eastmoney direct price-limit snapshots, and strict
corporate-action/factor reconciliation (xtquant primary, targeted BaoStock audit, with a TickFlow
raw/forward-adjusted ratio audit only for events still missing a factor), candidate composition,
increment validation, atomic componentized publish).
Afterwards every account configured under `daily:` in `config/fundlab.yaml` is advanced session by
session to the published head with its intent source (`static` weights or `agent-file` decisions).

The manual commands below remain the underlying, individually auditable machinery; the pipeline only
orchestrates them and inherits every systemic fail-closed gate. A bounded instrument-level data gap
can complete as `degraded` (exit 0) with non-tradable rows, stale valuation, deferred orders, and a
full structured report. A blocked stage exits 2; re-running resumes from the durable observation
warehouse.

## Commands

```powershell
# One idempotent daily cycle (data extension + account advancement) and its status
uv run fundlab daily run
uv run fundlab daily status

# Show channels, backend identities, capabilities and whether their clients are importable
uv run fundlab data sources

# Capture small, explicit source observations. These cannot be published directly.
uv run fundlab data collect --provider tickflow --capability daily_bars_raw `
  --start-date 2026-07-01 --end-date 2026-07-17 --instrument 600000.SH
uv run fundlab data collect --provider eastmoney-efinance --capability daily_bars_raw `
  --start-date 2026-07-01 --end-date 2026-07-17 --instrument 600000.SH
uv run fundlab data collect --provider baostock --capability daily_bars_raw `
  --start-date 2026-07-01 --end-date 2026-07-17 --instrument 600000.SH

# Reconcile explicit observations. A nonzero exit with status=incomplete is a trust result,
# not permission to force publication.
uv run fundlab data reconcile --readiness research_price --description "600000 canary" `
  --observation-id obs-... --observation-id obs-... --observation-id obs-... --publish

# Resumable full-history collection. A batch is complete only after both raw observations,
# field calibration and one immutable reconciled partition are durable.
uv run fundlab data build-history --start-date 2010-01-01 --end-date 2026-07-17 `
  --asset-type stock --asset-type etf --batch-size 100 `
  --source tickflow --source xtquant --publish

# Optional process-level sharding: use only when both source clients support concurrent
# sessions. BaoStock anonymous access and the local MiniQMT default should use one process.
# Run indexes 0..3 without --publish, then assemble once.
uv run fundlab data build-history --end-date 2026-07-17 --batch-size 100 `
  --source tickflow --source xtquant --shard-count 4 --shard-index 0
uv run fundlab data build-history --end-date 2026-07-17 --batch-size 100 `
  --source tickflow --source xtquant --shard-count 4 --assemble-only --publish

# Convert a ready field-level reconciliation into an exact EOD partition. This
# derives the rules, checks dense session coverage and binds two-provider direct
# limit evidence. The command emits the observation ID consumed below.
uv run fundlab data validate-simulation-increment `
  --candidate-observation-id obs-... `
  --calendar-observation-id obs-... `
  --universe-as-of 2026-07-17 `
  --start-date 2026-07-14 --end-date 2026-07-17 `
  --description "validated manual EOD partition"

# The only routine canonical publication path accepts validator-produced,
# disjoint partitions for contiguous new dates and refuses a stale predecessor.
uv run fundlab data extend-simulation `
  --predecessor-snapshot-id snap-... `
  --calendar-observation-id obs-... `
  --increment-observation-id obs-... `
  --universe-as-of 2026-07-17 --target-date 2026-07-17 `
  --description "manual EOD 2026-07-17" --publish

# Simulation cannot be published by build-snapshot, reconcile --publish, or the
# ordinary warehouse publish API. Those paths fail closed instead of bypassing CAS.

# Rebuild a scoped snapshot from an already reconciled canonical observation.
uv run fundlab data build-snapshot --observation-id obs-... --readiness research_price `
  --description "reviewed reconciled selection" --publish

# Inspect a pinned or current manifest
uv run fundlab data inspect --snapshot-id snap-...

# Persistent daily clock
uv run fundlab simulate --account-id paper-1 --date 2026-07-17 --weight 510300.SH=1

# Historical clock, same kernel
uv run fundlab simulate --account-id research-1 --start-date 2025-01-01 --end-date 2025-12-31 `
  --weight 510300.SH=1
```

Efinance and BaoStock are locked runtime dependencies and are installed by the normal project sync.
Their upstream access terms still apply. TickFlow uses the standard-library HTTP client and the free
historical endpoint when no API key is set.

## Current delivery state for Issue #7

The code now supports direct source capture, immutable hashes, backend-aware field reconciliation,
use-specific readiness, scoped publication, point-in-time ratio adjustment and resumable/sharded
history construction. TickFlow timestamps are normalized to the Asia/Shanghai session date and its
lot-rounded volume is calibrated against BaoStock's share units. Failed or fully excluded batches retry
normally; they are not mistaken for completed resume points.

The revision-2 current SH/SZ cohort is complete through 2026-07-24: 6,809 stocks and ETFs in 69
validated simulation partitions. The current audited component snapshot is
`snap-2a502eb188874c6ac7bbfb7f`, with
14,967,641 daily bars, 12,098 calendar rows, 53,184 corporate actions, and 46,325 adjustment factors.
For the latest session, 6,803 active bounded instruments were checked against direct xtquant and
Eastmoney upper/lower values with zero missing values or conflicts; five suspended instruments and one
unbounded IPO were represented explicitly. The weekly successor was atomically published from
`snap-27e45e704ec05e31679f6c7f`; the earlier EOD induction proof separately verified that reusing a stale
predecessor is rejected without changing the current pointer.
The previous schema-5 publication `snap-9a0c35efaf0ac6a9c9e6d82e` remains immutable and readable.

## Incremental publication state for Issue #8

The canonical snapshot remains one public contract, but its manifest now pins independent immutable
fact, rulebook and adjudication components plus derived simulation-view identities. Unchanged component
IDs are reused exactly. Routine EOD publication reads and writes only contiguous new dates, newly listed
instruments, new events and explicitly scoped corrections; it has no full-history fallback. The former
full simulation build command and warehouse incremental-copy method have been removed.

A real shadow replay of the already accepted 2026-07-18 through 2026-07-24 week reused all 761 baseline
components, created 17, and recorded `history_daily_reads=0`. Exhaustive structural validation proved
the unchanged historical fact blobs, and 142 bounded public-query slices compared the complete old
history and new week through their pinned view dependencies and builder version. All simulation
semantics matched the published snapshot. The only
recorded audit difference was historical calendar provenance: the new path correctly retained the
predecessor lineage instead of rewriting 12,084 unrelated rows to the latest observation.

Beijing and delisted-history completeness are intentionally outside revision 2, not hidden gaps in
this snapshot. No revision-2 validation mutated the then-protected legacy database or Parquet tree.

## Cutover rule

The cutover gates passed for the runtime: canonical hashes/counts/date ranges verify, the canonical API
can read the published snapshot, and the shared-kernel tests pass. Old code, APIs, CLIs, tests, paper
accounts, and ledgers were removed; Git history is their archive. The protected legacy market-data
files were retained as read-only evidence until 2026-07-27, then deleted with explicit user
authorization once the revision-2 delivery fully superseded them (see the retirement note above).
