# FundLab

FundLab is a single-user, local foundation for a future trading Agent. The current target is smaller
and stricter: trustworthy multi-provider market data, reproducible historical simulation, persistent
daily paper accounts, and an auditable feedback ledger. Agent learning and live trading are not in
scope yet.

The canonical path has two durable boundaries:

```text
named Provider -> immutable source observation -> field-level reconciliation -> pinned snapshot
PortfolioIntent -> risk assessment -> one daily trading kernel -> hash-chained ledger
```

Historical and daily simulation use the same kernel. They differ only in their clock. Raw prices drive
orders, fills, costs, and valuation; adjusted prices are exposed only through a point-in-time research
view. Completed runs emit deterministic feedback for equity/drawdown, orders and fills, turnover,
fees, slippage, realized P&L, dividends, and data/model incompleteness.

## Setup

```powershell
uv sync --dev --frozen --inexact
uv run fundlab --help
uv run pytest tests/canonical
```

`--inexact` is required because `xtquant` is installed outside `uv.lock` in this workspace.

## Trusted data canary

The current direct channels are TickFlow, Eastmoney through Efinance, BaoStock, and the explicitly
installed local MiniQMT/xtquant service. Their clients do not silently replace one another, and
open-stock-data is not a runtime dependency. Inspect the capability/installation matrix first:

```powershell
uv run fundlab data sources
```

Capture the same small symbol/date scope from named sources, then reconcile their observation IDs:

```powershell
uv run fundlab data collect --provider tickflow --capability daily_bars_raw `
  --start-date 2026-07-01 --end-date 2026-07-17 --instrument 600000.SH
uv run fundlab data collect --provider eastmoney-efinance --capability daily_bars_raw `
  --start-date 2026-07-01 --end-date 2026-07-17 --instrument 600000.SH
uv run fundlab data reconcile --readiness research_price --description "600000 canary" `
  --observation-id obs-... --observation-id obs-...
```

Critical raw OHLCV needs two independent backends. A third source resolves a two-versus-one conflict;
ties and incomplete coverage remain unpublishable. `research_price` can pass without simulation-only
fields, while `simulation` additionally requires daily tradability/rules, actions and other execution
facts. The revision-2 SH/SZ stock/ETF cohort has passed that full-market gate.

## Resumable history build

Build a multi-source research-price database. Every batch records immutable source observations before
reconciliation, failed batches retry on the next run, and only the exact adjudicated scope is
publishable. The first two sources are the baseline pair; the optional third source is called only
for conflicting instruments:

```powershell
uv run fundlab data build-history --start-date 2010-01-01 --end-date 2026-07-17 `
  --asset-type stock --asset-type etf --batch-size 100 `
  --source tickflow --source xtquant --source baostock --publish
```

The two independent sources inside one batch are captured concurrently. Disjoint process shards are
also supported, but use them only when both selected clients explicitly allow concurrent sessions.
BaoStock's anonymous client is single-session, and the local MiniQMT service should default to one
process unless concurrent supply has been validated. Shards never publish independently; assemble
them once all shard commands finish:

```powershell
uv run fundlab data build-history --end-date 2026-07-17 --batch-size 100 `
  --source tickflow --source xtquant --shard-count 4 --shard-index 0
uv run fundlab data build-history --end-date 2026-07-17 --batch-size 100 `
  --source tickflow --source xtquant --shard-count 4 --assemble-only --publish
```

Run shard indexes `0` through `3`. The final report lists every included and excluded instrument;
Beijing/delisted completeness is explicitly outside revision 2, while any one-source or conflicting
field inside the fixed SH/SZ scope remains a blocker.

## Simulation-ready foundation

The revision-2 builder fixes the universe at 2026-07-17 (5,200 current SH/SZ stocks and 1,602
current SH/SZ ETFs), resumes dense status/ST, stock and ETF action, and adjustment-factor evidence,
then materializes one explicit state for every in-lifecycle exchange session. Suspensions have zero
volume and null OHLC; prices are never forward-filled. ETF/stock actions retain both effect dates and
conservative public-known dates. Conflicting action economics require explicit evidence: official
holding terms remain canonical only when independent factor evidence supports them or proves that a
vendor event omitted a strict same-day cash component.

Price limits are derived from previous close plus the point-in-time exchange rule, then audited rather
than copied from a vendor. Historical high/low observations from at least two backends reject bounds
that are too narrow; on the latest completed session, MiniQMT/xtquant and Eastmoney direct upper/lower
limits must both match every active bounded instrument. This second check also covers overly wide
bounds and keeps the SSE unreformed `S`-share 5% rule separate from ST risk-warning rules.

The accepted 2010--2026 history is the immutable migration baseline. Routine publication uses only
already validated observations for the exact next dates:

```powershell
uv run fundlab data extend-simulation `
  --predecessor-snapshot-id snap-... `
  --calendar-observation-id obs-... `
  --increment-observation-id obs-... `
  --universe-as-of 2026-07-17 --target-date 2026-07-31 `
  --description "EOD through 2026-07-31" --publish
```

Internally one snapshot pins four content-addressed component classes: immutable market facts,
effective-dated trading rules, field-level adjudication evidence, and a disposable simulation view.
The command accepts only a contiguous increment, validated calendar and disjoint validated partitions.
It reuses unchanged component identities, rejects undeclared correction scope, validates a shadow
snapshot, and changes the current pointer with compare-and-swap only if the predecessor is still current.
It never silently falls back to a full-history rebuild.

The completed revision-2 real-data delivery is currently published through the Issue #8 component
manifest as `snap-2a502eb188874c6ac7bbfb7f`. It preserves the accepted extension through
2026-07-24 and contains 6,809 instruments, 14,967,641 daily bars, 12,098 calendar rows,
53,184 corporate actions, and 46,325 adjustment factors. Its 69 immutable increment partitions retain
the original market facts while recording exact upstream observation IDs and the final multi-provider
price-limit audit. The earlier one-day EOD induction proof also verified that a stale predecessor cannot
replace the current pointer.

Issue #8's real weekly shadow replay of that same 2026-07-18 through 2026-07-24 delivery reused all
761 predecessor components, created 17 components, and performed zero reads of pre-increment daily
facts. Its simulation semantics matched the published snapshot. Historical calendar provenance was
intentionally preserved instead of accepting the old builder's unrelated mass lineage rewrite.
The previous schema-5 publication `snap-9a0c35efaf0ac6a9c9e6d82e` remains immutable and readable.
Before cutover, 142 bounded public-query slices validated the complete historical and increment rows
through the pinned view dependency/version path.

## Safe legacy audit

The importer reads the existing collector and protected v1 warehouse without modifying them:

```powershell
uv run fundlab data audit-legacy
```

The current real-data audit verifies every report-bound partition checksum and row count. It does not
publish a canonical snapshot because coverage gates failed and the legacy data lacks ordinary stocks,
corporate actions, price-limit rules, and instrument settlement rules. An incomplete source observation
can be created only with an explicit flag; it still cannot be published as canonical data.

## Simulation

Create an isolated account after a ready canonical snapshot exists:

```powershell
uv run fundlab account create --account-id research-1 --name "Research 1" --initial-cash 1000000
uv run fundlab simulate --account-id research-1 --start-date 2026-01-05 --end-date 2026-03-31 `
  --weight 600000.SH=0.5 --weight 510300.SH=0.5
```

The checked-in fee schedule is a versioned simulation assumption: public taxes plus a 0.03% broker
commission with a CNY 5 minimum. It is trusted for reproducible simulation but does not claim to match
a real broker account. Live or broker-parity work must bind a separately verified account schedule.

See [docs/foundation.md](docs/foundation.md) for contracts, timing, realism boundaries, migration state,
and operating commands. Runtime consumers use only `fundlab.marketdata` and `fundlab.trading`; legacy
warehouse files remain read-only evidence and are not a second runtime path.
