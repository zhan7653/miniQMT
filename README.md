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
uv run ruff check .
uv run pytest tests/canonical
```

`--inexact` is required because `xtquant` is installed outside `uv.lock` in this workspace.

## Daily operations

One idempotent command runs the whole daily cycle — validate the two-source exchange calendar,
extend the published simulation snapshot through the componentized increment path, then advance
every configured paper account:

```powershell
uv run fundlab daily run
uv run fundlab daily status
```

Accounts, the session cutoff, and the agent decision directory live in the `daily:` section of
`config/fundlab.yaml`. Exit code 0 means ok, already up to date, or explicitly `degraded`; exit
code 2 is reserved for failures that physically prevent a trustworthy durable/atomic publication.
Normal prices are never discarded because an instrument or auxiliary evidence source failed.
Instrument price gaps use explicit quarantine (last trusted valuation, no execution, deferred due
orders) without a count or consecutive-day cutoff that could block unrelated prices. Missing
status, action, factor, or direct-limit evidence keeps real OHLC/volume visible but applies a
separate no-execution rule. A temporarily unavailable official universe carries the last trusted
membership with an explicit stale marker and the same no-execution rule. Only prefixes with a
complete coverage claim are composed with the new suffix. If a no-trade candidate has an active
row from another source, the exact instrument and conflicting observation IDs are quarantined
while the rest advances. Full
details remain in the console JSON and `data/reports/daily/`.

Schedule it Tuesday-Saturday at 06:00, after the previous trading day's upstream data has settled,
with catch-up and retries:

```powershell
pwsh -File scripts/register-daily-task.ps1
```

Operational requirement: the local MiniQMT client must be running so the `xtquant` provider can
serve data. If it is offline the data stage blocks cleanly and the next run resumes.

The canonical calendar carries exchange-announced future sessions (`daily.calendar_horizon_days`
past today, both calendar sources agreeing over the full window), so the account clock advances
all the way to the published data head: an intent decided at the close of T schedules its T+1
order inside the pinned snapshot calendar, and the next evening's publication executes it with
real T+1 prices.

## Dashboard

```powershell
uv run fundlab web
```

Starts a localhost dashboard (default `http://127.0.0.1:8610`, change with `--port`) with six
views: overview, a read-only crisis-strategy monitor, per-account equity curve / positions / orders / ledger events, daily run reports
with stage-level detail, Windows scheduled-task management plus a manual "run now" trigger with
live log tail, and agent decision submission with the same validation the account run applies.
The dashboard binds to localhost only and manages nothing that the CLI does not already own.

## Agent integration

The trading kernel accepts strategy decisions only as immutable `PortfolioIntent` objects through
the `fundlab.trading.IntentSource` protocol. Three implementations ship in `fundlab.strategies`:

- `StaticAllocationSource` — fixed target weights (the `strategy: static` account type).
- `FileIntentSource` — the out-of-process agent socket (the `strategy: agent-file` account type).
- `MovingAverageGridSource` — the deterministic, stateful MA-grid source (the
  `strategy: moving-average-grid` account type).

An external agent — any language, any LLM harness — participates by writing one JSON file per
account per session before the daily run, at `data/agent/decisions/<account_id>/<YYYY-MM-DD>.json`:

```json
{
  "account_id": "paper-agent",
  "decision_date": "2026-07-27",
  "target_weights": {"510300.SH": "0.6", "511010.SH": "0.4"},
  "reason": "why the agent wants this allocation",
  "agent_id": "my-agent"
}
```

No file means hold current positions — silence is a first-class outcome. A present-but-invalid
file (wrong account, wrong date, negative weight, missing reason) fails the account's run instead
of degrading to a hold, and the decision content is bound into the run's strategy config hash so a
replay cannot silently execute a different decision.

A local producer for that contract ships in `fundlab.agent`. Its deterministic policies are
`momentum-rotation`, month-end `dual-momentum`, `sector-momentum`,
`inverse-volatility`, correlation-aware risk parity,
trend/volatility targeting and liquid low-beta stocks, plus weekly `dividend-rules`, ST-removal
momentum, tightly capped active-ST momentum, and stateful `crisis-drawdown` ETF variants; only the
charter-bound `dividend-value` paper Agent
uses an LLM. The stock price/ST policies are prospective incubators from their account creation date:
the v2 universe does not claim complete delisted history, so they do not publish historical-backtest
performance. Eight independent crisis accounts are also prospective-only: they wait in
`511010.SH`, enter only after declared ETF-price drawdown/reversal or drawdown-ladder evidence,
and exit on the simulated account's own cost return or recovery toward the prior peak. Premium/
discount data is deliberately not an input; cross-border exposure is constrained through smaller
per-position caps instead. Every official non-dry-run crisis evaluation is first published as
immutable, validated evidence under `data/agent/evaluations/<account>/<as_of>/`; dry-runs never
enter that history, and an evidence-write failure prevents that account from publishing a new
decision. The dashboard reads those saved evaluations instead of recomputing signals on page load,
separates signal / queued decision / simulated execution, retains snapshot/config revisions, and
marks accounts whose return path contains a manual decision.

`paper-sector-momentum` is a prospective monthly rotation account over a manually confirmed,
versioned set of eleven domestic sector ETFs. It combines 20/60/120-session adjusted momentum at
20%/30%/50%, requires both positive 120-session momentum and a positive composite score, and selects
at most three sectors. Selected sectors share a 90% risk budget by inverse 60-session volatility,
subject to a 40% single-sector cap; the remainder goes to `511010.SH`, including 100% defensive
allocation when no sector qualifies. The whitelist stores explicit sector labels and instrument IDs;
the runtime never infers a sector from an ETF name.

The four `paper-ma-grid-*` accounts run one ETF each (`510050.SH`, `510300.SH`, `510500.SH`, or
`511380.SH`) through the same daily long-only state machine. A 60-session adjusted-price average
starts each grid cycle, then its anchor and volatility-scaled spacing are frozen until a confirmed
neutral reset. Target weights follow a convex nine-level ladder; a falling 120-session trend limits
new buying, persistent weakness stops it, and an extreme fifth-tier move reduces inventory toward
the neutral allocation. Each account keeps an explicit cash reserve through its instrument-specific
maximum weight. Historical research uses the same state machine through `PortfolioIntent`; daily
operation uses that same in-process `PortfolioIntent` source, so a signal at one session's close is
scheduled for the next trading session's open without an extra decision-file delay. The long research matrix is not
published performance: `511380.SH` runs are complete, while stock-ETF runs that held through cash
distributions remain explicitly incomplete until an investor-tax identity is modeled; one
`510500.SH` interval also reports an unmodeled fractional split. The kernel retains those quality
flags instead of silently treating gross distributions as final after-tax cash.

MA-grid v1 failed its 2026-08-08 research hurdle: annualized returns remained below both the
fixed-neutral exposure baseline and an acceptable cash-management hurdle because the strategy
spent too much time uninvested. The four account declarations are retained for audit and research
but have `enabled: false`; their persistent paper accounts are paused. Do not resume this version
without a new strategy version and a prospective approval after it beats the declared baselines.

The rules-only
dividend account is a direct comparison baseline: it requires a current trailing cash payment and
a completed fiscal dividend no more than two years old, then ranks the eligible stocks by
conservative sustainable yield, payout stability and liquidity. The LLM dividend Agent screens the
canonical stock universe deterministically, sends only the bounded
candidate/library/memory context to an OpenAI-compatible **Responses** endpoint, accepts a strict
JSON Schema result, then re-validates every selected instrument and portfolio limit before the
shared decision writer can publish anything. Its 10-stock paper portfolio is evaluated against
`159207.SZ` on an adjusted total-return basis from the first invested session. Relative performance
is diagnostic for the first 60 common sessions and can only be supporting — never sole — rebalance
evidence afterwards. The dashboard projects the saved weekly comparison onto the account view. The
scheduled task tries enabled policies before the daily cycle and again after a successful publication:

```powershell
uv run fundlab agent decide --all        # or --account-id paper-agent [--dry-run]
uv run fundlab agent decide --account-id paper-dividend --force-review --dry-run
```

`paper-dividend` is enabled for the weekly cadence after a successful real forced dry-run. Repeat
the command above whenever relay compatibility needs revalidation. Its relay key comes only from
`FUNDLAB_LLM_API_KEY`. The CLI automatically loads an ignored repository-root
`.env.local` without overriding explicit process environment values. Optional SMTP mail uses
`FUNDLAB_SMTP_HOST`, `FUNDLAB_SMTP_PORT`, `FUNDLAB_SMTP_USER`, and
`FUNDLAB_SMTP_PASSWORD`; `FUNDLAB_EMAIL_TO` optionally selects a different recipient and otherwise
the SMTP user receives the message. Keep credentials only in `.env.local` or the process
environment, never in committed YAML. A weekly review may email a new validated
opportunity without changing the portfolio. A policy or strict-response failure writes no decision
and sends no mail, which the kernel treats as hold. News intake and autonomous strategy learning
remain deferred; live trading is explicitly out of scope. See `docs/architecture/05-agent.md`.

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

The accepted 2010--2026 history is the immutable migration baseline. Routine publication first turns
one ready field-level reconciliation into an exact simulation partition. This stage derives trading
rules, verifies full open-session coverage, binds the upstream observations and requires two direct
provider limit values on the latest bounded session:

```powershell
uv run fundlab data validate-simulation-increment `
  --candidate-observation-id obs-... `
  --calendar-observation-id obs-... `
  --universe-as-of 2026-07-31 `
  --start-date 2026-07-27 --end-date 2026-07-31 `
  --description "validated EOD partition through 2026-07-31"

uv run fundlab data extend-simulation `
  --predecessor-snapshot-id snap-... `
  --calendar-observation-id obs-... `
  --increment-observation-id obs-... `
  --universe-as-of 2026-07-17 --target-date 2026-07-31 `
  --description "EOD through 2026-07-31" --publish
```

Internally one snapshot pins four content-addressed component classes: immutable market facts,
effective-dated trading rules, field-level adjudication evidence, and a disposable simulation view.
The extension command accepts only a contiguous increment, validated calendar and disjoint partitions
produced by the committed validator.
It reuses unchanged component identities, rejects undeclared correction scope, validates a shadow
snapshot, and changes the current pointer with compare-and-swap only if the predecessor is still current.
Generic `build-snapshot`, `reconcile --publish`, and ordinary `publish()` cannot create or publish a
simulation snapshot. The path never silently falls back to a full-history rebuild.

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

## Simulation

Create an isolated account after a ready canonical snapshot exists:

```powershell
uv run fundlab account create --account-id research-1 --name "Research 1" --initial-cash 1000000
uv run fundlab simulate --account-id research-1 --start-date 2026-01-05 --end-date 2026-03-31 `
  --weight 600000.SH=0.5 --weight 510300.SH=0.5

# Reuse a historically supported configured policy with an isolated research start date.
uv run fundlab simulate --account-id research-grid-50 --start-date 2015-08-03 --end-date 2026-08-07 `
  --policy-account-id paper-ma-grid-510050 --policy-activation-date 2015-08-03
```

The checked-in fee schedule is a versioned simulation assumption: public taxes plus a 0.03% broker
commission with a CNY 5 minimum. It is trusted for reproducible simulation but does not claim to match
a real broker account. Live or broker-parity work must bind a separately verified account schedule.

Architecture documentation (Chinese) lives under [docs/architecture/](docs/architecture/00-overview.md):
an overview plus per-module deep dives for marketdata, trading/strategies, the daily pipeline/CLI/ops,
and the web console.

See [docs/foundation.md](docs/foundation.md) for contracts, timing, realism boundaries, migration state,
and operating commands. Runtime consumers use only `fundlab.marketdata`, `fundlab.trading`,
`fundlab.strategies`, and the `fundlab.pipeline` orchestrator. The pre-v2 legacy warehouses and their
audit/import channel were retired in July 2026 after the revision-2 canonical delivery superseded them;
one-time build and validation reports are archived in `data/archive/build-reports-2026-07.zip`.
