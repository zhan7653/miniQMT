# Backtrader Portfolio NAV Parity Spec

## Background

FundLab already has a local warehouse, point-in-time `DataPortal`, feature generation, strategies, risk checks, and a simple next-open backtest engine. The next priority is not to improve strategy returns, but to prove that FundLab's backtest portfolio net asset value can match a mainstream external implementation under the same inputs and assumptions.

The current environment cannot connect to `xtquant`, but the local warehouse may contain previously downloaded real data. V1 should use that local real data and compare FundLab against Backtrader on the smallest meaningful benchmark: single-ETF buy-and-hold Portfolio NAV parity.

Backtrader is chosen as the first external benchmark because it is transparent, local, reproducible, and suitable for validating core backtest accounting behavior. Domestic hosted platforms such as JoinQuant or RiceQuant may be useful later, but their data, adjustment, fee, and execution details are less transparent and harder to automate as a first parity target.

## Requirements

### Functional Requirements

- FR-1: The parity check must scan the local real-data warehouse and select one ETF with at least 252 trading days of usable OHLCV daily-bar data.
- FR-2: The selected benchmark must run a single-ETF buy-and-hold strategy in both FundLab and Backtrader over the same date range.
- FR-3: The benchmark assumptions must be explicit, including initial cash, execution price rule, fee setting, slippage setting, start date, and end date.
- FR-4: The comparison must align FundLab and Backtrader daily Portfolio NAV series by date.
- FR-5: The parity check must fail when daily Portfolio NAV differs by more than the accepted tolerance.
- FR-6: When parity fails, the output must identify the selected symbol, assumptions, first mismatching date, FundLab NAV, Backtrader NAV, and absolute difference.
- FR-7: If no local ETF has enough usable real data, the check must fail clearly instead of falling back to fake data.
- FR-8: Backtrader must be added as a development/test dependency so the parity check can be repeated.

### Non-Functional Requirements

- NFR-1: Daily Portfolio NAV absolute difference must be no greater than `1e-6` for every aligned date.
- NFR-2: The benchmark must be deterministic when run repeatedly against the same local warehouse contents.
- NFR-3: The V1 benchmark must stay limited to single-ETF buy-and-hold Portfolio NAV parity.

## Chosen Approach

Approach A: Minimal Backtrader Parity Test.

The implementation will select a locally available real ETF with at least 252 trading days of usable data, run a buy-and-hold benchmark through FundLab and Backtrader using identical explicit assumptions, and compare the daily Portfolio NAV series with a `1e-6` tolerance.

This approach is intentionally narrow. It establishes a hard external validation loop for the core backtest net-value calculation before expanding to multi-asset rebalancing, transaction-ledger parity, domestic hosted platform comparisons, or strategy-performance validation.

## Out of Scope

- Multi-ETF rebalancing parity.
- Momentum or other production strategy parity.
- Order-by-order, trade-by-trade, or full ledger parity.
- Full dividend-accounting parity.
- Domestic hosted platform parity against JoinQuant, RiceQuant, or similar platforms.
- New data-source integration.
- Automatic fallback to fake data when local real data is insufficient.
- Judging whether a strategy is profitable or useful.

## Acceptance Criteria

### AC-1: Select local real ETF

Given the local warehouse contains real daily bar data  
When the parity check starts  
Then it selects one ETF with at least 252 trading days of usable OHLCV data, preferring the ETF with the longest continuous valid coverage.

### AC-2: Fail on insufficient real data

Given no local ETF has at least 252 trading days of usable real daily bars  
When the parity check starts  
Then it fails clearly and reports that no eligible local real-data ETF/range exists.

### AC-3: Explicit benchmark assumptions

Given an eligible ETF and date range  
When FundLab and Backtrader run the buy-and-hold benchmark  
Then both runs use the same explicitly configured initial cash, execution price rule, fee setting, slippage setting, start date, and end date.

### AC-4: Portfolio NAV parity

Given both benchmark runs complete  
When daily Portfolio NAV series are aligned by date  
Then every aligned daily NAV value differs by no more than `1e-6`.

### AC-5: Difference diagnostics

Given Portfolio NAV parity fails  
When the check reports the failure  
Then it includes the first mismatching date, FundLab NAV, Backtrader NAV, absolute difference, selected symbol, and benchmark assumptions.

### AC-6: Regression coverage

Given the benchmark has been implemented  
When the relevant test is run  
Then it can be executed repeatably from the local project environment with Backtrader installed as a dev dependency.

## Open Questions (resolved)

- External benchmark: Backtrader for V1, because it is transparent and locally reproducible.
- Validation layer: Portfolio NAV only for V1.
- Strategy: single-ETF buy-and-hold.
- Data source: local real data already present in the warehouse.
- Dependency handling: Backtrader may be added as a dev dependency.
- Trading assumptions: explicitly configured in the benchmark instead of relying on defaults.
- Tolerance: daily Portfolio NAV absolute difference must be no greater than `1e-6`.
- Insufficient data behavior: fail clearly; do not fall back to fake data.
- Minimum data range: at least 252 trading days.

## Premises

- The immediate goal is to validate FundLab's backtest calculation against an external mainstream implementation, not to improve strategy returns.
- Backtrader is the right first benchmark because it makes differences easier to reproduce and diagnose.
- A single-ETF buy-and-hold benchmark is the smallest useful test of Portfolio NAV parity.
- Portfolio NAV parity is necessary but not sufficient; later work may still need transaction-ledger parity and domestic platform comparisons.
- Local real data must be used for V1; fake data fallback would weaken the validation target.
