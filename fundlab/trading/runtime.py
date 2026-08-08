from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Protocol, runtime_checkable

from fundlab.marketdata.portal import CanonicalMarketData, PointInTimeMarketView
from fundlab.trading.fees import FeeSchedule
from fundlab.trading.intent import PortfolioIntent, RiskPolicy
from fundlab.trading.kernel import TradingKernel
from fundlab.trading.repository import (
    RunBinding,
    RunMode,
    RunRecord,
    RunStatus,
    TradingRepository,
)
from fundlab.trading.state import ExecutionPolicy, PortfolioState


@runtime_checkable
class IntentSource(Protocol):
    strategy_id: str
    strategy_version: str
    config_hash: str

    def decide(
        self,
        *,
        account_id: str,
        market: PointInTimeMarketView,
        state: PortfolioState,
    ) -> PortfolioIntent | None: ...


@dataclass(frozen=True)
class HistoricalClock:
    start_date: date
    end_date: date

    @property
    def mode(self) -> RunMode:
        return RunMode.HISTORICAL

    def sessions(self, market_data: CanonicalMarketData) -> tuple[date, ...]:
        return market_data.trading_days(self.start_date, self.end_date)


@dataclass(frozen=True)
class DailyClock:
    target_date: date

    @property
    def mode(self) -> RunMode:
        return RunMode.DAILY

    def sessions(self, market_data: CanonicalMarketData) -> tuple[date, ...]:
        sessions = market_data.trading_days(self.target_date, self.target_date)
        if sessions != (self.target_date,):
            raise ValueError(f"Daily target is not an open trading session: {self.target_date}")
        return sessions


@dataclass(frozen=True)
class SimulationOutcome:
    run: RunRecord
    final_state: PortfolioState
    event_chain_head: str
    reused: bool


class SimulationService:
    def __init__(
        self,
        *,
        market_data: CanonicalMarketData,
        repository: TradingRepository,
        execution_policy: ExecutionPolicy,
        risk_policy: RiskPolicy,
        fee_schedule: FeeSchedule,
        seed: int = 0,
    ) -> None:
        self.market_data = market_data
        self.repository = repository
        self.execution_policy = execution_policy
        self.risk_policy = risk_policy
        self.fee_schedule = fee_schedule
        self.seed = int(seed)

    def run_historical(
        self,
        account_id: str,
        start_date: date,
        end_date: date,
        intent_source: IntentSource,
        *,
        parent_run_id: str | None = None,
        promote: bool = False,
    ) -> SimulationOutcome:
        return self._run(
            account_id,
            HistoricalClock(start_date, end_date),
            intent_source,
            parent_run_id=parent_run_id,
            promote=promote,
        )

    def run_daily(
        self,
        account_id: str,
        target_date: date,
        intent_source: IntentSource,
        *,
        parent_run_id: str | None = None,
        promote: bool = True,
    ) -> SimulationOutcome:
        if parent_run_id is None:
            initial_state, selected_parent = self.repository.selected_state(account_id)
            if selected_parent is not None:
                selected = self.repository.run(selected_parent)
                if target_date == selected.binding.end_date:
                    if self._matches_daily_request(selected, account_id, target_date, intent_source):
                        state = self.repository.final_state(selected.run_id)
                        return SimulationOutcome(
                            selected, state, self.repository.verify_run(selected.run_id), True,
                        )
                    raise ValueError(
                        "The account already finalized this trading date with a different run binding; "
                        "replay from an earlier parent without promotion"
                    )
                if target_date < selected.binding.end_date:
                    raise ValueError(
                        f"Daily target {target_date} precedes account head {selected.binding.end_date}"
                    )
        else:
            initial_state = self.repository.state_after(parent_run_id, account_id)
            selected_parent = parent_run_id
        return self._run(
            account_id,
            DailyClock(target_date),
            intent_source,
            parent_run_id=selected_parent,
            promote=promote,
            initial_state=initial_state,
        )

    def _run(
        self,
        account_id: str,
        clock: HistoricalClock | DailyClock,
        intent_source: IntentSource,
        *,
        parent_run_id: str | None,
        promote: bool,
        initial_state: PortfolioState | None = None,
    ) -> SimulationOutcome:
        sessions = clock.sessions(self.market_data)
        if not sessions:
            raise ValueError("Simulation clock resolved no trading sessions")
        if parent_run_id is not None:
            parent = self.repository.run(parent_run_id)
            if sessions[0] <= parent.binding.end_date:
                raise ValueError(
                    f"Simulation continuation must start after parent end date {parent.binding.end_date}"
                )
        if initial_state is None:
            initial_state = self.repository.state_after(parent_run_id, account_id)
        binding = RunBinding(
            account_id,
            clock.mode,
            self.market_data.snapshot_id,
            intent_source.strategy_id,
            intent_source.strategy_version,
            intent_source.config_hash,
            self.execution_policy.config_hash,
            self.risk_policy.config_hash,
            self.fee_schedule.config_hash,
            initial_state.state_hash,
            sessions[0],
            sessions[-1],
            self.seed,
            parent_run_id,
        )
        begun = self.repository.begin_run(binding, initial_state)
        if not begun.created:
            if begun.record.status is not RunStatus.COMPLETE:
                raise RuntimeError("Idempotent run lookup returned a non-complete run")
            state = self.repository.final_state(begun.record.run_id)
            return SimulationOutcome(
                begun.record, state, self.repository.verify_run(begun.record.run_id), True,
            )

        all_sessions = self.market_data.all_trading_days()
        instruments = {item.instrument_id: item for item in self.market_data.instruments()}
        kernel = TradingKernel(
            instruments=instruments,
            sessions=all_sessions,
            execution_policy=self.execution_policy,
            risk_policy=self.risk_policy,
            fee_schedule=self.fee_schedule,
        )
        current = initial_state
        run_id = begun.record.run_id
        declared_scope = getattr(intent_source, "market_scope", None)
        scoped_sessions = None
        if declared_scope is not None:
            state_scope = {
                *(lot.instrument_id for lot in current.lots),
                *(order.instrument_id for order in current.pending_orders),
                *(item.instrument_id for item in current.entitlements),
            }
            scoped_sessions = self.market_data.session_range(
                sessions,
                instrument_ids=(*tuple(declared_scope), *sorted(state_scope)),
            )
        try:
            for day in sessions:
                if scoped_sessions is None:
                    market = self.market_data.session(day)
                else:
                    market = scoped_sessions[day]
                processed = kernel.process_session(current, market)
                current = processed.state
                events = list(processed.events)
                view = PointInTimeMarketView(self.market_data, day)
                intent = intent_source.decide(account_id=account_id, market=view, state=current)
                if intent is not None:
                    self._validate_intent(intent, account_id, day, intent_source)
                    submitted = kernel.submit_intent(
                        current,
                        intent,
                        market,
                        next_session=self.market_data.next_trading_day(day),
                    )
                    current = submitted.state
                    events.extend(submitted.events)
                self.repository.append_session(run_id, events, current, processed.valuation)
            completed = self.repository.complete_run(run_id, current, promote=promote)
        except Exception as exc:
            try:
                self.repository.fail_run(run_id, f"{type(exc).__name__}: {exc}")
            except ValueError:
                pass
            raise
        return SimulationOutcome(completed, current, self.repository.verify_run(run_id), False)

    def _matches_daily_request(
        self,
        run: RunRecord,
        account_id: str,
        target_date: date,
        source: IntentSource,
    ) -> bool:
        binding = run.binding
        return (
            run.status is RunStatus.COMPLETE
            and binding.mode is RunMode.DAILY
            and binding.account_id == account_id
            and binding.start_date == target_date
            and binding.end_date == target_date
            and binding.snapshot_id == self.market_data.snapshot_id
            and binding.strategy_id == source.strategy_id
            and binding.strategy_version == source.strategy_version
            and binding.strategy_config_hash == source.config_hash
            and binding.execution_policy_hash == self.execution_policy.config_hash
            and binding.risk_policy_hash == self.risk_policy.config_hash
            and binding.fee_schedule_hash == self.fee_schedule.config_hash
            and binding.seed == self.seed
        )

    def _validate_intent(
        self,
        intent: PortfolioIntent,
        account_id: str,
        day: date,
        source: IntentSource,
    ) -> None:
        if intent.account_id != account_id:
            raise ValueError("Intent account does not match the simulation account")
        if intent.decision_date != day:
            raise ValueError("Intent decision date does not match the current close")
        if intent.snapshot_id != self.market_data.snapshot_id:
            raise ValueError("Intent snapshot does not match the pinned simulation snapshot")
        if (
            intent.strategy_id != source.strategy_id
            or intent.strategy_version != source.strategy_version
            or intent.strategy_config_hash != source.config_hash
        ):
            raise ValueError("Intent strategy binding does not match the run binding")
        declared_scope = getattr(source, "market_scope", None)
        if declared_scope is not None and not set(intent.target_weights) <= set(declared_scope):
            raise ValueError("Intent target falls outside its declared fixed market scope")
