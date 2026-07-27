"""Deterministic safeguards around local deterministic and LLM policies."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Mapping, Protocol, Sequence

from fundlab.agent.charter import Charter, CharterError, load_charter
from fundlab.agent.dividend import DividendCandidate, build_dividend_candidates
from fundlab.agent.features import InstrumentSnapshot, build_instrument_snapshots
from fundlab.agent.llm import DividendValueAdviser
from fundlab.agent.tools import AgentMemory, ReadingLibrary
from fundlab.common.canonical import stable_digest
from fundlab.marketdata.portal import CanonicalMarketData
from fundlab.trading import PortfolioState


class AgentPolicyError(ValueError):
    """The policy cannot produce a trustworthy deterministic decision."""


@dataclass(frozen=True)
class PolicyDecision:
    target_weights: Mapping[str, Decimal]
    reason: str
    hold: bool = False
    highlights: tuple["Highlight", ...] = ()
    audit: Mapping[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class Highlight:
    kind: str
    instrument_id: str
    name: str
    headline: str
    detail: str
    evidence_hash: str


@dataclass(frozen=True)
class DividendPolicyRuntime:
    account_id: str
    adviser: DividendValueAdviser
    library: ReadingLibrary
    memory: AgentMemory
    recent_memory_entries: int
    max_memory_entry_chars: int
    max_memory_total_chars: int
    state: PortfolioState
    last_decision_date: date | None
    force_review: bool = False


class DecisionPolicy(Protocol):
    policy_id: str
    version: str

    @property
    def config_hash(self) -> str: ...

    def decide(self, market: CanonicalMarketData, as_of: date) -> PolicyDecision: ...


def _decimal(value: object, label: str) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ArithmeticError, ValueError, TypeError) as exc:
        raise AgentPolicyError(f"{label} is not a decimal number: {value!r}") from exc
    if not parsed.is_finite():
        raise AgentPolicyError(f"{label} must be a finite number: {value!r}")
    return parsed


def _weights(raw: object, label: str) -> Mapping[str, Decimal]:
    if not isinstance(raw, Mapping) or not raw:
        raise AgentPolicyError(f"{label} must be a non-empty mapping of instrument -> weight")
    parsed: dict[str, Decimal] = {}
    for key, value in raw.items():
        instrument_id = str(key).strip()
        if not instrument_id:
            raise AgentPolicyError(f"{label} contains an empty instrument id")
        if instrument_id in parsed:
            raise AgentPolicyError(f"{label} contains duplicate instrument {instrument_id!r}")
        parsed[instrument_id] = _decimal(value, f"{label}[{key}]")
    total = sum(parsed.values())
    if any(value < 0 for value in parsed.values()) or total > Decimal("1"):
        raise AgentPolicyError(f"{label} weights must be non-negative and sum to <= 1: {raw}")
    return dict(sorted(parsed.items()))


@dataclass(frozen=True)
class MomentumRotationPolicy:
    """Choose declared risk-on/off weights from adjusted-close momentum."""

    risk_instrument: str
    defensive_instrument: str
    momentum_days: int
    threshold: Decimal
    risk_on: Mapping[str, Decimal]
    risk_off: Mapping[str, Decimal]
    policy_id: str = "momentum-rotation"
    version: str = "1"

    def __post_init__(self) -> None:
        risk_instrument = self.risk_instrument.strip()
        defensive_instrument = self.defensive_instrument.strip()
        if not risk_instrument or not defensive_instrument:
            raise AgentPolicyError("risk_instrument and defensive_instrument cannot be empty")
        object.__setattr__(self, "risk_instrument", risk_instrument)
        object.__setattr__(self, "defensive_instrument", defensive_instrument)
        if self.momentum_days < 1:
            raise AgentPolicyError("momentum_days must be at least 1")
        object.__setattr__(self, "threshold", _decimal(self.threshold, "threshold"))
        object.__setattr__(self, "risk_on", _weights(self.risk_on, "risk_on"))
        object.__setattr__(self, "risk_off", _weights(self.risk_off, "risk_off"))

    @property
    def config_hash(self) -> str:
        return stable_digest({
            "policy_id": self.policy_id,
            "version": self.version,
            "risk_instrument": self.risk_instrument,
            "defensive_instrument": self.defensive_instrument,
            "momentum_days": self.momentum_days,
            "threshold": self.threshold,
            "risk_on": self.risk_on,
            "risk_off": self.risk_off,
        })

    @property
    def instrument_ids(self) -> tuple[str, ...]:
        instrument_ids = {self.risk_instrument, self.defensive_instrument}
        instrument_ids.update(self.risk_on)
        instrument_ids.update(self.risk_off)
        return tuple(sorted(instrument_ids))

    def decide(self, market: CanonicalMarketData, as_of: date) -> PolicyDecision:
        snapshots = build_instrument_snapshots(
            market,
            self.instrument_ids,
            as_of=as_of,
            windows=(self.momentum_days,),
        )
        return self.decide_from_snapshots(snapshots)

    def decide_from_snapshots(
        self, snapshots: Mapping[str, InstrumentSnapshot],
    ) -> PolicyDecision:
        """Pure feature-to-decision seam used by focused policy tests."""

        risk = snapshots.get(self.risk_instrument)
        if risk is None:
            raise AgentPolicyError(f"No features for risk instrument {self.risk_instrument}")
        momentum = risk.momentum_for(self.momentum_days)
        if momentum is None:
            raise AgentPolicyError(
                f"{self.risk_instrument} lacks {self.momentum_days} traded sessions of history "
                f"(have {risk.sessions}); refusing to guess"
            )
        momentum_value = Decimal(str(momentum))
        risk_on = momentum_value > self.threshold
        weights = self.risk_on if risk_on else self.risk_off
        stance = "risk-on" if risk_on else "risk-off"
        comparator = ">" if risk_on else "<="
        allocation = " ".join(f"{key}={value}" for key, value in weights.items())
        reason = (
            f"momentum-rotation v{self.version}: {self.risk_instrument} "
            f"{self.momentum_days}-session adjusted momentum {momentum_value * 100:.4f}% "
            f"{comparator} threshold {self.threshold * 100}% -> {stance}; "
            f"target {allocation}"
        )
        return PolicyDecision(target_weights=weights, reason=reason)


@dataclass(frozen=True)
class DividendValuePolicy:
    """Weekly LLM review inside deterministic charter and portfolio gates."""

    charter: Charter
    top_n: int
    min_yield: Decimal
    min_dividend_years: int
    min_avg_amount: Decimal
    alert_min_yield: Decimal
    cash_reserve: Decimal
    candidate_pool_size: int
    rebalance_cooldown_days: int
    runtime: DividendPolicyRuntime | None = field(default=None, repr=False, compare=False)
    policy_id: str = "dividend-value"
    version: str = "1"

    def __post_init__(self) -> None:
        if self.charter.charter_id != self.policy_id:
            raise CharterError(
                f"Charter {self.charter.charter_id!r} does not govern {self.policy_id!r}"
            )
        if set(map(str, self.charter.rule("asset_types", ()))) != {"stock"}:
            raise CharterError("dividend-value charter must allow stocks only")
        if not bool(self.charter.rule("exclude_st", False)):
            raise CharterError("dividend-value charter must exclude ST instruments")
        floor_years = self.charter.int_rule("min_dividend_years", 1)
        if self.min_dividend_years < floor_years:
            raise CharterError(
                f"Tactics min_dividend_years={self.min_dividend_years} crosses charter floor "
                f"{floor_years}"
            )
        yield_floor = self.charter.decimal_rule("min_yield_floor", "0")
        if self.min_yield < yield_floor:
            raise CharterError(
                f"Tactics min_yield={self.min_yield} crosses charter floor {yield_floor}"
            )
        min_positions = self.charter.int_rule("min_positions", 1)
        max_positions = self.charter.int_rule("max_positions", 100)
        if not min_positions <= self.top_n <= max_positions:
            raise CharterError(
                f"Tactics top_n={self.top_n} is outside charter range "
                f"[{min_positions}, {max_positions}]"
            )
        if self.candidate_pool_size < self.top_n:
            raise AgentPolicyError("candidate_pool_size cannot be smaller than top_n")
        if self.min_avg_amount < 0:
            raise AgentPolicyError("min_avg_amount cannot be negative")
        if self.alert_min_yield < self.min_yield:
            raise AgentPolicyError("alert_min_yield cannot be below min_yield")
        min_cash = self.charter.decimal_rule("min_cash_weight", "0")
        if not min_cash <= self.cash_reserve < 1:
            raise CharterError(
                f"Tactics cash_reserve={self.cash_reserve} crosses charter floor {min_cash}"
            )
        max_single = self.charter.decimal_rule("max_single_weight", "1")
        if (Decimal(1) - self.cash_reserve) / self.top_n > max_single:
            raise CharterError("Equal target weight exceeds charter max_single_weight")
        if self.rebalance_cooldown_days < 1:
            raise AgentPolicyError("rebalance_cooldown_days must be positive")

    @property
    def config_hash(self) -> str:
        runtime_hash = None
        if self.runtime is not None:
            runtime_hash = {
                "adviser": self.runtime.adviser.config_hash,
                "library": self.runtime.library.config_hash,
                "recent_memory_entries": self.runtime.recent_memory_entries,
                "max_memory_entry_chars": self.runtime.max_memory_entry_chars,
                "max_memory_total_chars": self.runtime.max_memory_total_chars,
            }
        return stable_digest({
            "policy_id": self.policy_id,
            "version": self.version,
            "charter": self.charter.content_hash,
            "top_n": self.top_n,
            "min_yield": self.min_yield,
            "min_dividend_years": self.min_dividend_years,
            "min_avg_amount": self.min_avg_amount,
            "alert_min_yield": self.alert_min_yield,
            "cash_reserve": self.cash_reserve,
            "candidate_pool_size": self.candidate_pool_size,
            "rebalance_cooldown_days": self.rebalance_cooldown_days,
            "runtime": runtime_hash,
        })

    def decide(self, market: CanonicalMarketData, as_of: date) -> PolicyDecision:
        runtime = self.runtime
        if runtime is None:
            raise AgentPolicyError("dividend-value needs its LLM runtime")
        period = _weekly_period(as_of)
        if not runtime.force_review and not _last_session_of_week(market, as_of):
            return PolicyDecision(
                target_weights={},
                reason=f"dividend-value: {as_of.isoformat()} is not this week's final session",
                hold=True,
                audit={"review_completed": False, "review_period": period},
            )
        if (
            not runtime.force_review
            and runtime.memory.has_review(period, self.config_hash)
        ):
            return PolicyDecision(
                target_weights={},
                reason=f"dividend-value: weekly review {period} is already recorded",
                hold=True,
                audit={
                    "review_completed": False,
                    "review_period": period,
                    "skipped": "already_reviewed",
                },
            )

        measured = build_dividend_candidates(
            market,
            as_of=as_of,
            min_dividend_years=self.charter.int_rule("min_dividend_years", 1),
            min_avg_amount=float(self.min_avg_amount),
            exclude_st=True,
        )
        hard_yield_floor = self.charter.decimal_rule("min_yield_floor", "0")
        hard_eligible = {
            candidate.instrument_id: candidate
            for candidate in measured
            if _yield_decimal(candidate) >= hard_yield_floor
        }
        eligible = [
            candidate for candidate in measured
            if candidate.dividend_years >= self.min_dividend_years
            and _yield_decimal(candidate) >= self.min_yield
        ]
        eligible.sort(
            key=lambda item: (
                -item.ttm_yield,
                item.payout_variability,
                -item.dividend_years,
                item.instrument_id,
            )
        )
        candidate_pool = eligible[: self.candidate_pool_size]
        if len(candidate_pool) < self.top_n:
            raise AgentPolicyError(
                f"Only {len(candidate_pool)} eligible dividend candidates at "
                f"{as_of.isoformat()}, need {self.top_n}; refusing to stretch the charter"
            )

        portfolio, violations = self._portfolio_evidence(hard_eligible)
        pending_orders = bool(runtime.state.pending_orders)
        empty = not portfolio["holdings"] and not pending_orders
        cooldown_elapsed = (
            runtime.last_decision_date is None
            or (as_of - runtime.last_decision_date).days >= self.rebalance_cooldown_days
        )
        rebalance_required = bool(violations and portfolio["holdings"] and not pending_orders)
        can_rebalance = not pending_orders and (empty or cooldown_elapsed or rebalance_required)

        documents = runtime.library.context()
        memory = runtime.memory.recent(
            runtime.recent_memory_entries,
            max_entry_chars=runtime.max_memory_entry_chars,
            max_total_chars=runtime.max_memory_total_chars,
        )
        context: dict[str, object] = {
            "schema_version": "dividend-value-context-v1",
            "as_of": as_of.isoformat(),
            "review_period": period,
            "charter": {
                "id": self.charter.charter_id,
                "version": self.charter.version,
                "content_hash": self.charter.content_hash,
                "philosophy": self.charter.philosophy,
                "hard_rules": dict(self.charter.hard_rules),
            },
            "tactics": {
                "select_exactly": self.top_n,
                "minimum_yield": str(self.min_yield),
                "minimum_dividend_years": self.min_dividend_years,
                "minimum_average_amount": str(self.min_avg_amount),
                "cash_reserve": str(self.cash_reserve),
                "opportunity_email_minimum_yield": str(self.alert_min_yield),
                "rebalance_cooldown_days": self.rebalance_cooldown_days,
            },
            "portfolio": portfolio | {
                "hard_rule_violations": violations,
                "has_pending_orders": pending_orders,
                "last_decision_date": (
                    None if runtime.last_decision_date is None
                    else runtime.last_decision_date.isoformat()
                ),
                "cooldown_elapsed": cooldown_elapsed,
                "rebalance_required": rebalance_required,
                "can_rebalance": can_rebalance,
            },
            "eligible_candidates": [item.evidence() for item in candidate_pool],
            "library_documents": [item.evidence() for item in documents],
            "recent_memory": memory,
        }
        context_hash = stable_digest(context)
        advised = runtime.adviser.review(context, top_n=self.top_n)
        by_id = {candidate.instrument_id: candidate for candidate in candidate_pool}
        selected = advised.review.selected_instruments
        if any(instrument_id not in by_id for instrument_id in selected):
            raise AgentPolicyError("LLM selected an instrument outside the deterministic pool")

        opportunity_ids: set[str] = set()
        highlights: list[Highlight] = []
        for opportunity in advised.review.opportunities:
            candidate = by_id.get(opportunity.instrument_id)
            if candidate is None:
                raise AgentPolicyError("LLM opportunity is outside the deterministic pool")
            if opportunity.instrument_id in opportunity_ids:
                raise AgentPolicyError("LLM returned a duplicate opportunity")
            opportunity_ids.add(opportunity.instrument_id)
            if _yield_decimal(candidate) < self.alert_min_yield:
                raise AgentPolicyError(
                    f"LLM opportunity {candidate.instrument_id} is below the email yield floor"
                )
            evidence = {
                "kind": "opportunity",
                "candidate": candidate.evidence(),
                "headline": opportunity.headline,
                "rationale": opportunity.rationale,
            }
            highlights.append(Highlight(
                kind="opportunity",
                instrument_id=candidate.instrument_id,
                name=candidate.name,
                headline=opportunity.headline,
                detail=opportunity.rationale,
                evidence_hash=stable_digest(evidence),
            ))
        for violation in violations:
            instrument_id = str(violation.get("instrument_id") or "portfolio")
            evidence = {"kind": "risk", "violation": violation}
            highlights.append(Highlight(
                kind="risk",
                instrument_id=instrument_id,
                name=str(violation.get("name") or instrument_id),
                headline=f"红利价值持仓硬规则提醒：{instrument_id}",
                detail=str(violation["detail"]),
                evidence_hash=stable_digest(evidence),
            ))

        should_rebalance = rebalance_required or (
            advised.review.action == "rebalance" and can_rebalance
        )
        weight = ((Decimal(1) - self.cash_reserve) / self.top_n).quantize(
            Decimal("0.0001")
        )
        weights = {instrument_id: weight for instrument_id in selected}
        if sum(weights.values()) > Decimal(1) - self.charter.decimal_rule(
            "min_cash_weight", "0"
        ):
            raise AgentPolicyError("Rounded LLM portfolio would cross the charter cash floor")
        disposition = "rebalance" if should_rebalance else "hold"
        if advised.review.action == "rebalance" and not can_rebalance:
            disposition = "hold (rebalance suppressed by cooldown or pending orders)"
        reason = (
            f"dividend-value v{self.version} charter v{self.charter.version}: weekly review "
            f"{period}; model {advised.model} response {advised.response_id}; "
            f"adviser={advised.review.action}, outcome={disposition}; "
            f"selected {' '.join(selected)}; {advised.review.summary}"
        )
        audit = {
            "review_completed": True,
            "review_period": period,
            "context_hash": context_hash,
            "response_id": advised.response_id,
            "model": advised.model,
            "usage": dict(advised.usage),
            "adviser_action": advised.review.action,
            "summary": advised.review.summary,
            "selected_instruments": list(selected),
            "selection_rationale": dict(advised.review.selection_rationale),
            "portfolio_hard_rule_violations": violations,
            "can_rebalance": can_rebalance,
            "rebalance_required": rebalance_required,
            "library": [
                {
                    "name": item.name,
                    "content_hash": item.content_hash,
                    "truncated": item.truncated,
                }
                for item in documents
            ],
            "memory_entries": len(memory),
        }
        return PolicyDecision(
            target_weights=weights if should_rebalance else {},
            reason=reason,
            hold=not should_rebalance,
            highlights=tuple(highlights),
            audit=audit,
        )

    def _portfolio_evidence(
        self, hard_eligible: Mapping[str, DividendCandidate],
    ) -> tuple[dict[str, object], list[dict[str, str]]]:
        assert self.runtime is not None
        state = self.runtime.state
        quantities: dict[str, int] = {}
        for lot in state.lots:
            quantities[lot.instrument_id] = quantities.get(lot.instrument_id, 0) + lot.quantity
        market_values: dict[str, Decimal | None] = {}
        for instrument_id, quantity in quantities.items():
            price = state.last_prices.get(instrument_id)
            market_values[instrument_id] = (
                None if price is None else Decimal(quantity) * price
            )
        known_value = sum(value for value in market_values.values() if value is not None)
        total_equity = state.cash + known_value
        holdings: list[dict[str, object]] = []
        violations: list[dict[str, str]] = []
        max_single = self.charter.decimal_rule("max_single_weight", "1")
        for instrument_id in sorted(quantities):
            value = market_values[instrument_id]
            weight = None if value is None or total_equity <= 0 else value / total_equity
            candidate = hard_eligible.get(instrument_id)
            name = candidate.name if candidate is not None else instrument_id
            holdings.append({
                "instrument_id": instrument_id,
                "name": name,
                "quantity": quantities[instrument_id],
                "market_value": None if value is None else str(value),
                "weight": None if weight is None else str(weight),
                "passes_charter_screen": candidate is not None,
            })
            if candidate is None:
                violations.append({
                    "code": "holding_fails_charter_screen",
                    "instrument_id": instrument_id,
                    "name": name,
                    "detail": "当前持仓不再通过股票/ST/流动性/连续分红/2%股息率硬筛选",
                })
            if weight is None:
                violations.append({
                    "code": "holding_missing_price",
                    "instrument_id": instrument_id,
                    "name": name,
                    "detail": "当前持仓缺少可信估值价格，无法验证集中度",
                })
            elif weight > max_single:
                violations.append({
                    "code": "holding_overweight",
                    "instrument_id": instrument_id,
                    "name": name,
                    "detail": f"当前权重 {weight:.4f} 超过单股上限 {max_single}",
                })
        count = len(holdings)
        min_positions = self.charter.int_rule("min_positions", 1)
        max_positions = self.charter.int_rule("max_positions", 100)
        if count and not min_positions <= count <= max_positions:
            violations.append({
                "code": "position_count_outside_charter",
                "instrument_id": "portfolio",
                "name": "组合",
                "detail": f"当前持仓数 {count} 不在章程范围 [{min_positions}, {max_positions}]",
            })
        cash_weight = None if total_equity <= 0 else state.cash / total_equity
        min_cash = self.charter.decimal_rule("min_cash_weight", "0")
        if cash_weight is not None and cash_weight < min_cash:
            violations.append({
                "code": "cash_below_charter_floor",
                "instrument_id": "portfolio",
                "name": "组合",
                "detail": f"当前现金权重 {cash_weight:.4f} 低于章程下限 {min_cash}",
            })
        return ({
            "cash": str(state.cash),
            "total_equity_from_known_prices": str(total_equity),
            "cash_weight": None if cash_weight is None else str(cash_weight),
            "holdings": holdings,
            "pending_orders": [
                {
                    "instrument_id": order.instrument_id,
                    "side": order.side.value,
                    "remaining_quantity": order.remaining_quantity,
                    "status": order.status.value,
                }
                for order in state.pending_orders
            ],
        }, violations)


def _yield_decimal(candidate: DividendCandidate) -> Decimal:
    return Decimal(f"{candidate.ttm_yield:.8f}")


def _weekly_period(day: date) -> str:
    year, week, _ = day.isocalendar()
    return f"{year}-W{week:02d}"


def _last_session_of_week(market: CanonicalMarketData, day: date) -> bool:
    following = market.next_trading_day(day)
    return following is not None and following.isocalendar()[:2] != day.isocalendar()[:2]


_PARAM_WHITELIST: Mapping[str, frozenset[str]] = {
    "momentum-rotation": frozenset({
        "risk_instrument",
        "defensive_instrument",
        "momentum_days",
        "threshold",
        "risk_on",
        "risk_off",
    }),
    "dividend-value": frozenset({
        "charter",
        "top_n",
        "min_yield",
        "min_dividend_years",
        "min_avg_amount",
        "alert_min_yield",
        "cash_reserve",
        "candidate_pool_size",
        "rebalance_cooldown_days",
    }),
}


def build_policy(
    kind: str,
    params: Mapping[str, object],
    *,
    runtime: DividendPolicyRuntime | None = None,
) -> DecisionPolicy:
    """Build a declared policy and fail closed on unknown or misspelled keys."""

    allowed = _PARAM_WHITELIST.get(kind)
    if allowed is None:
        raise AgentPolicyError(f"Unknown agent policy kind: {kind!r}")
    unknown = set(map(str, params)) - allowed
    if unknown:
        raise AgentPolicyError(
            f"Unknown {kind} params {sorted(unknown)}; allowed: {sorted(allowed)}"
        )
    try:
        if kind == "momentum-rotation":
            return MomentumRotationPolicy(
                risk_instrument=str(params["risk_instrument"]),
                defensive_instrument=str(params["defensive_instrument"]),
                momentum_days=int(str(params.get("momentum_days", 60))),
                threshold=_decimal(params.get("threshold", "0"), "threshold"),
                risk_on=params["risk_on"],
                risk_off=params["risk_off"],
            )
        return DividendValuePolicy(
            charter=load_charter(str(params["charter"])),
            top_n=int(str(params.get("top_n", 10))),
            min_yield=_decimal(params.get("min_yield", "0.04"), "min_yield"),
            min_dividend_years=int(str(params.get("min_dividend_years", 5))),
            min_avg_amount=_decimal(
                params.get("min_avg_amount", "20000000"), "min_avg_amount",
            ),
            alert_min_yield=_decimal(
                params.get("alert_min_yield", "0.06"), "alert_min_yield",
            ),
            cash_reserve=_decimal(params.get("cash_reserve", "0.05"), "cash_reserve"),
            candidate_pool_size=int(str(params.get("candidate_pool_size", 50))),
            rebalance_cooldown_days=int(str(params.get("rebalance_cooldown_days", 28))),
            runtime=runtime,
        )
    except KeyError as exc:
        raise AgentPolicyError(f"{kind} config is missing {exc}") from exc
    except (InvalidOperation, ArithmeticError) as exc:
        raise AgentPolicyError(f"{kind} config has a non-numeric value: {exc}") from exc
    except (TypeError, ValueError) as exc:
        if isinstance(exc, (AgentPolicyError, CharterError)):
            raise
        raise AgentPolicyError(f"{kind} config is invalid: {exc}") from exc
