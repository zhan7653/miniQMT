"""Deterministic safeguards around local deterministic and LLM policies."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal, InvalidOperation
from math import sqrt
from typing import Mapping, Protocol

import pandas as pd

from fundlab.agent.benchmark import (
    BenchmarkEvaluation,
    PortfolioPerformancePoint,
    evaluate_benchmark,
    unavailable_benchmark,
)
from fundlab.agent.charter import Charter, CharterError, load_charter
from fundlab.agent.dividend import DividendCandidate, build_dividend_candidates
from fundlab.agent.features import (
    CrisisInstrumentFeatures,
    InstrumentSnapshot,
    build_aligned_return_window,
    build_crisis_features,
    build_instrument_snapshots,
)
from fundlab.agent.llm import DividendReview, DividendValueAdviser
from fundlab.agent.price_signals import (
    PriceSignalCandidate,
    build_price_signal_candidates,
)
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
    performance_points: tuple[PortfolioPerformancePoint, ...] = ()
    force_review: bool = False


@dataclass(frozen=True)
class PortfolioPolicyRuntime:
    """Current account facts needed by deterministic stateful policies."""

    account_id: str
    state: PortfolioState
    last_decision_date: date | None
    last_risk_exit_date: date | None = None


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


def _boolean(value: object, label: str) -> bool:
    if not isinstance(value, bool):
        raise AgentPolicyError(f"{label} must be true or false")
    return value


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


def _instrument_ids(raw: object, label: str, *, minimum: int = 1) -> tuple[str, ...]:
    if not isinstance(raw, (list, tuple)):
        raise AgentPolicyError(f"{label} must be a list of instrument ids")
    parsed = tuple(str(item).strip() for item in raw)
    if len(parsed) < minimum or any(not item for item in parsed):
        raise AgentPolicyError(f"{label} needs at least {minimum} non-empty instrument ids")
    if len(set(parsed)) != len(parsed):
        raise AgentPolicyError(f"{label} contains duplicate instrument ids")
    return parsed


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
class DualMomentumPolicy:
    """Monthly absolute/relative momentum across a declared ETF universe."""

    risk_instruments: tuple[str, ...]
    defensive_instrument: str
    short_momentum_days: int
    long_momentum_days: int
    select_count: int
    threshold: Decimal
    policy_id: str = "dual-momentum"
    version: str = "1"

    def __post_init__(self) -> None:
        risks = _instrument_ids(self.risk_instruments, "risk_instruments", minimum=2)
        defensive = self.defensive_instrument.strip()
        if not defensive:
            raise AgentPolicyError("defensive_instrument cannot be empty")
        if defensive in risks:
            raise AgentPolicyError("defensive_instrument must be outside risk_instruments")
        if self.short_momentum_days < 1:
            raise AgentPolicyError("short_momentum_days must be positive")
        if self.long_momentum_days <= self.short_momentum_days:
            raise AgentPolicyError("long_momentum_days must exceed short_momentum_days")
        if not 1 <= self.select_count <= len(risks):
            raise AgentPolicyError("select_count must fit inside risk_instruments")
        object.__setattr__(self, "risk_instruments", risks)
        object.__setattr__(self, "defensive_instrument", defensive)
        object.__setattr__(self, "threshold", _decimal(self.threshold, "threshold"))

    @property
    def config_hash(self) -> str:
        return stable_digest({
            "policy_id": self.policy_id,
            "version": self.version,
            "risk_instruments": self.risk_instruments,
            "defensive_instrument": self.defensive_instrument,
            "short_momentum_days": self.short_momentum_days,
            "long_momentum_days": self.long_momentum_days,
            "select_count": self.select_count,
            "threshold": self.threshold,
            "cadence": "month_end",
        })

    def decide(self, market: CanonicalMarketData, as_of: date) -> PolicyDecision:
        if not _last_session_of_month(market, as_of):
            return PolicyDecision(
                target_weights={},
                reason=f"dual-momentum: {as_of.isoformat()} is not month end",
                hold=True,
                audit={"cadence": "month_end"},
            )
        snapshots = build_instrument_snapshots(
            market,
            (*self.risk_instruments, self.defensive_instrument),
            as_of=as_of,
            windows=(self.short_momentum_days, self.long_momentum_days),
        )
        return self.decide_from_snapshots(snapshots)

    def decide_from_snapshots(
        self, snapshots: Mapping[str, InstrumentSnapshot],
    ) -> PolicyDecision:
        defensive = snapshots.get(self.defensive_instrument)
        if defensive is None or defensive.last_close is None:
            raise AgentPolicyError(
                f"No trustworthy price for defensive instrument {self.defensive_instrument}"
            )
        scored: list[tuple[Decimal, str]] = []
        score_evidence: dict[str, str] = {}
        for instrument_id in self.risk_instruments:
            snapshot = snapshots.get(instrument_id)
            if snapshot is None:
                raise AgentPolicyError(f"No features for risk instrument {instrument_id}")
            short = snapshot.momentum_for(self.short_momentum_days)
            long = snapshot.momentum_for(self.long_momentum_days)
            if short is None or long is None:
                raise AgentPolicyError(
                    f"{instrument_id} lacks {self.long_momentum_days} traded sessions of "
                    "history; refusing to guess"
                )
            score = (Decimal(str(short)) + Decimal(str(long))) / Decimal("2")
            score_evidence[instrument_id] = str(score)
            if score > self.threshold:
                scored.append((score, instrument_id))
        selected = [
            instrument_id
            for _, instrument_id in sorted(scored, key=lambda item: (-item[0], item[1]))[
                : self.select_count
            ]
        ]
        if not selected:
            weights = {self.defensive_instrument: Decimal("1")}
            stance = "defensive"
        else:
            weight = Decimal("1") / Decimal(len(selected))
            weights = {instrument_id: weight for instrument_id in selected}
            stance = "risk-on"
        allocation = " ".join(f"{key}={value}" for key, value in weights.items())
        reason = (
            f"dual-momentum v{self.version}: month-end {stance}; average of "
            f"{self.short_momentum_days}/{self.long_momentum_days}-session adjusted momentum; "
            f"scores {score_evidence}; target {allocation}"
        )
        return PolicyDecision(
            target_weights=weights,
            reason=reason,
            audit={"scores": score_evidence, "selected_instruments": selected},
        )


@dataclass(frozen=True)
class InverseVolatilityPolicy:
    """Monthly capped inverse-volatility allocation over declared ETFs."""

    instruments: tuple[str, ...]
    volatility_days: int
    max_weight: Decimal
    policy_id: str = "inverse-volatility"
    version: str = "1"

    def __post_init__(self) -> None:
        instruments = _instrument_ids(self.instruments, "instruments", minimum=2)
        maximum = _decimal(self.max_weight, "max_weight")
        if self.volatility_days < 2:
            raise AgentPolicyError("volatility_days must be at least 2")
        if not Decimal("0") < maximum <= Decimal("1"):
            raise AgentPolicyError("max_weight must be in (0, 1]")
        if maximum * len(instruments) < Decimal("1"):
            raise AgentPolicyError("max_weight is too small to allocate the full portfolio")
        object.__setattr__(self, "instruments", instruments)
        object.__setattr__(self, "max_weight", maximum)

    @property
    def config_hash(self) -> str:
        return stable_digest({
            "policy_id": self.policy_id,
            "version": self.version,
            "instruments": self.instruments,
            "volatility_days": self.volatility_days,
            "max_weight": self.max_weight,
            "cadence": "month_end",
        })

    def decide(self, market: CanonicalMarketData, as_of: date) -> PolicyDecision:
        if not _last_session_of_month(market, as_of):
            return PolicyDecision(
                target_weights={},
                reason=f"inverse-volatility: {as_of.isoformat()} is not month end",
                hold=True,
                audit={"cadence": "month_end"},
            )
        snapshots = build_instrument_snapshots(
            market,
            self.instruments,
            as_of=as_of,
            windows=(self.volatility_days,),
        )
        return self.decide_from_snapshots(snapshots)

    def decide_from_snapshots(
        self, snapshots: Mapping[str, InstrumentSnapshot],
    ) -> PolicyDecision:
        volatilities: dict[str, Decimal] = {}
        for instrument_id in self.instruments:
            snapshot = snapshots.get(instrument_id)
            volatility = None if snapshot is None else snapshot.volatility_for(
                self.volatility_days
            )
            if volatility is None:
                raise AgentPolicyError(
                    f"{instrument_id} lacks {self.volatility_days} return observations; "
                    "refusing to guess"
                )
            parsed = Decimal(str(volatility))
            if parsed <= 0:
                raise AgentPolicyError(
                    f"{instrument_id} has non-positive realized volatility; refusing to divide"
                )
            volatilities[instrument_id] = parsed
        weights = _capped_inverse_volatility_weights(volatilities, self.max_weight)
        reason = (
            f"inverse-volatility v{self.version}: month-end "
            f"{self.volatility_days}-session annualized volatility "
            f"{dict(map(lambda item: (item[0], str(item[1])), volatilities.items()))}; "
            f"target {' '.join(f'{key}={value:.6f}' for key, value in weights.items())}"
        )
        return PolicyDecision(
            target_weights=weights,
            reason=reason,
            audit={
                "annualized_volatility": {
                    key: str(value) for key, value in volatilities.items()
                }
            },
        )


def _capped_inverse_volatility_weights(
    volatilities: Mapping[str, Decimal], max_weight: Decimal,
) -> dict[str, Decimal]:
    inverses = {key: Decimal("1") / value for key, value in volatilities.items()}
    remaining = set(inverses)
    remaining_weight = Decimal("1")
    allocated: dict[str, Decimal] = {}
    while remaining:
        total_inverse = sum(inverses[key] for key in remaining)
        proposals = {
            key: remaining_weight * inverses[key] / total_inverse for key in remaining
        }
        capped = sorted(key for key, value in proposals.items() if value > max_weight)
        if capped:
            for key in capped:
                allocated[key] = max_weight
                remaining.remove(key)
                remaining_weight -= max_weight
            continue
        ordered = sorted(remaining)
        for key in ordered[:-1]:
            allocated[key] = proposals[key]
        last = ordered[-1]
        allocated[last] = remaining_weight - sum(
            allocated[key] for key in ordered[:-1]
        )
        remaining.clear()
    return dict(sorted(allocated.items()))


@dataclass(frozen=True)
class CorrelationRiskParityPolicy:
    """Monthly equal-risk-contribution allocation using shrunk covariance."""

    instruments: tuple[str, ...]
    return_days: int
    covariance_shrinkage: Decimal
    max_weight: Decimal
    policy_id: str = "correlation-risk-parity"
    version: str = "1"

    def __post_init__(self) -> None:
        instruments = _instrument_ids(self.instruments, "instruments", minimum=3)
        shrinkage = _decimal(self.covariance_shrinkage, "covariance_shrinkage")
        maximum = _decimal(self.max_weight, "max_weight")
        if self.return_days < 20:
            raise AgentPolicyError("return_days must be at least 20")
        if not Decimal("0") <= shrinkage <= Decimal("1"):
            raise AgentPolicyError("covariance_shrinkage must be in [0, 1]")
        if not Decimal("0") < maximum <= Decimal("1"):
            raise AgentPolicyError("max_weight must be in (0, 1]")
        if maximum * len(instruments) < Decimal("1"):
            raise AgentPolicyError("max_weight is too small to allocate the full portfolio")
        object.__setattr__(self, "instruments", instruments)
        object.__setattr__(self, "covariance_shrinkage", shrinkage)
        object.__setattr__(self, "max_weight", maximum)

    @property
    def config_hash(self) -> str:
        return stable_digest({
            "policy_id": self.policy_id,
            "version": self.version,
            "instruments": self.instruments,
            "return_days": self.return_days,
            "covariance_shrinkage": self.covariance_shrinkage,
            "max_weight": self.max_weight,
            "cadence": "month_end",
        })

    def decide(self, market: CanonicalMarketData, as_of: date) -> PolicyDecision:
        if not _last_session_of_month(market, as_of):
            return PolicyDecision(
                target_weights={},
                reason=f"correlation-risk-parity: {as_of.isoformat()} is not month end",
                hold=True,
                audit={"cadence": "month_end"},
            )
        returns = build_aligned_return_window(
            market, self.instruments, as_of=as_of, sessions=self.return_days,
        )
        return self.decide_from_returns(returns)

    def decide_from_returns(self, returns: pd.DataFrame) -> PolicyDecision:
        frame = returns.reindex(columns=self.instruments).apply(pd.to_numeric, errors="coerce")
        frame = frame.dropna(how="any")
        if len(frame) != self.return_days:
            raise AgentPolicyError(
                f"Need {self.return_days} aligned return observations, have {len(frame)}; "
                "refusing to guess covariance"
            )
        covariance = frame.cov(ddof=0)
        shrinkage = float(self.covariance_shrinkage)
        for left in self.instruments:
            for right in self.instruments:
                if left != right:
                    covariance.loc[left, right] *= 1.0 - shrinkage
        weights, contributions = _equal_risk_contribution_weights(
            covariance, self.max_weight,
        )
        return PolicyDecision(
            target_weights=weights,
            reason=(
                f"correlation-risk-parity v{self.version}: month-end "
                f"{self.return_days}-session covariance with {self.covariance_shrinkage} "
                "diagonal shrinkage; target "
                + " ".join(f"{key}={value:.6f}" for key, value in weights.items())
            ),
            audit={
                "risk_contributions": contributions,
                "covariance_shrinkage": str(self.covariance_shrinkage),
            },
        )


def _equal_risk_contribution_weights(
    covariance: pd.DataFrame,
    max_weight: Decimal,
) -> tuple[dict[str, Decimal], dict[str, str]]:
    instruments = tuple(map(str, covariance.columns))
    matrix = [
        [float(covariance.loc[left, right]) for right in instruments]
        for left in instruments
    ]
    if any(matrix[index][index] <= 0 for index in range(len(instruments))):
        raise AgentPolicyError("Risk-parity covariance has non-positive variance")
    budget = 1.0 / len(instruments)
    solution = [1.0 / sqrt(matrix[index][index]) for index in range(len(instruments))]
    converged = False
    for _ in range(10_000):
        previous = tuple(solution)
        for index in range(len(instruments)):
            cross = sum(
                matrix[index][other] * solution[other]
                for other in range(len(instruments)) if other != index
            )
            diagonal = matrix[index][index]
            discriminant = cross * cross + 4.0 * diagonal * budget
            solution[index] = (-cross + sqrt(discriminant)) / (2.0 * diagonal)
        if max(abs(value - previous[index]) for index, value in enumerate(solution)) < 1e-12:
            converged = True
            break
    if not converged or any(value <= 0 for value in solution):
        raise AgentPolicyError("Equal-risk-contribution solver did not converge")
    total = sum(solution)
    raw = {instrument: solution[index] / total for index, instrument in enumerate(instruments)}
    weights = _cap_proportional_weights(raw, max_weight)
    numeric = [float(weights[instrument]) for instrument in instruments]
    portfolio_variance = sum(
        numeric[left] * matrix[left][right] * numeric[right]
        for left in range(len(instruments))
        for right in range(len(instruments))
    )
    contributions = {}
    for index, instrument in enumerate(instruments):
        marginal = sum(matrix[index][other] * numeric[other] for other in range(len(instruments)))
        contribution = numeric[index] * marginal / portfolio_variance
        contributions[instrument] = str(Decimal(str(contribution)))
    return weights, contributions


def _cap_proportional_weights(
    raw: Mapping[str, float], max_weight: Decimal,
) -> dict[str, Decimal]:
    remaining = set(raw)
    remaining_weight = Decimal("1")
    allocated: dict[str, Decimal] = {}
    while remaining:
        total_raw = sum(Decimal(str(raw[key])) for key in remaining)
        if total_raw <= 0:
            raise AgentPolicyError("Cannot allocate non-positive portfolio weights")
        proposals = {
            key: remaining_weight * Decimal(str(raw[key])) / total_raw
            for key in remaining
        }
        capped = sorted(key for key, value in proposals.items() if value > max_weight)
        if capped:
            for key in capped:
                allocated[key] = max_weight
                remaining.remove(key)
                remaining_weight -= max_weight
            continue
        ordered = sorted(remaining)
        for key in ordered[:-1]:
            allocated[key] = proposals[key]
        allocated[ordered[-1]] = Decimal("1") - sum(allocated.values())
        remaining.clear()
    return dict(sorted(allocated.items()))


@dataclass(frozen=True)
class TrendVolatilityTargetPolicy:
    """Monthly trend-filtered ETF basket scaled to a volatility target."""

    risk_instruments: tuple[str, ...]
    defensive_instrument: str
    momentum_days: int
    volatility_days: int
    trend_threshold: Decimal
    target_volatility: Decimal
    max_risk_weight: Decimal
    policy_id: str = "trend-volatility-target"
    version: str = "1"

    def __post_init__(self) -> None:
        risks = _instrument_ids(self.risk_instruments, "risk_instruments", minimum=2)
        defensive = self.defensive_instrument.strip()
        if not defensive or defensive in risks:
            raise AgentPolicyError("defensive_instrument must be non-empty and outside risk assets")
        if self.momentum_days < 20 or self.volatility_days < 20:
            raise AgentPolicyError("trend and volatility windows must be at least 20")
        threshold = _decimal(self.trend_threshold, "trend_threshold")
        target = _decimal(self.target_volatility, "target_volatility")
        maximum = _decimal(self.max_risk_weight, "max_risk_weight")
        if not Decimal("0") < target <= Decimal("1"):
            raise AgentPolicyError("target_volatility must be in (0, 1]")
        if not Decimal("0") < maximum <= Decimal("1"):
            raise AgentPolicyError("max_risk_weight must be in (0, 1]")
        object.__setattr__(self, "risk_instruments", risks)
        object.__setattr__(self, "defensive_instrument", defensive)
        object.__setattr__(self, "trend_threshold", threshold)
        object.__setattr__(self, "target_volatility", target)
        object.__setattr__(self, "max_risk_weight", maximum)

    @property
    def config_hash(self) -> str:
        return stable_digest({
            "policy_id": self.policy_id,
            "version": self.version,
            "risk_instruments": self.risk_instruments,
            "defensive_instrument": self.defensive_instrument,
            "momentum_days": self.momentum_days,
            "volatility_days": self.volatility_days,
            "trend_threshold": self.trend_threshold,
            "target_volatility": self.target_volatility,
            "max_risk_weight": self.max_risk_weight,
            "cadence": "month_end",
        })

    def decide(self, market: CanonicalMarketData, as_of: date) -> PolicyDecision:
        if not _last_session_of_month(market, as_of):
            return PolicyDecision(
                target_weights={},
                reason=f"trend-volatility-target: {as_of.isoformat()} is not month end",
                hold=True,
                audit={"cadence": "month_end"},
            )
        snapshots = build_instrument_snapshots(
            market,
            (*self.risk_instruments, self.defensive_instrument),
            as_of=as_of,
            windows=(self.momentum_days,),
        )
        selected = self._selected(snapshots)
        returns = (
            build_aligned_return_window(
                market, selected, as_of=as_of, sessions=self.volatility_days,
            )
            if selected else pd.DataFrame()
        )
        return self.decide_from_features(snapshots, returns)

    def _selected(
        self, snapshots: Mapping[str, InstrumentSnapshot],
    ) -> tuple[str, ...]:
        selected = []
        for instrument_id in self.risk_instruments:
            snapshot = snapshots.get(instrument_id)
            momentum = None if snapshot is None else snapshot.momentum_for(self.momentum_days)
            if momentum is None:
                raise AgentPolicyError(
                    f"{instrument_id} lacks {self.momentum_days} sessions; refusing trend guess"
                )
            if Decimal(str(momentum)) > self.trend_threshold:
                selected.append(instrument_id)
        return tuple(selected)

    def decide_from_features(
        self,
        snapshots: Mapping[str, InstrumentSnapshot],
        returns: pd.DataFrame,
    ) -> PolicyDecision:
        defensive = snapshots.get(self.defensive_instrument)
        if defensive is None or defensive.last_close is None:
            raise AgentPolicyError("Defensive instrument lacks a trustworthy current price")
        selected = self._selected(snapshots)
        momentum = {
            key: str(Decimal(str(snapshots[key].momentum_for(self.momentum_days))))
            for key in self.risk_instruments
        }
        if not selected:
            return PolicyDecision(
                target_weights={self.defensive_instrument: Decimal("1")},
                reason=(
                    f"trend-volatility-target v{self.version}: no risk asset exceeds "
                    f"{self.trend_threshold}; defensive"
                ),
                audit={"momentum": momentum, "selected_instruments": []},
            )
        frame = returns.reindex(columns=selected).apply(pd.to_numeric, errors="coerce").dropna()
        if len(frame) != self.volatility_days:
            raise AgentPolicyError(
                f"Need {self.volatility_days} aligned selected-basket returns, have {len(frame)}"
            )
        basket = frame.mean(axis=1)
        realized = float(basket.std(ddof=0)) * sqrt(252)
        if realized <= 0:
            raise AgentPolicyError("Trend basket has non-positive realized volatility")
        exposure = min(
            self.max_risk_weight,
            self.target_volatility / Decimal(str(realized)),
        )
        per_asset = exposure / Decimal(len(selected))
        weights = {key: per_asset for key in selected}
        weights[self.defensive_instrument] = Decimal("1") - sum(weights.values())
        weights = dict(sorted(weights.items()))
        return PolicyDecision(
            target_weights=weights,
            reason=(
                f"trend-volatility-target v{self.version}: selected {list(selected)}, "
                f"realized volatility {realized:.6f}, risk exposure {exposure}; target "
                + " ".join(f"{key}={value:.6f}" for key, value in weights.items())
            ),
            audit={
                "momentum": momentum,
                "selected_instruments": list(selected),
                "realized_volatility": str(Decimal(str(realized))),
                "risk_exposure": str(exposure),
            },
        )


@dataclass(frozen=True)
class CrisisDrawdownPolicy:
    """Prospective, stateful ETF crisis entry with sparse decision changes."""

    risk_instruments: tuple[str, ...]
    defensive_instrument: str
    entry_mode: str
    drawdown_days: int
    event_lookback_days: int
    confirmation_days: int
    volatility_days: int
    minimum_drawdown: Decimal
    rebound_threshold: Decimal
    recovery_exit_gap: Decimal
    profit_take: Decimal
    max_positions: int
    entry_risk_weight: Decimal
    max_risk_weight: Decimal
    max_position_weight: Decimal
    position_caps: Mapping[str, Decimal]
    ladder_step: Decimal
    tranche_weight: Decimal
    target_volatility: Decimal | None
    cooldown_days: int
    rebalance_threshold: Decimal
    runtime: PortfolioPolicyRuntime | None = field(default=None, repr=False, compare=False)
    policy_id: str = "crisis-drawdown"
    version: str = "1"

    def __post_init__(self) -> None:
        risks = _instrument_ids(self.risk_instruments, "risk_instruments")
        defensive = self.defensive_instrument.strip()
        if not defensive or defensive in risks:
            raise AgentPolicyError(
                "defensive_instrument must be non-empty and outside risk_instruments"
            )
        if self.entry_mode not in {"reversal", "ladder"}:
            raise AgentPolicyError("entry_mode must be reversal or ladder")
        if self.drawdown_days < 20:
            raise AgentPolicyError("drawdown_days must be at least 20")
        if not 1 <= self.event_lookback_days <= self.drawdown_days:
            raise AgentPolicyError(
                "event_lookback_days must be in [1, drawdown_days]"
            )
        if self.confirmation_days < 2 or self.volatility_days < 2:
            raise AgentPolicyError("confirmation and volatility windows must be at least 2")
        if not 1 <= self.max_positions <= len(risks):
            raise AgentPolicyError("max_positions must fit inside risk_instruments")
        if self.cooldown_days < 0:
            raise AgentPolicyError("cooldown_days cannot be negative")

        decimal_fields = {
            "minimum_drawdown": self.minimum_drawdown,
            "rebound_threshold": self.rebound_threshold,
            "recovery_exit_gap": self.recovery_exit_gap,
            "profit_take": self.profit_take,
            "entry_risk_weight": self.entry_risk_weight,
            "max_risk_weight": self.max_risk_weight,
            "max_position_weight": self.max_position_weight,
            "ladder_step": self.ladder_step,
            "tranche_weight": self.tranche_weight,
            "rebalance_threshold": self.rebalance_threshold,
        }
        parsed = {
            key: _decimal(value, key) for key, value in decimal_fields.items()
        }
        if not Decimal("0") < parsed["minimum_drawdown"] < Decimal("1"):
            raise AgentPolicyError("minimum_drawdown must be in (0, 1)")
        if not Decimal("0") <= parsed["rebound_threshold"] < Decimal("1"):
            raise AgentPolicyError("rebound_threshold must be in [0, 1)")
        if not Decimal("0") <= parsed["recovery_exit_gap"] < Decimal("1"):
            raise AgentPolicyError("recovery_exit_gap must be in [0, 1)")
        if parsed["profit_take"] <= 0:
            raise AgentPolicyError("profit_take must be positive")
        for key in (
            "entry_risk_weight", "max_risk_weight", "max_position_weight",
            "ladder_step", "tranche_weight",
        ):
            if not Decimal("0") < parsed[key] <= Decimal("1"):
                raise AgentPolicyError(f"{key} must be in (0, 1]")
        if parsed["entry_risk_weight"] > parsed["max_risk_weight"]:
            raise AgentPolicyError("entry_risk_weight cannot exceed max_risk_weight")
        if not Decimal("0") < parsed["rebalance_threshold"] < Decimal("1"):
            raise AgentPolicyError("rebalance_threshold must be in (0, 1)")

        target = None
        if self.target_volatility is not None:
            target = _decimal(self.target_volatility, "target_volatility")
            if not Decimal("0") < target <= Decimal("1"):
                raise AgentPolicyError("target_volatility must be in (0, 1]")
        if not isinstance(self.position_caps, Mapping):
            raise AgentPolicyError("position_caps must be a mapping")
        caps: dict[str, Decimal] = {}
        for raw_instrument, raw_cap in self.position_caps.items():
            instrument_id = str(raw_instrument).strip()
            if instrument_id not in risks:
                raise AgentPolicyError(
                    f"position_caps contains undeclared risk instrument {instrument_id!r}"
                )
            cap = _decimal(raw_cap, f"position_caps[{instrument_id}]")
            if not Decimal("0") < cap <= parsed["max_position_weight"]:
                raise AgentPolicyError(
                    "position caps must be positive and no larger than max_position_weight"
                )
            caps[instrument_id] = cap

        object.__setattr__(self, "risk_instruments", risks)
        object.__setattr__(self, "defensive_instrument", defensive)
        for key, value in parsed.items():
            object.__setattr__(self, key, value)
        object.__setattr__(self, "target_volatility", target)
        object.__setattr__(self, "position_caps", dict(sorted(caps.items())))

    @property
    def config_hash(self) -> str:
        return stable_digest({
            "policy_id": self.policy_id,
            "version": self.version,
            "risk_instruments": self.risk_instruments,
            "defensive_instrument": self.defensive_instrument,
            "entry_mode": self.entry_mode,
            "drawdown_days": self.drawdown_days,
            "event_lookback_days": self.event_lookback_days,
            "confirmation_days": self.confirmation_days,
            "volatility_days": self.volatility_days,
            "minimum_drawdown": self.minimum_drawdown,
            "rebound_threshold": self.rebound_threshold,
            "recovery_exit_gap": self.recovery_exit_gap,
            "profit_take": self.profit_take,
            "max_positions": self.max_positions,
            "entry_risk_weight": self.entry_risk_weight,
            "max_risk_weight": self.max_risk_weight,
            "max_position_weight": self.max_position_weight,
            "position_caps": self.position_caps,
            "ladder_step": self.ladder_step,
            "tranche_weight": self.tranche_weight,
            "target_volatility": self.target_volatility,
            "cooldown_days": self.cooldown_days,
            "rebalance_threshold": self.rebalance_threshold,
            "cadence": "daily_state_change_only",
            "prospective_only": True,
            "premium_data_used": False,
        })

    def decide(self, market: CanonicalMarketData, as_of: date) -> PolicyDecision:
        if self.runtime is None:
            raise AgentPolicyError("crisis-drawdown requires current portfolio runtime")
        features = build_crisis_features(
            market,
            self.risk_instruments,
            as_of=as_of,
            drawdown_days=self.drawdown_days,
            event_lookback_days=self.event_lookback_days,
            confirmation_days=self.confirmation_days,
            volatility_days=self.volatility_days,
        )
        return self.decide_from_features(features, as_of=as_of)

    def decide_from_features(
        self,
        features: Mapping[str, CrisisInstrumentFeatures],
        *,
        as_of: date,
    ) -> PolicyDecision:
        """Pure signal/state seam; runtime supplies only the account state."""

        if self.runtime is None:
            raise AgentPolicyError("crisis-drawdown requires current portfolio runtime")
        checked = self._checked_features(features)
        state = self.runtime.state
        if state.pending_orders:
            return self._hold(
                checked,
                "pending orders keep the crisis state unchanged",
                action="pending_orders",
            )

        values, equity = self._position_values(state)
        held_risk = tuple(
            instrument_id for instrument_id in self.risk_instruments
            if state.quantity(instrument_id) > 0
        )
        risk_value = sum(values.get(item, Decimal("0")) for item in held_risk)
        current_risk_weight = (
            risk_value / equity if equity > 0 else Decimal("0")
        )
        current_risk_weights = {
            item: values.get(item, Decimal("0")) / equity for item in held_risk
        }

        if held_risk:
            cost = sum(
                lot.cost_amount for lot in state.lots
                if lot.instrument_id in held_risk
            )
            if cost <= 0 or risk_value <= 0:
                raise AgentPolicyError("held crisis ETF cost/value cannot be proven")
            holding_return = risk_value / cost - Decimal("1")
            recovery = sum(
                values[instrument_id]
                * (Decimal("1") + Decimal(str(
                    checked[instrument_id].current_drawdown
                )))
                for instrument_id in held_risk
            ) / risk_value
            if (
                holding_return >= self.profit_take
                or recovery >= Decimal("1") - self.recovery_exit_gap
            ):
                trigger = (
                    f"cost return {holding_return:.6f} reached {self.profit_take}"
                    if holding_return >= self.profit_take
                    else (
                        f"weighted peak proximity {recovery:.6f} reached "
                        f"{Decimal('1') - self.recovery_exit_gap}"
                    )
                )
                return self._target_defensive(
                    checked,
                    reason=f"crisis-drawdown v{self.version}: exit risk; {trigger}",
                    action="exit",
                    extra={
                        "holding_return": str(holding_return),
                        "weighted_peak_proximity": str(recovery),
                    },
                )
            if self.entry_mode == "ladder":
                return self._ladder_add(
                    checked,
                    held_risk=held_risk,
                    current_risk_weight=current_risk_weight,
                    current_risk_weights=current_risk_weights,
                    holding_return=holding_return,
                )
            return self._hold(
                checked,
                (
                    f"risk sleeve held; cost return {holding_return:.6f}, "
                    f"weighted peak proximity {recovery:.6f}"
                ),
                action="hold_risk",
                extra={
                    "holding_return": str(holding_return),
                    "weighted_peak_proximity": str(recovery),
                },
            )

        last_exit = self.runtime.last_risk_exit_date
        if (
            last_exit is not None
            and (as_of - last_exit).days < self.cooldown_days
        ):
            remaining = self.cooldown_days - (as_of - last_exit).days
            return self._defensive_or_hold(
                checked,
                reason=(
                    f"crisis-drawdown v{self.version}: post-exit cooldown; "
                    f"{remaining} calendar days remain"
                ),
                action="cooldown",
                extra={"last_risk_exit_date": last_exit.isoformat()},
            )

        candidates = self._entry_candidates(checked)
        if not candidates:
            return self._defensive_or_hold(
                checked,
                reason=f"crisis-drawdown v{self.version}: no crisis entry trigger",
                action="defensive",
            )
        selected = tuple(item.instrument_id for item in candidates[: self.max_positions])
        desired = (
            self._ladder_exposure(candidates[: self.max_positions])
            if self.entry_mode == "ladder"
            else self.entry_risk_weight
        )
        desired = self._volatility_scaled_exposure(desired, selected, checked)
        weights = self._risk_target(selected, desired)
        return PolicyDecision(
            target_weights=weights,
            reason=(
                f"crisis-drawdown v{self.version}: {self.entry_mode} entry "
                f"{list(selected)} at risk weight {sum(weights[item] for item in selected)}"
            ),
            audit=self._audit(
                checked,
                action="enter",
                extra={
                    "selected_instruments": list(selected),
                    "risk_weight": str(sum(weights[item] for item in selected)),
                },
            ),
        )

    def _checked_features(
        self, features: Mapping[str, CrisisInstrumentFeatures],
    ) -> dict[str, CrisisInstrumentFeatures]:
        checked: dict[str, CrisisInstrumentFeatures] = {}
        for instrument_id in self.risk_instruments:
            item = features.get(instrument_id)
            required = () if item is None else (
                item.current_close,
                item.current_drawdown,
                item.event_drawdown,
                item.rebound_from_low,
                item.recovery_ratio,
                item.above_confirmation_average,
                item.annualized_volatility,
            )
            if item is None or any(value is None for value in required):
                sessions = 0 if item is None else item.sessions
                raise AgentPolicyError(
                    f"{instrument_id} lacks declared crisis history (have {sessions} sessions); "
                    "refusing to shorten a window"
                )
            if item.current_close <= 0 or item.annualized_volatility < 0:
                raise AgentPolicyError(
                    f"{instrument_id} has invalid crisis price/volatility evidence"
                )
            checked[instrument_id] = item
        return checked

    def _entry_candidates(
        self, features: Mapping[str, CrisisInstrumentFeatures],
    ) -> list[CrisisInstrumentFeatures]:
        minimum = -self.minimum_drawdown
        if self.entry_mode == "ladder":
            eligible = [
                item for item in features.values()
                if Decimal(str(item.current_drawdown)) <= minimum
            ]
            return sorted(
                eligible,
                key=lambda item: (
                    item.current_drawdown,
                    item.annualized_volatility,
                    item.instrument_id,
                ),
            )
        eligible = [
            item for item in features.values()
            if Decimal(str(item.event_drawdown)) <= minimum
            and Decimal(str(item.rebound_from_low)) >= self.rebound_threshold
            and Decimal(str(item.current_drawdown)) < -self.recovery_exit_gap
            and item.above_confirmation_average
        ]
        return sorted(
            eligible,
            key=lambda item: (
                -item.rebound_from_low,
                item.event_drawdown,
                item.annualized_volatility,
                item.instrument_id,
            ),
        )

    def _ladder_add(
        self,
        features: Mapping[str, CrisisInstrumentFeatures],
        *,
        held_risk: tuple[str, ...],
        current_risk_weight: Decimal,
        current_risk_weights: Mapping[str, Decimal],
        holding_return: Decimal,
    ) -> PolicyDecision:
        triggered = [
            features[instrument_id] for instrument_id in held_risk
            if Decimal(str(features[instrument_id].current_drawdown))
            <= -self.minimum_drawdown
        ]
        if not triggered:
            return self._hold(
                features,
                "ladder position has not deepened enough for another tranche",
                action="hold_risk",
                extra={"holding_return": str(holding_return)},
            )
        desired = self._ladder_exposure(triggered)
        desired = self._volatility_scaled_exposure(desired, held_risk, features)
        if desired <= current_risk_weight + self.rebalance_threshold:
            return self._hold(
                features,
                (
                    f"ladder target {desired:.6f} does not exceed current risk "
                    f"{current_risk_weight:.6f} by {self.rebalance_threshold}"
                ),
                action="hold_risk",
                extra={
                    "holding_return": str(holding_return),
                    "current_risk_weight": str(current_risk_weight),
                    "ladder_target": str(desired),
                },
            )
        weights = self._additive_risk_target(
            held_risk, desired, current_risk_weights,
        )
        actual = sum(weights[item] for item in held_risk)
        if actual <= current_risk_weight + self.rebalance_threshold:
            return self._hold(
                features,
                "position caps leave no material room for another tranche",
                action="hold_risk",
            )
        return PolicyDecision(
            target_weights=weights,
            reason=(
                f"crisis-drawdown v{self.version}: deepen ladder for {list(held_risk)}; "
                f"risk weight {actual}"
            ),
            audit=self._audit(
                features,
                action="add_tranche",
                extra={
                    "holding_return": str(holding_return),
                    "current_risk_weight": str(current_risk_weight),
                    "risk_weight": str(actual),
                },
            ),
        )

    def _ladder_exposure(
        self, candidates: list[CrisisInstrumentFeatures] | tuple[CrisisInstrumentFeatures, ...],
    ) -> Decimal:
        severity = max(
            -Decimal(str(item.current_drawdown)) for item in candidates
        )
        levels = 1 + int(
            (severity - self.minimum_drawdown) / self.ladder_step
        )
        return min(self.max_risk_weight, self.tranche_weight * levels)

    def _volatility_scaled_exposure(
        self,
        desired: Decimal,
        selected: tuple[str, ...],
        features: Mapping[str, CrisisInstrumentFeatures],
    ) -> Decimal:
        desired = min(desired, self.max_risk_weight)
        if self.target_volatility is None:
            return desired
        average = sum(
            Decimal(str(features[item].annualized_volatility)) for item in selected
        ) / Decimal(len(selected))
        if average <= 0:
            raise AgentPolicyError("crisis volatility target needs positive realized volatility")
        return min(desired, self.target_volatility / average)

    def _risk_target(
        self, selected: tuple[str, ...], total_risk: Decimal,
    ) -> dict[str, Decimal]:
        caps = {
            item: self.position_caps.get(item, self.max_position_weight)
            for item in selected
        }
        remaining = min(total_risk, sum(caps.values()))
        active = list(selected)
        allocated: dict[str, Decimal] = {}
        while active:
            share = remaining / Decimal(len(active))
            capped = [item for item in active if caps[item] <= share]
            if capped:
                for item in capped:
                    allocated[item] = caps[item]
                    remaining -= caps[item]
                    active.remove(item)
                continue
            for item in active[:-1]:
                allocated[item] = share
                remaining -= share
            allocated[active[-1]] = remaining
            active.clear()
        defensive = Decimal("1") - sum(allocated.values())
        allocated[self.defensive_instrument] = defensive
        return dict(sorted(allocated.items()))

    def _additive_risk_target(
        self,
        selected: tuple[str, ...],
        total_risk: Decimal,
        current_weights: Mapping[str, Decimal],
    ) -> dict[str, Decimal]:
        """Add a ladder tranche without reducing any existing risk position."""

        allocated = {
            item: current_weights.get(item, Decimal("0")) for item in selected
        }
        caps = {
            item: max(
                allocated[item],
                self.position_caps.get(item, self.max_position_weight),
            )
            for item in selected
        }
        target_total = min(total_risk, sum(caps.values()))
        remaining = max(Decimal("0"), target_total - sum(allocated.values()))
        active = [item for item in selected if allocated[item] < caps[item]]
        while active and remaining > 0:
            share = remaining / Decimal(len(active))
            filled = [
                item for item in active
                if caps[item] - allocated[item] <= share
            ]
            if filled:
                for item in filled:
                    increment = caps[item] - allocated[item]
                    allocated[item] += increment
                    remaining -= increment
                    active.remove(item)
                continue
            for item in active[:-1]:
                allocated[item] += share
                remaining -= share
            allocated[active[-1]] += remaining
            remaining = Decimal("0")
        allocated[self.defensive_instrument] = Decimal("1") - sum(
            value for item, value in allocated.items()
            if item != self.defensive_instrument
        )
        return dict(sorted(allocated.items()))

    def _position_values(
        self, state: PortfolioState,
    ) -> tuple[dict[str, Decimal], Decimal]:
        values: dict[str, Decimal] = {}
        for lot in state.lots:
            price = state.last_prices.get(lot.instrument_id)
            if price is None:
                raise AgentPolicyError(
                    f"No current account price for held instrument {lot.instrument_id}"
                )
            values[lot.instrument_id] = (
                values.get(lot.instrument_id, Decimal("0"))
                + Decimal(lot.quantity) * price
            )
        equity = state.cash + sum(values.values())
        if equity <= 0:
            raise AgentPolicyError("crisis account equity must be positive")
        return values, equity

    def _defensive_or_hold(
        self,
        features: Mapping[str, CrisisInstrumentFeatures],
        *,
        reason: str,
        action: str,
        extra: Mapping[str, object] | None = None,
    ) -> PolicyDecision:
        assert self.runtime is not None
        held = {lot.instrument_id for lot in self.runtime.state.lots}
        if held and held <= {self.defensive_instrument}:
            return self._hold(features, reason, action=action, extra=extra)
        return self._target_defensive(
            features, reason=reason, action=action, extra=extra,
        )

    def _target_defensive(
        self,
        features: Mapping[str, CrisisInstrumentFeatures],
        *,
        reason: str,
        action: str,
        extra: Mapping[str, object] | None = None,
    ) -> PolicyDecision:
        return PolicyDecision(
            target_weights={self.defensive_instrument: Decimal("1")},
            reason=reason,
            audit=self._audit(features, action=action, extra=extra),
        )

    def _hold(
        self,
        features: Mapping[str, CrisisInstrumentFeatures],
        reason: str,
        *,
        action: str,
        extra: Mapping[str, object] | None = None,
    ) -> PolicyDecision:
        return PolicyDecision(
            target_weights={},
            reason=f"crisis-drawdown v{self.version}: hold; {reason}",
            hold=True,
            audit=self._audit(features, action=action, extra=extra),
        )

    @staticmethod
    def _feature_audit(item: CrisisInstrumentFeatures) -> dict[str, object]:
        return {
            "current_drawdown": str(item.current_drawdown),
            "event_drawdown": str(item.event_drawdown),
            "rebound_from_low": str(item.rebound_from_low),
            "recovery_ratio": str(item.recovery_ratio),
            "above_confirmation_average": item.above_confirmation_average,
            "annualized_volatility": str(item.annualized_volatility),
        }

    def _audit(
        self,
        features: Mapping[str, CrisisInstrumentFeatures],
        *,
        action: str,
        extra: Mapping[str, object] | None = None,
    ) -> dict[str, object]:
        audit: dict[str, object] = {
            "prospective_only": True,
            "cadence": "daily_state_change_only",
            "premium_data_used": False,
            "action": action,
            "signals": {
                item: self._feature_audit(features[item])
                for item in self.risk_instruments
            },
        }
        if extra:
            audit.update(extra)
        return audit


@dataclass(frozen=True)
class LowBetaVolatilityPolicy:
    """Monthly liquid non-ST stock basket ranked by beta and volatility."""

    benchmark_instrument: str
    defensive_instrument: str
    top_n: int
    beta_days: int
    volatility_days: int
    amount_window: int
    min_avg_amount: Decimal
    preselection_count: int
    minimum_listing_days: int
    min_beta: Decimal
    max_beta: Decimal
    defensive_weight: Decimal
    policy_id: str = "low-beta-volatility"
    version: str = "1"

    def __post_init__(self) -> None:
        benchmark = self.benchmark_instrument.strip()
        defensive = self.defensive_instrument.strip()
        if not benchmark or not defensive or benchmark == defensive:
            raise AgentPolicyError("benchmark and defensive instruments must be distinct")
        if self.top_n < 2 or self.beta_days < 20 or self.volatility_days < 20:
            raise AgentPolicyError("low-beta position count/windows are too small")
        if self.amount_window < 1 or self.preselection_count < self.top_n:
            raise AgentPolicyError("low-beta preselection must cover the target portfolio")
        if self.minimum_listing_days < 0:
            raise AgentPolicyError("low-beta liquidity/listing parameters are invalid")
        minimum_amount = _decimal(self.min_avg_amount, "min_avg_amount")
        minimum_beta = _decimal(self.min_beta, "min_beta")
        maximum_beta = _decimal(self.max_beta, "max_beta")
        defensive_weight = _decimal(self.defensive_weight, "defensive_weight")
        if minimum_amount < 0 or minimum_beta >= maximum_beta:
            raise AgentPolicyError("low-beta amount/beta bounds are invalid")
        if not Decimal("0") <= defensive_weight < Decimal("1"):
            raise AgentPolicyError("defensive_weight must be in [0, 1)")
        object.__setattr__(self, "benchmark_instrument", benchmark)
        object.__setattr__(self, "defensive_instrument", defensive)
        object.__setattr__(self, "min_avg_amount", minimum_amount)
        object.__setattr__(self, "min_beta", minimum_beta)
        object.__setattr__(self, "max_beta", maximum_beta)
        object.__setattr__(self, "defensive_weight", defensive_weight)

    @property
    def config_hash(self) -> str:
        return stable_digest({
            "policy_id": self.policy_id,
            "version": self.version,
            "benchmark_instrument": self.benchmark_instrument,
            "defensive_instrument": self.defensive_instrument,
            "top_n": self.top_n,
            "beta_days": self.beta_days,
            "volatility_days": self.volatility_days,
            "amount_window": self.amount_window,
            "min_avg_amount": self.min_avg_amount,
            "preselection_count": self.preselection_count,
            "minimum_listing_days": self.minimum_listing_days,
            "min_beta": self.min_beta,
            "max_beta": self.max_beta,
            "defensive_weight": self.defensive_weight,
            "cadence": "month_end",
            "prospective_only": True,
        })

    def decide(self, market: CanonicalMarketData, as_of: date) -> PolicyDecision:
        if not _last_session_of_month(market, as_of):
            return PolicyDecision(
                target_weights={},
                reason=f"low-beta-volatility: {as_of.isoformat()} is not month end",
                hold=True,
                audit={"cadence": "month_end", "prospective_only": True},
            )
        candidates = build_price_signal_candidates(
            market,
            as_of=as_of,
            mode="non_st",
            momentum_windows=(self.volatility_days,),
            volatility_days=self.volatility_days,
            amount_window=self.amount_window,
            min_avg_amount=float(self.min_avg_amount),
            minimum_listing_days=self.minimum_listing_days,
            beta_benchmark=self.benchmark_instrument,
            beta_days=self.beta_days,
            preselection_count=self.preselection_count,
        )
        return self.decide_from_candidates(candidates)

    def decide_from_candidates(
        self, candidates: tuple[PriceSignalCandidate, ...],
    ) -> PolicyDecision:
        eligible = [
            item for item in candidates
            if not item.current_is_st
            and item.beta is not None
            and item.volatility is not None
            and self.min_beta <= Decimal(str(item.beta)) <= self.max_beta
            and item.volatility > 0
        ]
        if len(eligible) < self.top_n:
            raise AgentPolicyError(
                f"Only {len(eligible)} eligible low-beta stocks, need {self.top_n}"
            )
        beta_rank = {
            item.instrument_id: rank
            for rank, item in enumerate(sorted(eligible, key=lambda item: (item.beta, item.instrument_id)))
        }
        volatility_rank = {
            item.instrument_id: rank
            for rank, item in enumerate(sorted(
                eligible, key=lambda item: (item.volatility, item.instrument_id),
            ))
        }
        selected = sorted(
            eligible,
            key=lambda item: (
                beta_rank[item.instrument_id] + volatility_rank[item.instrument_id],
                item.beta,
                item.volatility,
                -item.avg_amount,
                item.instrument_id,
            ),
        )[: self.top_n]
        stock_weight = (Decimal("1") - self.defensive_weight) / Decimal(self.top_n)
        weights = {item.instrument_id: stock_weight for item in selected}
        weights[self.defensive_instrument] = Decimal("1") - sum(weights.values())
        weights = dict(sorted(weights.items()))
        return PolicyDecision(
            target_weights=weights,
            reason=(
                f"low-beta-volatility v{self.version}: prospective month-end rank of "
                f"{len(eligible)} liquid non-ST stocks; selected "
                + " ".join(item.instrument_id for item in selected)
                + f"; defensive weight {self.defensive_weight}"
            ),
            audit={
                "prospective_only": True,
                "eligible_count": len(eligible),
                "selected": [
                    {
                        "instrument_id": item.instrument_id,
                        "beta": str(item.beta),
                        "volatility": str(item.volatility),
                        "average_amount": str(item.avg_amount),
                    }
                    for item in selected
                ],
            },
        )


@dataclass(frozen=True)
class StPriceMomentumPolicy:
    """Weekly, tightly capped prospective momentum around daily ST status."""

    mode: str
    defensive_instrument: str
    top_n: int
    short_momentum_days: int
    long_momentum_days: int
    amount_window: int
    min_avg_amount: Decimal
    minimum_listing_days: int
    max_recent_limit_hits: int
    stock_allocation: Decimal
    st_removal_lookback: int = 0
    require_above_long_average: bool = False
    policy_id: str = "st-price-momentum"
    version: str = "1"

    def __post_init__(self) -> None:
        if self.mode not in {"st", "recently_removed"}:
            raise AgentPolicyError("ST price strategy mode is invalid")
        defensive = self.defensive_instrument.strip()
        if not defensive or self.top_n < 1:
            raise AgentPolicyError("ST strategy needs a defensive instrument and positions")
        if self.short_momentum_days < 5 or self.long_momentum_days <= self.short_momentum_days:
            raise AgentPolicyError("ST momentum windows are invalid")
        if self.amount_window < 1 or self.minimum_listing_days < 0:
            raise AgentPolicyError("ST liquidity/listing parameters are invalid")
        if self.max_recent_limit_hits < 0:
            raise AgentPolicyError("max_recent_limit_hits cannot be negative")
        minimum_amount = _decimal(self.min_avg_amount, "min_avg_amount")
        allocation = _decimal(self.stock_allocation, "stock_allocation")
        if minimum_amount < 0 or not Decimal("0") < allocation < Decimal("1"):
            raise AgentPolicyError("ST amount/allocation parameters are invalid")
        if self.mode == "recently_removed" and self.st_removal_lookback < 1:
            raise AgentPolicyError("ST removal strategy needs a positive transition lookback")
        object.__setattr__(self, "defensive_instrument", defensive)
        object.__setattr__(self, "min_avg_amount", minimum_amount)
        object.__setattr__(self, "stock_allocation", allocation)

    @property
    def config_hash(self) -> str:
        return stable_digest({
            "policy_id": self.policy_id,
            "version": self.version,
            "mode": self.mode,
            "defensive_instrument": self.defensive_instrument,
            "top_n": self.top_n,
            "short_momentum_days": self.short_momentum_days,
            "long_momentum_days": self.long_momentum_days,
            "amount_window": self.amount_window,
            "min_avg_amount": self.min_avg_amount,
            "minimum_listing_days": self.minimum_listing_days,
            "max_recent_limit_hits": self.max_recent_limit_hits,
            "stock_allocation": self.stock_allocation,
            "st_removal_lookback": self.st_removal_lookback,
            "require_above_long_average": self.require_above_long_average,
            "cadence": "weekly_review",
            "prospective_only": True,
        })

    def decide(self, market: CanonicalMarketData, as_of: date) -> PolicyDecision:
        if not _last_session_of_week(market, as_of):
            return PolicyDecision(
                target_weights={},
                reason=f"{self.policy_id}: {as_of.isoformat()} is not this week's final session",
                hold=True,
                audit={"cadence": "weekly_review", "prospective_only": True},
            )
        candidates = build_price_signal_candidates(
            market,
            as_of=as_of,
            mode=self.mode,
            momentum_windows=(self.short_momentum_days, self.long_momentum_days),
            volatility_days=self.short_momentum_days,
            amount_window=self.amount_window,
            min_avg_amount=float(self.min_avg_amount),
            minimum_listing_days=self.minimum_listing_days,
            st_removal_lookback=self.st_removal_lookback,
            limit_lookback=self.amount_window,
        )
        return self.decide_from_candidates(candidates)

    def decide_from_candidates(
        self, candidates: tuple[PriceSignalCandidate, ...],
    ) -> PolicyDecision:
        scored: list[tuple[Decimal, PriceSignalCandidate]] = []
        for item in candidates:
            if self.mode == "st" and not item.current_is_st:
                continue
            if self.mode == "recently_removed" and (
                item.current_is_st or item.st_removed_on is None
            ):
                continue
            short = item.momentum_for(self.short_momentum_days)
            long = item.momentum_for(self.long_momentum_days)
            if short is None or long is None or short <= 0 or long <= 0:
                continue
            if item.recent_limit_hits > self.max_recent_limit_hits:
                continue
            if self.require_above_long_average and not item.above_long_average:
                continue
            scored.append(((Decimal(str(short)) + Decimal(str(long))) / 2, item))
        selected = [
            item for _, item in sorted(
                scored,
                key=lambda pair: (
                    -pair[0], pair[1].volatility or float("inf"),
                    -pair[1].avg_amount, pair[1].instrument_id,
                ),
            )[: self.top_n]
        ]
        per_stock = self.stock_allocation / Decimal(self.top_n)
        weights = {item.instrument_id: per_stock for item in selected}
        weights[self.defensive_instrument] = Decimal("1") - sum(weights.values())
        weights = dict(sorted(weights.items()))
        return PolicyDecision(
            target_weights=weights,
            reason=(
                f"{self.policy_id} v{self.version}: prospective weekly {self.mode} "
                f"positive {self.short_momentum_days}/{self.long_momentum_days}-session "
                "momentum with liquidity/limit filters; selected "
                + (" ".join(item.instrument_id for item in selected) or "none")
                + f"; defensive remainder {weights[self.defensive_instrument]}"
            ),
            audit={
                "prospective_only": True,
                "candidate_count": len(candidates),
                "eligible_count": len(scored),
                "selected_instruments": [item.instrument_id for item in selected],
            },
        )


@dataclass(frozen=True)
class DividendRulesPolicy:
    """Weekly deterministic dividend selection without an LLM adviser."""

    charter: Charter
    top_n: int
    min_yield: Decimal
    min_dividend_years: int
    min_avg_amount: Decimal
    max_fiscal_year_lag: int
    require_ttm_cash: bool
    cash_reserve: Decimal
    rebalance_cooldown_days: int
    runtime: PortfolioPolicyRuntime | None = field(default=None, repr=False, compare=False)
    policy_id: str = "dividend-rules"
    version: str = "1"

    def __post_init__(self) -> None:
        if self.charter.charter_id != "dividend-value":
            raise CharterError("dividend-rules must use the dividend-value charter")
        if set(map(str, self.charter.rule("asset_types", ()))) != {"stock"}:
            raise CharterError("dividend-rules charter must allow stocks only")
        if not bool(self.charter.rule("exclude_st", False)):
            raise CharterError("dividend-rules charter must exclude ST instruments")
        if self.min_dividend_years < self.charter.int_rule("min_dividend_years", 1):
            raise CharterError("dividend-rules years cross the charter floor")
        if self.min_yield < self.charter.decimal_rule("min_yield_floor", "0"):
            raise CharterError("dividend-rules yield crosses the charter floor")
        minimum = self.charter.int_rule("min_positions", 1)
        maximum = self.charter.int_rule("max_positions", 100)
        if not minimum <= self.top_n <= maximum:
            raise CharterError("dividend-rules top_n is outside the charter range")
        if self.min_avg_amount < 0:
            raise AgentPolicyError("min_avg_amount cannot be negative")
        if self.max_fiscal_year_lag < 0:
            raise AgentPolicyError("max_fiscal_year_lag cannot be negative")
        if not self.charter.decimal_rule(
            "min_cash_weight", "0"
        ) <= self.cash_reserve < Decimal("1"):
            raise CharterError("dividend-rules cash reserve crosses the charter floor")
        if self.rebalance_cooldown_days < 1:
            raise AgentPolicyError("rebalance_cooldown_days must be positive")
        max_single = self.charter.decimal_rule("max_single_weight", "1")
        if (Decimal("1") - self.cash_reserve) / self.top_n > max_single:
            raise CharterError("dividend-rules equal weight crosses the single-name cap")

    @property
    def config_hash(self) -> str:
        return stable_digest({
            "policy_id": self.policy_id,
            "version": self.version,
            "charter": self.charter.content_hash,
            "top_n": self.top_n,
            "min_yield": self.min_yield,
            "min_dividend_years": self.min_dividend_years,
            "min_avg_amount": self.min_avg_amount,
            "max_fiscal_year_lag": self.max_fiscal_year_lag,
            "require_ttm_cash": self.require_ttm_cash,
            "cash_reserve": self.cash_reserve,
            "rebalance_cooldown_days": self.rebalance_cooldown_days,
            "cadence": "weekly_review",
        })

    def decide(self, market: CanonicalMarketData, as_of: date) -> PolicyDecision:
        runtime = self.runtime
        if runtime is None:
            raise AgentPolicyError("dividend-rules needs its portfolio runtime")
        if not _last_session_of_week(market, as_of):
            return PolicyDecision(
                target_weights={},
                reason=f"dividend-rules: {as_of.isoformat()} is not this week's final session",
                hold=True,
                audit={"cadence": "weekly_review"},
            )
        measured = build_dividend_candidates(
            market,
            as_of=as_of,
            min_dividend_years=self.charter.int_rule("min_dividend_years", 1),
            min_avg_amount=float(self.min_avg_amount),
            exclude_st=True,
        )
        hard_floor = self.charter.decimal_rule("min_yield_floor", "0")
        hard_eligible = {
            item.instrument_id for item in measured
            if _primary_yield_decimal(item) >= hard_floor
        }
        eligible = [
            item for item in measured
            if item.dividend_years >= self.min_dividend_years
            and _primary_yield_decimal(item) >= self.min_yield
            and item.latest_fiscal_year >= as_of.year - self.max_fiscal_year_lag
            and (not self.require_ttm_cash or item.ttm_dividend > 0)
        ]
        eligible.sort(key=lambda item: (
            -item.sustainable_yield,
            item.payout_variability,
            -item.normalized_yield,
            -item.latest_fiscal_yield,
            -item.dividend_years,
            item.instrument_id,
        ))
        if len(eligible) < self.top_n:
            raise AgentPolicyError(
                f"Only {len(eligible)} eligible dividend candidates at {as_of.isoformat()}, "
                f"need {self.top_n}; refusing to weaken the rules"
            )
        held = {lot.instrument_id for lot in runtime.state.lots}
        pending = bool(runtime.state.pending_orders)
        cooldown_elapsed = (
            runtime.last_decision_date is None
            or (as_of - runtime.last_decision_date).days >= self.rebalance_cooldown_days
        )
        count_violation = bool(held) and not (
            self.charter.int_rule("min_positions", 1)
            <= len(held)
            <= self.charter.int_rule("max_positions", 100)
        )
        screen_violations = sorted(held - hard_eligible)
        forced = count_violation or bool(screen_violations)
        if pending or (held and not cooldown_elapsed and not forced):
            reason = (
                "dividend-rules: hold; "
                + ("pending orders" if pending else "28-day rebalance cooldown")
            )
            return PolicyDecision(
                target_weights={},
                reason=reason,
                hold=True,
                audit={
                    "cadence": "weekly_review",
                    "cooldown_elapsed": cooldown_elapsed,
                    "screen_violations": screen_violations,
                },
            )
        selected = eligible[: self.top_n]
        weight = ((Decimal("1") - self.cash_reserve) / self.top_n).quantize(
            Decimal("0.0001")
        )
        weights = {item.instrument_id: weight for item in selected}
        if sum(weights.values()) > Decimal("1") - self.charter.decimal_rule(
            "min_cash_weight", "0"
        ):
            raise AgentPolicyError("Rounded dividend-rules portfolio crosses the cash floor")
        reason = (
            f"dividend-rules v{self.version} charter v{self.charter.version}: weekly "
            "deterministic rank by current cash payment, fiscal recency, sustainable "
            f"yield/stability/liquidity; selected "
            f"{' '.join(weights)}; cash reserve {self.cash_reserve}"
        )
        return PolicyDecision(
            target_weights=weights,
            reason=reason,
            audit={
                "selected_instruments": list(weights),
                "cooldown_elapsed": cooldown_elapsed,
                "forced_rebalance": forced,
                "screen_violations": screen_violations,
                "selection_evidence": [item.evidence() for item in selected],
            },
        )


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
    benchmark_instrument: str
    benchmark_min_common_sessions: int
    runtime: DividendPolicyRuntime | None = field(default=None, repr=False, compare=False)
    policy_id: str = "dividend-value"
    version: str = "3"

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
        benchmark_instrument = self.benchmark_instrument.strip()
        if not benchmark_instrument:
            raise AgentPolicyError("benchmark_instrument cannot be empty")
        object.__setattr__(self, "benchmark_instrument", benchmark_instrument)
        if self.benchmark_min_common_sessions < 1:
            raise AgentPolicyError("benchmark_min_common_sessions must be positive")

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
            "benchmark_instrument": self.benchmark_instrument,
            "benchmark_min_common_sessions": self.benchmark_min_common_sessions,
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
            if _primary_yield_decimal(candidate) >= hard_yield_floor
        }
        eligible = [
            candidate for candidate in measured
            if candidate.dividend_years >= self.min_dividend_years
            and _primary_yield_decimal(candidate) >= self.min_yield
        ]
        eligible.sort(
            key=lambda item: (
                -item.sustainable_yield,
                item.payout_variability,
                -item.normalized_yield,
                -item.latest_fiscal_yield,
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
        benchmark = self._benchmark_evaluation(market, as_of)

        documents = runtime.library.context()
        memory = runtime.memory.recent(
            runtime.recent_memory_entries,
            max_entry_chars=runtime.max_memory_entry_chars,
            max_total_chars=runtime.max_memory_total_chars,
        )
        context: dict[str, object] = {
            "schema_version": "dividend-value-context-v3",
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
                "primary_yield_metric": "conservative_sustainable_yield",
                "primary_yield_definition": (
                    "minimum of latest completed fiscal-year yield and "
                    "normalized 3-year fiscal median yield"
                ),
                "minimum_conservative_sustainable_yield": str(self.min_yield),
                "minimum_consecutive_completed_fiscal_dividend_years": (
                    self.min_dividend_years
                ),
                "minimum_average_amount": str(self.min_avg_amount),
                "cash_reserve": str(self.cash_reserve),
                "opportunity_email_minimum_conservative_sustainable_yield": str(
                    self.alert_min_yield
                ),
                "ttm_cash_yield_role": (
                    "diagnostic actual cash return; not the eligibility or ranking yield"
                ),
                "rebalance_cooldown_days": self.rebalance_cooldown_days,
                "benchmark_policy": {
                    "instrument_id": self.benchmark_instrument,
                    "role": "evaluation_only",
                    "minimum_common_sessions_before_actionable": (
                        self.benchmark_min_common_sessions
                    ),
                    "short_horizon_behavior": "diagnostic_only",
                    "persistent_lag_must_not_be_sole_rebalance_reason": True,
                    "benchmark_never_triggers_automatic_rebalance": True,
                },
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
            "benchmark": benchmark.evidence(),
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
        current_holding_ids = {
            str(item["instrument_id"]) for item in portfolio["holdings"]
        }
        assessed_holding_ids = {
            item.instrument_id for item in advised.review.holding_assessments
        }
        if assessed_holding_ids != current_holding_ids:
            raise AgentPolicyError(
                "LLM holding assessments do not exactly cover the current portfolio"
            )
        (
            adviser_rebalance_allowed,
            ignored_action_reasons,
            action_guard_reason,
        ) = _review_action_guard(advised.review, benchmark)

        opportunity_ids: set[str] = set()
        highlights: list[Highlight] = []
        if benchmark.status == "unavailable":
            benchmark_risk = {
                "kind": "benchmark_data_risk",
                "instrument_id": benchmark.instrument_id,
                "reason": benchmark.reason,
            }
            highlights.append(Highlight(
                kind="risk",
                instrument_id=benchmark.instrument_id,
                name=benchmark.name,
                headline=f"红利价值基准数据不可用：{benchmark.instrument_id}",
                detail=str(benchmark.reason or "unknown benchmark data error"),
                evidence_hash=stable_digest(benchmark_risk),
            ))
        for opportunity in advised.review.opportunities:
            candidate = by_id.get(opportunity.instrument_id)
            if candidate is None:
                raise AgentPolicyError("LLM opportunity is outside the deterministic pool")
            if opportunity.instrument_id in opportunity_ids:
                raise AgentPolicyError("LLM returned a duplicate opportunity")
            opportunity_ids.add(opportunity.instrument_id)
            if _primary_yield_decimal(candidate) < self.alert_min_yield:
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
            adviser_rebalance_allowed and can_rebalance
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
        if advised.review.action == "rebalance" and not adviser_rebalance_allowed:
            disposition = f"hold (rebalance suppressed: {action_guard_reason})"
        elif advised.review.action == "rebalance" and not can_rebalance:
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
            "benchmark": benchmark.evidence(),
            "benchmark_assessment": advised.review.benchmark_assessment,
            "portfolio_assessment": advised.review.portfolio_assessment,
            "action_reasons": [
                {"category": item.category, "detail": item.detail}
                for item in advised.review.action_reasons
            ],
            "ignored_action_reasons": list(ignored_action_reasons),
            "action_guard_reason": action_guard_reason,
            "adviser_rebalance_allowed": adviser_rebalance_allowed,
            "holding_assessments": [
                {
                    "instrument_id": item.instrument_id,
                    "stance": item.stance,
                    "rationale": item.rationale,
                }
                for item in advised.review.holding_assessments
            ],
            "watch_items": list(advised.review.watch_items),
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

    def _benchmark_evaluation(
        self, market: CanonicalMarketData, as_of: date,
    ) -> BenchmarkEvaluation:
        assert self.runtime is not None
        previous_review = _last_completed_review_as_of(
            self.runtime.memory.entries(), before=as_of,
        )
        try:
            return evaluate_benchmark(
                market,
                instrument_id=self.benchmark_instrument,
                as_of=as_of,
                portfolio_points=self.runtime.performance_points,
                minimum_actionable_sessions=self.benchmark_min_common_sessions,
                previous_review_as_of=previous_review,
            )
        except Exception as exc:  # noqa: BLE001 - benchmark cannot block the weekly review
            try:
                name = market.instrument(self.benchmark_instrument).name
            except Exception:  # noqa: BLE001 - the reason below remains explicit
                name = self.benchmark_instrument
            return unavailable_benchmark(
                self.benchmark_instrument,
                name=str(name),
                as_of=as_of,
                minimum_actionable_sessions=self.benchmark_min_common_sessions,
                status="unavailable",
                reason=f"{type(exc).__name__}: {str(exc)[:500]}",
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


def _primary_yield_decimal(candidate: DividendCandidate) -> Decimal:
    return Decimal(f"{candidate.sustainable_yield:.8f}")


def _review_action_guard(
    review: DividendReview,
    benchmark: BenchmarkEvaluation,
) -> tuple[bool, tuple[str, ...], str]:
    if review.action != "rebalance":
        return False, (), "adviser chose hold"
    categories = {item.category for item in review.action_reasons}
    persistent_lag = "persistent_benchmark_lag"
    ignored: tuple[str, ...] = ()
    if persistent_lag in categories and not benchmark.can_support_rebalance:
        ignored = (persistent_lag,)
    non_benchmark = categories - {persistent_lag}
    if not non_benchmark:
        return (
            False,
            ignored,
            "benchmark relative performance cannot be the sole rebalance reason",
        )
    return True, ignored, "independent non-benchmark evidence supports adviser action"


def _last_completed_review_as_of(
    entries: list[dict[str, object]], *, before: date,
) -> date | None:
    found: list[date] = []
    for entry in entries:
        if entry.get("event") != "review":
            continue
        raw = entry.get("as_of")
        if not isinstance(raw, str):
            continue
        try:
            parsed = date.fromisoformat(raw)
        except ValueError:
            continue
        if parsed < before:
            found.append(parsed)
    return max(found, default=None)


def _weekly_period(day: date) -> str:
    year, week, _ = day.isocalendar()
    return f"{year}-W{week:02d}"


def _last_session_of_week(market: CanonicalMarketData, day: date) -> bool:
    following = market.next_trading_day(day)
    return following is not None and following.isocalendar()[:2] != day.isocalendar()[:2]


def _last_session_of_month(market: CanonicalMarketData, day: date) -> bool:
    following = market.next_trading_day(day)
    return following is not None and (following.year, following.month) != (day.year, day.month)


_PARAM_WHITELIST: Mapping[str, frozenset[str]] = {
    "momentum-rotation": frozenset({
        "risk_instrument",
        "defensive_instrument",
        "momentum_days",
        "threshold",
        "risk_on",
        "risk_off",
    }),
    "dual-momentum": frozenset({
        "risk_instruments",
        "defensive_instrument",
        "short_momentum_days",
        "long_momentum_days",
        "select_count",
        "threshold",
    }),
    "inverse-volatility": frozenset({
        "instruments",
        "volatility_days",
        "max_weight",
    }),
    "correlation-risk-parity": frozenset({
        "instruments",
        "return_days",
        "covariance_shrinkage",
        "max_weight",
    }),
    "trend-volatility-target": frozenset({
        "risk_instruments",
        "defensive_instrument",
        "momentum_days",
        "volatility_days",
        "trend_threshold",
        "target_volatility",
        "max_risk_weight",
    }),
    "crisis-drawdown": frozenset({
        "risk_instruments",
        "defensive_instrument",
        "entry_mode",
        "drawdown_days",
        "event_lookback_days",
        "confirmation_days",
        "volatility_days",
        "minimum_drawdown",
        "rebound_threshold",
        "recovery_exit_gap",
        "profit_take",
        "max_positions",
        "entry_risk_weight",
        "max_risk_weight",
        "max_position_weight",
        "position_caps",
        "ladder_step",
        "tranche_weight",
        "target_volatility",
        "cooldown_days",
        "rebalance_threshold",
    }),
    "low-beta-volatility": frozenset({
        "benchmark_instrument",
        "defensive_instrument",
        "top_n",
        "beta_days",
        "volatility_days",
        "amount_window",
        "min_avg_amount",
        "preselection_count",
        "minimum_listing_days",
        "min_beta",
        "max_beta",
        "defensive_weight",
    }),
    "st-active-momentum": frozenset({
        "defensive_instrument",
        "top_n",
        "short_momentum_days",
        "long_momentum_days",
        "amount_window",
        "min_avg_amount",
        "minimum_listing_days",
        "max_recent_limit_hits",
        "stock_allocation",
    }),
    "st-removal-momentum": frozenset({
        "defensive_instrument",
        "top_n",
        "short_momentum_days",
        "long_momentum_days",
        "amount_window",
        "min_avg_amount",
        "minimum_listing_days",
        "max_recent_limit_hits",
        "stock_allocation",
        "st_removal_lookback",
        "require_above_long_average",
    }),
    "dividend-rules": frozenset({
        "charter",
        "top_n",
        "min_yield",
        "min_dividend_years",
        "min_avg_amount",
        "max_fiscal_year_lag",
        "require_ttm_cash",
        "cash_reserve",
        "rebalance_cooldown_days",
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
        "benchmark_instrument",
        "benchmark_min_common_sessions",
    }),
}


def build_policy(
    kind: str,
    params: Mapping[str, object],
    *,
    runtime: DividendPolicyRuntime | PortfolioPolicyRuntime | None = None,
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
        if kind == "dual-momentum":
            return DualMomentumPolicy(
                risk_instruments=_instrument_ids(
                    params["risk_instruments"], "risk_instruments", minimum=2,
                ),
                defensive_instrument=str(params["defensive_instrument"]),
                short_momentum_days=int(str(params.get("short_momentum_days", 60))),
                long_momentum_days=int(str(params.get("long_momentum_days", 120))),
                select_count=int(str(params.get("select_count", 2))),
                threshold=_decimal(params.get("threshold", "0"), "threshold"),
            )
        if kind == "inverse-volatility":
            return InverseVolatilityPolicy(
                instruments=_instrument_ids(params["instruments"], "instruments", minimum=2),
                volatility_days=int(str(params.get("volatility_days", 60))),
                max_weight=_decimal(params.get("max_weight", "0.60"), "max_weight"),
            )
        if kind == "correlation-risk-parity":
            return CorrelationRiskParityPolicy(
                instruments=_instrument_ids(params["instruments"], "instruments", minimum=3),
                return_days=int(str(params.get("return_days", 120))),
                covariance_shrinkage=_decimal(
                    params.get("covariance_shrinkage", "0.25"),
                    "covariance_shrinkage",
                ),
                max_weight=_decimal(params.get("max_weight", "0.50"), "max_weight"),
            )
        if kind == "trend-volatility-target":
            return TrendVolatilityTargetPolicy(
                risk_instruments=_instrument_ids(
                    params["risk_instruments"], "risk_instruments", minimum=2,
                ),
                defensive_instrument=str(params["defensive_instrument"]),
                momentum_days=int(str(params.get("momentum_days", 120))),
                volatility_days=int(str(params.get("volatility_days", 60))),
                trend_threshold=_decimal(
                    params.get("trend_threshold", "0"), "trend_threshold",
                ),
                target_volatility=_decimal(
                    params.get("target_volatility", "0.10"), "target_volatility",
                ),
                max_risk_weight=_decimal(
                    params.get("max_risk_weight", "0.90"), "max_risk_weight",
                ),
            )
        if kind == "crisis-drawdown":
            if runtime is not None and not isinstance(runtime, PortfolioPolicyRuntime):
                raise AgentPolicyError("crisis-drawdown received the wrong runtime type")
            target_raw = params.get("target_volatility")
            return CrisisDrawdownPolicy(
                risk_instruments=_instrument_ids(
                    params["risk_instruments"], "risk_instruments",
                ),
                defensive_instrument=str(
                    params.get("defensive_instrument", "511010.SH")
                ),
                entry_mode=str(params.get("entry_mode", "reversal")),
                drawdown_days=int(str(params.get("drawdown_days", 252))),
                event_lookback_days=int(str(
                    params.get("event_lookback_days", 60)
                )),
                confirmation_days=int(str(params.get("confirmation_days", 10))),
                volatility_days=int(str(params.get("volatility_days", 60))),
                minimum_drawdown=_decimal(
                    params.get("minimum_drawdown", "0.20"), "minimum_drawdown",
                ),
                rebound_threshold=_decimal(
                    params.get("rebound_threshold", "0.05"), "rebound_threshold",
                ),
                recovery_exit_gap=_decimal(
                    params.get("recovery_exit_gap", "0.05"), "recovery_exit_gap",
                ),
                profit_take=_decimal(
                    params.get("profit_take", "0.25"), "profit_take",
                ),
                max_positions=int(str(params.get("max_positions", 3))),
                entry_risk_weight=_decimal(
                    params.get("entry_risk_weight", "0.60"), "entry_risk_weight",
                ),
                max_risk_weight=_decimal(
                    params.get("max_risk_weight", "0.60"), "max_risk_weight",
                ),
                max_position_weight=_decimal(
                    params.get("max_position_weight", "0.25"),
                    "max_position_weight",
                ),
                position_caps=params.get("position_caps", {}),
                ladder_step=_decimal(
                    params.get("ladder_step", "0.08"), "ladder_step",
                ),
                tranche_weight=_decimal(
                    params.get("tranche_weight", "0.20"), "tranche_weight",
                ),
                target_volatility=(
                    None if target_raw is None
                    else _decimal(target_raw, "target_volatility")
                ),
                cooldown_days=int(str(params.get("cooldown_days", 120))),
                rebalance_threshold=_decimal(
                    params.get("rebalance_threshold", "0.05"),
                    "rebalance_threshold",
                ),
                runtime=runtime,
            )
        if kind == "low-beta-volatility":
            return LowBetaVolatilityPolicy(
                benchmark_instrument=str(params.get("benchmark_instrument", "510300.SH")),
                defensive_instrument=str(params.get("defensive_instrument", "511010.SH")),
                top_n=int(str(params.get("top_n", 20))),
                beta_days=int(str(params.get("beta_days", 120))),
                volatility_days=int(str(params.get("volatility_days", 60))),
                amount_window=int(str(params.get("amount_window", 20))),
                min_avg_amount=_decimal(
                    params.get("min_avg_amount", "50000000"), "min_avg_amount",
                ),
                preselection_count=int(str(params.get("preselection_count", 800))),
                minimum_listing_days=int(str(params.get("minimum_listing_days", 250))),
                min_beta=_decimal(params.get("min_beta", "-0.25"), "min_beta"),
                max_beta=_decimal(params.get("max_beta", "1"), "max_beta"),
                defensive_weight=_decimal(
                    params.get("defensive_weight", "0.10"), "defensive_weight",
                ),
            )
        if kind in {"st-active-momentum", "st-removal-momentum"}:
            removal = kind == "st-removal-momentum"
            return StPriceMomentumPolicy(
                mode="recently_removed" if removal else "st",
                defensive_instrument=str(params.get("defensive_instrument", "511010.SH")),
                top_n=int(str(params.get("top_n", 5))),
                short_momentum_days=int(str(
                    params.get("short_momentum_days", 20 if removal else 60)
                )),
                long_momentum_days=int(str(
                    params.get("long_momentum_days", 60 if removal else 120)
                )),
                amount_window=int(str(params.get("amount_window", 20))),
                min_avg_amount=_decimal(
                    params.get("min_avg_amount", "30000000"), "min_avg_amount",
                ),
                minimum_listing_days=int(str(params.get("minimum_listing_days", 250))),
                max_recent_limit_hits=int(str(
                    params.get("max_recent_limit_hits", 3 if removal else 5)
                )),
                stock_allocation=_decimal(
                    params.get("stock_allocation", "0.25" if removal else "0.15"),
                    "stock_allocation",
                ),
                st_removal_lookback=(
                    int(str(params.get("st_removal_lookback", 60))) if removal else 0
                ),
                require_above_long_average=(
                    _boolean(
                        params.get("require_above_long_average", True),
                        "require_above_long_average",
                    )
                    if removal else False
                ),
                policy_id=kind,
            )
        if kind == "dividend-rules":
            if runtime is not None and not isinstance(runtime, PortfolioPolicyRuntime):
                raise AgentPolicyError("dividend-rules received the wrong runtime type")
            return DividendRulesPolicy(
                charter=load_charter(str(params["charter"])),
                top_n=int(str(params.get("top_n", 10))),
                min_yield=_decimal(params.get("min_yield", "0.04"), "min_yield"),
                min_dividend_years=int(str(params.get("min_dividend_years", 5))),
                min_avg_amount=_decimal(
                    params.get("min_avg_amount", "20000000"), "min_avg_amount",
                ),
                max_fiscal_year_lag=int(str(params.get("max_fiscal_year_lag", 2))),
                require_ttm_cash=_boolean(
                    params.get("require_ttm_cash", True), "require_ttm_cash",
                ),
                cash_reserve=_decimal(params.get("cash_reserve", "0.05"), "cash_reserve"),
                rebalance_cooldown_days=int(str(
                    params.get("rebalance_cooldown_days", 28)
                )),
                runtime=runtime,
            )
        if runtime is not None and not isinstance(runtime, DividendPolicyRuntime):
            raise AgentPolicyError("dividend-value received the wrong runtime type")
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
            benchmark_instrument=str(
                params.get("benchmark_instrument", "159207.SZ")
            ),
            benchmark_min_common_sessions=int(str(
                params.get("benchmark_min_common_sessions", 60)
            )),
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
