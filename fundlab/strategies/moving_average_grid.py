from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal, ROUND_HALF_UP
from math import isclose, log, trunc
from statistics import fmean, pstdev
from typing import Mapping

import pandas as pd

from fundlab.common.canonical import stable_digest
from fundlab.marketdata.portal import CanonicalMarketData, PointInTimeMarketView
from fundlab.trading.intent import PortfolioIntent, decimal_value
from fundlab.trading.state import PortfolioState


WEIGHT_QUANTUM = Decimal("0.0001")
MOVING_AVERAGE_GRID_PARAMS = frozenset({
    "instrument",
    "activation_date",
    "max_weight",
    "minimum_grid_step",
    "moving_average_days",
    "trend_average_days",
    "trend_slope_days",
    "residual_window_days",
    "grid_step_multiplier",
    "neutral_weight_fraction",
    "linear_weight_step",
    "convex_weight_step",
    "cautious_weight_fraction",
    "defensive_confirm_days",
    "defensive_brake_days",
    "reset_confirm_days",
    "reset_band_fraction",
    "maximum_cycle_days",
    "pause_tier",
    "brake_tier",
    "startup_ramp_days",
    "ratchet_anchor_upward",
    "rolling_anchor",
})


@dataclass(frozen=True)
class MovingAverageGridConfig:
    instrument: str
    activation_date: date
    max_weight: Decimal
    minimum_grid_step: Decimal
    moving_average_days: int = 60
    trend_average_days: int = 120
    trend_slope_days: int = 20
    residual_window_days: int = 120
    grid_step_multiplier: Decimal = Decimal("0.50")
    neutral_weight_fraction: Decimal = Decimal("0.50")
    linear_weight_step: Decimal = Decimal("0.08")
    convex_weight_step: Decimal = Decimal("0.0125")
    cautious_weight_fraction: Decimal = Decimal("0.70")
    defensive_confirm_days: int = 5
    defensive_brake_days: int = 20
    reset_confirm_days: int = 5
    reset_band_fraction: Decimal = Decimal("0.25")
    maximum_cycle_days: int = 120
    pause_tier: int = 4
    brake_tier: int = 5
    startup_ramp_days: int = 3
    ratchet_anchor_upward: bool = False
    rolling_anchor: bool = False
    strategy_id: str = "moving-average-grid"
    strategy_version: str = "1"

    def __post_init__(self) -> None:
        instrument = str(self.instrument).strip()
        if not instrument:
            raise ValueError("instrument cannot be empty")
        decimal_fields = {
            "max_weight": self.max_weight,
            "minimum_grid_step": self.minimum_grid_step,
            "grid_step_multiplier": self.grid_step_multiplier,
            "neutral_weight_fraction": self.neutral_weight_fraction,
            "linear_weight_step": self.linear_weight_step,
            "convex_weight_step": self.convex_weight_step,
            "cautious_weight_fraction": self.cautious_weight_fraction,
            "reset_band_fraction": self.reset_band_fraction,
        }
        parsed = {key: decimal_value(value) for key, value in decimal_fields.items()}
        if not Decimal("0") < parsed["max_weight"] <= Decimal("1"):
            raise ValueError("max_weight must be in (0, 1]")
        if not Decimal("0") < parsed["minimum_grid_step"] < Decimal("1"):
            raise ValueError("minimum_grid_step must be in (0, 1)")
        if not Decimal("0") < parsed["grid_step_multiplier"] <= Decimal("2"):
            raise ValueError("grid_step_multiplier must be in (0, 2]")
        if not Decimal("0") < parsed["neutral_weight_fraction"] < Decimal("1"):
            raise ValueError("neutral_weight_fraction must be in (0, 1)")
        if parsed["linear_weight_step"] < 0 or parsed["convex_weight_step"] < 0:
            raise ValueError("weight-curve steps cannot be negative")
        if not (
            parsed["neutral_weight_fraction"]
            < parsed["cautious_weight_fraction"]
            < Decimal("1")
        ):
            raise ValueError(
                "cautious_weight_fraction must be above neutral and below one"
            )
        if not Decimal("0") < parsed["reset_band_fraction"] < Decimal("1"):
            raise ValueError("reset_band_fraction must be in (0, 1)")
        integer_fields = {
            "moving_average_days": self.moving_average_days,
            "trend_average_days": self.trend_average_days,
            "trend_slope_days": self.trend_slope_days,
            "residual_window_days": self.residual_window_days,
            "defensive_confirm_days": self.defensive_confirm_days,
            "defensive_brake_days": self.defensive_brake_days,
            "reset_confirm_days": self.reset_confirm_days,
            "maximum_cycle_days": self.maximum_cycle_days,
            "pause_tier": self.pause_tier,
            "brake_tier": self.brake_tier,
            "startup_ramp_days": self.startup_ramp_days,
        }
        if any(int(value) <= 0 for value in integer_fields.values()):
            raise ValueError("moving-average-grid integer parameters must be positive")
        if self.moving_average_days >= self.trend_average_days:
            raise ValueError("moving_average_days must be below trend_average_days")
        if self.defensive_confirm_days >= self.defensive_brake_days:
            raise ValueError("defensive brake must follow defensive confirmation")
        if self.pause_tier >= self.brake_tier:
            raise ValueError("brake_tier must be deeper than pause_tier")
        if not self.strategy_id.strip() or not self.strategy_version.strip():
            raise ValueError("strategy identity cannot be empty")
        if not isinstance(self.ratchet_anchor_upward, bool):
            raise ValueError("ratchet_anchor_upward must be true or false")
        if not isinstance(self.rolling_anchor, bool):
            raise ValueError("rolling_anchor must be true or false")
        if self.ratchet_anchor_upward and self.rolling_anchor:
            raise ValueError("ratchet_anchor_upward and rolling_anchor are mutually exclusive")
        object.__setattr__(self, "instrument", instrument)
        for key, value in parsed.items():
            object.__setattr__(self, key, value)

    @property
    def config_hash(self) -> str:
        return stable_digest(self)

    @property
    def warmup_sessions(self) -> int:
        return max(
            self.trend_average_days + self.trend_slope_days,
            self.moving_average_days + self.residual_window_days - 1,
        )


@dataclass(frozen=True)
class GridFeature:
    session_date: date
    close: float
    moving_average: float
    trend_average: float
    trend_slope: float
    residual_std: float
    live_grid_step: float
    below_trend: bool
    falling_trend: bool
    defensive_streak: int


@dataclass(frozen=True)
class MovingAverageGridEvaluation:
    as_of: date
    instrument: str
    status: str
    regime: str
    target_weight: Decimal
    target_fraction: Decimal
    last_change_date: date | None
    anchor: float | None
    grid_step: float | None
    tier: int | None
    cycle_age: int
    defensive_streak: int
    paused: bool
    hard_braked: bool
    reason: str

    @property
    def evidence_hash(self) -> str:
        return stable_digest(self)

    def audit(self) -> dict[str, object]:
        return {
            "status": self.status,
            "regime": self.regime,
            "target_weight": str(self.target_weight),
            "target_fraction": str(self.target_fraction),
            "last_change_date": (
                None if self.last_change_date is None else self.last_change_date.isoformat()
            ),
            "anchor": self.anchor,
            "grid_step": self.grid_step,
            "tier": self.tier,
            "cycle_age": self.cycle_age,
            "defensive_streak": self.defensive_streak,
            "paused": self.paused,
            "hard_braked": self.hard_braked,
            "evidence_hash": self.evidence_hash,
        }


@dataclass
class _CycleState:
    anchor: float
    grid_step: float
    started_on: date
    age: int = 0
    neutral_streak: int = 0
    launch_offset: float = 0.0
    hard_braked: bool = False
    brake_start_fraction: float | None = None
    brake_progress: int = 0


class MovingAverageGridEngine:
    """One-instrument, long-only state machine shared by Agent and backtest paths."""

    def __init__(self, config: MovingAverageGridConfig) -> None:
        self.config = config
        self._closes: list[float] = []
        self._ma120_history: list[float] = []
        self._residuals: list[float] = []
        self._defensive_streak = 0
        self._startup_progress = 0
        self._startup_complete = False
        self._target_fraction = 0.0
        self._last_change_date: date | None = None
        self._cycle: _CycleState | None = None
        self._last_feature: GridFeature | None = None
        self._last_evaluation: MovingAverageGridEvaluation | None = None

    @property
    def last_evaluation(self) -> MovingAverageGridEvaluation | None:
        return self._last_evaluation

    def rescale_history(self, multiplier: float) -> None:
        """Apply a newly visible ratio adjustment without changing scale-free signals."""

        multiplier = float(multiplier)
        if not multiplier > 0:
            raise ValueError("history multiplier must be positive")
        if isclose(multiplier, 1.0, rel_tol=1e-12, abs_tol=1e-12):
            return
        self._closes = [value * multiplier for value in self._closes]
        self._ma120_history = [value * multiplier for value in self._ma120_history]
        if self._cycle is not None:
            self._cycle.anchor *= multiplier

    def feed(self, session_date: date, close: float) -> MovingAverageGridEvaluation | None:
        if not close > 0:
            raise ValueError("moving-average-grid close must be positive")
        if self._last_feature is not None and session_date <= self._last_feature.session_date:
            raise ValueError("moving-average-grid observations must be chronological")
        self._closes.append(float(close))
        maximum_prices = max(
            self.config.trend_average_days + self.config.trend_slope_days + 2,
            self.config.moving_average_days + self.config.residual_window_days + 2,
        )
        if len(self._closes) > maximum_prices:
            self._closes = self._closes[-maximum_prices:]

        if len(self._closes) < self.config.moving_average_days:
            return None
        moving_average = fmean(self._closes[-self.config.moving_average_days :])
        residual = log(close / moving_average)
        self._residuals.append(residual)
        if len(self._residuals) > self.config.residual_window_days:
            self._residuals = self._residuals[-self.config.residual_window_days :]

        if len(self._closes) < self.config.trend_average_days:
            return None
        trend_average = fmean(self._closes[-self.config.trend_average_days :])
        self._ma120_history.append(trend_average)
        if len(self._ma120_history) > self.config.trend_slope_days + 1:
            self._ma120_history = self._ma120_history[-(self.config.trend_slope_days + 1) :]
        if (
            len(self._residuals) < self.config.residual_window_days
            or len(self._ma120_history) <= self.config.trend_slope_days
        ):
            return None

        trend_slope = trend_average - self._ma120_history[0]
        residual_std = pstdev(self._residuals)
        live_grid_step = max(
            float(self.config.grid_step_multiplier) * residual_std,
            float(self.config.minimum_grid_step),
        )
        below_trend = close < trend_average
        falling_trend = trend_slope < 0
        self._defensive_streak = (
            self._defensive_streak + 1 if below_trend and falling_trend else 0
        )
        feature = GridFeature(
            session_date,
            float(close),
            moving_average,
            trend_average,
            trend_slope,
            residual_std,
            live_grid_step,
            below_trend,
            falling_trend,
            self._defensive_streak,
        )
        self._last_feature = feature
        if session_date < self.config.activation_date:
            return None
        self._last_evaluation = self._transition(feature)
        return self._last_evaluation

    def _transition(self, feature: GridFeature) -> MovingAverageGridEvaluation:
        defensive = feature.defensive_streak >= self.config.defensive_confirm_days
        cautious = feature.below_trend or feature.falling_trend
        regime = "defensive" if defensive else "cautious" if cautious else "normal"
        base = float(self.config.neutral_weight_fraction)

        if not self._startup_complete:
            if defensive:
                return self._evaluation(
                    feature,
                    status="startup_wait",
                    regime=regime,
                    tier=None,
                    paused=True,
                    reason="startup waits in cash while the long trend is defensive",
                )
            self._startup_progress += 1
            desired = base * min(
                1.0, self._startup_progress / self.config.startup_ramp_days
            )
            self._set_target(desired, feature.session_date)
            if self._startup_progress < self.config.startup_ramp_days:
                return self._evaluation(
                    feature,
                    status="startup_ramp",
                    regime=regime,
                    tier=0,
                    paused=False,
                    reason=(
                        f"startup ramp {self._startup_progress}/"
                        f"{self.config.startup_ramp_days}"
                    ),
                )
            self._startup_complete = True
            live_z = log(feature.close / feature.moving_average) / feature.live_grid_step
            if abs(live_z) >= 1:
                tier = _grid_tier(live_z, self.config.brake_tier)
                launch_offset = (
                    max(0.0, self._curve_fraction(tier) - base) if tier < 0 else 0.0
                )
                self._cycle = _CycleState(
                    feature.moving_average,
                    feature.live_grid_step,
                    feature.session_date,
                    launch_offset=launch_offset,
                )
            return self._evaluation(
                feature,
                status="idle" if self._cycle is None else "active",
                regime=regime,
                tier=(None if self._cycle is None else tier),
                paused=False,
                reason="startup base position established",
            )

        if self._cycle is None:
            live_z = log(feature.close / feature.moving_average) / feature.live_grid_step
            if abs(live_z) < 1:
                self._set_target(base, feature.session_date)
                return self._evaluation(
                    feature,
                    status="idle",
                    regime=regime,
                    tier=0,
                    paused=False,
                    reason="price remains inside the live neutral band",
                )
            self._cycle = _CycleState(
                feature.moving_average,
                feature.live_grid_step,
                feature.session_date,
            )

        cycle = self._cycle
        assert cycle is not None
        if self.config.rolling_anchor:
            cycle.anchor = feature.moving_average
            cycle.grid_step = feature.live_grid_step
        elif (
            self.config.ratchet_anchor_upward
            and not feature.falling_trend
            and feature.moving_average > cycle.anchor
        ):
            cycle.anchor = feature.moving_average
        cycle.age += 1
        z_score = log(feature.close / cycle.anchor) / cycle.grid_step
        tier = _grid_tier(z_score, self.config.brake_tier)
        raw_fraction = self._curve_fraction(tier)
        if cycle.launch_offset > 0 and tier < 0:
            raw_fraction = max(base, raw_fraction - cycle.launch_offset)

        stale_downside = cycle.age > self.config.maximum_cycle_days and z_score < 0
        beyond_pause = z_score < -float(self.config.pause_tier)
        hard_trigger = (
            z_score <= -float(self.config.brake_tier)
            or feature.defensive_streak >= self.config.defensive_brake_days
        )
        if hard_trigger and not cycle.hard_braked:
            cycle.hard_braked = True
            cycle.brake_start_fraction = self._target_fraction
            cycle.brake_progress = 0

        paused = stale_downside or beyond_pause or defensive or cycle.hard_braked
        desired = raw_fraction
        if cycle.hard_braked:
            start = cycle.brake_start_fraction
            assert start is not None
            cycle.brake_progress = min(3, cycle.brake_progress + 1)
            goal = min(start, base)
            brake_target = start - (start - goal) * cycle.brake_progress / 3
            desired = min(raw_fraction, brake_target, self._target_fraction)
        elif raw_fraction > self._target_fraction:
            if paused:
                desired = self._target_fraction
            elif cautious:
                desired = min(raw_fraction, float(self.config.cautious_weight_fraction))
        self._set_target(desired, feature.session_date)

        if (
            abs(z_score) <= float(self.config.reset_band_fraction)
            and isclose(self._target_fraction, base, rel_tol=0, abs_tol=1e-12)
        ):
            cycle.neutral_streak += 1
        else:
            cycle.neutral_streak = 0
        if cycle.neutral_streak >= self.config.reset_confirm_days:
            self._cycle = None
            return self._evaluation(
                feature,
                status="idle",
                regime=regime,
                tier=0,
                paused=False,
                reason="frozen cycle completed after a confirmed neutral reset",
            )

        status = "hard_brake" if cycle.hard_braked else "paused" if paused else "active"
        return self._evaluation(
            feature,
            status=status,
            regime=regime,
            tier=tier,
            paused=paused,
            reason=(
                f"frozen anchor {cycle.anchor:.6f}, step {cycle.grid_step:.6f}, "
                f"z {z_score:.4f}, tier {tier}"
            ),
        )

    def _curve_fraction(self, tier: int) -> float:
        bounded = max(-self.config.pause_tier, min(self.config.pause_tier, tier))
        value = (
            float(self.config.neutral_weight_fraction)
            - float(self.config.linear_weight_step) * bounded
            - float(self.config.convex_weight_step) * bounded * abs(bounded)
        )
        return max(0.0, min(1.0, value))

    def _set_target(self, fraction: float, changed_on: date) -> None:
        bounded = max(0.0, min(1.0, fraction))
        if not isclose(bounded, self._target_fraction, rel_tol=0, abs_tol=1e-12):
            self._target_fraction = bounded
            self._last_change_date = changed_on

    def _evaluation(
        self,
        feature: GridFeature,
        *,
        status: str,
        regime: str,
        tier: int | None,
        paused: bool,
        reason: str,
    ) -> MovingAverageGridEvaluation:
        cycle = self._cycle
        target_fraction = Decimal(str(self._target_fraction)).quantize(
            WEIGHT_QUANTUM, rounding=ROUND_HALF_UP
        )
        target_weight = (self.config.max_weight * target_fraction).quantize(
            WEIGHT_QUANTUM, rounding=ROUND_HALF_UP
        )
        return MovingAverageGridEvaluation(
            feature.session_date,
            self.config.instrument,
            status,
            regime,
            target_weight,
            target_fraction,
            self._last_change_date,
            None if cycle is None else cycle.anchor,
            None if cycle is None else cycle.grid_step,
            tier,
            0 if cycle is None else cycle.age,
            feature.defensive_streak,
            paused,
            False if cycle is None else cycle.hard_braked,
            reason,
        )


def evaluate_moving_average_grid(
    market: CanonicalMarketData,
    as_of: date,
    config: MovingAverageGridConfig,
) -> MovingAverageGridEvaluation | None:
    instrument = market.instrument(config.instrument)
    history_start = config.activation_date - timedelta(days=800)
    if instrument.listed_date is not None:
        history_start = max(history_start, instrument.listed_date)
    frame = market.adjusted_history(
        (config.instrument,), history_start, as_of, as_of=as_of
    )
    engine = MovingAverageGridEngine(config)
    for session_date, close in _usable_closes(frame):
        engine.feed(session_date, close)
    return engine.last_evaluation


class MovingAverageGridSource:
    """Point-in-time historical source for the same MA-grid state machine."""

    strategy_id = "moving-average-grid"
    strategy_version = "1"

    def __init__(
        self,
        config: MovingAverageGridConfig,
        *,
        preload_end_date: date | None = None,
        initial_emitted_weight: Decimal | None = None,
    ) -> None:
        self.config = config
        self._engine = MovingAverageGridEngine(config)
        self._preload_end_date = preload_end_date
        self._precomputed: dict[date, MovingAverageGridEvaluation | None] | None = None
        self._last_session: date | None = None
        self._last_price_date: date | None = None
        self._last_adjusted_close: float | None = None
        seeded_weight = (
            None if initial_emitted_weight is None else decimal_value(initial_emitted_weight)
        )
        if seeded_weight is not None and not Decimal("0") <= seeded_weight <= config.max_weight:
            raise ValueError("initial emitted MA-grid weight is outside the configured range")
        self._last_emitted_weight = seeded_weight

    @property
    def config_hash(self) -> str:
        return self.config.config_hash

    @property
    def market_scope(self) -> tuple[str, ...]:
        return (self.config.instrument,)

    def decide(
        self,
        *,
        account_id: str,
        market: PointInTimeMarketView,
        state: PortfolioState,
    ) -> PortfolioIntent | None:
        if self._last_session is not None and market.as_of <= self._last_session:
            raise ValueError("moving-average-grid source requires a chronological clock")
        self._last_session = market.as_of
        evaluation = self._advance(market)
        if evaluation is None:
            return None
        if any(
            order.instrument_id == self.config.instrument for order in state.pending_orders
        ):
            return None
        if evaluation.target_weight == self._last_emitted_weight:
            return None
        observation_hash = stable_digest(
            {
                "snapshot_id": market.snapshot_id,
                "as_of": market.as_of,
                "state_hash": state.state_hash,
                "evaluation_hash": evaluation.evidence_hash,
            }
        )
        intent = PortfolioIntent.create(
            account_id=account_id,
            decision_date=market.as_of,
            snapshot_id=market.snapshot_id,
            strategy_id=self.strategy_id,
            strategy_version=self.strategy_version,
            strategy_config_hash=self.config_hash,
            observation_hash=observation_hash,
            target_weights={self.config.instrument: evaluation.target_weight},
            reason=evaluation.reason,
            metadata=evaluation.audit(),
        )
        self._last_emitted_weight = evaluation.target_weight
        return intent

    def _advance(
        self, market: PointInTimeMarketView
    ) -> MovingAverageGridEvaluation | None:
        if self._preload_end_date is not None:
            return self._advance_precomputed(market)
        if self._last_price_date is None:
            instrument = market.market_data.instrument(self.config.instrument)
            # A frozen cycle can outlive any rolling lookback. Rebuild from the
            # strategy's activation warmup so a process restart restores the
            # complete state machine rather than silently inventing a new anchor.
            history_start = self.config.activation_date - timedelta(days=800)
            if instrument.listed_date is not None:
                history_start = max(history_start, instrument.listed_date)
            frame = market.adjusted_history(
                (self.config.instrument,), history_start, market.as_of
            )
            for session_date, close in _usable_closes(frame):
                self._engine.feed(session_date, close)
                self._last_price_date = session_date
                self._last_adjusted_close = close
            return self._engine.last_evaluation

        frame = market.adjusted_history(
            (self.config.instrument,), self._last_price_date, market.as_of
        )
        closes = _usable_closes(frame)
        previous = next(
            (close for day, close in closes if day == self._last_price_date), None
        )
        if previous is not None and self._last_adjusted_close is not None:
            multiplier = previous / self._last_adjusted_close
            self._engine.rescale_history(multiplier)
            self._last_adjusted_close = previous
        for session_date, close in closes:
            if session_date <= self._last_price_date:
                continue
            self._engine.feed(session_date, close)
            self._last_price_date = session_date
            self._last_adjusted_close = close
        return self._engine.last_evaluation

    def _advance_precomputed(
        self, market: PointInTimeMarketView
    ) -> MovingAverageGridEvaluation | None:
        if self._precomputed is None:
            if self._preload_end_date is None or self._preload_end_date < market.as_of:
                raise ValueError("moving-average-grid preload end precedes the simulation clock")
            instrument = market.market_data.instrument(self.config.instrument)
            history_start = self.config.activation_date - timedelta(days=800)
            if instrument.listed_date is not None:
                history_start = max(history_start, instrument.listed_date)
            # Every model input is invariant to a positive common price scale:
            # P/MA, residual dispersion, MA slope sign and log(P/frozen anchor).
            # Therefore an adjustment factor effective after an earlier signal
            # scales that signal's complete price window and frozen anchor without
            # changing its decision.  Loading the end-date adjusted series once is
            # economically identical to rescaling the live state on each event,
            # while never letting a future raw price enter an earlier evaluation.
            frame = market.market_data.adjusted_history(
                (self.config.instrument,),
                history_start,
                self._preload_end_date,
                as_of=self._preload_end_date,
            )
            engine = MovingAverageGridEngine(self.config)
            precomputed: dict[date, MovingAverageGridEvaluation | None] = {}
            for session_date, close in _usable_closes(frame):
                precomputed[session_date] = engine.feed(session_date, close)
            self._precomputed = precomputed
        return self._precomputed.get(market.as_of)


def moving_average_grid_config(
    params: Mapping[str, object], *, activation_date: date | None = None
) -> MovingAverageGridConfig:
    unknown = set(map(str, params)) - MOVING_AVERAGE_GRID_PARAMS
    if unknown:
        raise ValueError(
            f"Unknown moving-average-grid params {sorted(unknown)}; "
            f"allowed: {sorted(MOVING_AVERAGE_GRID_PARAMS)}"
        )
    configured_activation = activation_date or date.fromisoformat(
        str(params["activation_date"])
    )
    return MovingAverageGridConfig(
        instrument=str(params["instrument"]),
        activation_date=configured_activation,
        max_weight=decimal_value(params["max_weight"]),
        minimum_grid_step=decimal_value(params["minimum_grid_step"]),
        moving_average_days=int(str(params.get("moving_average_days", 60))),
        trend_average_days=int(str(params.get("trend_average_days", 120))),
        trend_slope_days=int(str(params.get("trend_slope_days", 20))),
        residual_window_days=int(str(params.get("residual_window_days", 120))),
        grid_step_multiplier=decimal_value(params.get("grid_step_multiplier", "0.50")),
        neutral_weight_fraction=decimal_value(
            params.get("neutral_weight_fraction", "0.50")
        ),
        linear_weight_step=decimal_value(params.get("linear_weight_step", "0.08")),
        convex_weight_step=decimal_value(
            params.get("convex_weight_step", "0.0125")
        ),
        cautious_weight_fraction=decimal_value(
            params.get("cautious_weight_fraction", "0.70")
        ),
        defensive_confirm_days=int(str(params.get("defensive_confirm_days", 5))),
        defensive_brake_days=int(str(params.get("defensive_brake_days", 20))),
        reset_confirm_days=int(str(params.get("reset_confirm_days", 5))),
        reset_band_fraction=decimal_value(
            params.get("reset_band_fraction", "0.25")
        ),
        maximum_cycle_days=int(str(params.get("maximum_cycle_days", 120))),
        pause_tier=int(str(params.get("pause_tier", 4))),
        brake_tier=int(str(params.get("brake_tier", 5))),
        startup_ramp_days=int(str(params.get("startup_ramp_days", 3))),
        ratchet_anchor_upward=_strict_bool(
            params.get("ratchet_anchor_upward", False), "ratchet_anchor_upward"
        ),
        rolling_anchor=_strict_bool(params.get("rolling_anchor", False), "rolling_anchor"),
    )


def _grid_tier(z_score: float, maximum: int) -> int:
    return max(-maximum, min(maximum, trunc(z_score)))


def _strict_bool(value: object, label: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{label} must be true or false")
    return value


def _usable_closes(frame: pd.DataFrame) -> list[tuple[date, float]]:
    if frame.empty:
        return []
    usable = frame.dropna(subset=["close"]).sort_values("session_date", kind="stable")
    found: list[tuple[date, float]] = []
    for row in usable.itertuples(index=False):
        session_date = date.fromisoformat(str(row.session_date))
        close = float(row.close)
        if close > 0:
            found.append((session_date, close))
    return found
