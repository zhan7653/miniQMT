from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from types import SimpleNamespace

import pandas as pd
import pytest

from fundlab.agent import AgentPolicyError, PortfolioPolicyRuntime, build_policy
from fundlab.marketdata.portal import PointInTimeMarketView
from fundlab.pipeline.daily import DailyPipeline
from fundlab.strategies.moving_average_grid import (
    MovingAverageGridConfig,
    MovingAverageGridEngine,
    MovingAverageGridSource,
    moving_average_grid_config,
)
from fundlab.trading import PortfolioState, SimulationService, TradingRepository
from tests.canonical.fixtures import DAYS, ready_market
from tests.canonical.test_trading_kernel import fees, policies


START = date(2026, 1, 1)


def grid_config(**overrides) -> MovingAverageGridConfig:
    params = {
        "instrument": "510050.SH",
        "activation_date": START + timedelta(days=6),
        "max_weight": Decimal("0.80"),
        "minimum_grid_step": Decimal("0.02"),
        "moving_average_days": 3,
        "trend_average_days": 5,
        "trend_slope_days": 1,
        "residual_window_days": 3,
        "defensive_confirm_days": 3,
        "defensive_brake_days": 5,
        "reset_confirm_days": 2,
        "maximum_cycle_days": 20,
        "startup_ramp_days": 2,
    }
    params.update(overrides)
    return MovingAverageGridConfig(**params)


def feed(engine: MovingAverageGridEngine, prices: list[float]):
    found = []
    for index, price in enumerate(prices):
        found.append(engine.feed(START + timedelta(days=index), price))
    return found


def test_ma_grid_ramps_then_buys_and_sells_more_at_farther_frozen_tiers():
    engine = MovingAverageGridEngine(grid_config())

    evaluations = feed(
        engine,
        [100] * 8 + [96, 94, 92, 100, 103, 106, 110],
    )

    assert evaluations[6].status == "startup_ramp"
    assert evaluations[6].target_weight == Decimal("0.2000")
    assert evaluations[7].target_weight == Decimal("0.4000")
    anchor = evaluations[8].anchor
    assert anchor == pytest.approx(98.6666666667)
    assert evaluations[8].tier == -1
    assert evaluations[8].target_fraction == Decimal("0.5925")
    assert evaluations[9].tier == -2
    assert evaluations[9].target_fraction == Decimal("0.7000")
    assert evaluations[10].tier == -3
    assert evaluations[10].target_fraction == Decimal("0.7000")
    assert evaluations[10].regime == "defensive"
    assert evaluations[10].paused is True
    assert evaluations[11].anchor == pytest.approx(anchor)
    assert evaluations[12].tier == 2
    assert evaluations[12].target_fraction == Decimal("0.2900")
    assert evaluations[13].tier == 3
    assert evaluations[13].target_fraction == Decimal("0.1475")
    assert evaluations[14].target_weight == Decimal("0.0000")


def test_ma_grid_rescales_frozen_anchor_without_changing_scale_free_decision():
    original = MovingAverageGridEngine(grid_config())
    adjusted = MovingAverageGridEngine(grid_config())
    prefix = [100] * 8 + [96, 94]
    feed(original, prefix)
    feed(adjusted, prefix)

    adjusted.rescale_history(Decimal("0.5"))
    original_result = original.feed(START + timedelta(days=len(prefix)), 92)
    adjusted_result = adjusted.feed(START + timedelta(days=len(prefix)), 46)

    assert original_result is not None and adjusted_result is not None
    assert adjusted_result.anchor == pytest.approx(original_result.anchor * 0.5)
    assert adjusted_result.grid_step == pytest.approx(original_result.grid_step)
    assert adjusted_result.tier == original_result.tier
    assert adjusted_result.target_weight == original_result.target_weight
    assert adjusted_result.regime == original_result.regime


def test_optional_upward_ratchet_rearms_grid_without_lowering_downside_anchor():
    frozen = MovingAverageGridEngine(grid_config())
    ratcheted = MovingAverageGridEngine(grid_config(ratchet_anchor_upward=True))
    prices = [100] * 8 + [96, 100, 103, 106, 110, 112, 114, 115]

    frozen_evaluations = feed(frozen, prices)
    ratcheted_evaluations = feed(ratcheted, prices)

    initial_anchor = frozen_evaluations[8].anchor
    assert ratcheted_evaluations[8].anchor == pytest.approx(initial_anchor)
    assert frozen_evaluations[-1].anchor == pytest.approx(initial_anchor)
    assert ratcheted_evaluations[-1].anchor > initial_anchor
    assert frozen_evaluations[-1].target_fraction == Decimal("0.0000")
    assert ratcheted_evaluations[-1].target_fraction == Decimal("0.5000")

    rolling = MovingAverageGridEngine(grid_config(rolling_anchor=True))
    rolling_evaluations = feed(rolling, [100] * 8 + [96, 94, 92])
    assert rolling_evaluations[9].anchor < rolling_evaluations[8].anchor
    with pytest.raises(ValueError, match="mutually exclusive"):
        grid_config(ratchet_anchor_upward=True, rolling_anchor=True)


def test_ma_grid_hard_brake_reduces_downside_inventory_toward_neutral():
    engine = MovingAverageGridEngine(grid_config(
        defensive_confirm_days=2,
        defensive_brake_days=3,
        minimum_grid_step=Decimal("0.03"),
    ))
    evaluations = feed(engine, [100] * 8 + [96, 93, 90, 88, 86])

    before_brake = evaluations[9]
    hard_brake = next(item for item in evaluations if item and item.hard_braked)
    assert before_brake.target_fraction >= Decimal("0.5000")
    assert hard_brake.status == "hard_brake"
    assert hard_brake.target_fraction <= before_brake.target_fraction
    assert hard_brake.paused is True


def test_persistent_defense_brakes_before_the_fifth_downside_tier():
    engine = MovingAverageGridEngine(grid_config(
        defensive_confirm_days=2,
        defensive_brake_days=3,
        minimum_grid_step=Decimal("0.03"),
    ))

    evaluations = feed(engine, [100] * 8 + [94, 94, 94, 94])
    hard_brake = next(item for item in evaluations if item and item.hard_braked)

    assert hard_brake.defensive_streak == 3
    assert hard_brake.tier == -1
    assert hard_brake.target_fraction == Decimal("0.5617")
    assert hard_brake.status == "hard_brake"


def test_ma_grid_config_fails_closed_on_unsafe_boundaries_and_unknown_dates():
    with pytest.raises(ValueError, match="below trend_average_days"):
        grid_config(moving_average_days=5, trend_average_days=5)
    with pytest.raises(ValueError, match="brake_tier"):
        grid_config(pause_tier=5, brake_tier=5)
    with pytest.raises(ValueError):
        moving_average_grid_config({
            "instrument": "510050.SH",
            "activation_date": "not-a-date",
            "max_weight": "0.8",
            "minimum_grid_step": "0.015",
        })
    with pytest.raises(ValueError, match="ratchet_anchor_upward"):
        moving_average_grid_config({
            "instrument": "510050.SH",
            "activation_date": "2026-01-01",
            "max_weight": "0.8",
            "minimum_grid_step": "0.015",
            "ratchet_anchor_upward": "true",
        })
    params = {
        "instrument": "510050.SH",
        "activation_date": "2026-01-01",
        "max_weight": "0.8",
        "minimum_grid_step": "0.015",
    }
    policy = build_policy(
        "moving-average-grid",
        params,
        runtime=PortfolioPolicyRuntime(
            "paper-grid", PortfolioState.with_cash(1_000_000), None
        ),
    )
    assert policy.config.instrument == "510050.SH"
    with pytest.raises(AgentPolicyError, match="Unknown moving-average-grid params"):
        build_policy("moving-average-grid", {**params, "grid_steps": 4})


@pytest.mark.parametrize("preload", [False, True])
def test_historical_source_emits_only_changed_targets_through_portfolio_intent(preload):
    prices = [100] * 8 + [96, 94, 92, 100, 103, 103]
    rows = pd.DataFrame([
        {
            "instrument_id": "510050.SH",
            "session_date": (START + timedelta(days=index)).isoformat(),
            "close": price,
        }
        for index, price in enumerate(prices)
    ])

    class FakeMarket:
        snapshot_id = "snapshot-grid"

        def instrument(self, instrument_id):
            assert instrument_id == "510050.SH"
            return SimpleNamespace(listed_date=START)

        def adjusted_history(self, instrument_ids, start_date, end_date, *, as_of):
            assert tuple(instrument_ids) == ("510050.SH",)
            assert end_date <= as_of
            return rows[rows["session_date"].between(
                start_date.isoformat(), end_date.isoformat()
            )].copy()

    source = MovingAverageGridSource(
        grid_config(),
        preload_end_date=(
            START + timedelta(days=len(prices) - 1) if preload else None
        ),
    )
    state = PortfolioState.with_cash(1_000_000)
    emitted = []
    for index in range(len(prices)):
        day = START + timedelta(days=index)
        intent = source.decide(
            account_id="research-grid",
            market=PointInTimeMarketView(FakeMarket(), day),
            state=state,
        )
        if intent is not None:
            emitted.append((day, intent.target_weights["510050.SH"]))

    assert emitted == [
        (START + timedelta(days=6), Decimal("0.2000")),
        (START + timedelta(days=7), Decimal("0.4000")),
        (START + timedelta(days=8), Decimal("0.4740")),
        (START + timedelta(days=9), Decimal("0.5600")),
        (START + timedelta(days=11), Decimal("0.4000")),
        (START + timedelta(days=12), Decimal("0.2320")),
    ]

    restarted_daily_source = MovingAverageGridSource(
        grid_config(),
        initial_emitted_weight=Decimal("0.2320"),
    )
    assert restarted_daily_source.decide(
        account_id="research-grid",
        market=PointInTimeMarketView(
            FakeMarket(), START + timedelta(days=len(prices) - 1)
        ),
        state=state,
    ) is None


def test_restarted_daily_source_matches_historical_clock_exactly(tmp_path):
    market = ready_market(tmp_path / "market")
    repository = TradingRepository(tmp_path / "trading.sqlite3")
    repository.create_account(
        "paper-grid-clock",
        "Paper Grid Clock",
        PortfolioState.with_cash(100_000),
    )
    execution, risk = policies()
    service = SimulationService(
        market_data=market,
        repository=repository,
        execution_policy=execution,
        risk_policy=risk,
        fee_schedule=fees(),
    )
    config = MovingAverageGridConfig(
        instrument="600000.SH",
        activation_date=DAYS[2],
        max_weight=Decimal("0.80"),
        minimum_grid_step=Decimal("0.01"),
        moving_average_days=1,
        trend_average_days=2,
        trend_slope_days=1,
        residual_window_days=1,
        startup_ramp_days=1,
    )

    historical = service.run_historical(
        "paper-grid-clock",
        DAYS[0],
        DAYS[-1],
        MovingAverageGridSource(config),
    )
    daily = None
    for day in DAYS:
        _, parent_run_id = repository.selected_state("paper-grid-clock")
        seed = None
        if parent_run_id is not None:
            seed = DailyPipeline._last_moving_average_grid_weight(
                repository,
                parent_run_id,
                MovingAverageGridSource(config),
            )
        daily = service.run_daily(
            "paper-grid-clock",
            day,
            MovingAverageGridSource(config, initial_emitted_weight=seed),
        )

    assert daily is not None
    assert daily.final_state.state_hash == historical.final_state.state_hash
    assert daily.final_state.pending_orders == historical.final_state.pending_orders
    assert daily.final_state.lots == historical.final_state.lots


def test_daily_restart_replays_a_frozen_cycle_older_than_800_days():
    prices = [100] * 8 + [80] * 900 + [68]
    rows = pd.DataFrame([
        {
            "instrument_id": "510050.SH",
            "session_date": (START + timedelta(days=index)).isoformat(),
            "close": price,
        }
        for index, price in enumerate(prices)
    ])

    class LongCycleMarket:
        snapshot_id = "snapshot-long-cycle"

        def instrument(self, instrument_id):
            assert instrument_id == "510050.SH"
            return SimpleNamespace(listed_date=START)

        def adjusted_history(self, instrument_ids, start_date, end_date, *, as_of):
            assert tuple(instrument_ids) == ("510050.SH",)
            assert end_date <= as_of
            return rows[rows["session_date"].between(
                start_date.isoformat(), end_date.isoformat()
            )].copy()

    config = grid_config(
        minimum_grid_step=Decimal("0.10"),
        defensive_confirm_days=2,
        defensive_brake_days=4,
        maximum_cycle_days=2_000,
    )
    final_day = START + timedelta(days=len(prices) - 1)
    market = PointInTimeMarketView(LongCycleMarket(), final_day)
    unseeded = MovingAverageGridSource(config).decide(
        account_id="research-grid",
        market=market,
        state=PortfolioState.with_cash(1_000_000),
    )

    assert unseeded is not None
    assert unseeded.target_weights == {"510050.SH": Decimal("0.4000")}
    assert unseeded.metadata["hard_braked"] is True
    assert unseeded.metadata["anchor"] == pytest.approx(93.3333333333)
    restarted = MovingAverageGridSource(
        config,
        initial_emitted_weight=Decimal("0.4000"),
    )
    assert restarted.decide(
        account_id="research-grid",
        market=PointInTimeMarketView(LongCycleMarket(), final_day),
        state=PortfolioState.with_cash(1_000_000),
    ) is None
