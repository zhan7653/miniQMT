"""The local decision agent: point-in-time features -> policy -> decision file.

End-to-end discipline under test: the service decides only for the next
session after the account head, pins features to the published data head, and
its only side effect is a decision file the kernel's own loader accepts.  A
policy that cannot decide writes nothing — the contract turns that silence
into a hold.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import date
from decimal import Decimal
from math import sqrt
from statistics import pstdev
from types import MappingProxyType

import pandas as pd
import pytest

import fundlab.agent.policy as policy_module
import fundlab.agent.features as features_module
import fundlab.agent.service as service_module
from fundlab.agent import (
    AgentDecisionService,
    AgentEvaluationError,
    AgentPolicyError,
    AgentServiceError,
    CrisisInstrumentFeatures,
    DividendCandidate,
    PriceSignalCandidate,
    PolicyDecision,
    PortfolioPolicyRuntime,
    build_policy,
    evaluation_root,
    list_agent_evaluations,
)
from fundlab.agent.features import (
    InstrumentSnapshot,
    build_crisis_features,
    build_instrument_snapshots,
)
from fundlab.marketdata import CanonicalMarketData
from fundlab.pipeline import DailyPipeline
from fundlab.settings import AgentPolicySettings, AgentSettings, DailyAccountSettings
from fundlab.strategies import AgentDecisionError, load_agent_decision, write_agent_decision
from fundlab.trading import PortfolioState, PositionLot

from tests.canonical.fixtures import DAYS, FUTURE_DAYS, ready_market
from tests.canonical.test_daily_pipeline import (
    build_settings,
    evening_of,
    registry_with_calendars,
)


def policy_settings(**overrides) -> AgentPolicySettings:
    params = {
        "risk_instrument": "600000.SH",
        "defensive_instrument": "600000.SH",
        "momentum_days": 2,
        "threshold": "0",
        "risk_on": {"600000.SH": "0.6"},
        "risk_off": {"600000.SH": "0.1"},
    }
    params.update(overrides)
    return AgentPolicySettings("paper-agent", "momentum-rotation", params)


def snapshot_with_momentum(momentum: dict[int, float]) -> InstrumentSnapshot:
    return InstrumentSnapshot(
        instrument_id="600000.SH",
        as_of=DAYS[-1],
        sessions=10,
        last_close=13.0,
        momentum=MappingProxyType(momentum),
    )


def feature_snapshot(
    instrument_id: str,
    *,
    momentum: dict[int, float] | None = None,
    volatility: dict[int, float] | None = None,
) -> InstrumentSnapshot:
    return InstrumentSnapshot(
        instrument_id=instrument_id,
        as_of=DAYS[-1],
        sessions=200,
        last_close=13.0,
        momentum=MappingProxyType(momentum or {}),
        volatility=MappingProxyType(volatility or {}),
    )


def crisis_feature(
    instrument_id: str,
    *,
    current_drawdown: float = -0.22,
    event_drawdown: float = -0.30,
    rebound_from_low: float = 0.10,
    recovery_ratio: float = 0.77,
    above_average: bool = True,
    volatility: float = 0.20,
) -> CrisisInstrumentFeatures:
    return CrisisInstrumentFeatures(
        instrument_id=instrument_id,
        as_of=DAYS[-1],
        sessions=300,
        current_close=77.0,
        current_drawdown=current_drawdown,
        event_drawdown=event_drawdown,
        rebound_from_low=rebound_from_low,
        recovery_ratio=recovery_ratio,
        above_confirmation_average=above_average,
        annualized_volatility=volatility,
        event_low_close=70.0,
        event_peak_close=100.0,
    )


def crisis_policy(runtime: PortfolioPolicyRuntime, **overrides):
    params = {
        "risk_instruments": ["A.SH", "B.SH"],
        "defensive_instrument": "D.SH",
        "entry_mode": "reversal",
        "drawdown_days": 252,
        "event_lookback_days": 60,
        "confirmation_days": 10,
        "volatility_days": 60,
        "minimum_drawdown": "0.20",
        "rebound_threshold": "0.05",
        "recovery_exit_gap": "0.05",
        "profit_take": "0.25",
        "max_positions": 2,
        "entry_risk_weight": "0.60",
        "max_risk_weight": "0.60",
        "max_position_weight": "0.40",
        "cooldown_days": 120,
        "rebalance_threshold": "0.05",
    }
    params.update(overrides)
    return build_policy("crisis-drawdown", params, runtime=runtime)


def agent_settings(tmp_path, *accounts, policies=None):
    settings = build_settings(tmp_path, accounts or (
        DailyAccountSettings("paper-agent", "Paper Agent", Decimal("100000"), "agent-file"),
    ))
    if policies is None:
        policies = {"paper-agent": policy_settings()}
    return replace(settings, agent=AgentSettings(policies=policies))


def advance_once(settings) -> None:
    result = DailyPipeline(
        settings,
        registry=registry_with_calendars(),
        now_fn=lambda: evening_of(DAYS[-1]),
    ).run()
    assert result.status in {"ok", "up_to_date"}, result


# ---------------------------------------------------------------- features


def test_features_compute_adjusted_momentum_as_of_boundary(tmp_path):
    ready_market(tmp_path / "market", close_values=(10.0, 11.0, 12.0, 13.0))
    market = CanonicalMarketData.open(tmp_path / "market")

    full = build_instrument_snapshots(
        market, ["600000.SH"], as_of=DAYS[-1], windows=(2,),
    )["600000.SH"]
    assert full.sessions == len(DAYS)
    assert full.last_close == pytest.approx(13.0)
    assert full.momentum_for(2) == pytest.approx(13.0 / 11.0 - 1.0)
    expected_returns = (12.0 / 11.0 - 1.0, 13.0 / 12.0 - 1.0)
    assert full.volatility_for(2) == pytest.approx(pstdev(expected_returns) * sqrt(252))

    # Two sessions of visible history cannot support a 2-session momentum:
    # the window must be absent, never extrapolated.
    early = build_instrument_snapshots(
        market, ["600000.SH"], as_of=DAYS[1], windows=(2,),
    )["600000.SH"]
    assert early.sessions == 2
    assert early.momentum_for(2) is None


def test_crisis_features_pin_drawdown_reversal_to_as_of(tmp_path):
    ready_market(tmp_path / "market", close_values=(100.0, 80.0, 70.0, 77.0))
    market = CanonicalMarketData.open(tmp_path / "market")

    full = build_crisis_features(
        market,
        ["600000.SH"],
        as_of=DAYS[-1],
        drawdown_days=4,
        event_lookback_days=3,
        confirmation_days=2,
        volatility_days=2,
    )["600000.SH"]

    assert full.current_drawdown == pytest.approx(-0.23)
    assert full.event_drawdown == pytest.approx(-0.30)
    assert full.rebound_from_low == pytest.approx(0.10)
    assert full.recovery_ratio == pytest.approx(0.77)
    assert full.above_confirmation_average is True

    early = build_crisis_features(
        market,
        ["600000.SH"],
        as_of=DAYS[2],
        drawdown_days=4,
        event_lookback_days=3,
        confirmation_days=2,
        volatility_days=2,
    )["600000.SH"]
    assert early.sessions == 3
    assert early.current_drawdown is None


def test_crisis_features_reuse_same_snapshot_close_series(tmp_path, monkeypatch):
    ready_market(tmp_path / "market", close_values=(100.0, 80.0, 70.0, 77.0))
    market = CanonicalMarketData.open(tmp_path / "market")
    features_module._CRISIS_CLOSE_CACHE.clear()
    original = CanonicalMarketData.adjusted_history
    calls = 0

    def counted(self, *args, **kwargs):
        nonlocal calls
        calls += 1
        return original(self, *args, **kwargs)

    monkeypatch.setattr(CanonicalMarketData, "adjusted_history", counted)
    options = {
        "as_of": DAYS[-1],
        "drawdown_days": 4,
        "event_lookback_days": 3,
        "confirmation_days": 2,
        "volatility_days": 2,
    }
    first = build_crisis_features(market, ["600000.SH"], **options)
    second = build_crisis_features(market, ["600000.SH"], **options)

    assert first == second
    assert calls == 1


# ------------------------------------------------------------------ policy


def test_momentum_policy_is_deterministic_and_config_bound():
    policy = build_policy("momentum-rotation", policy_settings().params)

    on = policy.decide_from_snapshots({
        "600000.SH": snapshot_with_momentum({2: 0.05}),
    })
    assert on.target_weights == {"600000.SH": Decimal("0.6")}
    assert "risk-on" in on.reason and "5.0000%" in on.reason

    off = policy.decide_from_snapshots({
        "600000.SH": snapshot_with_momentum({2: -0.05}),
    })
    assert off.target_weights == {"600000.SH": Decimal("0.1")}
    assert "risk-off" in off.reason

    reconfigured = build_policy(
        "momentum-rotation", policy_settings(momentum_days=3).params,
    )
    assert policy.config_hash != reconfigured.config_hash
    assert policy.config_hash == build_policy(
        "momentum-rotation", policy_settings().params,
    ).config_hash


def test_policy_fails_closed_without_enough_history():
    policy = build_policy("momentum-rotation", policy_settings().params)
    with pytest.raises(AgentPolicyError, match="refusing to guess"):
        policy.decide_from_snapshots({"600000.SH": snapshot_with_momentum({})})


def test_dual_momentum_selects_positive_relative_leaders_and_defensive_fallback():
    policy = build_policy("dual-momentum", {
        "risk_instruments": ["A.SH", "B.SH", "C.SH"],
        "defensive_instrument": "D.SH",
        "short_momentum_days": 60,
        "long_momentum_days": 120,
        "select_count": 2,
        "threshold": "0",
    })
    snapshots = {
        "A.SH": feature_snapshot("A.SH", momentum={60: 0.10, 120: 0.20}),
        "B.SH": feature_snapshot("B.SH", momentum={60: 0.20, 120: 0.20}),
        "C.SH": feature_snapshot("C.SH", momentum={60: -0.10, 120: 0.05}),
        "D.SH": feature_snapshot("D.SH"),
    }
    decision = policy.decide_from_snapshots(snapshots)
    assert decision.target_weights == {"A.SH": Decimal("0.5"), "B.SH": Decimal("0.5")}

    falling = {
        key: feature_snapshot(key, momentum={60: -0.10, 120: -0.20})
        for key in ("A.SH", "B.SH", "C.SH")
    }
    falling["D.SH"] = feature_snapshot("D.SH")
    defensive = policy.decide_from_snapshots(falling)
    assert defensive.target_weights == {"D.SH": Decimal("1")}


def sector_policy_params(**overrides):
    params = {
        "whitelist_version": "cn-sector-etf-v1",
        "sector_mapping": {
            "bank": "A.SH",
            "broker": "B.SH",
            "consumer": "C.SH",
            "health": "E.SH",
        },
        "defensive_instrument": "D.SH",
        "short_momentum_days": 20,
        "medium_momentum_days": 60,
        "long_momentum_days": 120,
        "short_weight": "0.20",
        "medium_weight": "0.30",
        "long_weight": "0.50",
        "volatility_days": 60,
        "select_count": 3,
        "risk_budget": "0.90",
        "max_sector_weight": "0.40",
    }
    params.update(overrides)
    return params


def sector_snapshot(
    instrument_id,
    short=0.10,
    medium=0.10,
    long=0.10,
    volatility=0.20,
):
    return feature_snapshot(
        instrument_id,
        momentum={20: short, 60: medium, 120: long},
        volatility={60: volatility},
    )


def test_sector_momentum_selects_top_three_caps_inverse_volatility_and_defends():
    policy = build_policy("sector-momentum", sector_policy_params())
    snapshots = {
        "A.SH": sector_snapshot(
            "A.SH",
            short=0.20,
            medium=0.20,
            long=0.20,
            volatility=0.10,
        ),
        "B.SH": sector_snapshot(
            "B.SH",
            short=0.15,
            medium=0.15,
            long=0.15,
            volatility=0.20,
        ),
        "C.SH": sector_snapshot("C.SH", volatility=0.40),
        "E.SH": sector_snapshot("E.SH", short=-0.1, medium=-0.1, long=-0.1),
        "D.SH": feature_snapshot("D.SH"),
    }
    decision = policy.decide_from_snapshots(snapshots)
    assert decision.audit["selected_sectors"] == ["bank", "broker", "consumer"]
    assert decision.target_weights["A.SH"] == Decimal("0.40")
    assert float(decision.target_weights["B.SH"]) == pytest.approx(1 / 3)
    assert float(decision.target_weights["C.SH"]) == pytest.approx(1 / 6)
    assert decision.target_weights["D.SH"] == Decimal("0.10")
    assert sum(decision.target_weights.values()) == Decimal("1")

    falling = {
        key: sector_snapshot(key, short=-0.1, medium=-0.05, long=-0.01)
        for key in ("A.SH", "B.SH", "C.SH", "E.SH")
    }
    falling["D.SH"] = feature_snapshot("D.SH")
    assert policy.decide_from_snapshots(falling).target_weights == {
        "D.SH": Decimal("1")
    }


def test_sector_momentum_requires_positive_long_and_composite_momentum():
    policy = build_policy("sector-momentum", sector_policy_params())
    snapshots = {
        # Positive composite, but a negative long signal.
        "A.SH": sector_snapshot("A.SH", short=0.50, medium=0.50, long=-0.01),
        # Positive long signal, but a negative composite.
        "B.SH": sector_snapshot("B.SH", short=-0.50, medium=-0.50, long=0.01),
        "C.SH": sector_snapshot("C.SH", short=0.02, medium=0.02, long=0.02),
        "E.SH": sector_snapshot("E.SH", short=-0.10, medium=-0.10, long=-0.10),
        "D.SH": feature_snapshot("D.SH"),
    }

    decision = policy.decide_from_snapshots(snapshots)

    assert decision.audit["selected_sectors"] == ["consumer"]
    assert Decimal(decision.audit["scores"]["bank"]) > 0
    assert Decimal(decision.audit["momentum"]["broker"]["120"]) > 0
    assert decision.target_weights == {
        "C.SH": Decimal("0.40"),
        "D.SH": Decimal("0.60"),
    }


def test_sector_momentum_fails_closed_and_validates_cadence_hash_and_params(
    monkeypatch,
):
    policy = build_policy("sector-momentum", sector_policy_params())
    with pytest.raises(AgentPolicyError, match="lacks 60 return observations"):
        policy.decide_from_snapshots({
            "A.SH": sector_snapshot("A.SH"),
            "B.SH": sector_snapshot("B.SH"),
            "C.SH": feature_snapshot(
                "C.SH",
                momentum={20: 0.1, 60: 0.1, 120: 0.1},
            ),
            "E.SH": sector_snapshot("E.SH"),
            "D.SH": feature_snapshot("D.SH"),
        })
    with pytest.raises(AgentPolicyError, match="duplicate ETF"):
        build_policy(
            "sector-momentum",
            sector_policy_params(
                sector_mapping={
                    "bank": "A.SH",
                    "broker": "A.SH",
                    "consumer": "C.SH",
                }
            ),
        )
    with pytest.raises(AgentPolicyError, match="defensive_instrument"):
        build_policy(
            "sector-momentum",
            sector_policy_params(defensive_instrument="A.SH"),
        )
    with pytest.raises(
        AgentPolicyError,
        match="weights must be positive and sum to 1",
    ):
        build_policy(
            "sector-momentum",
            sector_policy_params(short_weight="0"),
        )
    with pytest.raises(AgentPolicyError, match="Unknown sector-momentum params"):
        build_policy("sector-momentum", sector_policy_params(typo=True))
    assert policy.config_hash != build_policy(
        "sector-momentum",
        sector_policy_params(whitelist_version="v2"),
    ).config_hash
    assert policy.config_hash != build_policy(
        "sector-momentum",
        sector_policy_params(
            sector_mapping={
                "bank": "A.SH",
                "broker": "B.SH",
                "consumer": "C.SH",
                "health": "F.SH",
            }
        ),
    ).config_hash

    class NotMonthEnd:
        @staticmethod
        def next_trading_day(day):
            assert day == date(2026, 7, 30)
            return date(2026, 7, 31)

    monkeypatch.setattr(
        policy_module,
        "build_instrument_snapshots",
        lambda *args, **kwargs: pytest.fail("non-month-end must hold"),
    )
    assert policy.decide(NotMonthEnd(), date(2026, 7, 30)).hold

    custom = build_policy(
        "sector-momentum",
        sector_policy_params(
            short_momentum_days=21,
            medium_momentum_days=61,
            long_momentum_days=121,
            volatility_days=61,
        ),
    )
    custom_snapshots = {
        key: feature_snapshot(
            key,
            momentum={21: 0.1, 61: 0.1, 121: 0.1},
            volatility={61: 0.2},
        )
        for key in ("A.SH", "B.SH", "C.SH", "E.SH")
    }
    custom_snapshots["D.SH"] = feature_snapshot("D.SH")
    custom_decision = custom.decide_from_snapshots(custom_snapshots)
    assert set(custom_decision.audit["momentum"]["bank"]) == {"21", "61", "121"}
    assert "21/61/121 composite momentum" in custom_decision.reason


def test_inverse_volatility_caps_concentration_and_fails_without_history():
    policy = build_policy("inverse-volatility", {
        "instruments": ["A.SH", "B.SH", "C.SH"],
        "volatility_days": 60,
        "max_weight": "0.50",
    })
    decision = policy.decide_from_snapshots({
        "A.SH": feature_snapshot("A.SH", volatility={60: 0.10}),
        "B.SH": feature_snapshot("B.SH", volatility={60: 0.20}),
        "C.SH": feature_snapshot("C.SH", volatility={60: 0.40}),
    })
    assert decision.target_weights["A.SH"] == Decimal("0.50")
    assert float(decision.target_weights["B.SH"]) == pytest.approx(1 / 3)
    assert float(decision.target_weights["C.SH"]) == pytest.approx(1 / 6)
    assert sum(decision.target_weights.values()) == Decimal("1")

    with pytest.raises(AgentPolicyError, match="lacks 60 return observations"):
        policy.decide_from_snapshots({
            "A.SH": feature_snapshot("A.SH", volatility={60: 0.10}),
            "B.SH": feature_snapshot("B.SH"),
            "C.SH": feature_snapshot("C.SH", volatility={60: 0.40}),
        })


def test_correlation_risk_parity_uses_covariance_and_caps_weights():
    policy = build_policy("correlation-risk-parity", {
        "instruments": ["A.SH", "B.SH", "C.SH"],
        "return_days": 20,
        "covariance_shrinkage": "0.25",
        "max_weight": "0.50",
    })
    returns = pd.DataFrame({
        "A.SH": [0.010 if index % 2 else -0.008 for index in range(20)],
        "B.SH": [0.004 if index % 3 else -0.006 for index in range(20)],
        "C.SH": [0.015 if index % 5 else -0.020 for index in range(20)],
    })

    decision = policy.decide_from_returns(returns)

    assert sum(decision.target_weights.values()) == Decimal("1")
    assert max(decision.target_weights.values()) <= Decimal("0.50")
    assert set(decision.audit["risk_contributions"]) == {"A.SH", "B.SH", "C.SH"}


def test_trend_volatility_target_scales_positive_assets_and_falls_back():
    policy = build_policy("trend-volatility-target", {
        "risk_instruments": ["A.SH", "B.SH"],
        "defensive_instrument": "D.SH",
        "momentum_days": 60,
        "volatility_days": 20,
        "trend_threshold": "0",
        "target_volatility": "0.10",
        "max_risk_weight": "0.80",
    })
    snapshots = {
        "A.SH": feature_snapshot("A.SH", momentum={60: 0.10}),
        "B.SH": feature_snapshot("B.SH", momentum={60: -0.10}),
        "D.SH": feature_snapshot("D.SH"),
    }
    returns = pd.DataFrame({
        "A.SH": [0.01 if index % 2 else -0.01 for index in range(20)],
    })

    decision = policy.decide_from_features(snapshots, returns)

    assert Decimal("0") < decision.target_weights["A.SH"] <= Decimal("0.80")
    assert decision.target_weights["D.SH"] == Decimal("1") - decision.target_weights["A.SH"]

    snapshots["A.SH"] = feature_snapshot("A.SH", momentum={60: -0.01})
    defensive = policy.decide_from_features(snapshots, pd.DataFrame())
    assert defensive.target_weights == {"D.SH": Decimal("1")}


def test_crisis_reversal_enters_only_after_deep_confirmed_rebound():
    runtime = PortfolioPolicyRuntime(
        "paper-crisis", PortfolioState.with_cash(Decimal("1000000")), None,
    )
    policy = crisis_policy(runtime)
    features = {
        "A.SH": crisis_feature("A.SH"),
        "B.SH": crisis_feature(
            "B.SH", event_drawdown=-0.12, rebound_from_low=0.02,
        ),
    }

    decision = policy.decide_from_features(features, as_of=DAYS[-1])

    assert decision.target_weights == {
        "A.SH": Decimal("0.40"), "D.SH": Decimal("0.60"),
    }
    assert decision.audit["action"] == "enter"
    assert decision.audit["prospective_only"] is True
    assert decision.audit["premium_data_used"] is False

    no_reversal = {
        key: crisis_feature(
            key, event_drawdown=-0.30, rebound_from_low=0.02, above_average=False,
        )
        for key in ("A.SH", "B.SH")
    }
    defensive = policy.decide_from_features(no_reversal, as_of=DAYS[-1])
    assert defensive.target_weights == {"D.SH": Decimal("1")}


def test_crisis_uses_account_cost_for_profit_exit():
    state = PortfolioState(
        initial_cash=Decimal("1000000"),
        cash=Decimal("0"),
        lots=(PositionLot(
            "lot-a", "A.SH", 100, DAYS[0], DAYS[0], Decimal("1000"),
        ),),
        last_prices={"A.SH": Decimal("13")},
    )
    policy = crisis_policy(PortfolioPolicyRuntime("paper-crisis", state, None))

    decision = policy.decide_from_features({
        "A.SH": crisis_feature("A.SH", recovery_ratio=0.80),
        "B.SH": crisis_feature("B.SH", event_drawdown=-0.10),
    }, as_of=DAYS[-1])

    assert decision.target_weights == {"D.SH": Decimal("1")}
    assert decision.audit["action"] == "exit"
    assert Decimal(decision.audit["holding_return"]) == Decimal("0.3")


def test_crisis_ladder_adds_only_when_drawdown_deepens_materially():
    state = PortfolioState(
        initial_cash=Decimal("1000"),
        cash=Decimal("800"),
        lots=(PositionLot(
            "lot-a", "A.SH", 20, DAYS[0], DAYS[0], Decimal("200"),
        ),),
        last_prices={"A.SH": Decimal("10")},
    )
    policy = crisis_policy(
        PortfolioPolicyRuntime("paper-crisis", state, None),
        entry_mode="ladder",
        ladder_step="0.10",
        tranche_weight="0.20",
        max_position_weight="0.60",
    )
    features = {
        "A.SH": crisis_feature(
            "A.SH", current_drawdown=-0.31, recovery_ratio=0.70,
        ),
        "B.SH": crisis_feature("B.SH", current_drawdown=-0.10),
    }

    decision = policy.decide_from_features(features, as_of=DAYS[-1])

    assert decision.target_weights == {
        "A.SH": Decimal("0.40"), "D.SH": Decimal("0.60"),
    }
    assert decision.audit["action"] == "add_tranche"


def test_crisis_volatility_target_and_exit_cooldown_reduce_exposure():
    empty = PortfolioState.with_cash(Decimal("1000"))
    features = {
        "A.SH": crisis_feature("A.SH", volatility=0.50),
        "B.SH": crisis_feature("B.SH", event_drawdown=-0.10, volatility=0.50),
    }
    volatility_policy = crisis_policy(
        PortfolioPolicyRuntime("paper-crisis", empty, None),
        target_volatility="0.10",
    )
    sized = volatility_policy.decide_from_features(features, as_of=DAYS[-1])
    assert sized.target_weights == {
        "A.SH": Decimal("0.2"), "D.SH": Decimal("0.8"),
    }

    cooldown_policy = crisis_policy(PortfolioPolicyRuntime(
        "paper-crisis", empty, None, last_risk_exit_date=DAYS[0],
    ))
    cooldown = cooldown_policy.decide_from_features(features, as_of=DAYS[-1])
    assert cooldown.target_weights == {"D.SH": Decimal("1")}
    assert cooldown.audit["action"] == "cooldown"


def test_crisis_policy_fails_closed_on_history_and_config_typos():
    runtime = PortfolioPolicyRuntime(
        "paper-crisis", PortfolioState.with_cash(Decimal("1000")), None,
    )
    policy = crisis_policy(runtime)
    incomplete = replace(crisis_feature("A.SH"), current_drawdown=None)
    with pytest.raises(AgentPolicyError, match="refusing to shorten"):
        policy.decide_from_features({
            "A.SH": incomplete,
            "B.SH": crisis_feature("B.SH"),
        }, as_of=DAYS[-1])
    with pytest.raises(AgentPolicyError, match="Unknown crisis-drawdown params"):
        crisis_policy(runtime, premium_filter="0.02")
    with pytest.raises(AgentPolicyError, match="undeclared risk instrument"):
        crisis_policy(runtime, position_caps={"X.SH": "0.10"})


def price_candidate(
    instrument_id: str,
    *,
    beta: float | None = 0.5,
    volatility: float | None = 0.2,
    current_is_st: bool = False,
    short: float = 0.1,
    long: float = 0.2,
    limit_hits: int = 0,
    removed_on: date | None = None,
    above_average: bool = True,
) -> PriceSignalCandidate:
    return PriceSignalCandidate(
        instrument_id=instrument_id,
        name=instrument_id,
        as_of=date(2026, 7, 31),
        listed_date=date(2020, 1, 1),
        current_is_st=current_is_st,
        last_close=10.0,
        avg_amount=100_000_000,
        momentum=MappingProxyType({20: short, 60: long, 120: long}),
        volatility=volatility,
        beta=beta,
        above_long_average=above_average,
        recent_limit_hits=limit_hits,
        st_removed_on=removed_on,
    )


def test_low_beta_volatility_ranks_price_only_candidates():
    policy = build_policy("low-beta-volatility", {
        "benchmark_instrument": "M.SH",
        "defensive_instrument": "D.SH",
        "top_n": 2,
        "beta_days": 60,
        "volatility_days": 20,
        "amount_window": 20,
        "min_avg_amount": "50000000",
        "preselection_count": 100,
        "minimum_listing_days": 250,
        "min_beta": "-0.25",
        "max_beta": "1",
        "defensive_weight": "0.10",
    })
    candidates = (
        price_candidate("A.SH", beta=0.2, volatility=0.3),
        price_candidate("B.SH", beta=0.4, volatility=0.1),
        price_candidate("C.SH", beta=0.8, volatility=0.2),
        price_candidate("ST.SH", beta=0.1, volatility=0.1, current_is_st=True),
    )

    decision = policy.decide_from_candidates(candidates)

    assert decision.target_weights == {
        "A.SH": Decimal("0.45"),
        "B.SH": Decimal("0.45"),
        "D.SH": Decimal("0.10"),
    }
    assert decision.audit["prospective_only"] is True


def test_st_policies_keep_small_stock_sleeves_and_defensive_remainder():
    active = build_policy("st-active-momentum", {
        "defensive_instrument": "D.SH",
        "top_n": 2,
        "short_momentum_days": 60,
        "long_momentum_days": 120,
        "amount_window": 20,
        "min_avg_amount": "30000000",
        "minimum_listing_days": 250,
        "max_recent_limit_hits": 2,
        "stock_allocation": "0.20",
    })
    active_decision = active.decide_from_candidates((
        price_candidate("A.SH", current_is_st=True, short=0.2, long=0.3),
        price_candidate("B.SH", current_is_st=True, short=-0.1, long=-0.1),
        price_candidate(
            "C.SH", current_is_st=True, short=0.3, long=0.4, limit_hits=3,
        ),
    ))
    assert active_decision.target_weights == {
        "A.SH": Decimal("0.10"), "D.SH": Decimal("0.90"),
    }

    removal = build_policy("st-removal-momentum", {
        "defensive_instrument": "D.SH",
        "top_n": 2,
        "short_momentum_days": 20,
        "long_momentum_days": 60,
        "amount_window": 20,
        "min_avg_amount": "30000000",
        "minimum_listing_days": 250,
        "max_recent_limit_hits": 3,
        "stock_allocation": "0.20",
        "st_removal_lookback": 60,
        "require_above_long_average": True,
    })
    removal_decision = removal.decide_from_candidates((
        price_candidate(
            "R.SH", short=0.1, long=0.2, removed_on=date(2026, 7, 15),
        ),
        price_candidate(
            "N.SH", short=0.2, long=0.3, removed_on=None,
        ),
    ))
    assert removal_decision.target_weights == {
        "D.SH": Decimal("0.90"), "R.SH": Decimal("0.10"),
    }


def test_dividend_rules_selects_deterministic_top_ten(tmp_path, monkeypatch):
    charter = tmp_path / "dividend.yaml"
    charter.write_text(
        """charter:
  id: dividend-value
  version: "1"
  philosophy: deterministic dividend test
  hard_rules:
    asset_types: [stock]
    exclude_st: true
    min_dividend_years: 3
    min_yield_floor: "0.02"
    max_single_weight: "0.15"
    min_cash_weight: "0.05"
    min_positions: 5
    max_positions: 20
""",
        encoding="utf-8",
    )
    candidates = tuple(
        DividendCandidate(
            instrument_id=f"{index:06d}.SZ",
            name=f"Candidate {index}",
            as_of=date(2026, 7, 31),
            last_close=10.0,
            ttm_dividend=1.0,
            ttm_yield=0.10,
            ttm_special_dividend=0.0,
            latest_fiscal_year=2025,
            latest_fiscal_dividend=1.0,
            latest_fiscal_yield=0.10 - index / 1000,
            normalized_dividend=1.0,
            normalized_yield=0.10 - index / 1000,
            sustainable_dividend=1.0,
            sustainable_yield=0.10 - index / 1000,
            normalization_years=3,
            dividend_years=5,
            avg_amount=30_000_000,
            annual_dividends=((2023, 1.0), (2024, 1.0), (2025, 1.0)),
            payout_variability=0.01 * index,
        )
        for index in range(12)
    )
    stale = replace(
        candidates[0],
        instrument_id="999999.SZ",
        latest_fiscal_year=2020,
        sustainable_yield=0.90,
    )
    monkeypatch.setattr(
        policy_module, "build_dividend_candidates", lambda *a, **k: (stale, *candidates),
    )

    class MonthEndMarket:
        @staticmethod
        def next_trading_day(day):
            assert day == date(2026, 7, 31)
            return date(2026, 8, 3)

    runtime = PortfolioPolicyRuntime(
        "paper-dividend-rules", PortfolioState.with_cash(Decimal("1000000")), None,
    )
    policy = build_policy("dividend-rules", {
        "charter": str(charter),
        "top_n": 10,
        "min_yield": "0.04",
        "min_dividend_years": 5,
        "min_avg_amount": "20000000",
        "max_fiscal_year_lag": 2,
        "require_ttm_cash": True,
        "cash_reserve": "0.05",
        "rebalance_cooldown_days": 28,
    }, runtime=runtime)
    decision = policy.decide(MonthEndMarket(), date(2026, 7, 31))
    assert list(decision.target_weights) == [f"{index:06d}.SZ" for index in range(10)]
    assert "999999.SZ" not in decision.target_weights
    assert set(decision.target_weights.values()) == {Decimal("0.0950")}
    assert sum(decision.target_weights.values()) == Decimal("0.9500")


def test_unknown_policy_kind_and_bad_weights_fail():
    with pytest.raises(AgentPolicyError, match="Unknown agent policy kind"):
        build_policy("mystery", {})
    with pytest.raises(AgentPolicyError, match="Unknown momentum-rotation params"):
        build_policy("momentum-rotation", policy_settings(momentun_days=3).params)
    with pytest.raises(AgentPolicyError, match="sum to <= 1"):
        build_policy("momentum-rotation", policy_settings(
            risk_on={"600000.SH": "0.9", "511010.SH": "0.3"},
        ).params)
    with pytest.raises(AgentPolicyError, match="empty instrument"):
        build_policy("momentum-rotation", policy_settings(risk_off={" ": "0.1"}).params)


# ----------------------------------------------------------------- service


class FakeCrisisPolicy:
    policy_id = "crisis-drawdown"
    version = "1"
    config_hash = "fake-crisis-config"

    def __init__(self, *, fail: bool = False):
        self.fail = fail

    def decide(self, market, as_of):
        if self.fail:
            raise AgentPolicyError("crisis history unavailable")
        return PolicyDecision(
            target_weights={"600000.SH": Decimal("0.6")},
            reason="crisis entry fixture",
            audit={
                "action": "enter",
                "premium_data_used": False,
                "signals": {
                    "600000.SH": {
                        "current_drawdown": "-0.25",
                        "event_drawdown": "-0.30",
                        "rebound_from_low": "0.08",
                        "recovery_ratio": "0.75",
                        "above_confirmation_average": True,
                        "annualized_volatility": "0.20",
                    },
                },
            },
        )


def crisis_service_settings(tmp_path):
    return agent_settings(tmp_path, policies={
        "paper-agent": AgentPolicySettings(
            "paper-agent",
            "crisis-drawdown",
            {
                "risk_instruments": ["600000.SH"],
                "defensive_instrument": "511010.SH",
                "minimum_drawdown": "0.20",
            },
        ),
    })


def test_official_crisis_evaluation_is_persisted_before_decision_and_idempotent(
    tmp_path, monkeypatch,
):
    ready_market(tmp_path / "market", close_values=(10.0, 11.0, 12.0, 13.0))
    settings = crisis_service_settings(tmp_path)
    advance_once(settings)
    monkeypatch.setattr(service_module, "build_policy", lambda *args, **kwargs: FakeCrisisPolicy())
    service = AgentDecisionService(settings)

    first = service.decide("paper-agent")
    repeated = service.decide("paper-agent")

    records = list_agent_evaluations(
        evaluation_root(settings.daily.agent_decision_root), "paper-agent",
    )
    assert first["written"] is True
    assert first["evaluation"]["revision_id"] == records[0].revision_id
    assert repeated["skipped"] == "already_present"
    assert repeated["evaluation"]["revision_id"] == records[0].revision_id
    assert len(records) == 1
    assert records[0].status == "ready"
    assert records[0].action == "enter"
    assert records[0].snapshot_id == first["snapshot_id"]


def test_crisis_dry_run_never_writes_evaluation_evidence(tmp_path, monkeypatch):
    ready_market(tmp_path / "market", close_values=(10.0, 11.0, 12.0, 13.0))
    settings = crisis_service_settings(tmp_path)
    advance_once(settings)
    monkeypatch.setattr(service_module, "build_policy", lambda *args, **kwargs: FakeCrisisPolicy())

    result = AgentDecisionService(settings).decide("paper-agent", dry_run=True)

    assert result["written"] is False
    assert list_agent_evaluations(
        evaluation_root(settings.daily.agent_decision_root), "paper-agent",
    ) == ()


def test_crisis_policy_failure_is_recorded_and_does_not_create_decision(
    tmp_path, monkeypatch,
):
    ready_market(tmp_path / "market", close_values=(10.0, 11.0, 12.0, 13.0))
    settings = crisis_service_settings(tmp_path)
    advance_once(settings)
    monkeypatch.setattr(
        service_module, "build_policy", lambda *args, **kwargs: FakeCrisisPolicy(fail=True),
    )

    with pytest.raises(AgentPolicyError, match="history unavailable"):
        AgentDecisionService(settings).decide("paper-agent")

    records = list_agent_evaluations(
        evaluation_root(settings.daily.agent_decision_root), "paper-agent",
    )
    assert len(records) == 1 and records[0].status == "error"
    assert records[0].error_type == "AgentPolicyError"
    assert load_agent_decision(
        settings.daily.agent_decision_root, "paper-agent", FUTURE_DAYS[0],
    ) is None


def test_crisis_evidence_write_failure_fails_closed_before_decision(tmp_path, monkeypatch):
    ready_market(tmp_path / "market", close_values=(10.0, 11.0, 12.0, 13.0))
    settings = crisis_service_settings(tmp_path)
    advance_once(settings)
    monkeypatch.setattr(service_module, "build_policy", lambda *args, **kwargs: FakeCrisisPolicy())

    def refuse_evidence(*args, **kwargs):
        raise AgentEvaluationError("evidence disk unavailable")

    monkeypatch.setattr(service_module, "write_agent_evaluation", refuse_evidence)
    with pytest.raises(AgentEvaluationError, match="disk unavailable"):
        AgentDecisionService(settings).decide("paper-agent")
    assert load_agent_decision(
        settings.daily.agent_decision_root, "paper-agent", FUTURE_DAYS[0],
    ) is None


def test_service_cooldown_tracks_risk_exit_not_initial_defense(tmp_path):
    settings = agent_settings(tmp_path)
    service = AgentDecisionService(settings)
    root = settings.daily.agent_decision_root
    write_agent_decision(
        root,
        account_id="paper-agent",
        decision_date=DAYS[0],
        target_weights={"D.SH": "1"},
        reason="initial defense",
        agent_id="crisis-drawdown-v1",
    )
    assert service._last_risk_exit_date(
        "paper-agent", risk_instruments=("A.SH",), before=DAYS[1],
    ) is None
    write_agent_decision(
        root,
        account_id="paper-agent",
        decision_date=DAYS[1],
        target_weights={"A.SH": "0.4", "D.SH": "0.6"},
        reason="crisis entry",
        agent_id="crisis-drawdown-v1",
    )
    write_agent_decision(
        root,
        account_id="paper-agent",
        decision_date=DAYS[2],
        target_weights={"D.SH": "1"},
        reason="profit exit",
        agent_id="crisis-drawdown-v1",
    )

    assert service._last_risk_exit_date(
        "paper-agent", risk_instruments=("A.SH",), before=DAYS[3],
    ) == DAYS[2]


def test_service_decides_next_session_and_drops_a_loadable_file(tmp_path):
    ready_market(tmp_path / "market", close_values=(10.0, 11.0, 12.0, 13.0))
    settings = agent_settings(tmp_path)
    advance_once(settings)

    result = AgentDecisionService(settings).decide("paper-agent")

    assert result["written"] is True
    assert result["decision_date"] == FUTURE_DAYS[0].isoformat()
    assert result["as_of"] == DAYS[-1].isoformat()
    assert result["target_weights"] == {"600000.SH": "0.6"}

    decision = load_agent_decision(
        settings.daily.agent_decision_root, "paper-agent", FUTURE_DAYS[0],
    )
    assert decision is not None
    assert decision.target_weights == {"600000.SH": Decimal("0.6")}
    assert decision.agent_id == "momentum-rotation-v1"
    assert result["snapshot_id"] in decision.reason


def test_service_risk_off_on_falling_prices(tmp_path):
    ready_market(tmp_path / "market", close_values=(13.0, 12.0, 11.0, 10.0))
    settings = agent_settings(tmp_path)
    advance_once(settings)

    result = AgentDecisionService(settings).decide("paper-agent")
    assert result["target_weights"] == {"600000.SH": "0.1"}


def test_service_overwrite_and_stale_date_guards(tmp_path):
    ready_market(tmp_path / "market", close_values=(10.0, 11.0, 12.0, 13.0))
    settings = agent_settings(tmp_path)
    advance_once(settings)
    service = AgentDecisionService(settings)

    first = service.decide("paper-agent")
    repeated = service.decide("paper-agent")
    dry_repeated = service.decide("paper-agent", dry_run=True)
    persisted = load_agent_decision(
        settings.daily.agent_decision_root, "paper-agent", FUTURE_DAYS[0],
    )
    assert persisted is not None
    assert repeated["written"] is False
    assert repeated["skipped"] == "already_present"
    assert dry_repeated["skipped"] == "already_present"
    assert repeated["existing_content_hash"] == persisted.content_hash
    assert first["written"] is True
    assert service.decide("paper-agent", overwrite=True)["written"] is True

    with pytest.raises(AgentServiceError, match="already advanced"):
        service.decide("paper-agent", target_date=DAYS[-1])


def test_service_dry_run_writes_nothing(tmp_path):
    ready_market(tmp_path / "market", close_values=(10.0, 11.0, 12.0, 13.0))
    settings = agent_settings(tmp_path)
    advance_once(settings)

    result = AgentDecisionService(settings).decide("paper-agent", dry_run=True)
    assert result["written"] is False
    assert load_agent_decision(
        settings.daily.agent_decision_root, "paper-agent", FUTURE_DAYS[0],
    ) is None


@pytest.mark.parametrize("options", (
    {},
    {"overwrite": True},
    {"dry_run": True},
    {"overwrite": True, "dry_run": True},
))
def test_service_rejects_an_existing_corrupt_decision_in_every_mode(tmp_path, options):
    ready_market(tmp_path / "market", close_values=(10.0, 11.0, 12.0, 13.0))
    settings = agent_settings(tmp_path)
    advance_once(settings)
    path = (
        settings.daily.agent_decision_root
        / "paper-agent"
        / f"{FUTURE_DAYS[0].isoformat()}.json"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("{}", encoding="utf-8")

    with pytest.raises(AgentServiceError, match="is invalid"):
        AgentDecisionService(settings).decide("paper-agent", **options)
    assert path.read_text(encoding="utf-8") == "{}"


def test_service_refuses_unconfigured_accounts(tmp_path):
    ready_market(tmp_path / "market")
    settings = agent_settings(tmp_path, policies={})
    with pytest.raises(AgentServiceError, match="no agent policy"):
        AgentDecisionService(settings).decide("paper-agent")

    static_settings = agent_settings(tmp_path, DailyAccountSettings(
        "paper-1", "Paper 1", Decimal("100000"), "static",
        {"600000.SH": Decimal("0.5")},
    ))
    with pytest.raises(AgentServiceError, match="Not a configured agent-file"):
        AgentDecisionService(static_settings).decide("paper-1")


def test_service_bootstraps_without_an_existing_ledger_head(tmp_path):
    ready_market(tmp_path / "market", close_values=(10.0, 11.0, 12.0, 13.0))
    settings = agent_settings(tmp_path)
    result = AgentDecisionService(settings).decide("paper-agent", dry_run=True)
    assert result["decision_date"] == FUTURE_DAYS[0].isoformat()
    assert result["as_of"] == DAYS[-1].isoformat()
    assert result["target_weights"] == {"600000.SH": "0.6"}
    assert not settings.paths.trading_database.exists()


def test_decide_all_isolates_per_account_failures(tmp_path, monkeypatch):
    ready_market(tmp_path / "market", close_values=(10.0, 11.0, 12.0, 13.0))
    settings = agent_settings(
        tmp_path,
        DailyAccountSettings("paper-agent", "Paper Agent", Decimal("100000"), "agent-file"),
        DailyAccountSettings("paper-agent-2", "Second", Decimal("100000"), "agent-file"),
    )
    advance_once(settings)
    shared_market = CanonicalMarketData.open(settings.paths.market_data)
    open_calls = 0

    def open_once(*args, **kwargs):
        nonlocal open_calls
        open_calls += 1
        return shared_market

    monkeypatch.setattr(service_module.CanonicalMarketData, "open", open_once)

    outcomes = AgentDecisionService(settings).decide_all()
    by_account = {item["account_id"]: item for item in outcomes}
    assert by_account["paper-agent"]["written"] is True
    assert by_account["paper-agent-2"]["written"] is False
    assert "no agent policy" in by_account["paper-agent-2"]["error"]
    assert open_calls == 1


# ------------------------------------------------------- shared write path


def test_write_agent_decision_never_leaves_a_bad_file(tmp_path):
    root = tmp_path / "decisions"
    with pytest.raises(AgentDecisionError):
        write_agent_decision(
            root,
            account_id="paper-agent",
            decision_date=date(2026, 7, 17),
            target_weights={"600000.SH": "-1"},
            reason="negative weight must be rejected",
            agent_id="test",
        )
    assert not (root / "paper-agent" / "2026-07-17.json").exists()

    written = write_agent_decision(
        root,
        account_id="paper-agent",
        decision_date=date(2026, 7, 17),
        target_weights={"600000.SH": Decimal("0.5")},
        reason="valid",
        agent_id="test",
    )
    assert written.target_weights == {"600000.SH": Decimal("0.5")}
    with pytest.raises(AgentDecisionError, match="already exists"):
        write_agent_decision(
            root,
            account_id="paper-agent",
            decision_date=date(2026, 7, 17),
            target_weights={"600000.SH": "0.5"},
            reason="no silent overwrite",
            agent_id="test",
        )

    path = root / "paper-agent" / "2026-07-17.json"
    path.write_text("{}", encoding="utf-8")
    with pytest.raises(AgentDecisionError, match="Agent decision"):
        write_agent_decision(
            root,
            account_id="paper-agent",
            decision_date=date(2026, 7, 17),
            target_weights={"600000.SH": "0.6"},
            reason="overwrite must not erase corrupt evidence",
            agent_id="test",
            overwrite=True,
        )
    assert path.read_text(encoding="utf-8") == "{}"


def test_concurrent_decision_writers_never_clobber_without_overwrite(tmp_path):
    root = tmp_path / "decisions"
    decision_date = date(2026, 7, 17)

    def attempt(index: int) -> str:
        try:
            write_agent_decision(
                root,
                account_id="paper-agent",
                decision_date=decision_date,
                target_weights={"600000.SH": Decimal(index) / Decimal("10")},
                reason=f"candidate {index}",
                agent_id=f"writer-{index}",
            )
        except AgentDecisionError:
            return "rejected"
        return "written"

    with ThreadPoolExecutor(max_workers=8) as pool:
        outcomes = list(pool.map(attempt, range(1, 9)))

    assert outcomes.count("written") == 1
    assert outcomes.count("rejected") == 7
    persisted = load_agent_decision(root, "paper-agent", decision_date)
    assert persisted is not None
    assert persisted.reason.startswith("candidate ")
    assert not (root / ".staging").exists()
