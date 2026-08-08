from __future__ import annotations

import importlib

import pytest


@pytest.mark.parametrize(
    "module_name",
    (
        "fundlab.backtest",
        "fundlab.paper",
        "fundlab.data",
        "fundlab.features",
        "fundlab.risk",
    ),
)
def test_removed_runtime_surfaces_stay_removed(module_name: str):
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module(module_name)


def test_strategies_package_exposes_only_intent_sources():
    strategies = importlib.import_module("fundlab.strategies")

    assert set(strategies.__all__) == {
        "AgentDecision",
        "AgentDecisionError",
        "FileIntentSource",
        "MovingAverageGridSource",
        "StaticAllocationSource",
        "load_agent_decision",
        "write_agent_decision",
    }
    from fundlab.trading import IntentSource

    assert isinstance(strategies.StaticAllocationSource({"600000.SH": 1}), IntentSource)


@pytest.mark.parametrize(
    "module_name",
    (
        "fundlab.trading.accounting",
        "fundlab.trading.decision",
        "fundlab.trading.execution",
        "fundlab.trading.models",
        "fundlab.trading.profiles",
    ),
)
def test_legacy_trading_modules_cannot_be_imported(module_name: str):
    with pytest.raises(ModuleNotFoundError, match=module_name):
        importlib.import_module(module_name)


def test_trading_public_surface_contains_only_canonical_contracts():
    import fundlab.trading as trading

    legacy_names = {
        "AccountBindings",
        "AccountingResult",
        "DecisionEnvelope",
        "DecisionSourceType",
        "ExecutionResult",
        "ExecutionProfile",
        "OrderStatus",
        "ResearchRiskProfile",
        "RiskOutcome",
        "RiskResult",
        "TargetOrder",
        "TradingAccountRecord",
        "TradingAccountStatus",
        "ValidationResult",
        "ValidationStatus",
        "apply_fill",
        "build_decision_envelope",
        "execute_quantity",
        "size_target_orders",
    }
    assert not legacy_names.intersection(trading.__all__)
    assert not any(hasattr(trading, name) for name in legacy_names)
    assert trading.AccountRecord.__module__ == "fundlab.trading.repository"
    assert trading.AccountStatus.__module__ == "fundlab.trading.repository"
