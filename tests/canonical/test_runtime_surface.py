from __future__ import annotations

import importlib

import pytest


@pytest.mark.parametrize(
    "module_name,replacement",
    (
        ("fundlab.backtest", "fundlab.trading"),
        ("fundlab.paper", "fundlab.trading"),
        ("fundlab.data", "fundlab.marketdata"),
        ("fundlab.features", "canonical snapshots"),
        ("fundlab.risk", "fundlab.trading.intent"),
        ("fundlab.strategies", "PortfolioIntent"),
    ),
)
def test_removed_runtime_surfaces_fail_with_the_canonical_replacement(
    module_name: str,
    replacement: str,
):
    with pytest.raises(ModuleNotFoundError, match=replacement):
        importlib.import_module(module_name)


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
