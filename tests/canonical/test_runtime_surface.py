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
