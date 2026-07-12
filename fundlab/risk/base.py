from __future__ import annotations

from dataclasses import dataclass

from typing import Protocol

from fundlab.backtest.models import Account, Order
from fundlab.data.portal import DataPortal


@dataclass
class RiskCheckResult:
    passed: bool
    adjusted_order: Order | None = None
    reason: str | None = None
    requested_quantity: int | None = None


class RiskRule(Protocol):
    name: str

    def check_order(self, order: Order, account: Account, date: str, data_portal: DataPortal) -> RiskCheckResult:
        ...

    def check_target_weights(self, target_weights: dict[str, float], account: Account, date: str, data_portal: DataPortal) -> dict[str, float]:
        return target_weights
