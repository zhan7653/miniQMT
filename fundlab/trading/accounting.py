from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class AccountingResult:
    cash_before: float
    cash_after: float
    quantity_before: int
    quantity_after: int


def apply_fill(*, cash: float, position_quantity: int, side: Literal["buy", "sell"],
               quantity: int, amount: float, commission: float) -> AccountingResult:
    if quantity < 0 or position_quantity < 0:
        raise ValueError("Quantities cannot be negative")
    if side == "buy":
        cash_after = cash - amount - commission
        quantity_after = position_quantity + quantity
    else:
        if quantity > position_quantity:
            raise ValueError("Sell quantity exceeds position")
        cash_after = cash + amount - commission
        quantity_after = position_quantity - quantity
    if cash_after < -1e-8:
        raise ValueError("Fill would make cash negative")
    return AccountingResult(cash, max(0.0, cash_after), position_quantity, quantity_after)
