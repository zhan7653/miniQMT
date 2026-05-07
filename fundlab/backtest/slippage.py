from __future__ import annotations

from typing import Literal


class SlippageModel:
    def __init__(self, base_bps: float = 0.0):
        self.base_bps = base_bps

    def adjust_price(self, price: float, side: Literal["buy", "sell"]) -> tuple[float, float]:
        slippage = price * self.base_bps / 10000
        if side == "buy":
            return price + slippage, slippage
        return price - slippage, slippage

