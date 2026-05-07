from __future__ import annotations


class CostModel:
    def __init__(self, commission_rate: float = 0.00003, min_commission: float = 0.0):
        self.commission_rate = commission_rate
        self.min_commission = min_commission

    def calculate(self, amount: float) -> float:
        return max(abs(amount) * self.commission_rate, self.min_commission)

