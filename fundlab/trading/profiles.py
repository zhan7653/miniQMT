from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ExecutionProfile:
    profile_id: str
    version: str
    commission_bps: float = 3.0
    minimum_commission: float = 0.0
    slippage_bps: float = 2.0
    lot_size: int = 100
    capacity_fraction: float | None = 0.05
    capacity_lookback_days: int = 20

    def __post_init__(self) -> None:
        if not self.profile_id or not self.version:
            raise ValueError("Execution profile identity cannot be empty")
        if min(self.commission_bps, self.minimum_commission, self.slippage_bps) < 0:
            raise ValueError("Execution costs cannot be negative")
        if self.lot_size <= 0 or self.capacity_lookback_days <= 0:
            raise ValueError("Lot size and capacity lookback must be positive")
        if self.capacity_fraction is not None and not 0 < self.capacity_fraction <= 1:
            raise ValueError("capacity_fraction must be in (0, 1] or None")


@dataclass(frozen=True)
class ResearchRiskProfile:
    profile_id: str
    version: str
    etf_only: bool = True
    allow_short: bool = False
    allow_leverage: bool = False
    require_nonnegative_cash: bool = True
    max_position_weight: float | None = None
    minimum_cash_weight: float | None = None
    reject_untrusted_premium_discount: bool = True

    def __post_init__(self) -> None:
        if not self.profile_id or not self.version:
            raise ValueError("Risk profile identity cannot be empty")
        for value in (self.max_position_weight, self.minimum_cash_weight):
            if value is not None and not 0 <= value <= 1:
                raise ValueError("Risk weight constraints must be in [0, 1] or None")
