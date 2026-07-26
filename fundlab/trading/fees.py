from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal, ROUND_HALF_UP
from typing import Iterable

from fundlab.common.canonical import stable_digest
from fundlab.trading.intent import decimal_value


CENT = Decimal("0.01")


def money(value: Decimal | str | int | float) -> Decimal:
    return decimal_value(value).quantize(CENT, rounding=ROUND_HALF_UP)


@dataclass(frozen=True)
class FeeRule:
    effective_from: date
    effective_to: date | None
    asset_types: frozenset[str]
    exchanges: frozenset[str]
    broker_commission_rate: Decimal
    minimum_commission: Decimal
    stamp_duty_sell_rate: Decimal = Decimal("0")
    transfer_fee_rate: Decimal = Decimal("0")
    exchange_handling_rate: Decimal = Decimal("0")
    regulatory_levy_rate: Decimal = Decimal("0")
    priority: int = 0
    evidence: str = ""

    def __post_init__(self) -> None:
        if self.effective_to is not None and self.effective_from > self.effective_to:
            raise ValueError("Fee rule effective_from must not exceed effective_to")
        for field_name in (
            "broker_commission_rate", "minimum_commission", "stamp_duty_sell_rate",
            "transfer_fee_rate", "exchange_handling_rate", "regulatory_levy_rate",
        ):
            value = decimal_value(getattr(self, field_name))
            if value < 0:
                raise ValueError(f"{field_name} cannot be negative")
            object.__setattr__(self, field_name, value)
        object.__setattr__(self, "asset_types", frozenset(self.asset_types))
        object.__setattr__(self, "exchanges", frozenset(self.exchanges))

    def matches(self, day: date, asset_type: str, exchange: str) -> bool:
        return (
            self.effective_from <= day
            and (self.effective_to is None or day <= self.effective_to)
            and (not self.asset_types or asset_type in self.asset_types)
            and (not self.exchanges or exchange in self.exchanges)
        )


@dataclass(frozen=True)
class FeeBreakdown:
    broker_commission: Decimal
    stamp_duty: Decimal
    transfer_fee: Decimal
    exchange_handling: Decimal
    regulatory_levy: Decimal

    @property
    def total(self) -> Decimal:
        return money(
            self.broker_commission + self.stamp_duty + self.transfer_fee
            + self.exchange_handling + self.regulatory_levy
        )


@dataclass(frozen=True)
class FeeSchedule:
    schedule_id: str
    version: str
    rules: tuple[FeeRule, ...]
    trusted_for_simulation: bool
    verification_note: str

    def __post_init__(self) -> None:
        if not self.schedule_id or not self.version or not self.rules:
            raise ValueError("Fee schedule identity and rules cannot be empty")
        if self.trusted_for_simulation and not self.verification_note.strip():
            raise ValueError("Trusted simulation fee schedules require a verification note")
        object.__setattr__(self, "rules", tuple(self.rules))

    @property
    def config_hash(self) -> str:
        return stable_digest(self)

    def calculate(
        self,
        *,
        day: date,
        asset_type: str,
        exchange: str,
        side: str,
        amount: Decimal,
    ) -> FeeBreakdown:
        candidates = [rule for rule in self.rules if rule.matches(day, asset_type, exchange)]
        if not candidates:
            raise LookupError(f"No fee rule for {day} {exchange} {asset_type}")
        maximum = max(rule.priority for rule in candidates)
        winners = [rule for rule in candidates if rule.priority == maximum]
        if len(winners) != 1:
            raise ValueError(f"Ambiguous fee rules for {day} {exchange} {asset_type}")
        rule = winners[0]
        commission = max(money(amount * rule.broker_commission_rate), money(rule.minimum_commission))
        stamp = money(amount * rule.stamp_duty_sell_rate) if side == "sell" else Decimal("0.00")
        return FeeBreakdown(
            commission,
            stamp,
            money(amount * rule.transfer_fee_rate),
            money(amount * rule.exchange_handling_rate),
            money(amount * rule.regulatory_levy_rate),
        )


def fee_schedule_from_rules(
    schedule_id: str,
    version: str,
    rules: Iterable[FeeRule],
    *,
    trusted_for_simulation: bool = False,
    verification_note: str = "",
) -> FeeSchedule:
    return FeeSchedule(
        schedule_id, version, tuple(rules), trusted_for_simulation, verification_note,
    )
