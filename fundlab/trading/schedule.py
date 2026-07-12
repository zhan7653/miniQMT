from __future__ import annotations

from datetime import date
from enum import StrEnum
from typing import Iterable


class RebalanceFrequency(StrEnum):
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"


def scheduled_trading_days(trading_days: Iterable[date], frequency: RebalanceFrequency | str) -> tuple[date, ...]:
    days = tuple(sorted(set(trading_days)))
    frequency = RebalanceFrequency(frequency)
    if frequency is RebalanceFrequency.DAILY:
        return days
    selected: list[date] = []
    for index, day in enumerate(days):
        next_day = days[index + 1] if index + 1 < len(days) else None
        if frequency is RebalanceFrequency.WEEKLY:
            boundary = next_day is None or next_day.isocalendar()[:2] != day.isocalendar()[:2]
        else:
            boundary = next_day is None or (next_day.year, next_day.month) != (day.year, day.month)
        if boundary:
            selected.append(day)
    return tuple(selected)


def is_rebalance_day(day: date, trading_days: Iterable[date], frequency: RebalanceFrequency | str) -> bool:
    return day in scheduled_trading_days(trading_days, frequency)
