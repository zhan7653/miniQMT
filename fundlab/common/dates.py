from __future__ import annotations

from datetime import date, datetime
from zoneinfo import ZoneInfo


AUDIT_TIMEZONE = ZoneInfo("Asia/Hong_Kong")


def audit_now() -> datetime:
    return datetime.now(AUDIT_TIMEZONE)


def normalize_date(value: str | date | datetime) -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return datetime.strptime(value, "%Y-%m-%d").date().isoformat()
