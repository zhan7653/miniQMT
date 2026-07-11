from __future__ import annotations

from typing import Any

from fundlab.common.dates import normalize_date


class LegacyDataPortal:
    """Explicit read-only facade for a v1 portal during parallel migration."""

    def __init__(self, portal: Any) -> None:
        object.__setattr__(self, "_portal", portal)

    @classmethod
    def calendar_only(cls, sqlite_store: Any) -> "LegacyDataPortal":
        """Build the narrow v1 compatibility boundary; no market datasets are exposed."""
        return cls(_LegacyCalendarReader(sqlite_store))

    def __getattr__(self, name: str) -> Any:
        if name.startswith(("write", "update", "insert", "delete", "create", "save")):
            raise AttributeError(f"Legacy data portal is read-only: {name}")
        return getattr(self._portal, name)

    def __setattr__(self, name: str, value: Any) -> None:
        raise AttributeError("Legacy data portal is read-only")


class _LegacyCalendarReader:
    def __init__(self, sqlite_store: Any) -> None:
        self._sqlite_store = sqlite_store

    def get_trading_days(self, start_date: str, end_date: str) -> list[str]:
        frame = self._sqlite_store.get_trading_days(normalize_date(start_date), normalize_date(end_date))
        return frame["date"].tolist()

    def is_trading_day(self, date: str) -> bool:
        frame = self._sqlite_store.read_frame(
            "SELECT is_trading_day FROM trading_calendar WHERE date = ?", [normalize_date(date)]
        )
        return False if frame.empty else bool(frame.iloc[0]["is_trading_day"])

    def next_trading_day(self, date: str) -> str | None:
        frame = self._sqlite_store.read_frame(
            "SELECT next_trading_day FROM trading_calendar WHERE date = ?", [normalize_date(date)]
        )
        return None if frame.empty else frame.iloc[0]["next_trading_day"]

    def previous_trading_day(self, date: str) -> str | None:
        frame = self._sqlite_store.read_frame(
            "SELECT previous_trading_day FROM trading_calendar WHERE date = ?", [normalize_date(date)]
        )
        return None if frame.empty else frame.iloc[0]["previous_trading_day"]
