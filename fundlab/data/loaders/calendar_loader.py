from __future__ import annotations

from datetime import date, timedelta

from fundlab.data.storage.sqlite_store import SQLiteStore


class CalendarLoader:
    def __init__(self, sqlite_store: SQLiteStore):
        self.sqlite_store = sqlite_store

    def load_business_days(self, start_date: date, end_date: date) -> int:
        trading_days = self._business_days(start_date, end_date)
        rows = []
        for index, trading_day in enumerate(trading_days):
            previous_day = trading_days[index - 1].isoformat() if index > 0 else None
            next_day = trading_days[index + 1].isoformat() if index + 1 < len(trading_days) else None
            is_week_end = index + 1 == len(trading_days) or trading_days[index + 1].weekday() < trading_day.weekday()
            is_month_end = index + 1 == len(trading_days) or trading_days[index + 1].month != trading_day.month
            is_quarter_end = is_month_end and trading_day.month in {3, 6, 9, 12}
            rows.append(
                (
                    trading_day.isoformat(),
                    "CN",
                    1,
                    previous_day,
                    next_day,
                    int(is_month_end),
                    int(is_week_end),
                    int(is_quarter_end),
                )
            )

        self.sqlite_store.execute_many(
            """
            INSERT INTO trading_calendar (
                date, exchange, is_trading_day, previous_trading_day, next_trading_day,
                is_month_end, is_week_end, is_quarter_end
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(date) DO UPDATE SET
                exchange=excluded.exchange,
                is_trading_day=excluded.is_trading_day,
                previous_trading_day=excluded.previous_trading_day,
                next_trading_day=excluded.next_trading_day,
                is_month_end=excluded.is_month_end,
                is_week_end=excluded.is_week_end,
                is_quarter_end=excluded.is_quarter_end,
                updated_at=CURRENT_TIMESTAMP
            """,
            rows,
        )
        return len(rows)

    def _business_days(self, start_date: date, end_date: date) -> list[date]:
        days = []
        current = start_date
        while current <= end_date:
            if current.weekday() < 5:
                days.append(current)
            current += timedelta(days=1)
        return days

