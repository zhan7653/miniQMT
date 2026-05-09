from __future__ import annotations

from datetime import datetime
from typing import Any

from fundlab.common.ids import new_id
from fundlab.data.storage.sqlite_store import SQLiteStore


def record_data_update(
    sqlite_store: SQLiteStore,
    *,
    job_name: str,
    source: str,
    table_name: str,
    start_date: str | None,
    end_date: str | None,
    row_count: int,
    status: str,
    error_message: str | None = None,
    data_version: str | None = None,
    started_at: str | None = None,
    extra: dict[str, Any] | None = None,
) -> str:
    update_id = new_id("upd")
    started_at = started_at or datetime.now().isoformat(timespec="seconds")
    finished_at = datetime.now().isoformat(timespec="seconds")
    message = error_message
    if extra:
        suffix = "; ".join(f"{key}={value}" for key, value in sorted(extra.items()))
        message = f"{message}; {suffix}" if message else suffix
    sqlite_store.execute_many(
        """
        INSERT INTO data_update_log (
            update_id, job_name, source, table_name, start_date, end_date,
            row_count, status, error_message, data_version, started_at, finished_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                update_id,
                job_name,
                source,
                table_name,
                start_date,
                end_date,
                row_count,
                status,
                message,
                data_version,
                started_at,
                finished_at,
            )
        ],
    )
    return update_id
