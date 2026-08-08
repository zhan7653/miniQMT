"""Non-blocking, in-process cache for dashboard scheduler reads.

The OS scheduler query may start PowerShell and take seconds.  Dashboard read
paths must never wait for it: the last successful result is returned from
memory while it is refreshed in the background.  Only a cold read with no
last-good value retains the established :class:`TaskSchedulerError` contract.
"""

from __future__ import annotations

from threading import Lock, Thread
from time import monotonic, sleep
from typing import Callable

from fundlab.web.schedule import ScheduledTaskState, TaskScheduler, TaskSchedulerError


class CachedTaskScheduler:
    """Wrap a scheduler with a bounded-TTL, single-flight read cache.

    ``query`` returns the latest successful value.  Expired values use
    stale-while-revalidate so a slow Windows scheduler query never makes an
    already-rendered dashboard flash into an error state.  With no last-good
    value it starts a background query and raises ``TaskSchedulerError``
    immediately instead of blocking a web worker.
    """

    _REFRESHING_MESSAGE = "Task scheduler state is refreshing"

    def __init__(
        self,
        scheduler: TaskScheduler,
        *,
        ttl_seconds: float = 30.0,
        refresh_ahead_seconds: float = 5.0,
        refresh_delay_seconds: float = 0.5,
        clock: Callable[[], float] = monotonic,
    ) -> None:
        if not 0 < ttl_seconds <= 30:
            raise ValueError("ttl_seconds must be greater than 0 and at most 30")
        if not 0 <= refresh_ahead_seconds < ttl_seconds:
            raise ValueError("refresh_ahead_seconds must be non-negative and below ttl_seconds")
        if refresh_delay_seconds < 0:
            raise ValueError("refresh_delay_seconds must not be negative")
        self._scheduler = scheduler
        self._ttl_seconds = ttl_seconds
        self._refresh_ahead_seconds = refresh_ahead_seconds
        self._refresh_delay_seconds = refresh_delay_seconds
        self._clock = clock
        self._lock = Lock()
        self._state: ScheduledTaskState | None = None
        self._cached_at: float | None = None
        self._refreshing = False
        self._mutating = False
        self._closed = False
        # A refresh only publishes when no mutation/close has superseded it.
        self._generation = 0

    def query(self) -> ScheduledTaskState:
        """Return a fresh cache entry or fail promptly while refreshing it."""
        now = self._clock()
        with self._lock:
            state = self._state
            cached_at = self._cached_at
            if state is not None and cached_at is not None:
                age = max(0.0, now - cached_at)
                if age < self._ttl_seconds:
                    if age >= self._ttl_seconds - self._refresh_ahead_seconds:
                        self._start_refresh_locked()
                    return state
                # Keep the last verified state visible while the comparatively
                # slow Windows scheduler query refreshes it.  Dashboard writes
                # never take this path: mutations synchronously replace or
                # invalidate the entry.
                self._start_refresh_locked()
                return state
            self._start_refresh_locked()
        raise TaskSchedulerError(self._REFRESHING_MESSAGE)

    def register(self, time_str: str) -> ScheduledTaskState:
        """Synchronously delegate registration and publish its returned state."""
        self._begin_mutation()
        try:
            state = self._scheduler.register(time_str)
        except BaseException:
            self._finish_failed_mutation()
            raise
        self._store_mutation_result(state)
        return state

    def set_enabled(self, enabled: bool) -> ScheduledTaskState:
        """Synchronously delegate enablement and publish its returned state."""
        self._begin_mutation()
        try:
            state = self._scheduler.set_enabled(enabled)
        except BaseException:
            self._finish_failed_mutation()
            raise
        self._store_mutation_result(state)
        return state

    def delete(self) -> None:
        """Synchronously delete, then remove any cached state."""
        self._begin_mutation()
        try:
            self._scheduler.delete()
        except BaseException:
            self._finish_failed_mutation()
            raise
        with self._lock:
            self._generation += 1
            self._mutating = False
            self._state = None
            self._cached_at = None

    def close(self) -> None:
        """Prevent future refreshes and release the worker when it is idle."""
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._generation += 1

    def _begin_mutation(self) -> None:
        with self._lock:
            if self._closed:
                raise TaskSchedulerError("Task scheduler cache is closed")
            if self._mutating:
                raise TaskSchedulerError("Task scheduler mutation is already running")
            self._generation += 1
            self._mutating = True
            self._state = None
            self._cached_at = None

    def _finish_failed_mutation(self) -> None:
        with self._lock:
            self._generation += 1
            self._mutating = False
            self._state = None
            self._cached_at = None

    def _store_mutation_result(self, state: ScheduledTaskState) -> None:
        with self._lock:
            if self._closed:
                return
            self._generation += 1
            self._mutating = False
            self._state = state
            self._cached_at = self._clock()

    def _start_refresh_locked(self) -> None:
        if self._closed or self._refreshing or self._mutating:
            return
        self._refreshing = True
        generation = self._generation
        Thread(
            target=self._refresh,
            args=(generation,),
            name="fundlab-schedule-refresh",
            daemon=True,
        ).start()

    def _refresh(self, generation: int) -> None:
        if self._refresh_delay_seconds:
            sleep(self._refresh_delay_seconds)
        with self._lock:
            if self._closed:
                self._refreshing = False
                return
        try:
            state = self._scheduler.query()
        except Exception:
            # A failed refresh must never alter the last good entry.  Once that
            # entry expires, query() keeps reporting the normal scheduler error.
            state = None
        with self._lock:
            if state is not None and not self._closed and generation == self._generation:
                self._state = state
                self._cached_at = self._clock()
            self._refreshing = False
