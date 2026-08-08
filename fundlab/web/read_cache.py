"""Small synchronous read cache with concurrent request coalescing."""

from __future__ import annotations

from threading import Condition
from time import monotonic
from typing import Callable, Generic, TypeVar


T = TypeVar("T")


class SingleFlightReadCache(Generic[T]):
    """Cache a read briefly and let concurrent misses share one computation."""

    def __init__(
        self,
        *,
        ttl_seconds: float = 1.0,
        clock: Callable[[], float] = monotonic,
    ) -> None:
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be positive")
        self._ttl_seconds = ttl_seconds
        self._clock = clock
        self._condition = Condition()
        self._value: T | None = None
        self._cached_at: float | None = None
        self._refreshing = False

    def get(self, loader: Callable[[], T]) -> T:
        with self._condition:
            while True:
                if self._is_fresh_locked():
                    assert self._value is not None
                    return self._value
                if not self._refreshing:
                    self._refreshing = True
                    break
                self._condition.wait()
        try:
            value = loader()
        except BaseException:
            with self._condition:
                self._refreshing = False
                self._condition.notify_all()
            raise
        with self._condition:
            self._value = value
            self._cached_at = self._clock()
            self._refreshing = False
            self._condition.notify_all()
            return value

    def invalidate(self) -> None:
        with self._condition:
            self._value = None
            self._cached_at = None

    def _is_fresh_locked(self) -> bool:
        return (
            self._value is not None
            and self._cached_at is not None
            and self._clock() - self._cached_at < self._ttl_seconds
        )
