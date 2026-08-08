from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import Event, Lock

from fundlab.web.read_cache import SingleFlightReadCache


class _Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


def test_concurrent_misses_share_one_loader_and_ttl_bounds_reuse():
    clock = _Clock()
    cache = SingleFlightReadCache[list[int]](ttl_seconds=1, clock=clock)
    started = Event()
    release = Event()
    lock = Lock()
    calls = 0

    def load() -> list[int]:
        nonlocal calls
        with lock:
            calls += 1
        started.set()
        assert release.wait(2)
        return [calls]

    with ThreadPoolExecutor(max_workers=20) as pool:
        futures = [pool.submit(cache.get, load) for _ in range(20)]
        assert started.wait(1)
        assert calls == 1
        release.set()
        assert [future.result() for future in futures] == [[1]] * 20

    assert cache.get(load) == [1]
    assert calls == 1
    clock.now = 1.0
    assert cache.get(load) == [2]
    assert calls == 2


def test_invalidate_forces_the_next_read_to_recompute():
    cache = SingleFlightReadCache[int]()
    calls = 0

    def load() -> int:
        nonlocal calls
        calls += 1
        return calls

    assert cache.get(load) == 1
    cache.invalidate()
    assert cache.get(load) == 2
