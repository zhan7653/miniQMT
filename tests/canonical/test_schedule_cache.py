from __future__ import annotations

from threading import Event, Thread, enumerate as enumerate_threads

import pytest

from fundlab.web.schedule import ScheduledTaskState, TaskSchedulerError
from fundlab.web.schedule_cache import CachedTaskScheduler


class Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class BlockingScheduler:
    def __init__(self) -> None:
        self.state = ScheduledTaskState(exists=True, enabled=True, time="19:30")
        self.query_started = Event()
        self.allow_query = Event()
        self.query_calls = 0
        self.fail_query = False
        self.register_calls: list[str] = []
        self.enabled_calls: list[bool] = []
        self.deleted = False
        self.mutation_started = Event()
        self.allow_mutation = Event()
        self.allow_mutation.set()
        self.fail_mutation = False

    def query(self) -> ScheduledTaskState:
        self.query_calls += 1
        self.query_started.set()
        assert self.allow_query.wait(2), "test did not release scheduler query"
        if self.fail_query:
            raise TaskSchedulerError("scheduler unavailable")
        return self.state

    def register(self, time_str: str) -> ScheduledTaskState:
        self.register_calls.append(time_str)
        self._mutation_guard()
        self.state = ScheduledTaskState(exists=True, enabled=True, time=time_str)
        return self.state

    def set_enabled(self, enabled: bool) -> ScheduledTaskState:
        self.enabled_calls.append(enabled)
        self._mutation_guard()
        self.state = ScheduledTaskState(exists=True, enabled=enabled, time=self.state.time)
        return self.state

    def delete(self) -> None:
        self._mutation_guard()
        self.deleted = True
        self.state = ScheduledTaskState(exists=False)

    def _mutation_guard(self) -> None:
        self.mutation_started.set()
        assert self.allow_mutation.wait(2), "test did not release scheduler mutation"
        if self.fail_mutation:
            raise TaskSchedulerError("mutation failed")


@pytest.fixture()
def cached_scheduler():
    clock = Clock()
    scheduler = BlockingScheduler()
    cache = CachedTaskScheduler(
        scheduler, ttl_seconds=30, refresh_ahead_seconds=5,
        refresh_delay_seconds=0, clock=clock,
    )
    try:
        yield cache, scheduler, clock
    finally:
        scheduler.allow_query.set()
        scheduler.allow_mutation.set()
        cache.close()


def _wait_for_query(scheduler: BlockingScheduler) -> None:
    assert scheduler.query_started.wait(1), "background query did not start"


def _wait_for_state(cache: CachedTaskScheduler) -> ScheduledTaskState:
    # The blocking fake is released before this helper; yielding lets its worker
    # publish without making the implementation under test synchronously wait.
    import time

    deadline = time.monotonic() + 1
    while time.monotonic() < deadline:
        try:
            return cache.query()
        except TaskSchedulerError:
            time.sleep(0.001)
    raise AssertionError("background result was not published")


def test_cold_queries_fail_fast_and_expired_queries_serve_last_good(cached_scheduler):
    cache, scheduler, clock = cached_scheduler

    with pytest.raises(TaskSchedulerError, match="refreshing"):
        cache.query()
    _wait_for_query(scheduler)
    start = Event()
    errors: list[Exception] = []

    def concurrent_query() -> None:
        assert start.wait(1)
        try:
            cache.query()
        except TaskSchedulerError as exc:
            errors.append(exc)

    requests = [Thread(target=concurrent_query) for _ in range(20)]
    for request in requests:
        request.start()
    start.set()
    for request in requests:
        request.join(1)
        assert not request.is_alive()
    assert len(errors) == 20
    assert scheduler.query_calls == 1

    scheduler.allow_query.set()
    assert _wait_for_state(cache) == scheduler.state
    clock.advance(30)
    scheduler.query_started.clear()
    scheduler.allow_query.clear()

    assert cache.query() == scheduler.state
    _wait_for_query(scheduler)
    assert scheduler.query_calls == 2


def test_near_expiry_returns_last_good_and_refreshes_once(cached_scheduler):
    cache, scheduler, clock = cached_scheduler
    scheduler.allow_query.set()
    with pytest.raises(TaskSchedulerError):
        cache.query()
    assert _wait_for_state(cache) == scheduler.state

    clock.advance(25)
    scheduler.query_started.clear()
    scheduler.allow_query.clear()
    assert cache.query() == scheduler.state
    _wait_for_query(scheduler)
    for _ in range(20):
        assert cache.query() == scheduler.state
    assert scheduler.query_calls == 2


def test_failed_refresh_keeps_last_good_entry_visible(cached_scheduler):
    cache, scheduler, clock = cached_scheduler
    scheduler.allow_query.set()
    with pytest.raises(TaskSchedulerError):
        cache.query()
    assert _wait_for_state(cache) == scheduler.state

    scheduler.fail_query = True
    clock.advance(30)
    scheduler.query_started.clear()
    scheduler.allow_query.clear()
    assert cache.query() == scheduler.state
    _wait_for_query(scheduler)
    scheduler.allow_query.set()
    # Failed background work cannot replace the verified value.  It remains
    # visible while a later request retries the single-flight refresh.
    assert cache.query() == scheduler.state


def test_mutations_delegate_synchronously_and_keep_cache_current(cached_scheduler):
    cache, scheduler, _ = cached_scheduler

    registered = cache.register("20:00")
    assert registered.time == "20:00" and scheduler.register_calls == ["20:00"]
    assert cache.query() == registered

    disabled = cache.set_enabled(False)
    assert disabled.enabled is False and scheduler.enabled_calls == [False]
    assert cache.query() == disabled

    cache.delete()
    assert scheduler.deleted is True
    with pytest.raises(TaskSchedulerError, match="refreshing"):
        cache.query()


def test_mutation_result_wins_over_an_older_background_refresh(cached_scheduler):
    cache, scheduler, _ = cached_scheduler
    with pytest.raises(TaskSchedulerError):
        cache.query()
    _wait_for_query(scheduler)

    registered = cache.register("21:00")
    scheduler.allow_query.set()
    assert cache.query() == registered


def test_mutation_invalidates_old_state_while_running_and_after_failure(
    cached_scheduler,
):
    cache, scheduler, _ = cached_scheduler
    registered = cache.register("20:00")
    assert cache.query() == registered

    scheduler.allow_mutation.clear()
    scheduler.mutation_started.clear()
    writer = Thread(target=cache.set_enabled, args=(False,))
    writer.start()
    assert scheduler.mutation_started.wait(1)
    with pytest.raises(TaskSchedulerError, match="refreshing"):
        cache.query()
    assert scheduler.query_calls == 0
    scheduler.allow_mutation.set()
    writer.join(1)
    assert not writer.is_alive()
    assert cache.query().enabled is False

    scheduler.fail_mutation = True
    with pytest.raises(TaskSchedulerError, match="mutation failed"):
        cache.set_enabled(True)
    with pytest.raises(TaskSchedulerError, match="refreshing"):
        cache.query()


def test_close_prevents_new_refreshes(cached_scheduler):
    cache, scheduler, _ = cached_scheduler
    cache.close()

    with pytest.raises(TaskSchedulerError, match="refreshing"):
        cache.query()
    assert scheduler.query_calls == 0


def test_running_refresh_uses_a_daemon_thread_and_cannot_hold_process_exit(
    cached_scheduler,
):
    cache, scheduler, _ = cached_scheduler
    scheduler.allow_query.clear()
    with pytest.raises(TaskSchedulerError, match="refreshing"):
        cache.query()
    _wait_for_query(scheduler)

    workers = [
        thread for thread in enumerate_threads()
        if thread.name == "fundlab-schedule-refresh"
    ]
    assert workers and all(thread.daemon for thread in workers)
    cache.close()
    scheduler.allow_query.set()
