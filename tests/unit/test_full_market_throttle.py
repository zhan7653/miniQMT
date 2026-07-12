from __future__ import annotations

import pytest

from fundlab.data.pipeline.throttle import AdaptiveThrottle, SpeedProfile, build_speed_profiles


class FakeTime:
    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def clock(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


def profiles():
    return (
        SpeedProfile("initial", 1, 2.0, 20, 60.0),
        SpeedProfile("step-1", 2, 1.0, 40, 45.0),
        SpeedProfile("maximum", 4, 0.5, 100, 30.0),
    )


def test_throttle_starts_safe_promotes_one_step_and_demotes_immediately():
    fake = FakeTime(); events = []
    throttle = AdaptiveThrottle(
        profiles(), approved_max_level="maximum", event_callback=lambda p, r: events.append((p.name, r)),
        sleeper=fake.sleep, clock=fake.clock, promote_after_successes=2,
    )
    assert throttle.profile.name == "initial"
    throttle.observe_success(); throttle.observe_success()
    assert throttle.profile.name == "step-1"
    throttle.observe_success(); throttle.observe_success()
    assert throttle.profile.name == "maximum"
    throttle.observe_failure("timeout", system_failure=False)
    assert throttle.profile.name == "step-1"
    assert events == [
        ("initial", "restart_at_initial"),
        ("step-1", "healthy_step_up"),
        ("maximum", "healthy_step_up"),
        ("step-1", "failure_step_down:timeout"),
    ]


def test_initial_approval_never_promotes_and_restart_returns_to_initial():
    throttle = AdaptiveThrottle(profiles(), approved_max_level="initial", promote_after_successes=1)
    for _ in range(10):
        throttle.observe_success()
    assert throttle.profile.name == "initial"
    throttle.observe_failure("service", system_failure=True)
    assert throttle.paused
    throttle.resume_at_initial()
    assert not throttle.paused and throttle.profile.name == "initial"


def test_request_interval_and_symbol_cooldown_are_enforced():
    fake = FakeTime()
    profile = SpeedProfile("initial", 1, 2.0, 2, 60.0)
    throttle = AdaptiveThrottle((profile,), sleeper=fake.sleep, clock=fake.clock)
    throttle.before_request(); throttle.before_request()
    throttle.complete_symbol(); throttle.complete_symbol()
    assert fake.sleeps == [2.0, 60.0]


def test_profiles_reject_more_than_two_requests_per_second():
    with pytest.raises(ValueError, match="two requests"):
        SpeedProfile("unsafe", 4, 0.49, 100, 30)


def test_config_profiles_preserve_confirmed_bounds():
    built = build_speed_profiles({
        "initial": {"workers": 1, "request_interval_seconds": 2, "cooldown_every_symbols": 20, "cooldown_seconds": 60},
        "maximum": {"workers": 4, "requests_per_second": 2, "cooldown_every_symbols": 100, "cooldown_seconds": 30},
    })
    assert built[0] == SpeedProfile("initial", 1, 2.0, 20, 60.0)
    assert built[-1] == SpeedProfile("maximum", 4, 0.5, 100, 30.0)
    assert all(item.workers <= 4 and item.request_interval_seconds >= 0.5 for item in built)
