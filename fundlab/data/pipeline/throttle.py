from __future__ import annotations

from dataclasses import dataclass
from time import monotonic, sleep
from typing import Callable, Sequence


@dataclass(frozen=True)
class SpeedProfile:
    name: str
    workers: int
    request_interval_seconds: float
    cooldown_every_symbols: int
    cooldown_seconds: float

    def __post_init__(self) -> None:
        if not self.name or self.workers < 1:
            raise ValueError("A speed profile requires a name and at least one worker")
        if self.request_interval_seconds < 0.5:
            raise ValueError("Aggregate request rate may not exceed two requests per second")
        if self.cooldown_every_symbols < 1 or self.cooldown_seconds < 0:
            raise ValueError("Cooldown settings must be positive")


ThrottleEventCallback = Callable[[SpeedProfile, str], None]


class AdaptiveThrottle:
    """Restart-safe bounded throttle with gradual promotion and immediate demotion."""

    def __init__(
        self,
        profiles: Sequence[SpeedProfile],
        *,
        approved_max_level: str = "initial",
        event_callback: ThrottleEventCallback | None = None,
        sleeper: Callable[[float], None] = sleep,
        clock: Callable[[], float] = monotonic,
        promote_after_successes: int = 20,
    ) -> None:
        if not profiles or profiles[0].name != "initial":
            raise ValueError("Throttle profiles must begin with the initial profile")
        names = [profile.name for profile in profiles]
        if len(names) != len(set(names)) or approved_max_level not in names:
            raise ValueError("Throttle profile names and approved maximum must be valid")
        self._profiles = tuple(profiles)
        self._approved_index = names.index(approved_max_level)
        self._index = 0
        self._event_callback = event_callback
        self._sleep = sleeper
        self._clock = clock
        self._promote_after = max(1, promote_after_successes)
        self._successes = 0
        self._last_request_at: float | None = None
        self._symbols_since_cooldown = 0
        self.paused = False
        self._emit("restart_at_initial")

    @property
    def profile(self) -> SpeedProfile:
        return self._profiles[self._index]

    def before_request(self) -> None:
        if self.paused:
            raise RuntimeError("Throttle is paused after a provider health failure")
        now = self._clock()
        if self._last_request_at is not None:
            remaining = self.profile.request_interval_seconds - (now - self._last_request_at)
            if remaining > 0:
                self._sleep(remaining)
        self._last_request_at = self._clock()

    def complete_symbol(self) -> None:
        self._symbols_since_cooldown += 1
        if self._symbols_since_cooldown >= self.profile.cooldown_every_symbols:
            self._sleep(self.profile.cooldown_seconds)
            self._symbols_since_cooldown = 0
            self._emit("scheduled_cooldown")

    def observe_success(self, *, latency_seconds: float | None = None) -> None:
        if self.paused:
            return
        if latency_seconds is not None and latency_seconds > max(5.0, self.profile.request_interval_seconds * 4):
            self.observe_failure("high_latency", system_failure=False)
            return
        self._successes += 1
        if self._successes >= self._promote_after and self._index < self._approved_index:
            self._index += 1
            self._successes = 0
            self._symbols_since_cooldown = 0
            self._emit("healthy_step_up")

    def observe_failure(self, reason: str, *, system_failure: bool) -> None:
        self._successes = 0
        if self._index > 0:
            self._index -= 1
            self._symbols_since_cooldown = 0
            self._emit(f"failure_step_down:{reason}")
            return
        if system_failure:
            self.paused = True
            self._emit(f"provider_pause:{reason}")
        else:
            self._emit(f"failure_hold_initial:{reason}")

    def resume_at_initial(self, reason: str = "manual_resume") -> None:
        self._index = 0
        self._successes = 0
        self._symbols_since_cooldown = 0
        self._last_request_at = None
        self.paused = False
        self._emit(reason)

    def _emit(self, reason: str) -> None:
        if self._event_callback is not None:
            self._event_callback(self.profile, reason)


def build_speed_profiles(config: dict) -> tuple[SpeedProfile, ...]:
    initial = config["initial"]
    maximum = config["maximum"]
    initial_profile = SpeedProfile(
        "initial",
        int(initial["workers"]),
        float(initial["request_interval_seconds"]),
        int(initial["cooldown_every_symbols"]),
        float(initial["cooldown_seconds"]),
    )
    maximum_profile = SpeedProfile(
        "maximum",
        int(maximum["workers"]),
        max(0.5, 1.0 / float(maximum["requests_per_second"])),
        int(maximum["cooldown_every_symbols"]),
        float(maximum["cooldown_seconds"]),
    )
    if initial_profile.workers > maximum_profile.workers:
        raise ValueError("Initial workers cannot exceed the approved maximum")
    if initial_profile.request_interval_seconds < maximum_profile.request_interval_seconds:
        raise ValueError("Initial request rate cannot exceed the approved maximum")
    if initial_profile == maximum_profile:
        return (initial_profile,)
    middle = SpeedProfile(
        "step-1",
        min(maximum_profile.workers, max(initial_profile.workers + 1, 2)),
        max(maximum_profile.request_interval_seconds, initial_profile.request_interval_seconds / 2),
        min(maximum_profile.cooldown_every_symbols, max(initial_profile.cooldown_every_symbols, 40)),
        max(maximum_profile.cooldown_seconds, min(initial_profile.cooldown_seconds, 45.0)),
    )
    return (initial_profile, middle, maximum_profile)
