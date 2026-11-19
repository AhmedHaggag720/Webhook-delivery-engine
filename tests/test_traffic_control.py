import time
import pytest
from unittest.mock import AsyncMock, patch

class SimulatedTokenBucket:
    def __init__(self, capacity: float, refill_rate: float):
        self.capacity = capacity
        self.refill_rate = refill_rate
        self.tokens = capacity
        self.last_updated = time.time()

    def request(self, requested: float = 1.0, now: float = None) -> tuple[bool, float, float]:
        current_time = now if now is not None else time.time()
        elapsed = max(0.0, current_time - self.last_updated)
        self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_rate)
        self.last_updated = current_time

        if self.tokens >= requested:
            self.tokens -= requested
            return True, self.tokens, 0.0
        else:
            needed = requested - self.tokens
            wait_time = needed / self.refill_rate
            return False, self.tokens, wait_time

class SimulatedCircuitBreaker:
    def __init__(self, failure_threshold: int = 5, cooldown_seconds: float = 30.0):
        self.threshold = failure_threshold
        self.cooldown = cooldown_seconds
        self.state = "closed"
        self.failure_count = 0
        self.open_until = 0.0
        self.probe_in_flight = 0

    def check(self, now: float) -> tuple[bool, str, float]:
        if self.state == "closed":
            return True, "closed", 0.0
        elif self.state == "open":
            if now >= self.open_until:
                self.state = "half-open"
                self.probe_in_flight = 1
                return True, "half-open", 0.0
            else:
                return False, "open", self.open_until - now
        elif self.state == "half-open":
            if self.probe_in_flight == 0:
                self.probe_in_flight = 1
                return True, "half-open", 0.0
            else:
                return False, "half-open", max(0.0, self.open_until - now)
        return True, "closed", 0.0

    def record_success(self) -> str:
        if self.state in ("half-open", "closed"):
            self.state = "closed"
            self.failure_count = 0
            self.probe_in_flight = 0
            self.open_until = 0.0
        return self.state

    def record_failure(self, now: float) -> tuple[str, int, float]:
        if self.state == "half-open":
            self.state = "open"
            self.open_until = now + self.cooldown
            self.probe_in_flight = 0
            return "open", 0, self.cooldown
        elif self.state == "closed":
            self.failure_count += 1
            if self.failure_count >= self.threshold:
                self.state = "open"
                self.open_until = now + self.cooldown
                self.probe_in_flight = 0
                return "open", self.failure_count, self.cooldown
            else:
                return "closed", self.failure_count, 0.0
        else:
            return "open", self.failure_count, 0.0

def test_token_bucket_rate_limiting():
    tb = SimulatedTokenBucket(capacity=5.0, refill_rate=2.0)
    t0 = 1000.0

    # Consume all 5 tokens
    for i in range(5):
        allowed, remaining, wait = tb.request(requested=1.0, now=t0)
        assert allowed is True
        assert remaining == 5.0 - (i + 1)
        assert wait == 0.0

    # 6th token should be throttled
    allowed, remaining, wait = tb.request(requested=1.0, now=t0)
    assert allowed is False
    assert wait == 1.0 / 2.0  # 0.5s needed to refill 1 token

    # Fast forward 0.5 seconds
    allowed, remaining, wait = tb.request(requested=1.0, now=t0 + 0.5)
    assert allowed is True
    assert wait == 0.0

def test_circuit_breaker_state_transitions():
    cb = SimulatedCircuitBreaker(failure_threshold=3, cooldown_seconds=10.0)
    t = 100.0

    # Initially closed
    allowed, state, rem = cb.check(now=t)
    assert allowed is True
    assert state == "closed"

    # 1st failure -> stays closed
    state, failures, _ = cb.record_failure(now=t)
    assert state == "closed"
    assert failures == 1

    # 2nd failure -> stays closed
    state, failures, _ = cb.record_failure(now=t)
    assert state == "closed"
    assert failures == 2

    # 3rd failure -> trips to OPEN
    state, failures, cooldown = cb.record_failure(now=t)
    assert state == "open"
    assert failures == 3
    assert cooldown == 10.0

    # During cooldown (t + 5s) -> short circuits
    allowed, state, rem = cb.check(now=t + 5.0)
    assert allowed is False
    assert state == "open"
    assert rem == 5.0

    # After cooldown (t + 10.1s) -> transitions to HALF-OPEN, allows 1 probe
    allowed, state, rem = cb.check(now=t + 10.1)
    assert allowed is True
    assert state == "half-open"

    # Concurrent request while probe in flight should be blocked
    allowed, state, rem = cb.check(now=t + 10.2)
    assert allowed is False
    assert state == "half-open"

    # Scenario A: Probe succeeds -> transitions to CLOSED
    new_state = cb.record_success()
    assert new_state == "closed"
    assert cb.state == "closed"
    assert cb.failure_count == 0

    # Subsequent request is allowed
    allowed, state, _ = cb.check(now=t + 11.0)
    assert allowed is True
    assert state == "closed"

def test_circuit_breaker_probe_fails_reopens():
    cb = SimulatedCircuitBreaker(failure_threshold=2, cooldown_seconds=10.0)
    t = 100.0

    # Trip to open
    cb.record_failure(now=t)
    cb.record_failure(now=t)
    assert cb.state == "open"

    # Probe in half-open
    allowed, state, _ = cb.check(now=t + 11.0)
    assert allowed is True
    assert state == "half-open"

    # Probe fails -> immediately re-opens with new cooldown
    state, _, cooldown = cb.record_failure(now=t + 11.5)
    assert state == "open"
    assert cb.state == "open"
    assert cb.open_until == t + 11.5 + 10.0

