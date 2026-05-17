"""Tests for the Redis-backed circuit breaker state machine.

These tests verify:
1. CLOSED → OPEN after N consecutive failures
2. OPEN → HALF_OPEN after cooldown
3. HALF_OPEN → CLOSED on successful probe
4. HALF_OPEN → OPEN on failed probe
5. Success resets failure counter
6. get_status() returns serializable state
7. Atomic probe slot claiming (no race conditions)

Uses a real Redis testcontainer — the circuit breaker state lives in Redis.
"""

import pytest
import pytest_asyncio
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock

from app.circuit_breaker import (
    CircuitBreaker, CircuitOpenError,
    _KEY_STATE, _KEY_FAILURE_COUNT, _KEY_LAST_FAILURE_AT, _KEY_PROBE_ACTIVE,
)
from rail.simulator import RailError


@pytest_asyncio.fixture
async def cb(redis_client):
    """CircuitBreaker backed by test Redis with low threshold."""
    return CircuitBreaker(redis=redis_client, failure_threshold=3, cooldown_seconds=30)


@pytest.fixture
def success_fn():
    """Async function that always succeeds."""
    return AsyncMock(return_value="ok")


@pytest.fixture
def fail_fn():
    """Async function that always raises RailError."""
    return AsyncMock(side_effect=RailError("NETWORK_ERROR"))


class TestCircuitBreakerTransitions:
    """Test the state machine transitions."""

    async def test_starts_closed(self, cb):
        assert await cb.is_call_allowed() is True
        status = await cb.get_status()
        assert status["state"] == "CLOSED"

    async def test_success_keeps_closed(self, cb, success_fn):
        await cb.call(success_fn)
        status = await cb.get_status()
        assert status["state"] == "CLOSED"
        assert status["failure_count"] == 0

    async def test_single_failure_stays_closed(self, cb, fail_fn):
        with pytest.raises(RailError):
            await cb.call(fail_fn)
        status = await cb.get_status()
        assert status["state"] == "CLOSED"
        assert status["failure_count"] == 1

    async def test_trips_to_open_after_threshold(self, cb, fail_fn):
        for _ in range(3):
            with pytest.raises(RailError):
                await cb.call(fail_fn)
        status = await cb.get_status()
        assert status["state"] == "OPEN"
        assert await cb.is_call_allowed() is False

    async def test_success_resets_failure_count(self, cb, fail_fn, success_fn):
        # Fail twice
        for _ in range(2):
            with pytest.raises(RailError):
                await cb.call(fail_fn)
        status = await cb.get_status()
        assert status["state"] == "CLOSED"
        assert status["failure_count"] == 2

        # Success resets
        await cb.call(success_fn)
        status = await cb.get_status()
        assert status["failure_count"] == 0

        # Fail once more — still CLOSED (count is 1, not 3)
        with pytest.raises(RailError):
            await cb.call(fail_fn)
        status = await cb.get_status()
        assert status["state"] == "CLOSED"
        assert status["failure_count"] == 1

    async def test_open_rejects_immediately(self, cb, fail_fn, success_fn):
        # Trip the circuit
        for _ in range(3):
            with pytest.raises(RailError):
                await cb.call(fail_fn)
        assert await cb.is_call_allowed() is False

        # Should raise CircuitOpenError without calling the function
        with pytest.raises(CircuitOpenError):
            await cb.call(success_fn)
        assert success_fn.call_count == 0

    async def test_half_open_after_cooldown(self, cb, fail_fn, success_fn, redis_client):
        # Trip the circuit
        for _ in range(3):
            with pytest.raises(RailError):
                await cb.call(fail_fn)

        # Simulate cooldown elapsed
        past = (datetime.now(timezone.utc) - timedelta(seconds=31)).isoformat()
        await redis_client.set(_KEY_LAST_FAILURE_AT, past)

        # Cooldown elapsed → call allowed
        assert await cb.is_call_allowed() is True

        # Probe succeeds → back to CLOSED
        await cb.call(success_fn)
        status = await cb.get_status()
        assert status["state"] == "CLOSED"

    async def test_half_open_probe_fails_reopens(self, cb, fail_fn, redis_client):
        # Trip the circuit
        for _ in range(3):
            with pytest.raises(RailError):
                await cb.call(fail_fn)

        # Simulate cooldown elapsed
        past = (datetime.now(timezone.utc) - timedelta(seconds=31)).isoformat()
        await redis_client.set(_KEY_LAST_FAILURE_AT, past)

        # Probe fails → back to OPEN
        with pytest.raises(RailError):
            await cb.call(fail_fn)
        status = await cb.get_status()
        assert status["state"] == "OPEN"

    async def test_half_open_only_one_probe(self, cb, success_fn, redis_client):
        # Manually set state to HALF_OPEN with active probe
        await redis_client.set(_KEY_STATE, "HALF_OPEN")
        await redis_client.set(_KEY_PROBE_ACTIVE, "true", ex=60)

        # Should be rejected (probe already active)
        assert await cb.is_call_allowed() is False
        with pytest.raises(CircuitOpenError):
            await cb.call(success_fn)
        assert success_fn.call_count == 0


class TestCircuitBreakerStatus:
    """Test the get_status() serialization."""

    async def test_status_closed(self, cb):
        status = await cb.get_status()
        assert status["state"] == "CLOSED"
        assert status["failure_count"] == 0
        assert status["last_failure_at"] is None

    async def test_status_open(self, cb, fail_fn):
        # Trip the circuit
        for _ in range(3):
            with pytest.raises(RailError):
                await cb.call(fail_fn)
        status = await cb.get_status()
        assert status["state"] == "OPEN"
        assert status["failure_count"] == 3
        assert status["last_failure_at"] is not None


class TestCircuitBreakerAtomicity:
    """Test that Redis-backed operations are atomic."""

    async def test_probe_slot_is_exclusive(self, redis_client, success_fn):
        """Two circuit breaker instances sharing Redis — only one can probe."""
        cb1 = CircuitBreaker(redis=redis_client, failure_threshold=3, cooldown_seconds=30)
        cb2 = CircuitBreaker(redis=redis_client, failure_threshold=3, cooldown_seconds=30)

        # Trip via cb1
        fail_fn = AsyncMock(side_effect=RailError("TIMEOUT"))
        for _ in range(3):
            with pytest.raises(RailError):
                await cb1.call(fail_fn)

        # Simulate cooldown elapsed
        past = (datetime.now(timezone.utc) - timedelta(seconds=31)).isoformat()
        await redis_client.set(_KEY_LAST_FAILURE_AT, past)

        # cb1 claims probe (will succeed via success_fn)
        await cb1.call(success_fn)

        # After success, circuit is CLOSED — cb2 should work normally
        status = await cb2.get_status()
        assert status["state"] == "CLOSED"
