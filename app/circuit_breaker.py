"""Circuit breaker — Redis-backed three-state machine protecting the bank rail.

State lives in Redis, not in process memory. This means:
- State survives process restarts (no "amnesia after deploy")
- Multiple workers share the same circuit state
- Atomic Lua scripts eliminate race conditions between coroutines/workers

State transitions:
    CLOSED ──(N consecutive failures)──▶ OPEN
    OPEN   ──(cooldown elapsed + probe claimed)──▶ HALF_OPEN
    HALF_OPEN ──(probe succeeds)──▶ CLOSED
    HALF_OPEN ──(probe fails)──▶ OPEN

Redis key model:
    circuit_breaker:state           "CLOSED" | "OPEN" | "HALF_OPEN"
    circuit_breaker:failure_count   integer as string
    circuit_breaker:last_failure_at ISO 8601 timestamp
    circuit_breaker:probe_active    exists = probe in flight (TTL for self-healing)

Default (keys absent): CLOSED. A fresh Redis or FLUSHDB self-heals to open traffic.
"""

from datetime import datetime, timezone, timedelta

from redis.asyncio import Redis

from rail.simulator import RailError


class CircuitOpenError(Exception):
    """Raised when a call is attempted while the circuit is OPEN."""
    code = "CIRCUIT_OPEN"


# State constants
STATE_CLOSED = "CLOSED"
STATE_OPEN = "OPEN"
STATE_HALF_OPEN = "HALF_OPEN"

# Redis key constants
_KEY_STATE = "circuit_breaker:state"
_KEY_FAILURE_COUNT = "circuit_breaker:failure_count"
_KEY_LAST_FAILURE_AT = "circuit_breaker:last_failure_at"
_KEY_PROBE_ACTIVE = "circuit_breaker:probe_active"

# Lua script: record a failure and conditionally trip the circuit.
# KEYS: [state, failure_count, last_failure_at, probe_active]
# ARGV: [threshold, now_iso]
# Returns: new state string
_RECORD_FAILURE_LUA = """
local state_key = KEYS[1]
local count_key = KEYS[2]
local last_fail_key = KEYS[3]
local probe_key = KEYS[4]
local threshold = tonumber(ARGV[1])
local now_iso = ARGV[2]

local current_state = redis.call('GET', state_key) or 'CLOSED'
local new_count = redis.call('INCR', count_key)
redis.call('SET', last_fail_key, now_iso)

if current_state == 'HALF_OPEN' then
    redis.call('SET', state_key, 'OPEN')
    redis.call('DEL', probe_key)
    redis.call('SET', count_key, '1')
    return 'OPEN'
elseif new_count >= threshold then
    redis.call('SET', state_key, 'OPEN')
    return 'OPEN'
end
return current_state
"""

# Lua script: record a success and reset the circuit.
# KEYS: [state, failure_count, last_failure_at, probe_active]
# Returns: "CLOSED"
_RECORD_SUCCESS_LUA = """
redis.call('SET', KEYS[1], 'CLOSED')
redis.call('SET', KEYS[2], '0')
redis.call('DEL', KEYS[3])
redis.call('DEL', KEYS[4])
return 'CLOSED'
"""

# Lua script: atomically claim the probe slot (OPEN → HALF_OPEN).
# KEYS: [state, probe_active]
# ARGV: [probe_ttl_seconds]
# Returns: 1 if claimed, 0 if not (already taken, or state isn't OPEN)
_CLAIM_PROBE_LUA = """
local state_key = KEYS[1]
local probe_key = KEYS[2]
local probe_ttl = tonumber(ARGV[1])

local current_state = redis.call('GET', state_key) or 'CLOSED'
if current_state ~= 'OPEN' then
    return 0
end

-- SET NX: only one caller can claim the probe
local claimed = redis.call('SET', probe_key, 'true', 'NX', 'EX', probe_ttl)
if claimed then
    redis.call('SET', state_key, 'HALF_OPEN')
    return 1
end
return 0
"""

_ALL_KEYS = [_KEY_STATE, _KEY_FAILURE_COUNT, _KEY_LAST_FAILURE_AT, _KEY_PROBE_ACTIVE]

# Self-healing: if a probe caller crashes without reporting success/failure,
# the probe_active key expires after this TTL and the circuit falls back to
# HALF_OPEN with no active probe — the next caller can try again.
_PROBE_TTL_SECONDS = 60


class CircuitBreaker:
    def __init__(self, redis: Redis, failure_threshold: int = 3, cooldown_seconds: int = 30):
        self._redis = redis
        self.failure_threshold = failure_threshold
        self.cooldown_seconds = cooldown_seconds

    async def is_call_allowed(self) -> bool:
        """Pre-flight check: can a new call proceed?

        Used by the withdrawal endpoint BEFORE debiting the user. If False,
        return 503 immediately — don't debit just to compensate.
        """
        state = await self._redis.get(_KEY_STATE) or STATE_CLOSED

        if state == STATE_CLOSED:
            return True

        if state == STATE_OPEN:
            if await self._cooldown_elapsed():
                return True
            return False

        if state == STATE_HALF_OPEN:
            probe_active = await self._redis.exists(_KEY_PROBE_ACTIVE)
            return not probe_active

        return False

    async def call(self, fn, *args, **kwargs):
        """Execute `fn` through the circuit breaker.

        Atomically handles state transitions:
        - OPEN + cooldown elapsed → claims probe slot via Lua (HALF_OPEN)
        - HALF_OPEN + probe active → rejects (CircuitOpenError)
        - Success → resets to CLOSED
        - RailError → records failure, may trip to OPEN
        """
        state = await self._redis.get(_KEY_STATE) or STATE_CLOSED

        if state == STATE_OPEN:
            if await self._cooldown_elapsed():
                claimed = await self._redis.eval(
                    _CLAIM_PROBE_LUA,
                    2,
                    _KEY_STATE, _KEY_PROBE_ACTIVE,
                    _PROBE_TTL_SECONDS,
                )
                if not claimed:
                    raise CircuitOpenError()
            else:
                raise CircuitOpenError()
        elif state == STATE_HALF_OPEN:
            probe_active = await self._redis.exists(_KEY_PROBE_ACTIVE)
            if probe_active:
                raise CircuitOpenError()
            claimed = await self._redis.set(
                _KEY_PROBE_ACTIVE, "true", nx=True, ex=_PROBE_TTL_SECONDS
            )
            if not claimed:
                raise CircuitOpenError()

        try:
            result = await fn(*args, **kwargs)
            await self._on_success()
            return result
        except RailError:
            await self._on_failure()
            raise

    async def _on_success(self):
        await self._redis.eval(
            _RECORD_SUCCESS_LUA,
            4,
            *_ALL_KEYS,
        )

    async def _on_failure(self):
        now_iso = datetime.now(timezone.utc).isoformat()
        await self._redis.eval(
            _RECORD_FAILURE_LUA,
            4,
            *_ALL_KEYS,
            self.failure_threshold,
            now_iso,
        )

    async def _cooldown_elapsed(self) -> bool:
        last_failure_raw = await self._redis.get(_KEY_LAST_FAILURE_AT)
        if last_failure_raw is None:
            return True
        last_failure = datetime.fromisoformat(last_failure_raw)
        elapsed = datetime.now(timezone.utc) - last_failure
        return elapsed >= timedelta(seconds=self.cooldown_seconds)

    async def get_status(self) -> dict:
        """Serializable status dict for GET /v1/health."""
        state = await self._redis.get(_KEY_STATE) or STATE_CLOSED
        failure_count = await self._redis.get(_KEY_FAILURE_COUNT) or "0"
        last_failure_at = await self._redis.get(_KEY_LAST_FAILURE_AT)
        return {
            "state": state,
            "failure_count": int(failure_count),
            "last_failure_at": last_failure_at,
        }
