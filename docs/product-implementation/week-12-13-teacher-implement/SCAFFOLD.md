# Week 12–13 Scaffold — What the Teacher Built

Everything listed here is **complete, working code** — no TODOs. It is the foundation your implementation builds on.

---

## 1. Circuit Breaker (`app/circuit_breaker.py`) — FULL implementation (Redis-backed)

A three-state machine (CLOSED → OPEN → HALF_OPEN → CLOSED) protecting the bank rail. State lives in Redis — survives restarts, shared across workers, atomic transitions via Lua scripts. **You do NOT implement this — it's teacher-provided.**

### What it does:
- Tracks consecutive rail failures in Redis
- After 3 failures: trips to OPEN (atomically via Lua script)
- After 30-second cooldown: transitions to HALF_OPEN (one probe allowed, claimed via `SET NX`)
- Probe succeeds → CLOSED. Probe fails → back to OPEN.
- Self-healing: probe slot has 60s TTL — if caller crashes, slot auto-releases.

### Redis keys:
```
circuit_breaker:state           "CLOSED" | "OPEN" | "HALF_OPEN"
circuit_breaker:failure_count   integer as string
circuit_breaker:last_failure_at ISO timestamp
circuit_breaker:probe_active    exists = probe in flight (60s TTL)
```

### Key API:
```python
cb = CircuitBreaker(redis=redis, failure_threshold=3, cooldown_seconds=30)
await cb.is_call_allowed()  # Async method: reads state from Redis
await cb.call(fn, ...)      # Execute fn through the breaker (Lua scripts for atomicity)
await cb.get_status()       # Returns {"state": "CLOSED", "failure_count": 0, "last_failure_at": null}
```

### Integration (already wired):
- **`app/dependencies.py`**: `get_circuit_breaker(redis)` — constructs CB with injected Redis client
- **`app/routers/withdrawals.py`**: Pre-flight check (`if not await circuit_breaker.is_call_allowed(): raise BankRailUnavailableError()`)
- **`app/services/withdrawal_service.py`**: Rail call goes through `circuit_breaker.call()` (records success/failure)
- **`app/main.py` lifespan**: Constructs CB from Redis for recovery worker

---

## 2. Health Endpoint (`app/routers/health.py`) — FULL implementation

`GET /v1/health` — unauthenticated, returns system status:
```json
{
  "status": "ok",
  "checks": { "database": "ok", "redis": "ok" },
  "circuit_breaker": { "state": "CLOSED", "failure_count": 0, "last_failure_at": null }
}
```

---

## 3. App Lifespan Update (`app/main.py`) — FULL implementation

The lifespan now:
1. Creates `CircuitBreaker` singleton → `app.state.circuit_breaker`
2. Creates `BankRailSimulator` singleton → `app.state.rail`
3. Runs startup saga recovery (fire-and-forget asyncio task)
4. Starts the background **recovery loop** (every 5 minutes)
5. Starts the background **scheduler loop** (every 10 seconds)
6. Cancels all background tasks on shutdown

---

## 4. Withdrawal Router Update (`app/routers/withdrawals.py`) — FULL implementation

Two changes from Week 11:
1. **Pre-flight circuit breaker check**: `if not circuit_breaker.is_call_allowed: raise BankRailUnavailableError()` — returns 503 before any DB work.
2. **Passes `circuit_breaker` to the service**: `create_withdrawal(..., circuit_breaker=circuit_breaker)`

---

## 5. Withdrawal Service Update (`app/services/withdrawal_service.py`) — FULL implementation

The rail call now routes through the circuit breaker:
```python
if circuit_breaker:
    result = await circuit_breaker.call(rail.send_withdrawal, ...)
else:
    result = await rail.send_withdrawal(...)
```
On `CircuitOpenError`: compensates (same as `RailError`).

---

## 6. Data Layer — Scheduled Payments (Week 13)

### `app/models/scheduled_payment.py` — FULL (ORM model)

| Column | Type | Constraint | Purpose |
|--------|------|------------|---------|
| `id` | UUID | PK | Row identity |
| `from_account_id` | UUID | FK → accounts | Sender |
| `to_account_id` | UUID | FK → accounts | Receiver |
| `amount` | NUMERIC(19,4) | CHECK > 0 | Payment amount |
| `currency` | VARCHAR(3) | default 'USD' | ISO 4217 |
| `frequency` | VARCHAR(20) | CHECK IN (daily, weekly, monthly) | Schedule cadence |
| `next_run_at` | TIMESTAMPTZ | NOT NULL | When the next execution fires |
| `status` | VARCHAR(20) | default 'active' | active \| cancelled |
| `created_at` | TIMESTAMPTZ | NOT NULL | Creation time |
| `updated_at` | TIMESTAMPTZ | NOT NULL | Last modification |

### `app/models/scheduled_payment_execution.py` — FULL (ORM model)

| Column | Type | Purpose |
|--------|------|---------|
| `id` | UUID | PK |
| `scheduled_payment_id` | UUID FK | Which payment this execution belongs to |
| `scheduled_for` | TIMESTAMPTZ | The `next_run_at` value that triggered this |
| `result` | VARCHAR(20) | executed \| skipped |
| `skip_reason` | VARCHAR(100) | NULL if executed. e.g. "INSUFFICIENT_BALANCE" |
| `transfer_id` | UUID | References transfers.id if executed. NULL if skipped. |
| `executed_at` | TIMESTAMPTZ | When this execution ran |

### `alembic/versions/0009_add_scheduled_payments.py` — FULL (migration)

Creates both tables with appropriate indexes.

### `app/schemas/scheduled_payment.py` — FULL (Pydantic schemas)

- `ScheduledPaymentRequest`: validates amount > 0, frequency in (daily, weekly, monthly)
- `ScheduledPaymentResponse`: API response shape

---

## 7. Event Payloads (`app/events/schemas.py`) — FULL additions

Two new payload models registered in `PAYLOAD_MODELS`:
- `PaymentExecutedPayload` — fields: scheduled_payment_id, transfer_id, amount, currency, from_account_id, to_account_id
- `PaymentSkippedPayload` — fields: scheduled_payment_id, amount, currency, from_account_id, to_account_id, skip_reason

---

## 8. Exception Classes (`app/exceptions.py`) — FULL additions

- `ScheduledPaymentNotFoundError` (404)
- `InvalidStartTimeError` (400, `INVALID_START_TIME`)
- `CannotPaySelfError` (400, `CANNOT_PAY_SELF`)

---

## 9. Router (`app/routers/scheduled_payments.py`) — FULL implementation

Wires the three endpoints to the service layer:
- `POST /v1/scheduled-payments` → `create_scheduled_payment()`
- `GET /v1/scheduled-payments` → `list_scheduled_payments()`
- `DELETE /v1/scheduled-payments/{id}` → `cancel_scheduled_payment()`

All derive `from_account_id` from JWT (same pattern as other routers).

---

## 10. Dependency (`python-dateutil`) — Added to `pyproject.toml`

Required for `relativedelta` in the scheduler's `advance_schedule()`. `timedelta` cannot do "+1 calendar month" correctly.

---

## 11. Test Infrastructure

- `tests/conftest.py`: imports new models (`ScheduledPayment`, `ScheduledPaymentExecution`) and TRUNCATEs their tables between tests.
- Test skeleton files created with TODO markers (see STUDENT-TASKS.md).

---

## 12. Worker Skeletons (function signatures + docstrings only)

### `workers/saga_recovery.py`
- `recover_stuck_withdrawals(db_session_factory, circuit_breaker, rail)` — entry point
- `_resolve_one(db_session_factory, withdrawal_id, circuit_breaker, rail)` — per-withdrawal resolver
- `_recover_pending(db, withdrawal, circuit_breaker, rail)` — pending recovery strategy
- `_recover_submitted(db, withdrawal, rail)` — submitted recovery strategy
- `_compensate(db, withdrawal, failure_code)` — write reversal entries
- `_complete(db, withdrawal, external_reference)` — mark as completed

### `workers/payment_scheduler.py`
- `scheduler_loop(db_session_factory, redis)` — main loop (called from lifespan)
- `_poll_and_execute(db_session_factory, redis)` — one poll cycle
- `_execute_payment(db_session_factory, redis, payment)` — execute + advance one payment
- `advance_schedule(current, frequency)` — **FULL implementation** (teacher-provided)

### `app/services/scheduled_payment_service.py`
- `create_scheduled_payment(...)` — validates + creates
- `list_scheduled_payments(...)` — query all for account
- `cancel_scheduled_payment(...)` — soft-delete
- `_payment_to_response(p)` — **FULL implementation** (teacher-provided helper)
