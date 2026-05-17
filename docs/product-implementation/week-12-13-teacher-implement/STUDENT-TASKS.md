# Week 12–13 Student Tasks — What You Implement

Three service files and five test files have `# TODO: student` markers. This document explains each one, what concepts it teaches, and how to verify your work.

---

## Week 12 Tasks (Circuit Breaker + Saga Recovery)

---

### Task 1: Circuit Breaker Tests (`tests/test_circuit_breaker.py`)

**Difficulty:** Warm-up
**Concepts:** State machine testing, mocking async functions
**Lines to write:** ~60 across 9 test functions

The circuit breaker itself is teacher-provided (fully working). Your job is to prove it works correctly by writing tests that exercise every state transition.

#### What to do

Each test in `TestCircuitBreakerTransitions` follows the pattern:
1. Start from a known state
2. Trigger N failures or successes via `cb.call(fn)`
3. Assert the resulting state, failure_count, and `is_call_allowed`

Key patterns:
- **Triggering failures:** `cb.call(fail_fn)` raises `RailError` — catch it with `pytest.raises(RailError)`
- **Simulating cooldown:** Manually set `cb.last_failure_at = datetime.now(timezone.utc) - timedelta(seconds=31)`
- **Asserting OPEN rejects:** `pytest.raises(CircuitOpenError)` and confirm `fn` was never called

#### Verify

```bash
uv run pytest tests/test_circuit_breaker.py -v
```

---

### Task 2: Saga Recovery Worker (`workers/saga_recovery.py`)

**Difficulty:** Core exercise (hardest task this week)
**Concepts:** FOR UPDATE SKIP LOCKED, two-phase claim+resolve, idempotent compensation, crash recovery patterns
**Lines to write:** ~80 across 5 functions

#### What to do

Implement the body of these functions (all marked with `# TODO: student`):

##### 2a. `recover_stuck_withdrawals()` — Phase 1: the claim query

```python
# Inside db_session_factory() + db.begin():
result = await db.execute(
    select(Withdrawal.id)
    .where(Withdrawal.status.in_(["pending", "submitted"]))
    .where(Withdrawal.updated_at < cutoff)
    .with_for_update(skip_locked=True)
    .limit(BATCH_LIMIT)
)
stuck_ids = [row[0] for row in result.all()]
```

Why `SKIP LOCKED`: if two recovery instances run simultaneously, they skip each other's claimed rows instead of blocking. No double-resolve.

Why `LIMIT 20`: avoid holding too many row locks. Multiple cycles will catch all stuck rows.

##### 2b. `_resolve_one()` — Phase 2: lock one row and dispatch

```python
async with db_session_factory() as db:
    async with db.begin():
        row = await db.execute(
            select(Withdrawal).where(Withdrawal.id == withdrawal_id).with_for_update()
        )
        w = row.scalar_one_or_none()
        if w is None or w.status not in ("pending", "submitted"):
            return  # already resolved by another instance
        if w.status == "pending":
            await _recover_pending(db, w, circuit_breaker, rail)
        elif w.status == "submitted":
            await _recover_submitted(db, w, rail)
```

##### 2c. `_recover_pending()` — Retry or compensate

The strategy mirrors the normal withdrawal flow:
1. Check circuit breaker → compensate if OPEN
2. Transition to `submitted` + flush
3. Call rail via `circuit_breaker.call()`
4. Success → `_complete()`. Failure → `_compensate()`

##### 2d. `_recover_submitted()` — Query rail or timeout

Two branches:
- **Has `external_reference`:** query rail status → complete or compensate
- **No `external_reference`:** compensate after 30-minute hard timeout

##### 2e. `_compensate()` and `_complete()` — Terminal state helpers

`_compensate`: two reversal ledger entries + status update + event publish. The idempotency_key on the credit leg (`reversal:{withdrawal.id}`) is the safety net against double-compensation.

`_complete`: status update + event publish. Set `submitted_at` if not already set (recovery path where we skipped the submitted transition).

#### Key insight: Why the lock is held during rail I/O

In `_resolve_one`, the `FOR UPDATE` lock is held for the full resolution (including the rail call). This is intentional:
- Only ONE row is locked at a time (not the batch)
- The lock prevents another recovery instance from double-resolving
- The rail simulator returns instantly; production would use a short HTTP timeout (10s)

If this becomes a bottleneck in production, replace with an optimistic `claimed_at` column.

#### Verify

```bash
uv run pytest tests/test_saga_recovery.py -v
```

---

### Task 3: Saga Recovery Tests (`tests/test_saga_recovery.py`)

**Difficulty:** Medium
**Concepts:** Testing crash scenarios by creating stuck state directly in DB
**Lines to write:** ~80 across 7 test functions

Each test follows the pattern:
1. Create a withdrawal row stuck in a specific state (using the `_create_stuck_withdrawal` helper)
2. Configure the circuit breaker and rail simulator for the scenario
3. Call `recover_stuck_withdrawals()`
4. Assert the withdrawal reached the expected terminal state
5. Assert ledger entries are correct (compensation exists or doesn't)

The `_create_stuck_withdrawal` helper is provided — it creates a withdrawal with the debit ledger entries already written (simulating TX 1 completed, then crash).

#### Verify

```bash
uv run pytest tests/test_saga_recovery.py -v
```

---

### Task 4: Health Endpoint Tests (`tests/test_health.py`)

**Difficulty:** Easy
**Concepts:** Testing unauthenticated endpoints, dependency override
**Lines to write:** ~15 across 2 test functions

#### Verify

```bash
uv run pytest tests/test_health.py -v
```

---

### Task 5: Withdrawal + Circuit Breaker Integration Tests (`tests/test_withdrawal_circuit_breaker.py`)

**Difficulty:** Medium
**Concepts:** Pre-flight rejection, asserting zero side effects
**Lines to write:** ~30 across 3 test functions

Key assertion: when the circuit is OPEN, no withdrawal row AND no ledger entries are created. The user's balance is untouched. This is the whole point of the pre-flight — avoid pointless debit-then-compensate noise.

#### Verify

```bash
uv run pytest tests/test_withdrawal_circuit_breaker.py -v
```

---

## Week 13 Tasks (Scheduled Payments)

---

### Task 6: Scheduled Payment Service (`app/services/scheduled_payment_service.py`)

**Difficulty:** Moderate
**Concepts:** CRUD with validation, soft-delete pattern
**Lines to write:** ~40 across 3 functions

#### 6a. `create_scheduled_payment()`

Validations:
1. `start_at` must be in the future → `InvalidStartTimeError`
2. `to_account_id != from_account_id` → `CannotPaySelfError`
3. Target account must exist and be active → `AccountNotFoundError`

Then create the `ScheduledPayment` row with `next_run_at = start_at` and commit.

#### 6b. `list_scheduled_payments()`

Simple query: all ScheduledPayment rows for the given `from_account_id`, ordered by `created_at DESC`.

#### 6c. `cancel_scheduled_payment()`

Load by ID, check ownership, set `status = 'cancelled'`, commit. 404 if not found or not owned.

#### Verify

```bash
uv run pytest tests/test_scheduled_payments.py::TestScheduledPaymentCRUD -v
```

---

### Task 7: Payment Scheduler Worker (`workers/payment_scheduler.py`)

**Difficulty:** Core exercise
**Concepts:** Two-phase claim+execute, FOR UPDATE SKIP LOCKED, idempotency key construction, crash safety
**Lines to write:** ~50 across 2 functions

#### 7a. `_poll_and_execute()` — Phase 1: Claim query

```python
async with db_session_factory() as db:
    async with db.begin():
        result = await db.execute(
            select(ScheduledPayment)
            .where(ScheduledPayment.status == "active")
            .where(ScheduledPayment.next_run_at <= datetime.now(timezone.utc))
            .with_for_update(skip_locked=True)
            .limit(CLAIM_BATCH_SIZE)
        )
        due_payments = result.scalars().all()
```

#### 7b. `_execute_payment()` — Execute + advance

**Step A: Call `transfer()`**
```python
try:
    async with db_session_factory() as db:
        result = await transfer(
            db=db, redis=redis,
            from_account_id=payment.from_account_id,
            to_account_id=payment.to_account_id,
            amount=payment.amount,
            idempotency_key=idempotency_key,
        )
        transfer_id = UUID(result.transfer_id)
except InsufficientBalanceError:
    skip_reason = "INSUFFICIENT_BALANCE"
except AccountNotFoundError:
    skip_reason = "ACCOUNT_INACTIVE"
except Exception:
    return  # unexpected — don't advance, retry next cycle
```

**Step B: Advance schedule**
```python
async with db_session_factory() as db:
    async with db.begin():
        fresh = await db.execute(
            select(ScheduledPayment).where(ScheduledPayment.id == payment.id).with_for_update()
        )
        fresh_payment = fresh.scalar_one()
        if fresh_payment.next_run_at != payment.next_run_at:
            return  # already advanced by another instance

        db.add(ScheduledPaymentExecution(...))
        fresh_payment.next_run_at = advance_schedule(fresh_payment.next_run_at, fresh_payment.frequency)
        fresh_payment.updated_at = datetime.now(timezone.utc)

        if skip_reason:
            publish_event(db, "payment.events", "payment.skipped", PaymentSkippedPayload(...))
        else:
            publish_event(db, "payment.events", "payment.executed", PaymentExecutedPayload(...))
```

#### Key insight: Crash between Step A and Step B

If the scheduler crashes after `transfer()` commits but before advancing `next_run_at`:
- Next poll re-claims the same payment (still due)
- Calls `transfer()` with the same idempotency key → returns cached result (no double-debit)
- Step B advances the schedule normally

This is why the idempotency key includes `next_run_at`: each execution slot gets a unique key.

#### Verify

```bash
uv run pytest tests/test_scheduled_payments.py::TestSchedulerExecution -v
```

---

### Task 8: Scheduled Payment Tests (`tests/test_scheduled_payments.py`)

**Difficulty:** Medium
**Concepts:** HTTP CRUD testing, worker testing by calling functions directly
**Lines to write:** ~100 across 12 test functions

Split into two groups:
- **`TestScheduledPaymentCRUD`**: HTTP-level tests (create, list, cancel, validation errors)
- **`TestSchedulerExecution`**: Worker-level tests (call `_poll_and_execute` directly, assert DB state)

For scheduler tests, you work with `consumer_db_factory` and `redis_client` fixtures — not the HTTP client. Create accounts via `account_factory`, seed via `seed_factory`, then call the scheduler function directly.

#### Verify

```bash
uv run pytest tests/test_scheduled_payments.py -v
```

---

## Recommended Implementation Order

### Week 12 (3–4 hours)

1. **Task 1** (circuit breaker tests) — warm up, understand the state machine by testing it
2. **Task 4** (health endpoint tests) — quick wins, build confidence
3. **Task 5** (withdrawal + circuit breaker integration) — verify the pre-flight wiring
4. **Task 2** (saga recovery implementation) — the main event
5. **Task 3** (saga recovery tests) — prove your recovery logic works

### Week 13 (3 hours)

6. **Task 6** (scheduled payment service CRUD) — straightforward validation + DB writes
7. **Task 8 CRUD tests** — test the endpoints you just implemented
8. **Task 7** (scheduler worker) — the core orchestration logic
9. **Task 8 scheduler tests** — prove the worker handles all scenarios

---

## Phase-Level Verification (after all tasks done)

Run the full test suite to confirm everything integrates:

```bash
uv run pytest -v
```

Then test the **phase-level ledger invariant**: after all deposits, withdrawals, compensations, and scheduled payments, `SUM(all account balances) = 0`.

```python
# Add to any test that exercises the full flow:
from sqlalchemy import func, case
result = await db.execute(
    select(func.sum(
        case(
            (LedgerEntry.direction == "credit", LedgerEntry.amount),
            else_=-LedgerEntry.amount,
        )
    ))
)
assert result.scalar() == Decimal("0")
```
