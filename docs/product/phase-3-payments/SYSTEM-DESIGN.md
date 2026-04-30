# System Design — Phase 3: Advanced Payments

**Phase:** 3 of 6
**Status:** `not started`

---

## 1. Architecture Overview

Phase 3 adds bank rail simulation, withdrawal saga, circuit breaker, and scheduled payment runner.

```mermaid
graph TD
    Client["HTTP Client"]

    subgraph API["FastAPI (Phase 1-2 unchanged)"]
        DepositSvc["DepositService\n(dev/simulate-deposit)"]
        WithdrawalSvc["WithdrawalService\n(saga orchestrator)"]
        ScheduledSvc["ScheduledPaymentService"]
    end

    subgraph Background["Background Processes"]
        Relay["Outbox Relay (Phase 2)"]
        Scheduler["Payment Scheduler\n(poll + FOR UPDATE SKIP LOCKED)"]
        RecoveryJob["Saga Recovery Job\n(startup + cron)"]
    end

    CB["Circuit Breaker\n(CLOSED / OPEN / HALF_OPEN)\nin-memory state"]
    Rail["Simulated Bank Rail\n(configurable fail rate)"]

    PG[("PostgreSQL\n+ deposits\n+ withdrawals\n+ scheduled_payments")]
    Kafka[["Kafka\n+ deposit.events\n+ withdrawal.events\n+ payment.events"]]

    Client --> API
    API --> PG
    WithdrawalSvc --> CB --> Rail
    DepositSvc --> PG

    Scheduler --> PG
    Scheduler -->|transfer()| PG
    RecoveryJob --> PG
    RecoveryJob --> CB --> Rail

    Relay --> Kafka
```

---

## 2. New Database Tables

```sql
-- Alembic 0005_add_deposits
CREATE TABLE deposits (
    id           UUID          PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id   UUID          NOT NULL REFERENCES accounts(id),
    amount       NUMERIC(20,8) NOT NULL,
    external_ref VARCHAR(255)  NOT NULL,
    status       VARCHAR(20)   NOT NULL DEFAULT 'pending',
                 -- pending | processing | completed | failed
    created_at   TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX idx_deposits_external_ref ON deposits (external_ref);
-- Unique constraint is the idempotency guarantee: same external_ref never double-credits.


-- Alembic 0006_add_withdrawals
CREATE TABLE withdrawals (
    id              UUID          PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id      UUID          NOT NULL REFERENCES accounts(id),
    amount          NUMERIC(20,8) NOT NULL,
    destination_ref VARCHAR(255)  NOT NULL,
    saga_status     VARCHAR(20)   NOT NULL DEFAULT 'pending',
                    -- pending | debited | completed | compensated
    external_ref    VARCHAR(255),           -- filled after rail accepts
    created_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_withdrawals_stuck
    ON withdrawals (created_at)
    WHERE saga_status = 'debited';
-- Index for recovery job: find stuck rows without a full table scan.


-- Alembic 0007_add_scheduled_payments
CREATE TABLE scheduled_payments (
    id              UUID          PRIMARY KEY DEFAULT gen_random_uuid(),
    from_account_id UUID          NOT NULL REFERENCES accounts(id),
    to_account_id   UUID          NOT NULL REFERENCES accounts(id),
    amount          NUMERIC(20,8) NOT NULL,
    frequency       VARCHAR(20)   NOT NULL,  -- daily | weekly | monthly
    next_run_at     TIMESTAMPTZ   NOT NULL,
    status          VARCHAR(20)   NOT NULL DEFAULT 'active',
                    -- active | cancelled
    created_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_scheduled_payments_due
    ON scheduled_payments (next_run_at)
    WHERE status = 'active';
```

---

## 3. Deposit Flow (Push Model)

Real bank rails push money to you — they don't wait for you to ask. The `simulate-deposit`
endpoint mimics that: it's a dev-only webhook receiver, not a user-initiated action.

```
POST /v1/dev/simulate-deposit
  { "account_id": "...", "amount": "100.00", "external_ref": "BANK-TXN-001" }
        │
        ▼
  INSERT deposits (external_ref, ...) — will raise on duplicate external_ref
        │
        ▼ (inside same DB transaction)
  INSERT ledger_entry (debit=system_account, credit=user_account, amount)
        │
        ▼
  UPDATE deposits SET status='completed'
        │
        ▼
  INSERT outbox row (topic=deposit.events, event_type=deposit.completed)
```

**Idempotency:** The `UNIQUE INDEX` on `external_ref` makes double-credit impossible. The
second arrival of the same `external_ref` raises a `UniqueViolation` — catch it, return 200
with the original deposit record (do not 409).

---

## 4. Withdrawal Saga (Orchestration)

Orchestration means one function owns the entire flow. The saga state is auditable in the
`withdrawals.saga_status` column — readable at 2am without reconstructing event sequences.

```
POST /v1/withdrawals
  { "account_id": "...", "amount": "50.00", "destination_ref": "MY-BANK-123" }
        │
        ▼
  INSERT withdrawals (saga_status='pending')
        │
        ▼ TX 1: debit sender
  SELECT account FOR UPDATE
  check balance >= amount
  INSERT ledger_entry (debit=user_account, credit=system_account)
  UPDATE withdrawals SET saga_status='debited'
  COMMIT
        │
        ▼ call bank rail (outside any TX)
  circuit_breaker.call(rail.send_withdrawal, withdrawal_id, amount, destination_ref)
        │
    ┌───┴───────────────┐
    ▼ success            ▼ failure
  TX 2a: complete      TX 2b: compensate
  UPDATE saga='completed'  INSERT ledger_entry (debit=system, credit=user — reversal)
  INSERT outbox(completed) UPDATE saga='compensated'
                           INSERT outbox(compensated)
```

**Why debit first?** The debit reserves the funds so concurrent transfers cannot overdraw
before the rail call completes. If the process crashes between TX 1 and TX 2, the recovery job
finds the stuck `saga_status='debited'` row and compensates.

---

## 5. Saga Recovery Job

The gap between TX 1 (debit) and TX 2 (complete/compensate) is where crashes cause limbo.
The recovery job closes that gap.

```python
# workers/saga_recovery.py
async def recover_stuck_withdrawals(db, circuit_breaker, rail):
    cutoff = datetime.utcnow() - timedelta(minutes=5)
    stuck = await db.execute(
        select(Withdrawal)
        .where(Withdrawal.saga_status == "debited")
        .where(Withdrawal.created_at < cutoff)
        .with_for_update(skip_locked=True)  # safe if two recovery jobs run
    )
    for w in stuck:
        try:
            result = await circuit_breaker.call(rail.retry_withdrawal, w.id)
            if result.success:
                await complete_withdrawal(db, w)
            else:
                await compensate_withdrawal(db, w)
        except CircuitOpenError:
            await compensate_withdrawal(db, w)  # rail down — compensate immediately
```

The recovery job runs:
1. On application startup (catches crashes from the last downtime)
2. Every 5 minutes via the scheduler

---

## 6. Circuit Breaker

Custom 50-line implementation — no library needed. State lives in-memory (Redis in production
for multi-instance coordination; in-memory is fine for this single-process learning project).

```python
# app/circuit_breaker.py
class CircuitBreaker:
    """
    States:
      CLOSED     — normal operation, calls pass through
      OPEN       — rail is down, calls fail fast with CircuitOpenError
      HALF_OPEN  — cooldown elapsed, one probe call allowed through
    """
    def __init__(self, failure_threshold=3, cooldown_seconds=30):
        self.state = "CLOSED"
        self.failure_count = 0
        self.last_failure_at = None
        self.failure_threshold = failure_threshold
        self.cooldown_seconds = cooldown_seconds

    async def call(self, fn, *args, **kwargs):
        if self.state == "OPEN":
            elapsed = (datetime.utcnow() - self.last_failure_at).seconds
            if elapsed >= self.cooldown_seconds:
                self.state = "HALF_OPEN"
            else:
                raise CircuitOpenError("Bank rail unavailable")

        try:
            result = await fn(*args, **kwargs)
            if self.state == "HALF_OPEN":
                self._reset()          # probe succeeded → close circuit
            return result
        except RailError as e:
            self._record_failure()
            raise

    def _record_failure(self):
        self.failure_count += 1
        self.last_failure_at = datetime.utcnow()
        if self.failure_count >= self.failure_threshold:
            self.state = "OPEN"

    def _reset(self):
        self.state = "CLOSED"
        self.failure_count = 0
        self.last_failure_at = None

    def status(self) -> dict:
        return {"state": self.state, "failure_count": self.failure_count}
```

**State transitions:**

```
CLOSED ──(3 consecutive failures)──▶ OPEN
OPEN   ──(30s cooldown elapsed)────▶ HALF_OPEN
HALF_OPEN ──(probe succeeds)───────▶ CLOSED
HALF_OPEN ──(probe fails)──────────▶ OPEN
```

Circuit state is exposed in `GET /v1/health`:
```json
{ "circuit_breaker": { "state": "OPEN", "failure_count": 3 } }
```

---

## 7. Scheduled Payments

The scheduler is a simple polling loop — no APScheduler dependency. `FOR UPDATE SKIP LOCKED`
prevents two scheduler instances from executing the same payment concurrently.

```python
# workers/payment_scheduler.py
async def scheduler_loop(db, transfer_service):
    while True:
        async with db.begin():
            due = await db.execute(
                select(ScheduledPayment)
                .where(ScheduledPayment.status == "active")
                .where(ScheduledPayment.next_run_at <= datetime.utcnow())
                .with_for_update(skip_locked=True)
                .limit(50)
            )
            for payment in due:
                try:
                    idempotency_key = f"scheduled:{payment.id}:{payment.next_run_at.isoformat()}"
                    await transfer_service.transfer(
                        from_account_id=payment.from_account_id,
                        to_account_id=payment.to_account_id,
                        amount=payment.amount,
                        idempotency_key=idempotency_key,
                    )
                    payment.next_run_at = advance_schedule(payment.next_run_at, payment.frequency)
                except InsufficientBalanceError:
                    payment.next_run_at = advance_schedule(payment.next_run_at, payment.frequency)
                    # INSERT outbox row: payment.skipped event
        await asyncio.sleep(10)
```

**Idempotency key construction:** `scheduled:{payment_id}:{next_run_at}` — unique per
scheduled slot. If the scheduler crashes after executing the transfer but before advancing
`next_run_at`, the same slot is retried but the idempotency key prevents double-execution.

---

## 8. New API Endpoints

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| `POST` | `/v1/dev/simulate-deposit` | dev-only | Simulate inbound bank webhook |
| `GET` | `/v1/deposits/{id}` | JWT | Deposit status |
| `POST` | `/v1/withdrawals` | JWT | Initiate withdrawal with saga |
| `GET` | `/v1/withdrawals/{id}` | JWT | Withdrawal status + saga_status |
| `POST` | `/v1/scheduled-payments` | JWT | Create recurring payment |
| `GET` | `/v1/scheduled-payments` | JWT | List user's scheduled payments |
| `DELETE` | `/v1/scheduled-payments/{id}` | JWT | Cancel a scheduled payment |

---

## 9. New Events

| Topic | Event Type | Trigger |
|-------|-----------|---------|
| `deposit.events` | `deposit.completed` | Deposit credited successfully |
| `deposit.events` | `deposit.failed` | Deposit processing failed |
| `withdrawal.events` | `withdrawal.initiated` | Withdrawal debited (saga_status=debited) |
| `withdrawal.events` | `withdrawal.completed` | Rail accepted, saga_status=completed |
| `withdrawal.events` | `withdrawal.compensated` | Rail failed, balance restored |
| `payment.events` | `payment.executed` | Scheduled payment transferred successfully |
| `payment.events` | `payment.skipped` | Scheduled payment skipped (insufficient balance) |

All events use the same envelope from Phase 2:
```json
{ "event_id": "uuid", "event_type": "...", "occurred_at": "ISO8601", "version": "1", "payload": {} }
```

---

## 10. Codebase Structure (Phase 3 additions)

New files only. Phase 1–2 structure unchanged.

```
minibank/
├── alembic/versions/
│   ├── 0005_add_deposits.py
│   ├── 0006_add_withdrawals.py
│   └── 0007_add_scheduled_payments.py
├── app/
│   ├── models/
│   │   ├── deposit.py              # Deposit ORM model
│   │   ├── withdrawal.py           # Withdrawal ORM model (saga_status column)
│   │   └── scheduled_payment.py   # ScheduledPayment ORM model
│   ├── schemas/
│   │   ├── deposit.py              # SimulateDepositRequest, DepositResponse
│   │   ├── withdrawal.py           # WithdrawalRequest, WithdrawalResponse (saga_status)
│   │   └── scheduled_payment.py   # ScheduledPaymentRequest/Response
│   ├── routers/
│   │   ├── deposits.py             # GET /v1/deposits/{id}
│   │   ├── withdrawals.py          # POST /v1/withdrawals, GET /v1/withdrawals/{id}
│   │   ├── scheduled_payments.py  # POST/GET/DELETE /v1/scheduled-payments
│   │   └── dev.py                  # + POST /v1/dev/simulate-deposit
│   ├── services/
│   │   ├── deposit_service.py      # Deposit flow: insert + ledger + outbox in one TX
│   │   └── withdrawal_service.py  # Saga orchestrator: debit → rail → complete/compensate
│   └── circuit_breaker.py         # CircuitBreaker class (50 lines, no external dependency)
├── workers/
│   ├── payment_scheduler.py        # Poll scheduled_payments + FOR UPDATE SKIP LOCKED
│   └── saga_recovery.py            # Find saga_status='debited' stuck >5min → compensate
├── rail/
│   └── simulator.py                # Simulated bank rail (configurable fail rate via env var)
└── tests/
    ├── test_deposits.py            # Idempotency: same external_ref → one credit
    ├── test_withdrawals.py         # Saga happy path + compensation path
    ├── test_saga_recovery.py       # Simulate crash mid-saga → recovery compensates
    ├── test_circuit_breaker.py     # 3 failures → OPEN, probe, CLOSED
    └── test_scheduled_payments.py  # Correct time, two schedulers → one execution
```

---

## 11. Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Saga style | Orchestration (one function, explicit saga_status) | Choreography via events is harder to audit and debug mid-saga; orchestration is how banks actually do it |
| Circuit breaker implementation | Custom class, no library | 50 lines, no magic — you can read and own it |
| Circuit breaker state | In-memory | Redis adds complexity; in-memory is correct for single process; document the production upgrade path |
| Deposit model | Push (dev webhook) | Banks receive webhooks; users don't initiate deposits — teaching the real model from the start |
| Deposit idempotency | DB unique constraint on external_ref | Stronger than Redis (survives Redis restart); prevents double-credit even if app crashes mid-request |
| Scheduler | Polling loop + FOR UPDATE SKIP LOCKED | No APScheduler dependency; locking logic is explicit and teachable |
| Scheduled payment idempotency | `scheduled:{id}:{next_run_at}` key | Prevents double-execution if scheduler crashes after transfer but before advancing next_run_at |
