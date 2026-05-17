# MiniBank — Digital Banking Backend

A digital banking backend implementing core financial system design patterns used in modern neobanks. Demonstrates expertise in event-driven architecture, transactional correctness, concurrency control, and distributed system resilience.

## Table of Contents

- [Overview](#overview)
- [Tech Stack](#tech-stack)
- [System Architecture](#system-architecture)
- [Core Financial Patterns](#core-financial-patterns)
  - [Double-Entry Accounting](#double-entry-accounting)
  - [Idempotency](#idempotency)
  - [Concurrency Control](#concurrency-control)
  - [Event-Driven Architecture](#event-driven-architecture)
  - [Outbox Pattern](#outbox-pattern)
  - [CQRS](#cqrs-command-query-responsibility-segregation)
  - [Saga Pattern](#saga-pattern)
  - [Circuit Breaker](#circuit-breaker)
  - [Scheduled Payments](#scheduled-payments)
- [API Overview](#api-overview)
- [Running Locally](#running-locally)
- [Testing](#testing)

---

## Overview

MiniBank is a simplified digital banking system inspired by neobanks like Revolut, Monzo, and GX Bank. It implements the foundational patterns required for handling money movement safely and reliably:

**Features:**
- User registration and JWT-based authentication
- Account opening (single account per user)
- P2P transfers with strict ACID guarantees
- Deposits (simulated inbound bank rail)
- Withdrawals with saga-based compensation
- Scheduled recurring payments
- Event-driven audit trail and notifications
- Transaction history with CQRS read models

**Core capabilities:**
- **Financial correctness**: Guarantees no money created or lost, even under failures or concurrency
- **Distributed systems**: Event-driven architecture with Kafka, outbox pattern, saga orchestration
- **Resilience**: Circuit breakers, saga recovery jobs, dead letter queues
- **Scalability**: Lock-free concurrent workers using `FOR UPDATE SKIP LOCKED`

**Out of scope:** Frontend, real bank rails, KYC providers, card issuing, lending, investments.

---

## Tech Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Language** | Python 3.12+ with FastAPI | Async API framework |
| **Database** | PostgreSQL | ACID transactions for financial data |
| **Event Bus** | Apache Kafka | Event-driven architecture |
| **Cache** | Redis | Idempotency cache, token storage |
| **ORM** | SQLAlchemy 2.0 (async) | Database access with migrations via Alembic |
| **API Schema** | OpenAPI 3.1 | Contract-first API design |
| **Containers** | Docker Compose | Local infrastructure (Postgres, Kafka, Redis) |
| **Testing** | pytest + testcontainers | Integration tests with real dependencies |

---

## System Architecture

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         FastAPI Application                      │
├─────────────────────────────────────────────────────────────────┤
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
│  │ Auth Router  │  │ Accounts     │  │ Transfers    │          │
│  │ /auth/*      │  │ /accounts/*  │  │ /transfers/* │          │
│  └──────────────┘  └──────────────┘  └──────────────┘          │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
│  │ Withdrawals  │  │ Scheduled    │  │ Health       │          │
│  │ /withdrawals │  │ /scheduled-* │  │ /health      │          │
│  └──────────────┘  └──────────────┘  └──────────────┘          │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Service Layer                               │
├─────────────────────────────────────────────────────────────────┤
│  TransferService  │  WithdrawalService  │  ScheduledPaymentSvc  │
│  ────────────────────────────────────────────────────────────── │
│  • Balance derivation           • Saga orchestration            │
│  • SELECT FOR UPDATE locking    • Compensation logic            │
│  • Idempotency checks           • Circuit breaker integration   │
│  • Outbox event writes          • Recovery job support          │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Data Layer (PostgreSQL)                       │
├─────────────────────────────────────────────────────────────────┤
│  users  │  accounts  │  ledger_entries  │  transfers            │
│  ──────────────────────────────────────────────────────────────│
│  withdrawals  │  scheduled_payments  │  scheduled_executions   │
│  ──────────────────────────────────────────────────────────────│
│  outbox  │  audit_events  │  transaction_activity (CQRS)       │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Event Bus (Kafka)                             │
├─────────────────────────────────────────────────────────────────┤
│  Topics: account.*, transfer.*, withdrawal.*, payment.*         │
│          + dead letter queue (*.dlq)                             │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Background Workers                            │
├─────────────────────────────────────────────────────────────────┤
│  • Outbox Relay: Publishes events from DB → Kafka               │
│  • Audit Log Consumer: Builds append-only audit trail           │
│  • Activity Consumer: Builds CQRS read model                    │
│  • Payment Scheduler: Executes scheduled payments               │
│  • Saga Recovery: Resumes stuck withdrawals after crashes       │
└─────────────────────────────────────────────────────────────────┘
```

### Data Flow Examples

**P2P Transfer (Synchronous, Single Transaction):**
```
Client → POST /transfers → Redis idempotency check → 
BEGIN TRANSACTION →
  SELECT accounts FOR UPDATE (acquire locks) →
  Check balances →
  INSERT ledger_entries (debit sender, credit recipient) →
  INSERT transfers →
  INSERT outbox (transfer.completed event) →
COMMIT →
Return 201 Created
```

**Withdrawal (Saga with External Call):**
```
Client → POST /withdrawals →
  Step 1: BEGIN → Debit user account → INSERT withdrawal (saga_status=debited) → COMMIT
  Step 2: Call bank rail simulator (may fail)
  Step 3a: Success → UPDATE withdrawal (saga_status=completed) → INSERT outbox
  Step 3b: Failure → BEGIN → Credit user back → UPDATE (saga_status=compensated) → COMMIT
  
Recovery Job (runs periodically):
  SELECT withdrawals WHERE saga_status=debited AND created_at < now() - 5 minutes
  FOR EACH: retry rail call OR compensate if max retries exceeded
```

**Event Publishing (Outbox Pattern):**
```
Outbox Relay Worker (continuous):
  SELECT FROM outbox WHERE status=pending ORDER BY created_at FOR UPDATE SKIP LOCKED
  → Publish to Kafka
  → UPDATE outbox SET status=published
  
Consumers:
  Audit Log Consumer → Append to audit_events (idempotent via event_id unique constraint)
  Activity Consumer → Update transaction_activity CQRS read model
```

---

## Core Financial Patterns

### Double-Entry Accounting

Every financial transaction creates equal and opposite ledger entries. The fundamental invariant: **the sum of all ledger entries across all accounts must equal zero**.

**Implementation:**
- `ledger_entries` table: `(debit_account_id, credit_account_id, amount)`
- **System account**: A special account used to balance operations that cross system boundaries (deposits, withdrawals, initial seeds)
- Balances are **derived**, not stored: `SUM(ledger_entries WHERE account_id = X)`

**Example — P2P Transfer ($50 from Alice to Bob):**
```sql
INSERT INTO ledger_entries (debit_account_id, credit_account_id, amount, entry_type)
VALUES 
  (alice_account_id, bob_account_id, 50.00, 'p2p_transfer');
```
Result: Alice's balance decreases by $50, Bob's increases by $50, total system balance unchanged.

**Example — Deposit ($100 from external bank to Alice):**
```sql
INSERT INTO ledger_entries (debit_account_id, credit_account_id, amount, entry_type)
VALUES 
  (system_account_id, alice_account_id, 100.00, 'deposit');
```
Result: Alice credited $100, system account debited $100, net zero.

**Why this matters:**
- Prevents money creation or loss bugs
- Enables real-time reconciliation (ledger sum must always be zero)
- Matches how real banks work (regulatory requirement)

**Code:** `app/services/transfer_service.py`, `app/models/ledger_entry.py`

---

### Idempotency

Guarantee that retrying the same request produces the same outcome, preventing duplicate charges.

**Two-layer defense:**
1. **Redis cache (fast path)**: `Idempotency-Key` header → check Redis first
2. **Database unique constraint (safety net)**: `UNIQUE(idempotency_key)` catches duplicates if Redis fails

**Implementation:**
```python
# Fast path: check Redis
cached = await redis.get(f"idempotency:{key}")
if cached:
    return cached  # Return cached 2xx response

# Slow path: execute + cache
try:
    result = await execute_transfer(...)
    await redis.setex(f"idempotency:{key}", 86400, result)  # 24h TTL
    return result
except IntegrityError:  # DB unique constraint caught duplicate
    return get_existing_transfer(key)
```

**Why Redis + DB?**
- Redis: Fast, handles 99% of retries (network hiccups, client restarts)
- DB constraint: Safety net for Redis failures, ensures correctness even if cache is lost

**Cache policy:**
- ✅ Cache `2xx` successful responses (idempotent replay)
- ❌ Do NOT cache `4xx` client errors (user may fix and retry with same key)

**Code:** `app/services/transfer_service.py:create_transfer()`, `app/models/transfer.py` (unique constraint)

---

### Concurrency Control

Prevent race conditions in concurrent transfers using pessimistic locking.

**Problem:** Two simultaneous $60 transfers from an account with $100 balance should result in:
- First transfer succeeds
- Second transfer fails with `INSUFFICIENT_BALANCE`
- **Never:** Both succeed (overdraft) or both read balance before debit (double-spend)

**Solution:** `SELECT ... FOR UPDATE` on the `accounts` row

**Implementation:**
```python
async def transfer(from_account_id, to_account_id, amount):
    async with db.begin():  # Start transaction
        # 1. Acquire row locks (blocks other transfers on these accounts)
        sender = await db.execute(
            select(Account).where(Account.id == from_account_id).with_for_update()
        )
        recipient = await db.execute(
            select(Account).where(Account.id == to_account_id).with_for_update()
        )
        
        # 2. Derive balances (safe — we hold the lock)
        sender_balance = await get_balance(from_account_id)
        
        # 3. Check + execute (atomic)
        if sender_balance < amount:
            raise InsufficientBalanceError
        
        await db.execute(
            insert(LedgerEntry).values(
                debit_account_id=from_account_id,
                credit_account_id=to_account_id,
                amount=amount
            )
        )
        # 4. Commit releases locks
```

**What `SELECT FOR UPDATE` locks:**
- Locks the **`accounts` row**, not individual ledger entries
- Balance is derived from `SUM(ledger_entries)`, but we lock the account row to serialize access
- No other transaction can read/write this account until we commit

**Concurrency test proof:**
```python
# Test: 10 parallel $60 transfers from account with $100 balance
results = await asyncio.gather(*[transfer(alice, bob, 60) for _ in range(10)])
# Expected: 1 success, 9 INSUFFICIENT_BALANCE errors
# Invariant: final balance = $40, not negative
```

**Code:** `app/services/transfer_service.py:create_transfer()`, `tests/test_concurrency.py`

---

### Event-Driven Architecture

Decouple core payment flows from side effects (notifications, audit log, analytics) using Kafka.

**Pattern:**
- **Write side:** API writes to Postgres, emits events to Kafka
- **Read side:** Consumers build derived views from events

**Event schema (JSON envelope):**
```json
{
  "event_id": "550e8400-e29b-41d4-a716-446655440000",
  "event_type": "transfer.completed",
  "occurred_at": "2026-05-18T10:30:00Z",
  "version": "1.0",
  "payload": {
    "transfer_id": "txn_123",
    "from_account_id": "acc_456",
    "to_account_id": "acc_789",
    "amount": "50.00"
  }
}
```

**Event types:**
- `account.opened`
- `transfer.completed`, `transfer.failed`
- `deposit.received`, `deposit.completed`
- `withdrawal.completed`, `withdrawal.compensated`
- `payment.executed`, `payment.skipped`

**Why async events?**
- **Decoupling**: Transfer succeeds even if notification service is down
- **Scalability**: Consumers scale independently
- **Audit trail**: Immutable event log for compliance
- **CQRS**: Build optimized read models without slowing writes

**Code:** `app/events/`, Kafka consumers in `workers/`

---

### Outbox Pattern

Guarantee that events are published to Kafka **exactly once** per database transaction, even if Kafka is temporarily unavailable.

**Problem (direct publish):**
```python
# ❌ Fragile approach:
async with db.begin():
    await db.execute(insert(Transfer).values(...))
    await kafka_producer.send("transfers", event)  # What if Kafka is down?
# Result: Transfer committed to DB, event lost → audit gap
```

**Solution (outbox):**
```python
# ✅ Transactional outbox:
async with db.begin():
    await db.execute(insert(Transfer).values(...))
    await db.execute(
        insert(Outbox).values(
            topic="transfers",
            event_type="transfer.completed",
            payload=event,
            status="pending"
        )
    )
    # Both writes committed atomically
```

**Relay process (separate worker):**
```python
while True:
    # FOR UPDATE SKIP LOCKED: concurrent relays claim different rows
    pending = await db.execute(
        select(Outbox)
        .where(Outbox.status == "pending")
        .order_by(Outbox.created_at)
        .limit(100)
        .with_for_update(skip_locked=True)
    )
    
    for row in pending:
        await kafka_producer.send(row.topic, row.payload)
        await db.execute(
            update(Outbox)
            .where(Outbox.id == row.id)
            .values(status="published", published_at=utcnow())
        )
```

**Guarantees:**
- ✅ Event published **at least once** (may retry after crash before marking published)
- ✅ Event published **in order** (ORDER BY created_at)
- ✅ No event lost (survives Kafka downtime, app crashes, DB failover)

**Why `FOR UPDATE SKIP LOCKED`?**
- Multiple relay workers can run concurrently
- Each worker claims different rows (no duplicate publishes)
- Critical for horizontal scaling

**Code:** `app/models/outbox.py`, `workers/outbox_relay.py`

---

### CQRS (Command Query Responsibility Segregation)

Separate write models (ledger entries) from read models (transaction activity) for performance and scalability.

**Write model (source of truth):**
- `ledger_entries`: Immutable, append-only, optimized for consistency
- Used for: Transfers, balance derivation, reconciliation

**Read model (materialized view):**
- `transaction_activity`: Denormalized, optimized for querying
- Columns: `account_id`, `transaction_id`, `type`, `amount`, `counterparty_name`, `description`, `created_at`
- Built from Kafka events by a consumer

**Flow:**
```
Transfer API → ledger_entries (write) → outbox → Kafka → 
Activity Consumer → transaction_activity (read)
```

**Trade-off:**
- ✅ Fast reads: `GET /accounts/me/activity` queries denormalized table (no joins)
- ❌ Eventual consistency: Read model lags behind writes by ~100ms

**Why this matters:**
- **Write optimization**: Ledger optimized for correctness, not query performance
- **Read optimization**: Activity table has indexes for common filters (date range, type)
- **Scalability**: Read replicas can serve activity queries without touching write DB

**API response includes lag:**
```json
{
  "data": [...],
  "as_of": "2026-05-18T10:30:00.123Z"  // Timestamp of last processed event
}
```

**Code:** `app/models/transaction_activity.py`, `workers/activity_consumer.py`

---

### Saga Pattern

Coordinate multi-step transactions across system boundaries (external bank rails) with compensation logic.

**Problem:** Withdrawals require two steps:
1. Debit user's MiniBank account (local DB transaction)
2. Credit user's external bank account (API call to bank rail)

If step 2 fails, we must undo step 1 (compensate).

**Orchestration approach (used in MiniBank):**
```python
async def withdraw(account_id, amount):
    # Step 1: Local debit (atomic, persisted)
    async with db.begin():
        await debit_account(account_id, amount)
        await db.execute(
            insert(Withdrawal).values(
                account_id=account_id,
                amount=amount,
                saga_status="debited",  # Persisted state
                status="processing"
            )
        )
    withdrawal_id = result.inserted_primary_key[0]
    
    # Step 2: Call external bank rail (may fail, may timeout)
    try:
        rail_response = await bank_rail_client.send_withdrawal(
            account_id, amount, external_ref=withdrawal_id
        )
        
        # Step 3a: Success path
        async with db.begin():
            await db.execute(
                update(Withdrawal)
                .where(Withdrawal.id == withdrawal_id)
                .values(
                    saga_status="completed",
                    status="completed",
                    external_ref=rail_response.ref
                )
            )
            await publish_event("withdrawal.completed", {...})
    
    except BankRailError as e:
        # Step 3b: Compensation path (refund user)
        async with db.begin():
            await credit_account(account_id, amount)  # Reverse the debit
            await db.execute(
                update(Withdrawal)
                .where(Withdrawal.id == withdrawal_id)
                .values(
                    saga_status="compensated",
                    status="failed",
                    error_reason=str(e)
                )
            )
            await publish_event("withdrawal.compensated", {...})
```

**Saga states:**
- `debited`: Step 1 complete, rail call pending
- `completed`: Step 2 succeeded, money left the system
- `compensated`: Step 2 failed, user refunded

**Saga recovery (critical):**
What if the process crashes between step 1 (commit) and step 2 (rail call)?
- Withdrawal stuck at `saga_status=debited`
- Money debited but never sent to bank
- **User loses money unless recovery job runs**

**Recovery job (runs on startup + every 5 minutes):**
```python
async def recover_stuck_withdrawals():
    stuck = await db.execute(
        select(Withdrawal)
        .where(
            Withdrawal.saga_status == "debited",
            Withdrawal.created_at < utcnow() - timedelta(minutes=5)
        )
    )
    
    for withdrawal in stuck:
        if withdrawal.retry_count < MAX_RETRIES:
            await retry_bank_rail_call(withdrawal)
        else:
            await compensate_withdrawal(withdrawal)  # Refund after max retries
```

**Why orchestration over choreography?**
- **Debuggability**: One row (`withdrawals.saga_status`) shows full saga state
- **Auditability**: Regulators can query a single table to see what happened
- **Recovery**: Simple SQL query finds stuck sagas

**Code:** `app/services/withdrawal_service.py`, `workers/saga_recovery.py`

---

### Circuit Breaker

Prevent cascading failures when external bank rail is degraded by failing fast instead of waiting for timeouts.

**States:**
1. **CLOSED** (normal): Requests pass through, failures counted
2. **OPEN** (tripped): All requests fast-fail with `BANK_RAIL_UNAVAILABLE` (no network call)
3. **HALF_OPEN** (probing): After cooldown, allow one request to test recovery

**State machine:**
```
CLOSED ──[N consecutive failures]──> OPEN
  ▲                                    │
  │                                    │ (cooldown: 30s)
  │                                    ▼
  └──[probe success]─── HALF_OPEN ◄───┘
                            │
                            └──[probe failure]──> OPEN
```

**Implementation:**
```python
class CircuitBreaker:
    def __init__(self, failure_threshold=3, cooldown_seconds=30):
        self.state = "CLOSED"
        self.failure_count = 0
        self.last_failure_time = None
        self.failure_threshold = failure_threshold
        self.cooldown = timedelta(seconds=cooldown_seconds)
    
    async def call(self, func, *args, **kwargs):
        if self.state == "OPEN":
            if utcnow() - self.last_failure_time > self.cooldown:
                self.state = "HALF_OPEN"  # Try probe
            else:
                raise CircuitBreakerOpenError("Bank rail circuit open")
        
        try:
            result = await func(*args, **kwargs)
            if self.state == "HALF_OPEN":
                self.state = "CLOSED"  # Probe succeeded
                self.failure_count = 0
            return result
        
        except Exception as e:
            self.failure_count += 1
            self.last_failure_time = utcnow()
            
            if self.failure_count >= self.failure_threshold:
                self.state = "OPEN"
            
            raise
```

**Usage in withdrawal service:**
```python
circuit_breaker = CircuitBreaker()

async def send_withdrawal(account_id, amount):
    return await circuit_breaker.call(
        bank_rail_client.send_withdrawal,
        account_id,
        amount
    )
```

**Why this matters:**
- **Fast failure**: No waiting 30s for timeout when rail is down (user gets immediate error)
- **Reduced load**: Stops hammering a degraded service (gives it time to recover)
- **Observable**: Circuit state exposed in `GET /v1/health` for monitoring

**Future enhancements:**
- Distributed circuit state via Redis for multi-instance deployments
- Prometheus metrics for circuit breaker state transitions

**Code:** `app/circuit_breaker.py`, `app/services/withdrawal_service.py`

---

### Scheduled Payments

Execute recurring payments (daily/weekly/monthly) with exactly-once guarantees under concurrent schedulers.

**Schema:**
```sql
CREATE TABLE scheduled_payments (
    id UUID PRIMARY KEY,
    from_account_id UUID NOT NULL,
    to_account_id UUID NOT NULL,
    amount DECIMAL(19, 4) NOT NULL,
    frequency VARCHAR(20) NOT NULL,  -- 'daily', 'weekly', 'monthly'
    next_run_at TIMESTAMP NOT NULL,
    status VARCHAR(20) NOT NULL,     -- 'active', 'paused', 'cancelled'
    created_at TIMESTAMP NOT NULL
);
```

**Scheduler worker (polls every minute):**
```python
async def run_scheduler():
    while True:
        due = await db.execute(
            select(ScheduledPayment)
            .where(
                ScheduledPayment.next_run_at <= utcnow(),
                ScheduledPayment.status == "active"
            )
            .with_for_update(skip_locked=True)  # Key: prevents double-execution
        )
        
        for payment in due:
            try:
                # Reuse transfer service (inherits locking, idempotency)
                await transfer_service.create_transfer(
                    from_account_id=payment.from_account_id,
                    to_account_id=payment.to_account_id,
                    amount=payment.amount,
                    idempotency_key=f"sched_{payment.id}_{payment.next_run_at.isoformat()}"
                )
                
                # Advance next run time
                await db.execute(
                    update(ScheduledPayment)
                    .where(ScheduledPayment.id == payment.id)
                    .values(next_run_at=calculate_next_run(payment))
                )
                
                await publish_event("payment.executed", {...})
            
            except InsufficientBalanceError:
                # Skip this cycle, try again next time
                await db.execute(
                    update(ScheduledPayment)
                    .where(ScheduledPayment.id == payment.id)
                    .values(next_run_at=calculate_next_run(payment))
                )
                await publish_event("payment.skipped", {...})
```

**Concurrency guarantee:**
- **Problem:** Two scheduler workers run simultaneously → same payment executed twice
- **Solution:** `FOR UPDATE SKIP LOCKED` — first worker locks the row, second worker skips it
- **Result:** Exactly-once execution per cycle

**Idempotency key:**
- `sched_{payment_id}_{next_run_at}` ensures uniqueness per payment per cycle
- If worker crashes after transfer but before advancing `next_run_at`, restart will retry with same key → transfer service deduplicates

**Code:** `app/models/scheduled_payment.py`, `workers/payment_scheduler.py`

---

## API Overview

**Base URL:** `http://localhost:8000`

### Authentication
```bash
POST /v1/auth/register   # Create user account
POST /v1/auth/login      # Get JWT access token (15 min) + refresh token
POST /v1/auth/refresh    # Rotate tokens
GET  /v1/users/me        # Current user profile
```

### Accounts
```bash
POST /v1/accounts        # Open the user's single account
GET  /v1/accounts/me     # View account + balance
GET  /v1/accounts/me/balance  # Balance only
```

### Transfers
```bash
POST /v1/transfers       # P2P transfer (requires Idempotency-Key header)
GET  /v1/transfers/{id}  # Transfer status
GET  /v1/accounts/me/transactions  # Paginated transaction list
GET  /v1/accounts/me/activity      # CQRS read model (eventually consistent)
```

### Withdrawals
```bash
POST /v1/withdrawals     # Withdraw to external bank
GET  /v1/withdrawals/{id}  # Withdrawal status (includes saga_status)
```

### Scheduled Payments
```bash
POST   /v1/scheduled-payments      # Create recurring payment
GET    /v1/scheduled-payments      # List user's scheduled payments
GET    /v1/scheduled-payments/{id} # Payment details
DELETE /v1/scheduled-payments/{id} # Cancel payment
```

### Health
```bash
GET /v1/health  # System health (DB, Kafka, Redis, circuit breaker state)
```

### Dev/Testing Endpoints
```bash
POST /v1/dev/seed              # Seed funds into user account (dev only)
POST /v1/dev/simulate-deposit  # Simulate inbound bank rail webhook (dev only)
```

**Full API documentation:** OpenAPI spec at `/docs` (Swagger UI) and `/redoc`

---

## Running Locally

### Prerequisites
- Python 3.12+
- Docker Desktop
- `uv` (Python package manager)

### Setup

1. **Clone and install dependencies:**
   ```bash
   git clone <repo-url>
   cd minibank
   uv sync
   ```

2. **Start infrastructure (Postgres, Kafka, Redis):**
   ```bash
   docker compose up -d
   ```

3. **Run database migrations:**
   ```bash
   uv run alembic upgrade head
   ```

4. **Start API server:**
   ```bash
   uv run uvicorn app.main:app --reload
   ```

5. **Start background workers (in separate terminals):**
   ```bash
   # Outbox relay
   uv run python -m workers.outbox_relay
   
   # Event consumers
   uv run python -m workers.audit_consumer
   uv run python -m workers.activity_consumer
   
   # Scheduled payments
   uv run python -m workers.payment_scheduler
   
   # Saga recovery
   uv run python -m workers.saga_recovery
   ```

6. **Access API:**
   - API: http://localhost:8000
   - Swagger docs: http://localhost:8000/docs
   - Health check: http://localhost:8000/v1/health

---

## Testing

### Run all tests:
```bash
uv run pytest
```

### Key test categories:

**Concurrency tests** (`tests/test_concurrency.py`):
```bash
# Proves SELECT FOR UPDATE prevents overdrafts
uv run pytest tests/test_concurrency.py::test_concurrent_transfers_no_overdraft
```

**Saga tests** (`tests/test_saga_recovery.py`):
```bash
# Proves recovery job fixes stuck withdrawals
uv run pytest tests/test_saga_recovery.py::test_saga_recovery_compensates_stuck_withdrawals
```

**Circuit breaker tests** (`tests/test_circuit_breaker.py`):
```bash
# Proves circuit opens after N failures and recovers
uv run pytest tests/test_circuit_breaker.py::test_circuit_breaker_state_transitions
```

**Scheduled payments tests** (`tests/test_scheduled_payments.py`):
```bash
# Proves FOR UPDATE SKIP LOCKED prevents double-execution
uv run pytest tests/test_scheduled_payments.py::test_concurrent_schedulers_execute_once
```

**Integration tests:**
- Use `testcontainers` to spin up real Postgres, Kafka, Redis
- No mocks for critical paths (transfers, sagas) — test against real dependencies
- Mocks only for external bank rail simulator

---

## Key Design Principles

This system implements critical patterns for financial systems:

1. **Correctness first**: Money never created or lost, even under concurrency/failures
2. **Explicit state**: Saga status, circuit breaker state, outbox status — all queryable
3. **Idempotency everywhere**: Retries are safe (network, Kafka, scheduler)
4. **Recovery mechanisms**: Saga recovery job, dead letter queues, circuit breaker auto-recovery
5. **Observability**: Health endpoints expose internal state for monitoring
6. **Lock-free concurrency**: `FOR UPDATE SKIP LOCKED` enables horizontal scaling of workers

---

## Future Enhancements

- Full observability stack (OpenTelemetry, Prometheus, Grafana)
- gRPC inter-service communication
- Rate limiting and cursor-based pagination
- Reconciliation job for ledger validation
- Multi-currency support
- Real bank rail integration (PCI-DSS compliance)
- Card issuing, lending, investment products

---

## License

MIT License

