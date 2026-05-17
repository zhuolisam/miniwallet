# Week 10–11 Student Tasks — What You Implement

Seven files have `# TODO: student` markers. This document explains each one, what concepts it teaches, and how to verify your work.

The work splits cleanly along the week boundary:

- **Week 10 — US-3.1 Deposit:** Tasks 1, 2.
- **Week 11 — US-3.2 Withdrawal saga:** Tasks 3, 4.
- **Cross-cutting consumer + tests:** Tasks 5, 6, 7.

Recommended order at the bottom.

---

## Task 1 — Deposit service (`app/services/deposit_service.py`)

**Difficulty:** Core exercise
**Concepts:** single-TX flow, UNIQUE-constraint idempotency, double-entry ledger, event publishing via outbox
**Lines to write:** ~80

### What to do

Implement two public functions and one helper:

- `simulate_deposit(db, account_id, amount, currency, source_type, external_reference) -> DepositResponse`
- `get_deposit(db, deposit_id, requesting_account_id) -> DepositResponse`
- `_deposit_to_response(d) -> DepositResponse`

### The flow (inside a single transaction per deposit)

1. **Try INSERT** a `Deposit` row with `status='pending'` and the client-provided `external_reference`. Flush to DB to force the UNIQUE constraint to fire now.
2. **If `IntegrityError` (UniqueViolation):** `await db.rollback()`, then SELECT the existing deposit by `external_reference` and return its current state. Do NOT re-validate — the original attempt already did. Duplicate webhooks are a normal part of life (partners retry on timeout); returning 200 with the original record is the right semantics.
3. **Validate.** On failure, UPDATE the deposit to `status='rejected'` with the `rejection_reason` string, publish `deposit.rejected`, commit. No ledger entry ever written.
    - Account exists → else `ACCOUNT_NOT_FOUND`.
    - Account `status = 'active'` → else `ACCOUNT_NOT_ACTIVE`.
    - `amount > 0` → else `INVALID_AMOUNT` (defensive; Pydantic also catches this).
    - `currency` ∈ `SUPPORTED_CURRENCIES` → else `UNSUPPORTED_CURRENCY`.
4. **On validation pass:**
    - `SELECT account FOR UPDATE` (balance derivation must not race).
    - INSERT two `LedgerEntry` rows sharing one `transaction_id`:
        - Debit leg: `account=SYSTEM_ACCOUNT_ID`, `direction='debit'`, `entry_type='deposit'`, `reference_id=deposit.id`.
        - Credit leg: `account=user_account`, `direction='credit'`, `entry_type='deposit'`, `reference_id=deposit.id`.
    - UPDATE the deposit to `status='completed'`, set `completed_at`, refresh `updated_at`.
    - `publish_event("deposit.events", "deposit.completed", DepositCompletedPayload(...), actor_id=None)`.
    - COMMIT.

### Why a single transaction

Unlike a withdrawal, there is no external call between the deposit row write and the ledger write. The rail has already confirmed the money arrived — we are just recording it. If the process crashes mid-transaction, Postgres rolls back and the deposit row never existed; the rail retries the webhook and we process it fresh.

### Reference implementations

- `app/services/account_service.py::seed()` — same IntegrityError-then-lookup idempotency pattern for the seed flow.
- `app/services/transfer_service.py::transfer()` — `SELECT account FOR UPDATE` before ledger writes.
- `app/events/publisher.py::publish_event()` — outbox write inside the caller's transaction.

### Verify

```bash
uv run pytest tests/test_deposits.py -v
```

### Key insight

The `external_reference` IS the idempotency key — it comes from the rail, not from the client. That's why deposits don't need Redis: the DB UNIQUE constraint on a natural key is stronger (it survives Redis restarts). Compare with transfers, which accept a client-chosen `Idempotency-Key` header — there Redis is a fast-path cache over an eventually-consistent DB safety net.

---

## Task 2 — Deposit routers (`app/routers/deposits.py` + `app/routers/dev.py`)

**Difficulty:** Straightforward
**Concepts:** FastAPI dependencies, JWT auth resolution, dev-endpoint guarding
**Lines to write:** ~15

### 2a. `POST /v1/dev/simulate-deposit` (in `routers/dev.py`)

1. Gate on `settings.app_env != "development"` → raise `ForbiddenError()`. **No JWT check** — the production equivalent would be HMAC from the partner, not a user token.
2. Call `deposit_service.simulate_deposit(db=db, account_id=UUID(body.account_id), amount=Decimal(body.amount), currency=body.currency, source_type=body.source_type, external_reference=body.external_reference)`.
3. Return `{"data": result.model_dump()}`.

### 2b. `GET /v1/deposits/{deposit_id}` (in `routers/deposits.py`)

1. Resolve `sender_account = await account_service.get_account_by_user(db, current_user.id)`. Raise `AccountNotFoundError` if None.
2. Call `deposit_service.get_deposit(db, UUID(deposit_id), sender_account.id)`.
3. Return `{"data": result.model_dump()}`.

### Why the dev endpoint has no auth

Real bank webhooks don't carry user JWTs — the rail is a system, not a user. Our simulator mirrors that. The `APP_ENV` gate ensures this can't be hit in prod; the JWT gate would be wrong here (and would force tests to manufacture fake JWTs to test the deposit flow — noise without value).

### Verify

```bash
uv run pytest tests/test_deposits.py -v
```

---

## Task 3 — Withdrawal saga service (`app/services/withdrawal_service.py`)

**Difficulty:** Core exercise — this is the headline piece of Week 11
**Concepts:** saga orchestration, transaction boundaries around external I/O, compensating ledger entries, idempotent compensation via natural keys
**Lines to write:** ~130 across `create_withdrawal`, `_complete`, `_compensate`, `get_withdrawal`, `_withdrawal_to_response`

### Read the docstring first

`app/services/withdrawal_service.py` has a detailed top-of-file docstring describing the state machine, transaction boundaries, and ledger invariants. Read it before touching a line of code — the subtle parts (why debit first, why append-only compensation, what `updated_at` gives you for recovery) are explained there.

### The flow

**Idempotency fast path (Redis cache + DB UNIQUE safety net — mirrors `/v1/transfers`).**

Two layers, two jobs:

- **Redis** — latency and lock avoidance. A retry that hits the cache skips TX 1 entirely: no `SELECT FOR UPDATE`, no balance derivation over `ledger_entries`, no INSERTs, no rollback work. This matters because TX 1 acquires the sender's account lock; serializing every retry through that lock would bottleneck legitimate concurrent flows on the same account.
- **DB UNIQUE on `withdrawals.idempotency_key`** — correctness. If Redis is down, evicted, or TTL'd, the DB constraint still prevents double-debit. Catch `IntegrityError`, SELECT the existing row, return its state.

Cache the creation-time response snapshot **after TX 1 commits** — never before, or a crashed request would leave a cache entry referencing a DB row that doesn't exist.

Staleness is acceptable and expected: the cached snapshot is `status='pending'` even though the saga may have progressed to `completed` or `failed` by the time the client retries. The client MUST poll `GET /v1/withdrawals/{id}` to observe terminal state. This matches Stripe's idempotency semantics.

See `app/services/transfer_service.py` for the reference implementation, including the `_hash_request` helper that detects "same key, different body" misuse.

**TX 1 — Reserve funds.**

```
SELECT account FOR UPDATE
check balance >= amount  (raise InsufficientBalanceError if not)
INSERT withdrawals (status='pending', idempotency_key=...)
INSERT ledger debit leg  (account=user,   entry_type='withdrawal')
INSERT ledger credit leg (account=system, entry_type='withdrawal')
publish_event('withdrawal.events', 'withdrawal.initiated', ...)
COMMIT
```

Direction matters — a WITHDRAWAL debits the user (money leaving) and credits the system account. A seed is the opposite direction; don't copy-paste that one.

**Step 2 — Mark submitted + call the rail.**

```python
async with db.begin():
    withdrawal.status = 'submitted'
    withdrawal.submitted_at = now
# COMMIT — row lock released before the network call

try:
    result = await rail.send_withdrawal(
        withdrawal_id=withdrawal.id,
        amount=withdrawal.amount,
        destination=withdrawal.destination_details,
    )
except RailError as e:
    await _compensate(db, withdrawal, e.code)
    return _withdrawal_to_response(withdrawal)

await _complete(db, withdrawal, result.reference)
return _withdrawal_to_response(withdrawal)
```

**Never hold a DB transaction open across an external network call.** That's the single most important rule of saga implementation. The rail could hang for 10 seconds; you'd be blocking a Postgres connection, starving the pool, and potentially deadlocking other flows that need the same account row.

`updated_at` is refreshed automatically by the `before_flush` listener in `app/models/withdrawal.py`. You do **not** need to set it manually. This is deliberate — forgetting to set `updated_at` is the class of bug that makes stuck rows invisible to recovery.

**TX 3a `_complete`:** mark `completed`, record `external_reference`, publish `withdrawal.completed`. No new ledger entries — the original debit stands.

**TX 3b `_compensate`:** write TWO NEW ledger entries (never UPDATE the original debit) with `entry_type='withdrawal_reversal'`. The credit leg carries `idempotency_key='reversal:{withdrawal.id}'` — this is the safety net that makes running compensation twice safe (recovery retry + saga compensation both firing would both succeed only once; the second attempt raises IntegrityError and aborts its TX).

### Verify

```bash
uv run pytest tests/test_withdrawals.py -v
```

### Key insights

1. **Debit immediately, compensate on failure.** Between "user clicks withdraw" and "rail confirms" the user could initiate other flows, receive scheduled payments, or attempt a second withdrawal. If you wait to debit, their available balance is a lie. Every neobank works this way — it is not a design choice.

2. **Compensation is append-only.** Two new rows, same `reference_id` pointing at the withdrawal. An auditor sees the debit AND the reversal as separate, dated, traceable events. Auditors and regulators recognize "reversal" as standard terminology.

3. **Idempotency via natural key.** `reversal:{withdrawal_id}` is derived from the withdrawal, not client-supplied. The DB rejects the second attempt automatically.

---

## Task 4 — Withdrawal router (`app/routers/withdrawals.py`)

**Difficulty:** Straightforward
**Concepts:** `Idempotency-Key` header, JWT → account resolution, FastAPI Depends injection for the rail
**Lines to write:** ~15

### `POST /v1/withdrawals`

1. `sender_account = await account_service.get_account_by_user(db, current_user.id)`. Raise `AccountNotFoundError` if None.
2. Call `withdrawal_service.create_withdrawal(db=db, redis=redis, rail=rail, account_id=sender_account.id, amount=Decimal(body.amount), currency=body.currency, destination_type=body.destination_type, destination_details=body.destination_details, idempotency_key=idempotency_key, actor_user_id=current_user.id)`.
3. Return `{"data": result.model_dump()}`.

### `GET /v1/withdrawals/{withdrawal_id}`

Ownership-enforced lookup — mirror the transfer pattern.

### Verify

```bash
uv run pytest tests/test_withdrawals.py -v
```

---

## Task 5 — Activity consumer dispatch (`workers/activity_consumer.py`)

**Difficulty:** Straightforward (follow the existing pattern)
**Concepts:** CQRS read-model fan-out, what events create activity rows vs not
**Lines to write:** ~30

### What to do

Three new branches in `ActivityConsumer.process`:

- `DepositCompletedPayload` → ONE credit row for the user.
- `WithdrawalInitiatedPayload` → ONE debit row for the user (must appear immediately — this is what the user sees in their statement the moment they click "withdraw").
- `WithdrawalFailedPayload` → ONE credit row (`entry_type='withdrawal_reversal'`) representing the reversal.

And two deliberate no-ops:

- `WithdrawalCompletedPayload` — the debit row already exists from `withdrawal.initiated`; don't double-write. The branch exists so a later dev doesn't add the row by accident.
- `deposit.rejected` — money never moved; no activity row.

Follow the `TransferCompletedPayload` branch for the ORM shape. Idempotency is enforced by the UNIQUE constraint on `(event_id, account_id)` — duplicates raise `IntegrityError` which is already caught in the outer `try`.

### Verify

```bash
uv run pytest tests/test_activity_consumer.py -v
# Existing tests should still pass. Add a deposit/withdrawal test in the same file
# or as a separate integration test if you want explicit coverage.
```

---

## Task 6 — Deposit tests (`tests/test_deposits.py`)

**Difficulty:** Straightforward
**Lines to write:** ~80 across 7 test functions

| Test | What it verifies |
|------|-----------------|
| `test_deposit_happy_path` | 201 + status=completed + balance credited |
| `test_deposit_idempotent_same_reference` | Duplicate webhook returns ORIGINAL record; only one credit |
| `test_deposit_rejected_account_not_found` | Unknown account → status=rejected, no ledger entry |
| `test_deposit_rejected_unsupported_currency` | `currency='EUR'` → UNSUPPORTED_CURRENCY, no ledger entry |
| `test_deposit_ledger_invariant` | After deposit, SUM(all ledger entries, signed) = 0 |
| `test_deposit_get_by_id_enforces_ownership` | Bob can't GET Alice's deposit → 404 |
| `test_deposit_dev_only_in_non_dev_env` | `APP_ENV=production` → 403 |

---

## Task 7 — Withdrawal tests (`tests/test_withdrawals.py`)

**Difficulty:** Core exercise (covers the saga compensation path)
**Lines to write:** ~100 across 7 test functions

| Test | What it verifies |
|------|-----------------|
| `test_withdrawal_happy_path` | 201 + status=completed + balance reduced |
| `test_withdrawal_rail_failure_compensates` | `force_outcome('fail:TIMEOUT')` → status=failed, balance RESTORED, debit + reversal both in ledger |
| `test_withdrawal_insufficient_balance` | 422 INSUFFICIENT_BALANCE, no row written |
| `test_withdrawal_idempotent_same_key` | Same `Idempotency-Key` twice → one row, one debit |
| `test_withdrawal_requires_idempotency_key` | Missing header → 400 MISSING_IDEMPOTENCY_KEY |
| `test_withdrawal_get_by_id_enforces_ownership` | Bob can't GET Alice's withdrawal → 404 |
| `test_withdrawal_ledger_invariant_after_compensation` | SUM over all entries = 0 even after a compensated withdrawal |

### How to force rail failure deterministically

```python
from app.dependencies import get_rail
from rail.simulator import BankRailSimulator

# In the test body, BEFORE the POST:
test_rail = BankRailSimulator()
# You need the withdrawal_id to force an outcome, but you don't know it yet.
# Option 1: pre-seed a well-known id by patching uuid.uuid4 in the service.
# Option 2 (recommended): subclass BankRailSimulator to always raise, and inject it:
class AlwaysFail(BankRailSimulator):
    async def send_withdrawal(self, withdrawal_id, amount, destination):
        raise RailError("TIMEOUT")

client.app.dependency_overrides[get_rail] = lambda: AlwaysFail()
```

Pick whichever you find cleaner.

---

## Recommended Implementation Order

1. **Task 1** (deposit service) — the simplest flow; anchors your mental model for the ledger two-leg write.
2. **Task 2** (deposit routers) — 15 lines; gets you an end-to-end green bar for deposits.
3. **Task 6** (deposit tests) — validates Task 1 + 2 before you move on.
4. **Task 3** (withdrawal saga) — the headline work. Read the docstring first.
5. **Task 4** (withdrawal router) — wiring.
6. **Task 7** (withdrawal tests) — forces you to exercise the compensation path, which is where the interesting bugs hide.
7. **Task 5** (activity consumer) — last, because it needs events from completed Tasks 1 + 3 to emit into tests.

---

## Integration Smoke Test

Once all tasks are done:

```bash
docker compose up --build -d

# Deposit path
curl -X POST http://localhost:8000/v1/dev/simulate-deposit \
  -H "Content-Type: application/json" \
  -d '{
    "account_id": "<alice-account-uuid>",
    "amount": "500.00",
    "currency": "USD",
    "source_type": "bank_transfer",
    "external_reference": "BANK-TXN-001"
  }'

# Withdrawal path (assumes Alice is registered and logged in)
curl -X POST http://localhost:8000/v1/withdrawals \
  -H "Authorization: Bearer <alice-jwt>" \
  -H "Idempotency-Key: wd-1" \
  -H "Content-Type: application/json" \
  -d '{
    "amount": "50.00",
    "currency": "USD",
    "destination_type": "bank_transfer",
    "destination_details": {"sort_code": "12-34-56", "account_number": "12345678"}
  }'

# Verify the audit trail
docker compose exec postgres psql -U minibank -c \
  "SELECT event_type, payload->>'deposit_id', payload->>'withdrawal_id'
   FROM audit_events ORDER BY occurred_at DESC LIMIT 10;"

# Verify the CQRS read model
docker compose exec postgres psql -U minibank -c \
  "SELECT entry_type, direction, amount FROM transaction_activity ORDER BY occurred_at DESC LIMIT 10;"

# Force a withdrawal failure (set env then restart the api service)
# export RAIL_FAILURE_RATE=1.0 and re-POST a withdrawal — observe status=failed
# and verify the user's balance is unchanged.
```

---

## What You Will NOT Implement This Cycle

From Phase 3 / Weeks 12–13 — leave these alone until their week arrives:

- `app/circuit_breaker.py` and `BankRailUnavailableError` at the POST endpoint (Week 12).
- `workers/saga_recovery.py` and its lifespan integration (Week 12).
- `GET /v1/health` endpoint (Week 12).
- Scheduled payments (Week 13).

The scaffold leaves just enough surface for those to slot in cleanly — the recovery index on `withdrawals(updated_at)` and the `before_flush` listener on the Withdrawal model are the two structural pieces Week 12 will rely on.
