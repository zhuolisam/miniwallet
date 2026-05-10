# PRD — Phase 3: Advanced Payments

**Phase:** 3 of 6
**Scope:** Deposit simulation · Withdrawal saga · Saga recovery · Circuit breaker · Scheduled payments
**Weeks:** 10–13 · ~3–4 hrs/week
**Status:** `not started`

---

## Week-by-Week Schedule

Five user stories distributed across four weeks. Ordering is deliberate: start with the simplest flow (deposit = no saga), build up to the saga pattern, add the recovery primitives that require the saga to exist, and finish with the scheduler (which reuses existing `transfer()` and adds no new banking primitives).

| Week | Focus | User Stories | Deliverables |
|------|-------|--------------|--------------|
| **Week 10** | Deposit flow | US-3.1 | Migration 0007 (deposits) · `deposit_service.py` · `POST /v1/dev/simulate-deposit` · `GET /v1/deposits/{id}` · deposit events to outbox · tests: idempotency, rejection paths, ledger invariant |
| **Week 11** | Withdrawal saga (happy + compensation) | US-3.2 | Migration 0008 (withdrawals) · `withdrawal_service.py` saga orchestrator · `POST /v1/withdrawals` · `GET /v1/withdrawals/{id}` · `rail/simulator.py` (basic send/query) · TX 1 → TX 2 → TX 3a/3b flow · tests: happy path, rail failure compensation, idempotency |
| **Week 12** | Circuit breaker + Saga recovery | US-3.3, US-3.4 | `app/circuit_breaker.py` · `GET /v1/health` endpoint · pre-flight check wired into withdrawal endpoint · `workers/saga_recovery.py` (two-phase claim + resolve) · app lifespan integration (startup recovery + 5-min loop) · tests: state transitions, recovery scenarios, idempotent compensation |
| **Week 13** | Scheduled payments + Phase-level invariants | US-3.5 | Migration 0009 (scheduled_payments + executions) · `scheduled_payment_service.py` CRUD · `workers/payment_scheduler.py` (two-phase claim + execute) · scheduler loop with try/except resilience · `python-dateutil` dep · activity consumer updates · tests: due payment execution, skip on insufficient balance, concurrent scheduler safety, full-phase ledger invariant |

**Why this ordering?**

- **Week 10 → 11 → 12 → 13** mirrors the conceptual complexity curve: single-TX → saga → recovery of saga → orchestration of existing primitives.
- **US-3.3 (recovery) and US-3.4 (circuit breaker) ship together in Week 12** because they are tightly coupled — the recovery job calls through the circuit breaker, and the circuit breaker is useful only once withdrawals exist (Week 11).
- **US-3.5 (scheduled payments) is last** not because it's the hardest, but because it depends on zero new banking primitives (it reuses `transfer()`). Leaving it for Week 13 means you can focus on pure orchestration code at the end, after the saga/recovery mental model is solidified.

**Risk callouts:**

- Week 11 is the heaviest week. The saga flow spans 3 transactions with a rail call in between, plus idempotency wiring. If you slip, let it slip into Week 12 — don't rush correctness.
- Week 12 depends on Week 11 completing. The recovery job operates on the `withdrawals` table; you can't test it without withdrawals existing.
- Week 13 is the lightest week deliberately — reserve buffer time for integration testing the phase-level invariant (`SUM(all account balances) = 0` across deposits + withdrawals + compensations + scheduled payments).

---

## Problem Statement

Phase 1 handles P2P transfers as a synchronous single-DB transaction — correct and simple. Real banking introduces two harder problems: money crossing a system boundary (external bank rails can fail mid-operation), and payments that must execute automatically on a schedule. Phase 3 introduces the patterns that handle these cases correctly.

---

## Goals

1. Users can receive funds via a simulated inbound bank rail (deposit)
2. Users can send funds via a simulated outbound bank rail (withdrawal)
3. If the outbound rail fails after funds are debited, the user's balance is restored automatically (saga compensation)
4. If the process crashes mid-withdrawal, a recovery job detects and resolves stuck operations
5. The system degrades gracefully when the rail fails repeatedly (circuit breaker)
6. Users can create recurring payments that execute automatically on a schedule

---

## Out of Scope

- Real bank API integration (simulated only)
- Multi-currency FX conversion
- Withdrawal limits and fraud scoring
- Real AML/sanctions screening (stubbed only)
- Deposit-to-user matching by virtual IBAN (we pass `account_id` directly)

---

## User Stories

### US-3.1 — Deposit (inbound rail simulation)

> As a developer, I can simulate an incoming bank deposit via a dev endpoint. The system validates the inbound payment, credits the user account via a double-entry ledger write, and publishes an event.

**Background — how real deposits work:**
In production, a banking partner (ClearBank, Railsr, Modulr) receives funds into your settlement account and sends your system a webhook. The user never calls your API to deposit — deposits are push events from the external rail. Our `simulate-deposit` endpoint mimics that webhook.

**Acceptance criteria:**

- `POST /v1/dev/simulate-deposit` — dev-only (guarded by `APP_ENV=development` check, same pattern as `/v1/dev/seed`)
- Request body:
  ```json
  {
    "account_id": "uuid",
    "amount": "100.00",
    "currency": "USD",
    "source_type": "bank_transfer",
    "external_reference": "BANK-TXN-001"
  }
  ```
- Deposit is **idempotent**: the same `external_reference` arriving twice results in one credit. Second call returns 200 with the original deposit record (not 409 — matches real webhook retry behavior where partners retry on timeout).
- **State machine:** `pending → completed | rejected`
  - `pending` — webhook received, validation starting
  - `completed` — validation passed, ledger entry written (money exists only after this)
  - `rejected` — validation failed, no ledger entry ever written
- **Validation (simplified for study project):**
  - Account exists and `status = 'active'`
  - `amount > 0`
  - `currency` is a supported ISO 4217 code (just "USD" for now)
  - Production note: real systems run AML/sanctions screening and daily limits here
- Ledger entry is written ONLY on transition to `completed`:
  - Debit leg: system account, `entry_type = 'deposit'`
  - Credit leg: user account, `entry_type = 'deposit'`
  - Both legs share a `transaction_id` (same pattern as transfer/seed)
- `GET /v1/deposits/{id}` — returns deposit status and details (JWT-protected, user can only see their own deposits)
- Note: `GET /v1/deposits` (list endpoint) is out of scope for Phase 3. Users can poll individual deposits by ID. List view is deferred to Phase 4 (transaction history UI).
- Every deposit carries `currency` (ISO 4217) and `source_type` (bank_transfer | card_topup | direct_debit)
- **Events (via outbox):** `deposit.completed`, `deposit.rejected`

---

### US-3.2 — Withdrawal (outbound rail with saga)

> As a user, I can withdraw funds to an external bank account. If the bank rail fails after my account is debited, my balance is restored automatically via a compensating ledger entry.

**Background — why debit first:**
Between "user clicks withdraw" and "rail confirms" (could be seconds or days depending on the payment scheme), the user could initiate transfers, other withdrawals, or receive scheduled payments. If you don't debit immediately, their available balance is a lie — they can overdraw. Every neobank debits on submission, compensates on failure. This is not a design choice — it's the only correct approach.

**Acceptance criteria:**

- `POST /v1/withdrawals` — JWT-protected
- Request body:
  ```json
  {
    "amount": "50.00",
    "currency": "USD",
    "destination_type": "bank_transfer",
    "destination_details": {
      "sort_code": "12-34-56",
      "account_number": "12345678"
    }
  }
  ```
- **Requires `Idempotency-Key` header** — same pattern as `/v1/transfers`
- **Pre-flight check:** Before debiting, check circuit breaker state. If OPEN, reject immediately with 503 and error code `BANK_RAIL_UNAVAILABLE`. Do not debit the user just to compensate them immediately — that's pointless churn and confusing UX.
- **State machine:** `pending → submitted → completed | failed`
  - `pending` — debit ledger entry written, funds reserved in user's balance
  - `submitted` — rail API called, awaiting confirmation
  - `completed` — rail confirmed success
  - `failed` — rail rejected or timed out; compensating ledger entry written (money returned to user)
- On failure: a **compensating ledger entry** reverses the debit:
  - Debit leg: system account, `entry_type = 'withdrawal_reversal'`
  - Credit leg: user account, `entry_type = 'withdrawal_reversal'`
  - This is append-only — never UPDATE a ledger row. Auditors see both the debit and the reversal as separate entries.
- `failure_code` records why the rail rejected: `INVALID_ACCOUNT | BENEFICIARY_CLOSED | TIMEOUT | NETWORK_ERROR | CIRCUIT_OPEN`
  - Note: `CIRCUIT_OPEN` in `failure_code` is rare — it only appears if the circuit trips DURING the saga (after pre-flight passed but before rail call). The common case for circuit-open is a 503 rejection before withdrawal creation (no row at all).
- `GET /v1/withdrawals/{id}` — shows full status, timestamps, and failure reason (JWT-protected, user's own withdrawals only)
- **Events (via outbox):** `withdrawal.initiated`, `withdrawal.completed`, `withdrawal.failed`

---

### US-3.3 — Saga recovery after crash

> As a system, if the process crashes after debiting the user but before calling the rail (or before receiving the rail's response), a recovery job detects the stuck withdrawal and resolves it.

**Background — why this is necessary:**
The withdrawal saga has a gap between TX 1 (debit committed) and TX 2 (rail call). If the process dies in that gap, the user's money is debited but nothing happened at the rail. Without recovery, that money is gone forever. Every neobank has a "stuck payment reconciler" that closes this gap.

**Acceptance criteria:**

- Withdrawals stuck at `status = 'pending'` for more than 5 minutes are picked up by recovery
- Withdrawals stuck at `status = 'submitted'` for more than 5 minutes are picked up by recovery
- For `pending` stuck withdrawals: recovery retries the rail call. If circuit is OPEN, compensates immediately.
- For `submitted` stuck withdrawals:
  - If `external_reference` is present: query rail for status, then complete or compensate accordingly
  - If `external_reference` is NULL (crashed during submission): compensate after 30-minute timeout
- Recovery job uses `FOR UPDATE SKIP LOCKED` — safe if two instances run concurrently
- Recovery job runs:
  1. On application startup (catches crashes from last downtime)
  2. Every 5 minutes via background scheduler
- Recovery is **idempotent**: compensating entry has `idempotency_key = 'reversal:{withdrawal_id}'` — running recovery twice on the same withdrawal doesn't double-credit

---

### US-3.4 — Circuit breaker

> As a system, if the bank rail fails 3 times consecutively, the circuit trips and subsequent withdrawal requests fail fast with `BANK_RAIL_UNAVAILABLE` rather than waiting for a timeout.

**Acceptance criteria:**

- Three-state machine: `CLOSED → OPEN → HALF_OPEN → CLOSED`
- `CLOSED → OPEN` after 3 consecutive failures (not cumulative — a success resets the counter)
- `OPEN → HALF_OPEN` after 30-second cooldown
- `HALF_OPEN → CLOSED` on one successful probe call
- `HALF_OPEN → OPEN` on probe failure
- When OPEN: withdrawal endpoint returns 503 immediately (before debit)
- When HALF_OPEN: one withdrawal is allowed through as a probe; others fast-fail
- Circuit state visible in `GET /v1/health` response
- Implementation: custom class (~50 lines), in-memory state. Production note: use Redis for multi-instance coordination.

---

### US-3.5 — Scheduled recurring payments

> As a user, I can create a recurring payment to another user that executes automatically on a daily, weekly, or monthly schedule.

**Acceptance criteria:**

- `POST /v1/scheduled-payments` — creates a recurring payment (JWT-protected)
  ```json
  {
    "to_account_id": "uuid",
    "amount": "25.00",
    "frequency": "monthly",
    "start_at": "2025-02-01T00:00:00Z"
  }
  ```
  **Validation:**
  - `amount > 0` (422 if not)
  - `start_at` must be in the future (400 `INVALID_START_TIME` if past)
  - `to_account_id` must not equal sender's account (400 `CANNOT_PAY_SELF`)
  - `to_account_id` must exist and be active (404 `ACCOUNT_NOT_FOUND` if not)
  - `frequency` must be one of: daily, weekly, monthly (422 if not)
  
  **Response (201):** Returns `ScheduledPaymentResponse` (id, from_account_id, to_account_id, amount, currency, frequency, next_run_at, status, created_at)
- `GET /v1/scheduled-payments` — list user's scheduled payments (active and cancelled)
- `DELETE /v1/scheduled-payments/{id}` — cancel (sets status to `cancelled`, does not delete row)
- **Frequencies:** `daily` (+1 day), `weekly` (+7 days), `monthly` (+1 calendar month via `relativedelta`)
- Scheduler reuses `transfer()` from Phase 1 — same locking, same ledger write, same events
- **Idempotency key:** `scheduled:{payment_id}:{next_run_at_iso}` — unique per execution slot. If scheduler crashes after transfer but before advancing `next_run_at`, retry hits idempotency and is a no-op.
- Insufficient balance: mark execution as `skipped`, publish `payment.skipped` event, advance `next_run_at` to next cycle. No retry within the same cycle.
- Invalid target account (closed/frozen): mark execution as `skipped` with reason, advance schedule. Do NOT auto-cancel the scheduled payment (account might reopen).
- Two scheduler instances running simultaneously: `FOR UPDATE SKIP LOCKED` ensures exactly one execution per payment per cycle
- **Events (via outbox):** `payment.executed`, `payment.skipped`

---

## Acceptance Criteria (Phase-level)

| Scenario | Expected outcome |
|----------|-----------------|
| Deposit: same `external_reference` sent twice | Only one credit in ledger, second call returns 200 with original record |
| Deposit: account doesn't exist or is frozen | Status = rejected, no ledger entry, balance unchanged |
| Withdrawal: happy path | Debit immediately, rail succeeds, status = completed |
| Withdrawal: rail fails | Debit + compensating entry, net balance unchanged, failure_code recorded |
| Withdrawal: circuit OPEN | 503 returned immediately, no debit written |
| Recovery: crash after debit, before rail call | Recovery job compensates or retries within 5 minutes |
| Recovery: crash after rail call, no response | Recovery queries rail or compensates after 30-min timeout |
| Circuit breaker: 3 consecutive failures | Trips to OPEN, fast-fails, recovers after 30s cooldown |
| Scheduled payment: correct time | Transfer executes, next_run_at advances |
| Scheduled payment: insufficient balance | Skipped, event published, schedule advances |
| Scheduled payment: two schedulers concurrent | Exactly one execution (FOR UPDATE SKIP LOCKED) |
| **Invariant: `SUM(all account balances) = 0`** | Holds after every deposit, withdrawal, compensation, and scheduled payment |

---

## API Endpoints Summary

| Method | Path | Auth | Idempotency | Description |
|--------|------|------|-------------|-------------|
| `POST` | `/v1/dev/simulate-deposit` | dev-only | `external_reference` in body | Simulate inbound bank webhook |
| `GET` | `/v1/deposits/{id}` | JWT | — | Deposit status |
| `POST` | `/v1/withdrawals` | JWT | `Idempotency-Key` header | Initiate withdrawal |
| `GET` | `/v1/withdrawals/{id}` | JWT | — | Withdrawal status + failure reason |
| `POST` | `/v1/scheduled-payments` | JWT | — | Create recurring payment |
| `GET` | `/v1/scheduled-payments` | JWT | — | List user's scheduled payments |
| `DELETE` | `/v1/scheduled-payments/{id}` | JWT | — | Cancel (soft-delete) |

---

## Non-Functional Requirements

- All monetary amounts: `Decimal` in Python, `NUMERIC(19,4)` in Postgres. Never floats.
- All timestamps: `TIMESTAMPTZ` in Postgres, UTC in Python.
- All ledger writes: within a DB transaction alongside the outbox row. No dual-write.
- Circuit breaker: in-memory, single-process. Document Redis upgrade path.
- Scheduler: polling loop, no APScheduler dependency. `FOR UPDATE SKIP LOCKED` for safety.
- Rail simulator: configurable failure rate via `RAIL_FAILURE_RATE` env var (0.0–1.0).
- **Database isolation level:** `READ COMMITTED` (Postgres default). Do not change to `SERIALIZABLE` — it breaks `FOR UPDATE SKIP LOCKED` semantics (can deadlock instead of skip).
