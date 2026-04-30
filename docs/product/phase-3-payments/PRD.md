# PRD — Phase 3: Advanced Payments

**Phase:** 3 of 6
**Scope:** Deposit simulation · Withdrawal saga · Saga recovery · Circuit breaker · Scheduled payments
**Weeks:** 10–13 · ~3–4 hrs/week
**Status:** `not started`

> **Note:** Expand to full detail before starting Phase 3 implementation.

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

---

## User Stories

**US-3.1 — Deposit (inbound rail)**
> As a developer, I can simulate an incoming bank deposit via a dev endpoint. The system credits the user account, records a deposit, and publishes an event.

Acceptance criteria:
- `POST /v1/dev/simulate-deposit` — dev-only, simulates a bank rail webhook
- Deposit is idempotent: the same `external_ref` can arrive twice without double-crediting
- State machine: `pending → processing → completed / failed`
- Events: `deposit.received`, `deposit.completed`

**US-3.2 — Withdrawal (outbound rail)**
> As a user, I can withdraw funds to an external bank account. If the bank rail fails after my account is debited, my balance is restored automatically.

Acceptance criteria:
- Funds are debited before the rail is called (debit-then-send)
- If the rail fails → compensating debit is reversed → `saga_status = compensated`
- `GET /v1/withdrawals/{id}` shows `saga_status`: `debited | completed | compensated`
- Events: `withdrawal.initiated`, `withdrawal.completed`, `withdrawal.compensated`

**US-3.3 — Saga recovery after crash**
> As a system, if the process crashes after debiting the user but before calling the rail, a recovery job detects the stuck withdrawal and compensates it.

Acceptance criteria:
- Withdrawals stuck at `saga_status = debited` for more than 5 minutes are detected by the recovery job
- Recovery job either retries the rail call or compensates — never leaves money in limbo
- Recovery job runs on startup and on a schedule

**US-3.4 — Circuit breaker**
> As a system, if the bank rail fails 3 times in a row, the circuit trips and subsequent withdrawal requests fail fast with `BANK_RAIL_UNAVAILABLE` rather than waiting for a timeout.

Acceptance criteria:
- `CLOSED → OPEN` after 3 consecutive failures
- `OPEN → HALF_OPEN` after 30-second cooldown
- `HALF_OPEN → CLOSED` on one successful probe call; `HALF_OPEN → OPEN` on failure
- Circuit state visible in `GET /v1/health`

**US-3.5 — Scheduled recurring payments**
> As a user, I can create a recurring payment to another user that executes automatically on a daily, weekly, or monthly schedule.

Acceptance criteria:
- `POST /v1/scheduled-payments` creates the payment
- Scheduler runs `transfer()` from Phase 1 at the correct time
- Insufficient balance → mark `skipped`, publish `payment.skipped`, advance schedule
- Two scheduler instances must not double-execute the same payment (`FOR UPDATE SKIP LOCKED`)
- `DELETE /v1/scheduled-payments/{id}` cancels the payment

---

## Acceptance Criteria (Phase)

- Deposit: same `external_ref` sent twice → only one credit
- Withdrawal: saga compensates on rail failure — no money lost
- Recovery: simulate crash mid-withdrawal → recovery job compensates the stuck withdrawal
- Circuit breaker: trips after 3 failures, fast-fails, recovers after cooldown
- Scheduled: payment fires at correct time; two schedulers running simultaneously → exactly one execution
