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
> As a developer, I can simulate an incoming bank deposit via a dev endpoint. The system validates the inbound payment, credits the user account, records a deposit, and publishes an event.

Acceptance criteria:
- `POST /v1/dev/simulate-deposit` — dev-only, simulates a bank rail webhook (real banks receive webhooks from their banking partner; users never initiate deposits)
- Deposit is idempotent: the same `external_reference` can arrive twice without double-crediting
- State machine: `pending → completed | held | rejected`
  - `pending` — webhook received, validation in progress
  - `completed` — validation passed, ledger entry written (money exists only after this)
  - `held` — flagged for manual review (AML screening, limits exceeded)
  - `rejected` — validation failed, no ledger entry ever written
- Ledger entry is written ONLY on transition to `completed` — the ledger is truth, money does not exist until it's in the ledger
- Every deposit carries `currency` (ISO 4217) and `source_type` (bank_transfer | card_topup | direct_debit)
- Events: `deposit.completed`, `deposit.rejected`, `deposit.held`

**US-3.2 — Withdrawal (outbound rail)**
> As a user, I can withdraw funds to an external bank account. If the bank rail fails after my account is debited, my balance is restored automatically via a compensating ledger entry.

Acceptance criteria:
- Funds are debited BEFORE the rail is called (debit-then-send pattern — this is how every neobank works)
  - Why: between "user clicks send" and "rail confirms" (could be seconds or days), the user could initiate other transactions. If you don't debit immediately, available balance is a lie and double-spend is possible.
- State machine: `pending → submitted → completed | failed`
  - `pending` — withdrawal created, balance check passed, ledger entry written (debit)
  - `submitted` — rail API called, awaiting confirmation
  - `completed` — rail confirmed success
  - `failed` — rail rejected; compensating ledger entry written (credit back to user)
- On failure: a COMPENSATING ledger entry reverses the debit (`entry_type = 'withdrawal_compensation'`, `reference_id` = same `withdrawals.id`)
- Every withdrawal carries `currency`, `destination_type` (bank_transfer | card_withdrawal), and `destination_details` (JSONB — sort code, account number, IBAN)
- `failure_code` records why the rail rejected (INVALID_ACCOUNT, BENEFICIARY_CLOSED, TIMEOUT, etc.)
- `GET /v1/withdrawals/{id}` shows full status and failure reason
- Events: `withdrawal.initiated`, `withdrawal.completed`, `withdrawal.failed`

**US-3.3 — Saga recovery after crash**
> As a system, if the process crashes after debiting the user but before calling the rail (or before receiving the rail's response), a recovery job detects the stuck withdrawal and resolves it.

Acceptance criteria:
- Withdrawals stuck at `status = 'pending'` or `status = 'submitted'` for more than 5 minutes are detected by the recovery job
- Recovery job either retries the rail call or compensates — never leaves money in limbo
- Recovery job runs on startup and on a schedule
- `FOR UPDATE SKIP LOCKED` prevents two recovery instances from processing the same row

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

- Deposit: same `external_reference` sent twice → only one credit, idempotent response
- Deposit: rejected deposit → no ledger entry ever written, balance unchanged
- Withdrawal: debit happens immediately on submission, user balance reflects debit before rail confirms
- Withdrawal: rail failure → compensating entry restores balance, `failure_code` records reason
- Recovery: simulate crash mid-withdrawal → recovery job compensates the stuck withdrawal
- Circuit breaker: trips after 3 failures, fast-fails, recovers after cooldown
- Scheduled: payment fires at correct time; two schedulers running simultaneously → exactly one execution
- Invariant: `SUM(all account balances) = 0` holds after every deposit, withdrawal, and compensation
