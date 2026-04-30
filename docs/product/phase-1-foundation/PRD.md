# PRD — Phase 1: Foundation

**Phase:** 1 of 6
**Scope:** User auth · Single account per user · Double-entry ledger · P2P transfer · Idempotency · Concurrency safety
**Weeks:** 1–5 · ~3–4 hrs/week
**Status:** `not started`

---

## Problem Statement

A banking system needs a correct, tamper-resistant record of every money movement. The most common bugs in fintech are: money created or destroyed by concurrent writes, duplicate operations crediting or debiting twice, and balances stored as floats that silently lose precision. Phase 1 builds the primitives that every later phase depends on — and builds them correctly from the start.

---

## Goals

1. Users can register, authenticate, and manage JWT sessions
2. Each user has exactly one account
3. Every balance change is recorded as an immutable double-entry ledger entry — the ledger always sums to zero
4. P2P transfers between users are atomic, concurrent-safe, and idempotent
5. The API contract is defined in OpenAPI before implementation (contract-first)

---

## Out of Scope

- Deposits and withdrawals via bank rails — Phase 3
- Event publishing — Phase 2
- Rate limiting and cursor pagination — Phase 6
- Frontend, mobile app, real KYC

---

## User Stories

### Authentication

**US-1.1 — Register**
> As a new user, I can register with my email and a password so that I have an account in the system.

Acceptance criteria:
- Email must be unique; duplicate registration returns `409 EMAIL_ALREADY_EXISTS`
- Password is hashed (bcrypt); plaintext is never stored or logged
- Registration returns the user's ID
- No auto-login on registration — explicit login step required

**US-1.2 — Login**
> As a registered user, I can log in and receive an access token and a refresh token.

Acceptance criteria:
- Access token is a short-lived JWT (15 min)
- Refresh token is a long-lived opaque token stored in Redis (7 days)
- Invalid credentials return `401 INVALID_CREDENTIALS` with no field-level distinction
- Successful login returns both tokens and `expires_in`

**US-1.3 — Refresh session**
> As a logged-in user, I can exchange a refresh token for a new access token without re-entering my password.

Acceptance criteria:
- Valid refresh token → new access token + new refresh token (rotation)
- Old refresh token is invalidated immediately on use
- Expired or revoked refresh token → `401 INVALID_REFRESH_TOKEN`

**US-1.4 — View my profile**
> As a logged-in user, I can view my profile (email).

Acceptance criteria:
- Requires valid access token
- Returns user ID and email

---

### Accounts

**US-1.5 — Open an account**
> As a registered user, I can open my single account.

Acceptance criteria:
- Each user has exactly one account — `UNIQUE(user_id)` enforced at DB level
- Attempting to open a second account returns `409 ACCOUNT_ALREADY_EXISTS`
- New account starts with zero balance
- Account status is `active` on creation

**US-1.6 — View my account**
> As a logged-in user, I can see my account and its current balance.

Acceptance criteria:
- Balance is derived from ledger entries (never a stored column) — always accurate
- Returns `404 NOT_FOUND` if no account opened yet

**US-1.7 — View transaction history**
> As a logged-in user, I can view my transaction history, paginated and filterable.

Acceptance criteria:
- Returns ledger entries in reverse chronological order
- Supports offset-based pagination (`page`, `limit`) for now — cursor pagination in Phase 6
- Filterable by `from_date`, `to_date`, `entry_type`
- Each entry shows direction (debit/credit), amount, type, and reference ID

---

### Transfers

**US-1.8 — Send money to another user**
> As a user with a funded account, I can transfer funds to another user by their email or account ID.

Acceptance criteria:
- Transfer is atomic — debit and credit happen in the same DB transaction or neither happens
- Sender's balance check and debit happen inside `SELECT ... FOR UPDATE` to prevent concurrent overdraft
- Insufficient balance returns `422 INSUFFICIENT_BALANCE`
- Cannot transfer to yourself — `422 SAME_ACCOUNT`
- Transfer to non-existent email/account returns `404 NOT_FOUND`
- `Idempotency-Key` header required; sending the same key twice only debits once

**US-1.9 — View transfer status**
> As a user, I can look up the status of a transfer I sent or received.

Acceptance criteria:
- Returns transfer amount, status, direction (sent/received), and timestamps
- Accessible by both sender and receiver

---

### Developer / Testing

**US-1.10 — Seed an account balance (dev only)**
> As a developer, I can inject funds into any account so I can test transfers without a deposit flow.

Acceptance criteria:
- Endpoint only accessible when `APP_ENV=development`
- Debits the system account, credits the user account — double-entry preserved
- Returns the new balance after seeding

---

## Open Questions

| # | Question | Resolution |
|---|----------|------------|
| 1 | Refresh token storage: Redis vs DB table? | Redis — TTL is native, no migration needed |
| 2 | Idempotency key TTL? | 24h — matches typical client retry window |
