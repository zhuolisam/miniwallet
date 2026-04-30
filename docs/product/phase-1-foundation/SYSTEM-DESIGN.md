# System Design — Phase 1: Foundation

**Phase:** 1 of 6
**Status:** `not started`

---

## 1. Architecture Overview

Phase 1 is a single-process monolith. No queues, no external services beyond PostgreSQL and Redis.

```mermaid
graph TD
    Client["HTTP Client"]

    subgraph API["FastAPI Application"]
        MW["Middleware\n(CorrelationID)"]
        AuthRouter["Auth Router\n/v1/auth/*"]
        UserRouter["User Router\n/v1/users/*"]
        AccountRouter["Account Router\n/v1/accounts/*"]
        TransferRouter["Transfer Router\n/v1/transfers/*"]
        DevRouter["Dev Router\n/v1/dev/* (dev env only)"]

        AuthSvc["AuthService"]
        AccountSvc["AccountService\n(owns get_balance())"]
        TransferSvc["TransferService\n(imports get_balance from AccountSvc)"]
    end

    PG[("PostgreSQL\nusers · accounts\nledger_entries · transfers")]
    Redis[("Redis\nRefresh tokens\nIdempotency cache")]

    Client --> MW
    MW --> AuthRouter & UserRouter & AccountRouter & TransferRouter & DevRouter
    AuthRouter --> AuthSvc
    UserRouter --> AuthSvc
    AccountRouter --> AccountSvc
    TransferRouter --> TransferSvc
    DevRouter --> AccountSvc

    AuthSvc --> PG & Redis
    AccountSvc --> PG
    TransferSvc --> PG & Redis
```

---

## 2. Database Schema

### Design decisions
- **UUID primary keys** — no sequential ID leakage, safe to expose in URLs
- **`NUMERIC(20, 8)`** for all monetary amounts — never `FLOAT` or `DOUBLE`
- **Balance is never stored** — always derived from `SUM` of ledger entries; immune to update-race bugs
- **`ledger_entries` is append-only** — no `UPDATE` or `DELETE` ever; `created_at` has no `updated_at`
- **System account** — a special account row (seeded at deploy time) used as the debit counterparty for seed operations; ensures every credit has a matching debit; its balance will be negative (representing money issued into the system) and that is correct by design

```sql
-- Users
CREATE TABLE users (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    email           VARCHAR(255) UNIQUE NOT NULL,
    hashed_password VARCHAR(255) NOT NULL,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_users_email ON users (email);


-- Accounts (one per user + one system account)
CREATE TABLE accounts (
    id         UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id    UUID        REFERENCES users(id) ON DELETE RESTRICT,
               -- NULL for the system account
    status     VARCHAR(20) NOT NULL DEFAULT 'active',
               -- active | frozen | closed
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_one_account_per_user UNIQUE (user_id)
    -- NOTE: PostgreSQL does not enforce UNIQUE on NULL values.
    -- uq_one_account_per_user prevents duplicate user accounts correctly,
    -- but does NOT prevent a second system account (user_id = NULL).
    -- The partial index below protects against that.
);
CREATE INDEX idx_accounts_user_id ON accounts (user_id);
-- Prevents more than one system account from being inserted:
CREATE UNIQUE INDEX uq_one_system_account ON accounts ((user_id IS NULL)) WHERE user_id IS NULL;


-- Ledger Entries (append-only, never updated or deleted)
CREATE TABLE ledger_entries (
    id                UUID          PRIMARY KEY DEFAULT gen_random_uuid(),
    debit_account_id  UUID          NOT NULL REFERENCES accounts(id),
    credit_account_id UUID          NOT NULL REFERENCES accounts(id),
    amount            NUMERIC(20,8) NOT NULL CHECK (amount > 0),
    entry_type        VARCHAR(30)   NOT NULL,
                      -- Phase 1: transfer | seed
                      -- Phase 3 adds: deposit | withdrawal | withdrawal_compensation
    reference_id      UUID,
                      -- Soft reference (no FK constraint) — in Phase 1 points to transfers.id,
                      -- in Phase 3 will also point to withdrawals.id and deposits.id.
                      -- No FK because it references multiple tables.
    idempotency_key   VARCHAR(255)  UNIQUE NOT NULL,
                      -- Phase 1: client-provided Idempotency-Key header
                      -- Phase 3 system-generated entries use constructed keys:
                      --   deposit:     "deposit:{deposit_id}"
                      --   withdrawal:  "withdrawal:{withdrawal_id}"
                      --   compensation:"compensation:{withdrawal_id}"
                      --   scheduled:   "scheduled:{payment_id}:{next_run_at}"
    created_at        TIMESTAMPTZ   NOT NULL DEFAULT NOW(),

    CONSTRAINT chk_different_accounts CHECK (debit_account_id != credit_account_id)
);
CREATE INDEX idx_ledger_debit  ON ledger_entries (debit_account_id,  created_at DESC);
CREATE INDEX idx_ledger_credit ON ledger_entries (credit_account_id, created_at DESC);
CREATE INDEX idx_ledger_ref    ON ledger_entries (reference_id);


-- Transfers
CREATE TABLE transfers (
    id               UUID          PRIMARY KEY DEFAULT gen_random_uuid(),
    from_account_id  UUID          NOT NULL REFERENCES accounts(id),
    to_account_id    UUID          NOT NULL REFERENCES accounts(id),
    amount           NUMERIC(20,8) NOT NULL CHECK (amount > 0),
    status           VARCHAR(20)   NOT NULL DEFAULT 'completed',
                     -- completed — transfer committed successfully
                     -- (no 'failed' row is ever inserted — failed transfers raise
                     --  an exception and are never persisted)
    idempotency_key  VARCHAR(255)  UNIQUE NOT NULL,
    created_at       TIMESTAMPTZ   NOT NULL DEFAULT NOW(),

    CONSTRAINT chk_different_transfer_accounts CHECK (from_account_id != to_account_id)
);
CREATE INDEX idx_transfers_from ON transfers (from_account_id, created_at DESC);
CREATE INDEX idx_transfers_to   ON transfers (to_account_id,   created_at DESC);
```

**System account bootstrap** (Alembic migration `0001`):
```sql
INSERT INTO accounts (id, user_id, status)
VALUES ('00000000-0000-0000-0000-000000000000', NULL, 'active');
```
The system account ID is a known constant (`SYSTEM_ACCOUNT_ID`) in `app/config.py`.

---

## 3. API Contract

### Conventions
- **Base path:** `/v1`
- **Auth:** `Authorization: Bearer <access_token>` on all protected routes
- **Idempotency:** `Idempotency-Key: <uuid>` required on `POST /v1/transfers` and `POST /v1/dev/seed`
- **Response envelope:**
  ```json
  { "data": { ... } }
  { "data": [ ... ], "meta": { "total": 100, "page": 1, "limit": 20 } }
  { "error": { "code": "SNAKE_CASE", "message": "...", "details": {} } }
  ```
- **Amounts:** always strings (`"100.50000000"`), never numbers
- **Timestamps:** ISO 8601 with timezone (`2024-01-15T10:30:00Z`)

---

### Auth Endpoints

**POST /v1/auth/register**
```
Request:  { "email": "user@example.com", "password": "string (min 8 chars)" }
Response 201: { "data": { "user_id": "uuid", "email": "user@example.com" } }
Errors:   409 EMAIL_ALREADY_EXISTS · 422 VALIDATION_ERROR
```

**POST /v1/auth/login**
```
Request:  { "email": "...", "password": "..." }
Response 200: { "data": { "access_token": "eyJ...", "refresh_token": "opaque",
                          "token_type": "bearer", "expires_in": 900 } }
Errors:   401 INVALID_CREDENTIALS
```

**POST /v1/auth/refresh**
```
Request:  { "refresh_token": "opaque" }
Response 200: { "data": { "access_token": "eyJ...", "refresh_token": "new-opaque",
                          "token_type": "bearer", "expires_in": 900 } }
Errors:   401 INVALID_REFRESH_TOKEN
```

**GET /v1/users/me**
```
Response 200: { "data": { "user_id": "uuid", "email": "user@example.com" } }
Errors:   401 UNAUTHORIZED
```

---

### Account Endpoints

**POST /v1/accounts**
```
Request:  (no body required)
Response 201: { "data": { "account_id": "uuid", "user_id": "uuid",
                          "status": "active", "balance": "0.00000000",
                          "created_at": "..." } }
Errors:   409 ACCOUNT_ALREADY_EXISTS
```

**GET /v1/accounts/me**
```
Response 200: { "data": { "account_id": "uuid", "status": "active",
                          "balance": "1000.00000000", "created_at": "..." } }
Errors:   404 NOT_FOUND
```

**GET /v1/accounts/me/balance**
```
Response 200: { "data": { "account_id": "uuid", "balance": "1000.00000000",
                          "as_of": "..." } }
Errors:   404 NOT_FOUND
```

**GET /v1/accounts/me/transactions**
```
Query: page=1 · limit=20 (max 100) · from_date · to_date · entry_type

Response 200:
{
  "data": [
    { "entry_id": "uuid", "direction": "credit", "amount": "100.00000000",
      "entry_type": "transfer", "reference_id": "uuid", "created_at": "..." }
  ],
  "meta": { "total": 45, "page": 1, "limit": 20 }
}
Errors:   404 NOT_FOUND
```

Note: `direction` is derived at query time — if `credit_account_id == current_user_account_id`
then `direction = "credit"`, otherwise `direction = "debit"`. It is not a stored column.

---

### Transfer Endpoints

**POST /v1/transfers**
```
Headers:  Idempotency-Key: <uuid>
Request:  { "to_email": "bob@example.com",   // or "to_account_id": "uuid"
            "amount": "50.00" }

Response 201:
{
  "data": { "transfer_id": "uuid", "from_account_id": "uuid",
            "to_account_id": "uuid", "amount": "50.00000000",
            "status": "completed", "created_at": "..." }
}

Errors:
  400 MISSING_IDEMPOTENCY_KEY  — header absent
  404 NOT_FOUND                — recipient not found, or recipient has no account
  422 INSUFFICIENT_BALANCE     — sender balance < amount
  422 SAME_ACCOUNT             — sender == receiver
  409 IDEMPOTENCY_CONFLICT     — same key, different request body
```

**GET /v1/transfers/{transfer_id}**
```
Response 200:
{
  "data": { "transfer_id": "uuid", "from_account_id": "uuid",
            "to_account_id": "uuid", "amount": "50.00000000",
            "status": "completed", "created_at": "..." }
}
Errors:   404 NOT_FOUND
```

---

### Dev Endpoints (`APP_ENV=development` only)

**POST /v1/dev/seed**
```
Headers:  Idempotency-Key: <uuid>
Request:  { "account_id": "uuid", "amount": "1000.00" }
Response 201: { "data": { "entry_id": "uuid", "account_id": "uuid",
                          "amount": "1000.00000000", "new_balance": "1000.00000000" } }
Errors:   403 FORBIDDEN · 404 NOT_FOUND
```

---

## 4. Core Logic: `transfer()`

### 4a. Recipient lookup (router responsibility)

The router resolves `to_email` or `to_account_id` to a concrete `to_account_id` UUID before
calling the service. This lookup lives in the router, not the service — the service only deals
with account IDs.

```python
# app/routers/transfers.py
@router.post("/", status_code=201)
async def create_transfer(
    body: TransferRequest,
    idempotency_key: str = Header(..., alias="Idempotency-Key"),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    redis: Redis = Depends(get_redis),
):
    # Resolve sender account
    sender_account = await account_service.get_account_by_user(db, current_user.id)
    if sender_account is None:
        raise HTTPException(404, {"code": "NOT_FOUND"})

    # Resolve recipient account
    if body.to_email:
        recipient_user = await user_service.get_by_email(db, body.to_email)
        if recipient_user is None:
            raise HTTPException(404, {"code": "NOT_FOUND"})
        recipient_account = await account_service.get_account_by_user(db, recipient_user.id)
    else:
        recipient_account = await account_service.get_account_by_id(db, body.to_account_id)

    if recipient_account is None:
        raise HTTPException(404, {"code": "NOT_FOUND"})

    if sender_account.id == recipient_account.id:
        raise HTTPException(422, {"code": "SAME_ACCOUNT"})

    result = await transfer_service.transfer(
        db=db,
        redis=redis,
        from_account_id=sender_account.id,
        to_account_id=recipient_account.id,
        amount=body.amount,
        idempotency_key=idempotency_key,
    )
    return {"data": result}
```

### 4b. `transfer()` service implementation

```python
import hashlib, json
from decimal import Decimal
from uuid import UUID
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError
from redis.asyncio import Redis

async def transfer(
    db: AsyncSession,
    redis: Redis,
    from_account_id: UUID,
    to_account_id: UUID,
    amount: Decimal,
    idempotency_key: str,
) -> Transfer:

    # 1. Idempotency check — Redis fast path
    cached_raw = await redis.get(f"idempotency:{idempotency_key}")
    if cached_raw:
        cached = json.loads(cached_raw)
        request_hash = _hash_request(from_account_id, to_account_id, amount)
        if cached["request_hash"] != request_hash:
            raise IdempotencyConflictError()
        return Transfer.model_validate_json(cached["response"])

    try:
        async with db.begin():
            # 2. Lock sender's account row — serializes concurrent transfers from same sender.
            #    Balance is a derived SUM, not a stored column — we lock the accounts row to
            #    serialize the balance check + debit atomically.
            result = await db.execute(
                select(Account)
                .where(Account.id == from_account_id)
                .with_for_update()
            )
            sender = result.scalar_one_or_none()
            if sender is None:
                raise AccountNotFoundError()

            # 3. Derive balance from ledger (safe: sender row is locked)
            balance = await get_balance(db, from_account_id)
            if balance < amount:
                raise InsufficientBalanceError()

            # 4. Atomic double-entry: debit sender, credit receiver
            entry = LedgerEntry(
                debit_account_id=from_account_id,
                credit_account_id=to_account_id,
                amount=amount,
                entry_type="transfer",
                idempotency_key=idempotency_key,
            )
            transfer_record = Transfer(
                from_account_id=from_account_id,
                to_account_id=to_account_id,
                amount=amount,
                status="completed",
                idempotency_key=idempotency_key,
            )
            db.add(entry)
            db.add(transfer_record)
            # COMMIT — both rows written or neither

    except IntegrityError as e:
        # Safety net: Redis cache expired between commit and setex below.
        # The DB unique constraint on idempotency_key fired — this request already
        # committed. Fetch the committed transfer and return it.
        if "idempotency_key" in str(e.orig):
            existing = await db.execute(
                select(Transfer).where(Transfer.idempotency_key == idempotency_key)
            )
            transfer_record = existing.scalar_one()
        else:
            raise

    # 5. Cache successful response — only after confirmed commit
    #    Never cache 4xx (client may retry with fix) or 5xx (may not have committed)
    await redis.setex(
        f"idempotency:{idempotency_key}",
        86400,
        json.dumps({
            "request_hash": _hash_request(from_account_id, to_account_id, amount),
            "response": transfer_record.model_dump_json(),
        })
    )

    return transfer_record


def _hash_request(from_account_id: UUID, to_account_id: UUID, amount: Decimal) -> str:
    payload = f"{from_account_id}:{to_account_id}:{amount}"
    return hashlib.sha256(payload.encode()).hexdigest()
```

**Why `SELECT ... FOR UPDATE` on the `accounts` row:**
Balance is derived from `SUM(ledger_entries)`, not stored in `accounts`. This means there is
no column to lock on. Instead, we lock the `accounts` row itself to **serialize** concurrent
transactions for the same sender. Without this, two concurrent transfers can both read the same
balance, both pass the balance check, and both insert debit entries — overdrafting the account.

Only the sender's row is locked. The receiver just receives a credit entry — no balance check
is needed on the receiver side, so no lock is needed.

---

## 5. `get_balance()` — Ownership and Implementation

`get_balance()` lives in `account_service.py`. It is imported and called by `transfer_service.py`.
Do not duplicate it.

```python
# app/services/account_service.py
async def get_balance(db: AsyncSession, account_id: UUID) -> Decimal:
    result = await db.execute(
        text("""
            SELECT
                COALESCE(SUM(CASE WHEN credit_account_id = :id THEN amount ELSE 0 END), 0)
              - COALESCE(SUM(CASE WHEN debit_account_id  = :id THEN amount ELSE 0 END), 0)
            AS balance
            FROM ledger_entries
            WHERE credit_account_id = :id OR debit_account_id = :id
        """),
        {"id": str(account_id)}
    )
    row = result.scalar()
    return Decimal(str(row)) if row is not None else Decimal("0")
    # Avoid `row or Decimal("0")` — a legitimate zero balance (0.0) is falsy
    # and would be replaced by Decimal("0"), which is correct by coincidence but wrong in intent.
```

**Invariant:** `SUM(all balances across all accounts, including system account) = 0`

The system account balance will be negative — equal to the total money seeded into the system.
This is correct: it represents the bank's liability to its users.

---

## 6. Idempotency Design

```
Request arrives with Idempotency-Key K
    │
    ▼
Redis: key "idempotency:K" exists?
    │
    ├─ Yes → compare request_hash
    │           ├─ hash matches   → return cached response (no DB touch)
    │           └─ hash mismatch  → 409 IDEMPOTENCY_CONFLICT
    │
    └─ No  → process request
               │
               ├─ 2xx success → store { request_hash, response } in Redis (TTL 24h) → return
               ├─ 4xx error   → do NOT cache (client may fix and retry same key)
               └─ 5xx error   → do NOT cache (operation may not have committed)
```

DB unique constraint on `ledger_entries.idempotency_key` and `transfers.idempotency_key` acts
as a safety net if the Redis cache expires between a commit and the cache write — the second
attempt will hit a `UniqueViolation` rather than double-debiting.

---

## 7. Auth Flow

```mermaid
sequenceDiagram
    participant C as Client
    participant API
    participant DB as PostgreSQL
    participant R as Redis

    Note over C,R: Registration
    C->>API: POST /v1/auth/register
    API->>DB: INSERT users (email, bcrypt(password))
    API-->>C: 201 {user_id, email}

    Note over C,R: Login
    C->>API: POST /v1/auth/login
    API->>DB: SELECT user WHERE email = ?
    API->>API: bcrypt.verify(password, hash)
    API->>R: SET refresh:{token} = user_id (TTL 7d)
    API-->>C: 200 {access_token (JWT 15min), refresh_token}

    Note over C,R: Token Refresh
    C->>API: POST /v1/auth/refresh
    API->>R: GET refresh:{old_token} → user_id
    API->>R: DEL refresh:{old_token}
    API->>R: SET refresh:{new_token} = user_id (TTL 7d)
    API-->>C: 200 {new_access_token, new_refresh_token}
```

---

## 8. Middleware Stack and Auth Pattern

```
Incoming request
    │
    ▼
1. CorrelationIDMiddleware  — attach X-Request-ID to every request/response
    │
    ▼
Handler (router → service)
    │
    └── protected routes declare: current_user: User = Depends(get_current_user)
        — JWT decoded here, not in middleware
```

**Auth is a FastAPI dependency, not middleware.** Using middleware for auth requires manually
excluding public paths (`/v1/auth/*`). Using `Depends(get_current_user)` on each route is
idiomatic FastAPI — public routes simply don't declare the dependency, no exclusion list needed.

```python
# Protected route example
@router.get("/me")
async def get_account(
    current_user: User = Depends(get_current_user),  # ← auth enforced here
    db: AsyncSession = Depends(get_db),
):
    ...

# Public route — no Depends(get_current_user), no exclusion logic needed
@router.post("/register")
async def register(body: RegisterRequest, db: AsyncSession = Depends(get_db)):
    ...
```

Rate limiting and API key auth added in Phase 6.

---

## 9. Error Reference

| HTTP | Code | When |
|------|------|------|
| 400 | `BAD_REQUEST` | Malformed JSON, missing required fields |
| 400 | `MISSING_IDEMPOTENCY_KEY` | `POST /v1/transfers` without header |
| 401 | `UNAUTHORIZED` | Missing or expired access token |
| 401 | `INVALID_CREDENTIALS` | Wrong email or password |
| 401 | `INVALID_REFRESH_TOKEN` | Refresh token expired or revoked |
| 403 | `FORBIDDEN` | Dev endpoint called outside dev env |
| 404 | `NOT_FOUND` | Account, user, or transfer does not exist |
| 409 | `EMAIL_ALREADY_EXISTS` | Duplicate registration |
| 409 | `ACCOUNT_ALREADY_EXISTS` | User already has an account |
| 409 | `IDEMPOTENCY_CONFLICT` | Same key, different request body (hash mismatch) |
| 422 | `VALIDATION_ERROR` | Pydantic schema validation failure |
| 422 | `INSUFFICIENT_BALANCE` | Transfer amount exceeds balance |
| 422 | `SAME_ACCOUNT` | Sender and receiver are the same |
| 500 | `INTERNAL_ERROR` | Unhandled exception — log trace, return safe message |

---

## 10. Test Cases

All tests use real PostgreSQL and Redis via `testcontainers` — no mocks.

### `test_auth.py`

| Scenario | Expected |
|----------|----------|
| Register new user | 201, `user_id` returned |
| Register same email again | 409 `EMAIL_ALREADY_EXISTS` |
| Login with correct credentials | 200, `access_token` + `refresh_token` |
| Login with wrong password | 401 `INVALID_CREDENTIALS` |
| Login with unknown email | 401 `INVALID_CREDENTIALS` (no field-level distinction) |
| Refresh with valid token | 200, new token pair returned |
| Refresh again with old token (already rotated) | 401 `INVALID_REFRESH_TOKEN` |
| Refresh with expired token | 401 `INVALID_REFRESH_TOKEN` |
| Access protected route with valid access token | 200 |
| Access protected route with expired access token | 401 `UNAUTHORIZED` |

### `test_accounts.py`

| Scenario | Expected |
|----------|----------|
| Open account | 201, `balance = "0.00000000"` |
| Open account twice | 409 `ACCOUNT_ALREADY_EXISTS` |
| Get account before opening | 404 `NOT_FOUND` |
| Get account after opening | 200, balance derived from ledger |
| Seed account | 201, new balance reflects seeded amount |
| Seed non-existent account | 404 `NOT_FOUND` |
| Call seed endpoint with `APP_ENV=production` | 403 `FORBIDDEN` |

### `test_transfers.py`

| Scenario | Expected |
|----------|----------|
| Transfer happy path | 201, sender balance decreases, receiver balance increases |
| Transfer with insufficient balance | 422 `INSUFFICIENT_BALANCE` |
| Transfer exact balance amount | 201, sender balance = 0 after (not rejected) |
| Transfer to yourself | 422 `SAME_ACCOUNT` |
| Transfer to non-existent email | 404 `NOT_FOUND` |
| Transfer to user with no account | 404 `NOT_FOUND` |
| Transfer without `Idempotency-Key` header | 400 `MISSING_IDEMPOTENCY_KEY` |
| Same `Idempotency-Key` sent twice, same body | 201, second call returns cached response, balance debited only once |
| Same `Idempotency-Key`, different body (different amount) | 409 `IDEMPOTENCY_CONFLICT` |
| Transfer amount = 0 | 422 `VALIDATION_ERROR` |
| Transfer negative amount | 422 `VALIDATION_ERROR` |

### `test_transactions.py`

| Scenario | Expected |
|----------|----------|
| List transactions with no history | 200, empty `data` array |
| List after transfer (sender view) | Entry with `direction = "debit"` |
| List after transfer (receiver view) | Entry with `direction = "credit"` |
| Filter by `from_date` / `to_date` | Only entries within range returned |
| Filter by `entry_type` | Only matching entries returned |
| Page 1 and page 2 | Non-overlapping entries |
| `limit` > 100 | 422 `VALIDATION_ERROR` |

### `test_concurrency.py`

| Scenario | Expected |
|----------|----------|
| 10 concurrent transfers from Alice, each for `balance / 10` | All 10 succeed, final balance = 0, no deadlock |
| 10 concurrent transfers from Alice, each for full `balance` (would overdraw 10×) | Exactly 1 succeeds, 9 fail with `INSUFFICIENT_BALANCE`, final balance = 0 |
| Alice → Bob and Bob → Alice simultaneously (10 rounds) | No deadlock, no money created or destroyed |

```python
# test_concurrency.py — how to write the overdraft test
async def test_no_overdraft_under_concurrency(client, alice, bob):
    await seed(alice, amount=Decimal("100"))

    tasks = [
        transfer(from_=alice, to_=bob, amount=Decimal("100"), key=f"key-{i}")
        for i in range(10)
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    successes = [r for r in results if not isinstance(r, Exception)]
    failures  = [r for r in results if isinstance(r, Exception)]

    assert len(successes) == 1
    assert len(failures)  == 9
    assert await get_balance(alice) == Decimal("0")
    assert await get_balance(bob)   == Decimal("100")
```

### `test_ledger_invariant.py`

Run after every other test to verify the double-entry invariant is never violated.

| Scenario | Expected |
|----------|----------|
| After seed | `SUM(all credits) - SUM(all debits) = 0` |
| After transfer | `SUM(all credits) - SUM(all debits) = 0` |
| After multiple mixed operations | `SUM(all credits) - SUM(all debits) = 0` |
| System account balance after seeding Alice 1000 | `system_account balance = -1000` |

```python
# test_ledger_invariant.py
from decimal import Decimal
from sqlalchemy import text

async def assert_ledger_sums_to_zero(db):
    """
    The double-entry invariant: the sum of every account's derived balance must equal zero.
    system_account balance is negative (money issued into the system).
    All user account balances are positive.
    They must cancel out exactly.

    WRONG approach — always passes regardless of corruption:
        credits = SUM(amount) FROM ledger_entries   ← same query twice
        debits  = SUM(amount) FROM ledger_entries
        assert credits == debits  ← trivially true

    CORRECT approach — sum the derived balance of every account:
    """
    total = await db.scalar(text("""
        SELECT COALESCE(SUM(balance), 0)
        FROM (
            SELECT
                account_id,
                SUM(CASE WHEN credit_account_id = account_id THEN amount
                         WHEN debit_account_id  = account_id THEN -amount
                         ELSE 0 END) AS balance
            FROM accounts
            LEFT JOIN ledger_entries
                ON ledger_entries.credit_account_id = accounts.id
                OR ledger_entries.debit_account_id  = accounts.id
            GROUP BY accounts.id
        ) balances
    """))
    assert Decimal(str(total)) == Decimal("0"), (
        f"Ledger invariant violated: sum of all balances = {total} (expected 0)"
    )


async def test_invariant_after_seed(client, auth_headers, seeded_account, db_session):
    await assert_ledger_sums_to_zero(db_session)


async def test_invariant_after_transfer(client, auth_headers, seeded_account, bob_account, db_session):
    await client.post("/v1/transfers", headers={"Idempotency-Key": "t1"} | auth_headers,
                      json={"to_account_id": bob_account["account_id"], "amount": "100.00"})
    await assert_ledger_sums_to_zero(db_session)


async def test_system_account_is_negative_after_seed(db_session):
    from app.services.account_service import get_balance
    from app.config import SYSTEM_ACCOUNT_ID
    balance = await get_balance(db_session, SYSTEM_ACCOUNT_ID)
    assert balance < Decimal("0"), "System account should be negative after seeding"
```

---

## 11. Codebase Structure

```
minibank/
├── pyproject.toml                     # uv: fastapi, sqlalchemy, alembic, redis, pytest
├── uv.lock                            # committed lockfile — reproducible installs
├── .env.example                       # DATABASE_URL, REDIS_URL, JWT_SECRET, APP_ENV
├── docker-compose.yml                 # postgres:16, redis:7
├── alembic.ini
├── alembic/
│   └── versions/
│       └── 0001_initial_schema.py     # users, accounts (+ system account seed),
│                                      # ledger_entries, transfers
├── app/
│   ├── main.py                        # FastAPI app factory, router registration,
│   │                                  # middleware attachment
│   ├── config.py                      # pydantic-settings: reads .env
│   │                                  # exposes SYSTEM_ACCOUNT_ID constant
│   ├── database.py                    # async engine, AsyncSessionLocal,
│   │                                  # get_db() dependency
│   ├── dependencies.py                # get_current_user() — decodes JWT,
│   │                                  # returns User; get_db() re-export
│   ├── models/
│   │   ├── user.py                    # User ORM model
│   │   ├── account.py                 # Account ORM model
│   │   ├── ledger_entry.py            # LedgerEntry ORM model (append-only)
│   │   └── transfer.py                # Transfer ORM model
│   ├── schemas/
│   │   ├── common.py                  # DataResponse[T], ErrorResponse,
│   │   │                              # PaginatedResponse[T]
│   │   ├── auth.py                    # RegisterRequest, LoginRequest,
│   │   │                              # TokenResponse
│   │   ├── account.py                 # AccountResponse, BalanceResponse,
│   │   │                              # TransactionItem, TransactionListResponse
│   │   └── transfer.py                # TransferRequest, TransferResponse
│   ├── routers/
│   │   ├── auth.py                    # POST /v1/auth/{register,login,refresh}
│   │   ├── users.py                   # GET /v1/users/me
│   │   ├── accounts.py                # POST /v1/accounts
│   │   │                              # GET /v1/accounts/me
│   │   │                              # GET /v1/accounts/me/balance
│   │   │                              # GET /v1/accounts/me/transactions
│   │   ├── transfers.py               # POST /v1/transfers
│   │   │                              # GET /v1/transfers/{id}
│   │   └── dev.py                     # POST /v1/dev/seed
│   │                                  # guarded: APP_ENV == "development"
│   ├── services/
│   │   ├── auth_service.py            # register(), login(), refresh_token()
│   │   │                              # bcrypt (direct, not passlib) hashing
│   │   │                              # PyJWT (not python-jose) token creation, Redis ops
│   │   ├── account_service.py         # open_account(), get_balance(),
│   │   │                              # get_transactions(), seed()
│   │   │                              # get_balance() is imported by transfer_service
│   │   └── transfer_service.py        # transfer() — SELECT FOR UPDATE,
│   │                                  # balance check, ledger insert,
│   │                                  # idempotency (Redis + DB constraint)
│   └── middleware/
│       └── correlation_id.py          # Generates/propagates X-Request-ID
└── tests/
    ├── conftest.py                    # testcontainers: real Postgres + Redis
    │                                  # fixtures: registered user, opened account,
    │                                  # seeded balance
    ├── test_auth.py                   # Registration, login, refresh, expiry, rotation
    ├── test_accounts.py               # Open, balance, duplicate, seed, forbidden in prod
    ├── test_transfers.py              # Happy path, insufficient balance, same account,
    │                                  # idempotency, conflict detection, edge amounts
    ├── test_transactions.py           # Direction derivation, filters, pagination
    ├── test_concurrency.py            # 10 parallel overdraft attempts, no money created
    └── test_ledger_invariant.py       # SUM(all balances) = 0 after every operation
```

---

## 12. Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Balance storage | Derived from ledger SUM | Race-free, self-auditing. No stored column to go stale. |
| Money type | `NUMERIC(20,8)` / Python `Decimal` | Float arithmetic loses precision for monetary values. |
| Locking target | `accounts` row via `SELECT ... FOR UPDATE` | Balance is a derived SUM — lock the row to serialize access to the sender. |
| Lock scope | Sender only | Receiver just receives a credit — no balance check needed, no lock needed. |
| Idempotency layers | Redis (fast path) + DB unique constraint (safety net) | Redis handles the common case; DB constraint catches race between cache expiry and re-use. |
| Idempotency cache content | `{ request_hash, response }` | Hash enables conflict detection (same key, different body) on cache hit. |
| `transfers.status` | Only `completed` ever inserted | Failed transfers raise exceptions and are never persisted — no half-written transfer records. |
| Token storage | JWT (access) + Redis opaque (refresh) | Short-lived JWT is stateless. Refresh in Redis enables instant revocation. |
| System account | One special `accounts` row (`user_id = NULL`) | Preserves double-entry invariant for seed operations without a real counterparty. |
| System account balance | Negative (by design) | Represents money issued into the system — a liability, not an asset. Total across all accounts still sums to zero. |
| Pagination | Offset for now | Simple to implement; replaced with cursor in Phase 6 once the need is understood. |

---

## 13. Bootstrap Files

Everything needed to run `uv sync`, `docker compose up`, `alembic upgrade head`, and `pytest` on day one.

---

### `pyproject.toml`

```toml
[project]
name = "minibank"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = [
    "fastapi>=0.111.0",
    "uvicorn[standard]>=0.29.0",
    "sqlalchemy[asyncio]>=2.0.0",
    "asyncpg>=0.29.0",          # async PostgreSQL driver for SQLAlchemy
    "alembic>=1.13.0",
    "redis[asyncio]>=5.0.0",
    "pydantic>=2.7.0",
    "pydantic-settings>=2.2.0",
    "PyJWT>=2.8.0",                       # JWT — python-jose is unmaintained
    "bcrypt>=4.1.0",                      # password hashing — passlib is unmaintained
]

[dependency-groups]
dev = [
    "pytest>=8.0.0",
    "pytest-asyncio>=0.23.0",
    "httpx>=0.27.0",            # async test client for FastAPI
    "testcontainers[postgres,redis]>=4.4.0",
]

[tool.pytest.ini_options]
asyncio_mode = "auto"           # every async test function runs without @pytest.mark.asyncio
```

---

### `.env.example`

```bash
# Application
APP_ENV=development             # development | production (guards /v1/dev/* endpoints)

# PostgreSQL
DATABASE_URL=postgresql+asyncpg://minibank:minibank@localhost:5432/minibank

# Redis
REDIS_URL=redis://localhost:6379/0

# JWT
JWT_SECRET=change-me-in-production-use-openssl-rand-hex-32
JWT_ALGORITHM=HS256
ACCESS_TOKEN_EXPIRE_MINUTES=15
REFRESH_TOKEN_EXPIRE_DAYS=7

# System account — fixed UUID seeded in 0001_initial_schema.py
SYSTEM_ACCOUNT_ID=00000000-0000-0000-0000-000000000000
```

---

### `docker-compose.yml`

```yaml
services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_USER: minibank
      POSTGRES_PASSWORD: minibank
      POSTGRES_DB: minibank
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U minibank"]
      interval: 5s
      timeout: 5s
      retries: 5

  redis:
    image: redis:7
    ports:
      - "6379:6379"
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 5s
      retries: 5
```

Phase 2 adds Kafka + Zookeeper. Phase 4 adds Jaeger, Prometheus, Grafana. Add them then, not now.

---

### `app/config.py`

```python
from uuid import UUID
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    app_env: str = "development"
    database_url: str
    redis_url: str

    jwt_secret: str
    jwt_algorithm: str = "HS256"
    access_token_expire_minutes: int = 15
    refresh_token_expire_days: int = 7

    system_account_id: UUID = UUID("00000000-0000-0000-0000-000000000000")

settings = Settings()

# Convenience constant — import this everywhere instead of settings.system_account_id
SYSTEM_ACCOUNT_ID: UUID = settings.system_account_id
```

---

### `app/database.py`

```python
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase
from app.config import settings

engine = create_async_engine(
    settings.database_url,
    echo=False,        # set True to log all SQL — useful during development
    pool_size=10,
    max_overflow=20,
)

AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,  # prevent lazy-load errors after commit
)

class Base(DeclarativeBase):
    pass

# FastAPI dependency — yields a session per request, rolls back on exception
async def get_db() -> AsyncSession:
    async with AsyncSessionLocal() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
```

---

### `alembic.ini` (key line only)

```ini
# Change the default sqlalchemy.url — we override it in env.py instead
sqlalchemy.url = driver://user:pass@localhost/dbname
```

---

### `alembic/env.py`

Async SQLAlchemy requires a non-standard Alembic `env.py`. The default generated file uses a
synchronous connection — it will fail silently or error against an `asyncpg` URL.

```python
import asyncio
from logging.config import fileConfig

from alembic import context
from sqlalchemy.ext.asyncio import create_async_engine

from app.config import settings
from app.database import Base

# Import all models so Alembic can see them for autogenerate
from app.models import user, account, ledger_entry, transfer  # noqa: F401

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def run_migrations_offline() -> None:
    context.configure(
        url=settings.database_url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online() -> None:
    connectable = create_async_engine(settings.database_url)
    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)
    await connectable.dispose()


def do_run_migrations(connection):
    context.configure(connection=connection, target_metadata=target_metadata)
    with context.begin_transaction():
        context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())
```

---

### `tests/conftest.py`

**Fixture dependency graph:**
```
postgres_container (session) ──► db_session (function) ──► client (function)
                                                      └──► redis_client (function)
redis_container (session) ────────────────────────────────┘

client ──► registered_user ──► auth_headers ──► opened_account ──► seeded_account
client ──► bob_registered ──► bob_headers ──► bob_account
```

**Key design decisions:**
- **One engine per test** — Each test gets its own engine (reusing the shared postgres_container). This avoids connection pool exhaustion and allows TRUNCATE CASCADE cleanup without event loop conflicts.
- **TRUNCATE CASCADE for cleanup** — Truncates all test-created tables before each test and re-seeds the system account. Safer than DELETE (respects FK order automatically) and resilient to schema additions.
- **Same URL for db_session and client** — Both use the same postgres_container connection string so app-committed data is immediately visible to db_session queries.
- **Bob fixtures eliminate boilerplate** — `bob_registered`, `bob_headers`, `bob_account` replace the 8-line registration sequence duplicated across transfer, transaction, and concurrency tests.
- **No @pytest.mark.asyncio needed** — `asyncio_mode = "auto"` in pyproject.toml auto-marks async test functions.

```python
import pytest
import pytest_asyncio
from collections.abc import AsyncGenerator
from httpx import AsyncClient, ASGITransport
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from testcontainers.postgres import PostgresContainer
from testcontainers.redis import RedisContainer
from redis.asyncio import Redis

from app.main import create_app
from app.database import Base, get_db
from app.dependencies import get_redis
from app.config import SYSTEM_ACCOUNT_ID


@pytest.fixture(scope="session")
def postgres_container():
    with PostgresContainer("postgres:16") as pg:
        yield pg


@pytest.fixture(scope="session")
def redis_container():
    with RedisContainer("redis:7") as r:
        yield r


@pytest_asyncio.fixture
async def db_session(postgres_container) -> AsyncGenerator[AsyncSession, None]:
    """
    Create a fresh session for each test with TRUNCATE CASCADE cleanup.
    Each test gets its own engine and session bound to the shared postgres_container.
    """
    url = postgres_container.get_connection_url().replace("psycopg2", "asyncpg")
    engine = create_async_engine(url, pool_size=5, max_overflow=10)

    # Create tables on first use (idempotent via run_sync)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # Clean up non-system data before each test
    async with engine.begin() as conn:
        await conn.execute(text("TRUNCATE transfers, ledger_entries, accounts, users RESTART IDENTITY CASCADE"))
        await conn.execute(
            text("""
                INSERT INTO accounts (id, user_id, status, created_at, updated_at)
                VALUES (:id, NULL, 'active', NOW(), NOW())
            """),
            {"id": str(SYSTEM_ACCOUNT_ID)},
        )

    session_factory = async_sessionmaker(bind=engine, expire_on_commit=False)
    async with session_factory() as session:
        yield session

    await engine.dispose()


@pytest_asyncio.fixture
async def redis_client(redis_container) -> AsyncGenerator[Redis, None]:
    host = redis_container.get_container_host_ip()
    port = redis_container.get_exposed_port(6379)
    client = Redis.from_url(f"redis://{host}:{port}/0", decode_responses=True)
    yield client
    await client.flushdb()
    await client.aclose()


@pytest_asyncio.fixture
async def client(postgres_container, db_session, redis_client) -> AsyncGenerator[AsyncClient, None]:
    """
    HTTP test client with DB and Redis overridden to use test containers.
    Uses the same postgres_container as db_session so data is immediately visible.
    """
    app = create_app()
    url = postgres_container.get_connection_url().replace("psycopg2", "asyncpg")

    async def override_get_db() -> AsyncGenerator[AsyncSession, None]:
        # Reuse the same engine/url as db_session for data visibility
        engine = create_async_engine(url, pool_size=5, max_overflow=10)
        session_factory = async_sessionmaker(bind=engine, expire_on_commit=False)
        async with session_factory() as session:
            try:
                yield session
            except Exception:
                await session.rollback()
                raise
            finally:
                await engine.dispose()

    async def override_get_redis() -> Redis:
        return redis_client

    app.dependency_overrides[get_db] = override_get_db
    app.dependency_overrides[get_redis] = override_get_redis

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        yield ac


@pytest_asyncio.fixture
async def registered_user(client) -> dict:
    resp = await client.post("/v1/auth/register", json={
        "email": "alice@example.com", "password": "password123"
    })
    return resp.json()["data"]


@pytest_asyncio.fixture
async def auth_headers(client, registered_user) -> dict:
    resp = await client.post("/v1/auth/login", json={
        "email": "alice@example.com", "password": "password123"
    })
    token = resp.json()["data"]["access_token"]
    return {"Authorization": f"Bearer {token}"}


@pytest_asyncio.fixture
async def opened_account(client, auth_headers) -> dict:
    resp = await client.post("/v1/accounts", headers=auth_headers)
    return resp.json()["data"]


@pytest_asyncio.fixture
async def seeded_account(client, auth_headers, opened_account) -> dict:
    headers = {"Idempotency-Key": "seed-fixture"} | auth_headers
    await client.post(
        "/v1/dev/seed",
        headers=headers,
        json={"account_id": opened_account["account_id"], "amount": "1000.00"},
    )
    return opened_account


@pytest_asyncio.fixture
async def bob_registered(client) -> dict:
    resp = await client.post("/v1/auth/register", json={
        "email": "bob@example.com", "password": "password123"
    })
    return resp.json()["data"]


@pytest_asyncio.fixture
async def bob_headers(client, bob_registered) -> dict:
    resp = await client.post("/v1/auth/login", json={
        "email": "bob@example.com", "password": "password123"
    })
    token = resp.json()["data"]["access_token"]
    return {"Authorization": f"Bearer {token}"}


@pytest_asyncio.fixture
async def bob_account(client, bob_headers) -> dict:
    resp = await client.post("/v1/accounts", headers=bob_headers)
    return resp.json()["data"]
```

---

### `app/dependencies.py`

```python
import jwt
from jwt.exceptions import InvalidTokenError
from fastapi import Depends, Header, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import get_db
from app.models.user import User

bearer_scheme = HTTPBearer()

# --- Redis ---

_redis_client: Redis | None = None

async def get_redis() -> Redis:
    """FastAPI dependency — returns a shared Redis client (one connection pool per process)."""
    global _redis_client
    if _redis_client is None:
        _redis_client = Redis.from_url(settings.redis_url, decode_responses=True)
    return _redis_client

# --- Auth ---

async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme),
    db: AsyncSession = Depends(get_db),
) -> User:
    """
    FastAPI dependency for protected routes.
    Decodes JWT, loads the user from DB, raises 401 on any failure.
    Usage: current_user: User = Depends(get_current_user)
    """
    credentials_exception = HTTPException(
        status_code=401,
        detail={"code": "UNAUTHORIZED", "message": "Invalid or expired token"},
    )
    try:
        payload = jwt.decode(
            credentials.credentials,
            settings.jwt_secret,
            algorithms=[settings.jwt_algorithm],
        )
        user_id: str | None = payload.get("sub")
        if user_id is None:
            raise credentials_exception
    except InvalidTokenError:
        raise credentials_exception

    user = await db.get(User, user_id)
    if user is None:
        raise credentials_exception
    return user
```

---

### `app/main.py`

```python
from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.routers import auth, users, accounts, transfers, dev
from app.middleware.correlation_id import CorrelationIDMiddleware


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup logic here (e.g. warm Redis connection pool)
    yield
    # Shutdown logic here


def create_app() -> FastAPI:
    app = FastAPI(title="MiniBank", version="1.0.0", lifespan=lifespan)

    app.add_middleware(CorrelationIDMiddleware)

    app.include_router(auth.router,      prefix="/v1/auth",      tags=["auth"])
    app.include_router(users.router,     prefix="/v1/users",     tags=["users"])
    app.include_router(accounts.router,  prefix="/v1/accounts",  tags=["accounts"])
    app.include_router(transfers.router, prefix="/v1/transfers",  tags=["transfers"])
    app.include_router(dev.router,       prefix="/v1/dev",        tags=["dev"])

    return app


app = create_app()  # module-level instance for uvicorn
```
