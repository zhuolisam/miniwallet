# Explorer Report
Task: Minibank Codebase Audit
Date: 2026-05-06
Status: COMPLETED

## Codebase Structure

```
minibank/
├── app/                          # FastAPI application core
│   ├── main.py                   # App factory + error handlers
│   ├── config.py                 # Settings + SYSTEM_ACCOUNT_ID
│   ├── database.py               # SQLAlchemy engine + Base
│   ├── dependencies.py           # FastAPI dependency injection
│   ├── exceptions.py             # Domain exception classes
│   │
│   ├── models/                   # SQLAlchemy ORM models
│   │   ├── user.py               # Users table
│   │   ├── account.py            # Accounts table (double-entry ledger root)
│   │   ├── ledger_entry.py       # Double-entry bookkeeping (debit/credit pairs)
│   │   ├── transfer.py           # Transfer records + idempotency + status
│   │   ├── audit_event.py        # Phase 2: audit event log
│   │   └── outbox.py             # Phase 2: transactional event outbox
│   │
│   ├── services/                 # Business logic
│   │   ├── auth_service.py       # JWT, password hashing
│   │   ├── user_service.py       # User CRUD
│   │   ├── account_service.py    # Balance derivation, account creation
│   │   └── transfer_service.py   # Core transfer logic (idempotency, atomicity)
│   │
│   ├── routers/                  # FastAPI API endpoints
│   │   ├── auth.py               # POST /v1/auth/register, /login, /refresh
│   │   ├── users.py              # GET /v1/users/me
│   │   ├── accounts.py           # Account CRUD + balance + transactions
│   │   ├── transfers.py          # POST /v1/transfers, GET /v1/transfers/{id}
│   │   └── dev.py                # POST /v1/dev/seed (test fixture)
│   │
│   ├── schemas/                  # Pydantic request/response models
│   │   ├── common.py             # DataResponse, ErrorResponse, pagination
│   │   ├── auth.py               # Login, register, token schemas
│   │   ├── account.py            # Account, balance, transaction schemas
│   │   └── transfer.py           # TransferRequest, TransferResponse
│   │
│   ├── events/                   # Phase 2: Event-driven architecture
│   │   ├── publisher.py          # publish_event() — writes to outbox atomically
│   │   └── schemas.py            # Event envelope + payload Pydantic models
│   │
│   └── middleware/
│       └── correlation_id.py     # Request tracing
│
├── workers/                      # Background processing
│   ├── outbox_relay.py           # Phase 2: polls outbox, publishes to Kafka
│   └── audit_consumer.py         # Phase 2: consumes transfer.events, persists audit log
│
├── alembic/                      # Database migrations
│   └── versions/
│       ├── 0001_initial_schema.py       # Users, accounts, ledger_entries, transfers
│       ├── 0002_transfer_failed_support.py # Add failure_code column
│       ├── 0003_add_audit_events.py     # Audit event table
│       └── 0004_add_outbox.py           # Outbox table + indexes
│
├── tests/                        # Test suite
│   ├── conftest.py               # Pytest fixtures (postgres, redis, kafka testcontainers)
│   ├── test_auth.py
│   ├── test_accounts.py
│   ├── test_transfers.py
│   ├── test_concurrency.py       # Double-entry atomicity tests
│   ├── test_ledger_invariant.py  # Ledger sum = 0 invariant
│   ├── test_transactions.py      # Balance calculation from ledger
│   ├── test_event_schemas.py     # Event envelope validation
│   ├── test_audit_consumer.py    # Audit consumer idempotency
│   ├── test_outbox_integration.py # Outbox → Kafka integration
│   └── test_outbox_relay.py      # Relay claim/confirm/recovery logic
│
├── docker-compose.yml            # PostgreSQL, Redis, Kafka, Zookeeper stack
├── pyproject.toml                # Dependencies (FastAPI, SQLAlchemy, Kafka, Redis)
└── CLAUDE.md                     # Teaching philosophy + operational guidance
```

## Relevant Files

### Core Transaction Logic
- `/Users/sam.zhuoli/personal-playground/minibank/app/services/transfer_service.py` — Heart of the system
  - `transfer()`: idempotency (Redis + DB), account locking (sorted UUID order), double-entry atomicity, outbox event publishing
  - `get_transfer()`: permission-checked transfer retrieval
  - Helper: `_hash_request()` for idempotency key validation

### Data Models (Double-Entry Bookkeeping)
- `/Users/sam.zhuoli/personal-playground/minibank/app/models/ledger_entry.py` — Debit/credit pair (essence of ledger)
- `/Users/sam.zhuoli/personal-playground/minibank/app/models/transfer.py` — Transfer metadata + status tracking
- `/Users/sam.zhuoli/personal-playground/minibank/app/models/account.py` — Account root (no balance column)
- `/Users/sam.zhuoli/personal-playground/minibank/app/models/outbox.py` — Transactional outbox (status: pending → publishing → published/failed)

### Event-Driven Architecture (Phase 2)
- `/Users/sam.zhuoli/personal-playground/minibank/app/events/publisher.py` — Single API to write events to outbox atomically
- `/Users/sam.zhuoli/personal-playground/minibank/app/events/schemas.py` — Typed event contracts (TransferCompletedPayload, TransferFailedPayload, etc.)
- `/Users/sam.zhuoli/personal-playground/minibank/workers/outbox_relay.py` — Claim-publish-confirm loop + recovery/cleanup
- `/Users/sam.zhuoli/personal-playground/minibank/workers/audit_consumer.py` — Minimal consumer (Week 6 baseline, no retry/DLQ yet)

### API Layer
- `/Users/sam.zhuoli/personal-playground/minibank/app/routers/transfers.py` — POST transfer (requires Idempotency-Key header), GET transfer detail
- `/Users/sam.zhuoli/personal-playground/minibank/app/routers/accounts.py` — Account CRUD, balance query, transaction history
- `/Users/sam.zhuoli/personal-playground/minibank/app/main.py` — App factory, error handlers, router registration

### Testing Infrastructure
- `/Users/sam.zhuoli/personal-playground/minibank/tests/conftest.py` — Testcontainer fixtures (postgres, redis, kafka), client factory, seeded account fixtures

## Patterns & Conventions

### Double-Entry Bookkeeping
- Every money movement is a `LedgerEntry` with `debit_account_id` and `credit_account_id`
- Balance = SUM(credits) - SUM(debits) WHERE account_id is either side
- `Transfer` table links to the `LedgerEntry` via `reference_id` for traceability
- Decimal precision: `NUMERIC(20, 8)` (18 integral digits, 8 decimal places — typical for fintech)

### Idempotency (Week 5 Foundation)
- Client supplies `Idempotency-Key` header (HTTP status 201 implies newly created)
- Two-tier check: Redis fast path (hash-based conflict detection) → DB unique constraint
- Key formula: `SHA256(from_account_id:to_account_id:amount:.8f)` for request hash
- If key reused with different params → `IdempotencyConflictError`
- If key reused with same params → return cached response

### Concurrency Control
- Account locking in sorted UUID order to prevent bidirectional deadlock
- `SELECT ... FOR UPDATE` on both accounts before ledger write
- Caller must lock the account row; SQLAlchemy autobegin on first query, manual commit after locks

### Transactional Outbox (Week 7 Pattern)
- Events written to `outbox` table in the SAME transaction as domain state
- Outbox status flow: `pending` → `claiming` → `publishing` → `published` | `failed` | `pending` (retry)
- Relay uses `FOR UPDATE SKIP LOCKED` so multiple relay instances claim different rows
- Recovery: rows stuck in `publishing` > 5 min are reset to `pending` (process crash detection)
- Cleanup: published rows deleted after 7 days, failed rows after 30 days

### Event Schema Versioning (Type Safety)
- `EventEnvelope` wraps all events with `event_id`, `occurred_at`, `actor_id`, `version`
- Payload models are Pydantic classes registered in `PAYLOAD_MODELS` dispatch table
- Consumer-side `parse_event()` validates structure; unknown types log warning but don't crash
- Typos in event construction caught at publish time (Pydantic validation)

### Error Handling
- All domain errors inherit from `MiniBankError` (status_code, error_code, message)
- FastAPI exception handlers convert to JSON envelope: `{"error": {"code": "...", "message": "..."}}`
- Validation errors normalized to 422 with field-level details

### Decimal Handling
- All monetary amounts passed as strings (API) → `Decimal` (Python) → `NUMERIC(20,8)` (DB)
- Never float in financial systems (rounding errors)
- String formatting: `.8f` (8 decimal places) for JSON serialization

### Testing Philosophy
- Testcontainers (postgres, redis, kafka) for isolation + determinism
- `db_session` fixture provides fresh DB per test with TRUNCATE CASCADE
- `conftest.py` has helper `make_event()` for constructing event envelopes in tests
- Consumer tests use `account_factory()` to create ORM objects directly (bypasses HTTP stack)

## Key Findings

### Architecture Highlights
1. **True Double-Entry Bookkeeping**: Every transfer creates two `LedgerEntry` rows (debit/credit), not a single balance mutation. This ensures ledger invariant: SUM(all entries) = 0.

2. **Idempotency is Built-In**: Requests are idempotent by design (not bolted-on). Redis caches responses; DB unique constraint on `idempotency_key` catches duplicates.

3. **Outbox Pattern (Week 7)**: Eliminates the dual-write problem from Week 6. Events are atomic with state changes—if the process crashes after DB commit, events are already persisted for relay.

4. **Concurrency Safety**: Account locking in sorted UUID order prevents deadlock in bidirectional transfers. Tests validate this (see `test_concurrency.py`).

5. **Type-Safe Events**: Pydantic models define the event contract. A field typo at publish time is a runtime error, not a silent bug at consume time.

### Critical Production-Grade Decisions
- **Ledger Invariant Enforcement**: No balance column on `Account` — balance always computed from ledger. If ledger is corrupt, balance query reveals it.
- **Relay Recovery**: Stuck rows (process crash between claim and confirm) are automatically reset after 5 minutes. No manual intervention needed for common failure modes.
- **Outbox Cleanup**: Prevents unbounded table growth. Published rows → 7 days, failed rows → 30 days for investigation.
- **Consumer Idempotency**: `AuditEvent` table has `UNIQUE(event_id)` constraint. Duplicate Kafka redelivery (offset commit failure) is a safe no-op.

### What the User is Learning
- Financial ledgers are double-entry, not balance mutations
- Concurrency control requires explicit locking to prevent race conditions
- Events must be atomic with state or you have dual-write bugs (solution: outbox pattern)
- Idempotency is not free—it requires caching, DB uniqueness, and request hash validation
- Decimal precision matters—use NUMERIC, never float

### Deployment Model
- Single API container (no Kafka producer—outbox relay handles publishing)
- Separate `outbox-relay` container (polls DB, publishes to Kafka)
- Separate `audit-consumer` container (subscribes to `transfer.events`, persists to audit_events table)
- All share PostgreSQL (app + workers), Redis (idempotency cache), Kafka (event bus)

### Migration History (4 versions)
1. Initial schema: users, accounts, ledger_entries, transfers, idempotency key unique constraint
2. Transfer failed support: added `failure_code` column for insufficient balance tracking
3. Audit events table: Phase 2 event logging
4. Outbox table: Phase 2 transactional event guarantee
