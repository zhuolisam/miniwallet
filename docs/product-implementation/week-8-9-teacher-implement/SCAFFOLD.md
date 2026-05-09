# Week 8 Scaffold — What the Teacher Built

Everything listed here is **complete, working code** — no TODOs. It is the foundation your implementation builds on.

---

## 1. Data Layer

### `app/models/transaction_activity.py` — TransactionActivity ORM model

| Column | Type | Constraint | Purpose |
|--------|------|------------|---------|
| `id` | UUID | PK | Row identity |
| `event_id` | UUID | NOT NULL | Links back to the originating event |
| `account_id` | UUID | NOT NULL, FK→accounts | The account this activity row belongs to |
| `direction` | VARCHAR(10) | NOT NULL | `"debit"` or `"credit"` |
| `amount` | NUMERIC(20,8) | NOT NULL | Transfer/seed amount |
| `entry_type` | VARCHAR(30) | NOT NULL | `"transfer"` or `"seed"` |
| `reference_id` | UUID | nullable | Transfer ID for transfer events; NULL for seeds |
| `occurred_at` | TIMESTAMPTZ | NOT NULL | When the event happened (from event envelope) |

**Unique constraint:** `(event_id, account_id)` — one transfer event creates two rows (debit + credit for different accounts). Unique on just `event_id` would reject the second row. This pair ensures consumer idempotency.

### `alembic/versions/0005_add_transaction_activity.py` — Migration

Creates the `transaction_activity` table with raw SQL. Adds the composite unique constraint and the `(account_id, occurred_at DESC)` index for efficient per-account queries.

### `alembic/env.py` — Model import

Added `transaction_activity` to the import line so Alembic's autogenerate sees the model.

---

## 2. BaseConsumer Infrastructure

### `consumers/__init__.py` — Package marker

Empty file — makes `consumers/` a Python package so `from consumers.consumer_base import BaseConsumer` works.

### `consumers/consumer_base.py` — BaseConsumer class

The retry and DLQ machinery that all consumers inherit. **Teacher-provided (complete):**
- `__init__(self, db_factory)` — stores the session factory
- `process(self, event)` — abstract method (subclasses implement)
- `run(self)` — full consumer loop with startup, lag logging, shutdown (**complete**)
- `handle_message(self, message, producer, consumer)` — **SKELETON (student implements)**

The `run()` method is complete because it's boilerplate (consumer lifecycle, lag logging). The interesting logic is in `handle_message()` — retry counting, DLQ routing, commit semantics.

---

## 3. Upgraded Audit Consumer

### `workers/audit_consumer.py` — AuditConsumer (extends BaseConsumer)

Week 6's minimal consumer is replaced with a proper `BaseConsumer` subclass. The `process()` method is **complete** (teacher-provided) — it's the same logic from Week 6, now structured as a class method using `self.db_factory`.

This gives the student a working reference implementation when building the activity and notification consumers.

---

## 4. Worker Skeletons

### `workers/activity_consumer.py` — ActivityConsumer

Class definition, imports, logging setup, `__main__` entrypoint — all complete. The `process()` method is a **skeleton** with detailed TODO:student comments.

### `workers/notification_consumer.py` — NotificationConsumer

Same pattern. Class definition complete, `process()` is a skeleton.

---

## 5. Backfill Management Command

### `management/backfill_events.py` — `backfill()` function

Function signature, imports, docstring, and `__main__` entrypoint are complete. The implementation (preflight guard + 3 data loops) is a **skeleton** with detailed TODO:student comments.

---

## 6. CQRS Migration (Schema + Router)

### `app/schemas/common.py` — PaginationMeta gains `as_of`

```python
class PaginationMeta(BaseModel):
    total: int
    page: int
    limit: int
    as_of: datetime | None = None  # ← Phase 2 addition
```

Backward compatible — `as_of` defaults to None so existing responses still validate.

### `app/routers/accounts.py` — Response construction

The router now unpacks a 3-tuple `(items, total, as_of)` from `get_transactions()` and includes `as_of` in the response meta. **Complete** — no student work here.

### `app/services/account_service.py` — `get_transactions()` migrated

The function signature changes from `-> tuple[list, int]` to `-> tuple[list, int, datetime | None]`. The implementation is a **skeleton** — student builds the TransactionActivity query.

---

## 7. Docker Infrastructure

### `docker-compose.yml` — Two new services

| Service | Command | Purpose |
|---------|---------|---------|
| `activity-consumer` | `python -m workers.activity_consumer` | Builds CQRS read model |
| `notification-consumer` | `python -m workers.notification_consumer` | Logs simulated notifications |

Both depend on `kafka-init` (topics must exist). Activity consumer also depends on `postgres`.

### `kafka/create-topics.sh` — DLQ topics added

Three new topics with infinite retention:
- `minibank.audit-consumer.dlq`
- `minibank.activity-consumer.dlq`
- `minibank.notification-consumer.dlq`

### `Dockerfile` — Added `COPY consumers/ consumers/`

The `consumers/` package must be available in the Docker image for worker imports.

---

## 8. Test Infrastructure

### `tests/conftest.py` additions

- **Import:** `TransactionActivity` model (so `Base.metadata.create_all` creates the table)
- **TRUNCATE:** Added `transaction_activity` to the cleanup list
- **New fixtures:** `transfer_factory` and `seed_factory` for backfill tests

### Test skeletons (5 files)

All test files have complete class/function structure, fixture wiring, and test data setup. The assertion logic is marked `TODO:student`.

| File | Tests |
|------|-------|
| `tests/test_activity_consumer.py` | 5 tests (transfer→2 rows, seed→1 row, idempotent, ignore account.opened, ignore transfer.failed) |
| `tests/test_notification_consumer.py` | 4 tests (completed, failed, opened, seed no-op) |
| `tests/test_transactions_cqrs.py` | 6 tests (reads activity, created_at preserved, as_of, null as_of, date filter, entry_type filter) |
| `tests/test_backfill.py` | 4 tests (all data, failed transfers, double-run guard, force bypass) |
| `tests/test_dlq_routing.py` | 3 tests (malformed JSON→DLQ, retry→DLQ, idempotent replay safe) |
