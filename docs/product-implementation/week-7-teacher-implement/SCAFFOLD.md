# Week 7 Scaffold — What the Teacher Built

Everything listed here is **complete, working code** — no TODOs. It is the foundation your implementation builds on.

---

## 1. Data Layer

### `app/models/outbox.py` — OutboxRow ORM model

| Column | Type | Constraint | Purpose |
|--------|------|------------|---------|
| `id` | UUID | PK | Row identity |
| `topic` | VARCHAR(100) | NOT NULL | Kafka topic to publish to (`transfer.events`, `account.events`) |
| `event_type` | VARCHAR(100) | NOT NULL | Domain event name (`transfer.completed`, etc.) |
| `payload` | JSONB | NOT NULL | Full event envelope including `event_id` — travels unchanged to Kafka consumers |
| `status` | VARCHAR(20) | NOT NULL DEFAULT 'pending' | Lifecycle: `pending` → `publishing` → `published` (or `failed`) |
| `retry_count` | INT | NOT NULL DEFAULT 0 | Number of failed publish attempts |
| `created_at` | TIMESTAMPTZ | NOT NULL DEFAULT NOW() | When the outbox row was written |
| `published_at` | TIMESTAMPTZ | nullable | When the relay delivered it to Kafka |

**Partial indexes:**
- `idx_outbox_pending`: `WHERE status = 'pending'` — relay only scans rows it needs
- `idx_outbox_publishing`: `WHERE status = 'publishing'` — recovery of stuck rows

### `alembic/versions/0004_add_outbox.py` — Migration

Creates the `outbox` table with raw SQL (same pattern as 0001–0003). Includes both partial indexes.

### `alembic/env.py` — Model import

Added `outbox` to the import line so Alembic's autogenerate sees the model.

---

## 2. Event Publisher Helper

### `app/events/publisher.py` — `publish_event()`

The **only way** to write to the outbox. Generates a fresh `event_id`, constructs the full event envelope, and inserts an `OutboxRow` in the caller's current DB transaction.

```python
def publish_event(
    db: AsyncSession,
    topic: str,
    event_type: str,
    payload: BaseModel,        # Pydantic model, NOT raw dict
    actor_id: uuid.UUID | None = None,
) -> None:
```

**Key design decisions:**
- `publish_event()` calls `db.add()` — it does NOT commit. The caller commits. This is what makes it transactional — the outbox row is in the same transaction as the domain data.
- `event_id` is a fresh UUID generated inside `publish_event()`, not the entity's ID. Consumer idempotency relies on `event_id` being stable across replays.
- `payload` must be a Pydantic model. `.model_dump()` guarantees JSON-serializable primitives.

---

## 3. Docker Infrastructure Changes

### `docker-compose.yml`

| Change | Detail |
|--------|--------|
| **Removed** `kafka-init` dependency from `api` | The API no longer connects to Kafka — only writes to the DB |
| **Added** `outbox-relay` service | `python -m workers.outbox_relay`, depends on `kafka-init` + `postgres` |

The `api` service now depends only on `postgres` and `redis`. The outbox relay is the only process that connects to Kafka for publishing.

### `Dockerfile`

Added `COPY management/ management/` for the backfill script directory (Week 8).

---

## 4. Transfer Service Refactor

### `app/services/transfer_service.py`

**Removed:**
- Module-level `kafka_producer` global variable
- `start_producer()` and `stop_producer()` functions
- All inline `kafka_producer.send_and_wait()` calls after `db.commit()`
- All `aiokafka` imports

**Added:**
- `# TODO: student` markers at the two places where `publish_event()` must be called:
  1. Before `await db.commit()` on the **failure path** (transfer.failed)
  2. Before `await db.commit()` on the **success path** (transfer.completed)

The TODO comments include exact function signatures, parameter values, and the key insight: `publish_event()` is called BEFORE commit, not after.

---

## 5. Account Service Events

### `app/services/account_service.py`

**Added** `# TODO: student` markers at two places:
1. `open_account()` — publish `account.opened` event before `await db.commit()`
2. `seed()` — publish `seed.completed` event before `await db.commit()`

Both comments include the exact `publish_event()` call with topic, event_type, and payload construction.

---

## 6. API Lifespan Cleanup

### `app/main.py`

**Removed:** `start_producer()` / `stop_producer()` imports and calls from the lifespan context manager. The lifespan is now a pass-through `yield` — the API has no Kafka lifecycle to manage.

**Added:** `# TODO: student` comment explaining why the producer was removed.

---

## 7. Outbox Relay Worker (Skeleton)

### `workers/outbox_relay.py`

The relay file is provided with:
- All imports, logging setup, and constants (`BATCH_SIZE`, `MIN_SLEEP`, `MAX_SLEEP`, `MAX_OUTBOX_RETRIES`)
- Function signatures with full docstrings for: `claim_batch()`, `confirm_batch()`, `recover_stuck_rows()`, `cleanup_published_rows()`, `relay_loop()`
- The `main()` entrypoint (FULL — creates the Kafka producer and calls `relay_loop`)
- `# TODO: student` markers inside each function body with detailed implementation instructions

---

## 8. Test Infrastructure

### `tests/conftest.py` — Updated TRUNCATE

Both `db_session` and `consumer_db_factory` now TRUNCATE `outbox` alongside existing tables.

Added `OutboxRow` model import so `Base.metadata.create_all` creates the outbox table in tests.

### `tests/test_outbox_relay.py` — Relay test skeleton

10 tests covering:
- `claim_batch`: returns pending rows, skips non-pending, respects batch size, handles empty outbox
- `confirm_batch`: persists published status, persists retry
- `recover_stuck_rows`: resets old publishing rows, ignores recent ones
- `cleanup_published_rows`: deletes old published rows

Includes helper functions `_insert_outbox_row()` and `_count_rows()`.

### `tests/test_outbox_integration.py` — Integration test skeleton

5 tests covering:
- `transfer.completed` outbox row created on successful transfer
- `transfer.failed` outbox row created on insufficient balance
- `account.opened` outbox row created on account open
- `seed.completed` outbox row created on seed
- Atomicity: no outbox row when domain write fails (404)

Uses the full HTTP client stack to make API calls, then queries the outbox table.

---

## 9. Management Package

### `management/__init__.py`

Empty file — makes `management/` a Python package so `python -m management.backfill_events` works (Week 8).
