# Week 6 Scaffold — What the Teacher Built

Everything listed here is **complete, working code** — no TODOs. It is the foundation your implementation builds on.

---

## 1. Configuration (two one-liners)

### `app/config.py` — Kafka bootstrap server setting

```python
kafka_bootstrap_servers: str = "localhost:9092"
```

Added to the `Settings` class. Pydantic-settings reads `KAFKA_BOOTSTRAP_SERVERS` from the environment (or `.env`). Default is `localhost:9092` for host dev; Docker overrides to `kafka:9092` via `.env`.

### `app/database.py` — Worker session factory alias

```python
db_factory = AsyncSessionLocal
```

Workers (audit consumer, future relay) run outside FastAPI so they can't use the `get_db()` dependency. They import `db_factory` instead — same session factory, just a different name. `expire_on_commit=False` is already set (inherited from `AsyncSessionLocal`), which prevents `MissingGreenlet` errors when accessing ORM attributes on detached objects.

---

## 2. Data Layer

### `app/models/audit_event.py` — AuditEvent ORM model

| Column | Type | Constraint | Purpose |
|--------|------|------------|---------|
| `id` | UUID | PK | Row identity |
| `event_id` | UUID | UNIQUE NOT NULL | **Idempotency key** — one row per event, duplicates rejected by DB |
| `event_type` | VARCHAR(100) | NOT NULL | Domain event name: `transfer.completed`, `transfer.failed`, `account.opened`, `seed.completed` |
| `actor_id` | UUID | nullable | User who triggered the action (NULL for backfilled historical events) |
| `resource_id` | UUID | nullable | Primary entity the event is about — `transfer_id` for transfer events, `account_id` for account events |
| `resource_type` | VARCHAR(50) | nullable | Entity type: `"transfer"` or `"account"` |
| `payload` | JSONB | NOT NULL | Full event payload (the inner `payload` dict from the event envelope) |
| `occurred_at` | TIMESTAMPTZ | NOT NULL | When the event happened (from the event envelope, not insertion time) |

### `alembic/versions/0003_add_audit_events.py` — Migration

Creates the `audit_events` table with raw SQL (same pattern as 0001/0002). `event_id` gets a UNIQUE constraint for consumer idempotency.

### `alembic/env.py` — Model import

Added `audit_event` to the import line so Alembic's autogenerate sees the model.

---

## 3. Docker Infrastructure

### `Dockerfile`

- Base: `python:3.12-slim`
- Uses `uv` for dependency installation (layer-cached)
- `ENV PATH="/app/.venv/bin:$PATH"` so `python -m workers.audit_consumer` finds project deps
- Default CMD: `uvicorn app.main:app --host 0.0.0.0 --port 8000`
- Each docker-compose service overrides CMD for its specific worker

### `docker-compose.yml` — New services added to existing postgres + redis

| Service | Image | Purpose | Depends on |
|---------|-------|---------|------------|
| `zookeeper` | cp-zookeeper:7.6.0 | Kafka coordination | — |
| `kafka` | cp-kafka:7.6.0 | Event bus. Dual listener: `kafka:9092` (Docker internal) + `localhost:29092` (host dev) | zookeeper |
| `kafka-init` | cp-kafka:7.6.0 | One-shot: runs `create-topics.sh` then exits | kafka (healthy) |
| `api` | build . | FastAPI app (containerised in Phase 2) | postgres, redis, kafka-init |
| `audit-consumer` | build . | `python -m workers.audit_consumer` | kafka-init, postgres |

**Dual Kafka listener explained:** A single listener advertising `kafka:9092` resolves inside Docker but fails on the host (DNS can't resolve `kafka`). Two listeners solve this — containers connect via `kafka:9092`, host tools via `localhost:29092`. Set `KAFKA_BOOTSTRAP_SERVERS` accordingly.

**Week 6 note:** The `api` service depends on `kafka-init` because the API publishes events directly to Kafka inline. Remove this dependency in Week 7 when the outbox replaces inline publishing — the API no longer connects to Kafka directly.

### `kafka/create-topics.sh`

Creates two topics with 1 partition and replication-factor 1:
- `transfer.events` — carries `transfer.completed` and `transfer.failed`
- `account.events` — carries `account.opened` and `seed.completed` (Week 7+)

### `.env`

Docker-internal hostnames. Not committed to git. Contents:

```
DATABASE_URL=postgresql+asyncpg://minibank:minibank@postgres:5432/minibank
REDIS_URL=redis://redis:6379/0
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
APP_ENV=development
JWT_SECRET=dev-secret-change-in-production
```

---

## 4. Router Wiring

### `app/routers/transfers.py`

One change: passes `actor_user_id=current_user.id` to `transfer_service.transfer()`. This is needed for the event envelope's `actor_id` field.

---

## 5. Worker Package

### `workers/__init__.py`

Empty file — makes `workers/` a Python package so `python -m workers.audit_consumer` works.

---

## 6. Test Infrastructure (`tests/conftest.py` additions)

### New imports

- `KafkaContainer` from testcontainers (session-scoped Kafka broker for integration tests)
- `AuditEvent` model (so `Base.metadata.create_all` creates the `audit_events` table in tests)
- `User`, `Account` models (for `account_factory`)
- `jwt` (for `auth_headers` factory)

### TRUNCATE update

`db_session` fixture now TRUNCATEs `audit_events` alongside existing tables — prevents stale rows from leaking between tests.

### New fixtures

| Fixture | Scope | Purpose |
|---------|-------|---------|
| `kafka_container` | session | Starts a testcontainers Kafka broker (one per test run) |
| `kafka_bootstrap` | session | Returns the mapped bootstrap server address (e.g. `localhost:32789`) |
| `consumer_db_factory` | function | `async_sessionmaker` against test Postgres. Pass to consumer constructors. TRUNCATEs Phase 2 tables before each test. |
| `account_factory` | function | Creates User + Account rows via ORM (no HTTP client needed). Returns callable `async factory() → Account`. |
| `auth_headers` | function | Returns callable `make_headers(account) → dict` that signs a JWT accepted by the API. For CQRS integration tests. |

### `make_event()` helper (module-level function, not a fixture)

Builds a valid event envelope matching the Phase 2 contract. Used in test files:

```python
from tests.conftest import make_event

event = make_event("transfer.completed", {
    "transfer_id": str(uuid.uuid4()),
    "from_account_id": str(uuid.uuid4()),
    ...
})
```
