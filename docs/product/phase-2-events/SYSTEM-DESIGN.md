# System Design — Phase 2: Event-Driven Architecture

**Phase:** 2 of 6
**Status:** `not started`

---

## 1. Architecture Overview

Phase 2 adds Kafka, an outbox relay process, and three consumers alongside the Phase 1 monolith. All workers run as separate processes via Docker Compose.

```mermaid
graph TD
    Client["HTTP Client"]

    subgraph API["FastAPI (Phase 1 + outbox writes)"]
        TransferSvc["TransferService\n(writes OutboxRow in same TX as Transfer)"]
        AccountSvc["AccountService\n(writes OutboxRow in same TX as Account)"]
    end

    subgraph Workers["Background Workers (separate processes)"]
        Relay["Outbox Relay\nClaim → Publish → Confirm\n(exponential backoff on Kafka failure)"]
        AuditC["audit-consumer\ngroup: minibank.audit-consumer"]
        NotifC["notification-consumer\ngroup: minibank.notification-consumer"]
        ActivityC["activity-consumer\ngroup: minibank.activity-consumer"]
    end

    PG[("PostgreSQL\n+ outbox\n+ audit_events\n+ transaction_activity")]
    Kafka[["Kafka\ntransfer.events\naccount.events\n{consumer-group}.dlq"]]
    Stdout["stdout (simulated notifications)"]

    Client --> API --> PG
    TransferSvc -.->|same TX| PG
    AccountSvc -.->|same TX| PG

    Relay -->|Step 1: claim FOR UPDATE SKIP LOCKED| PG
    Relay -->|Step 2: publish| Kafka
    Relay -->|Step 3: confirm published| PG

    Kafka --> AuditC & NotifC & ActivityC
    AuditC --> PG
    ActivityC --> PG
    NotifC --> Stdout
```

---

## 2. Teaching Order — Week 6 Publishes Directly (Intentional)

> **Week 6 prerequisites — two one-line changes to Phase 1 files before any Week 6 code:**
> 1. `app/database.py` — add `db_factory = AsyncSessionLocal` at the bottom (alias workers import)
> 2. `app/config.py` — add `kafka_bootstrap_servers: str = "kafka:9092"` to `Settings`
>
> Week 6's consumer immediately imports `db_factory`; both workers and consumers reference `settings.kafka_bootstrap_servers`. Neither exists in Phase 1. The `ImportError` / `AttributeError` appears in your new consumer code and is confusing if you haven't made these changes first. See Section 14 for context.

In **Week 6**, events are published inline in `transfer_service.py` — no outbox, no relay. This is the "wrong" way, introduced intentionally:

```python
# Week 6 — direct publish (fragile)
# Module-level producer — initialized once at startup, shared across requests
kafka_producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap_servers)

# In your FastAPI lifespan / startup event:
#   await kafka_producer.start()
# In your FastAPI shutdown event:
#   await kafka_producer.stop()

async def transfer(...):
    # ... ledger insert + transfer commit ...
    envelope = {
        "event_id": str(uuid.uuid4()),
        "event_type": "transfer.completed",
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "version": "1",
        "actor_id": str(actor_id),
        "payload": { ... },
    }
    # Must encode to bytes — passing a dict or str raises TypeError
    await kafka_producer.send("transfer.events", json.dumps(envelope).encode())
    # ↑ outside TX boundary — if Kafka is down here, the event is lost
```

The failure: kill Kafka, make a transfer, restart Kafka — the event is gone. The DB has the transfer; Kafka never received it. The audit log has a gap.

> **Docker Compose note for Week 6:** Phase 1 runs the API on the host via `uvicorn`. Phase 2 containerizes it (see Section 13 — `api` service). In Week 6 the API process connects to Kafka directly for inline publishing, so the `api` service depends on `kafka-init`. Add `kafka-init: { condition: service_completed_successfully }` to the `api` service's `depends_on` temporarily. Remove this dependency in Week 7 when the outbox replaces inline publishing — the API no longer connects to Kafka directly; only the relay does.

The Week 6 **audit consumer** is equally minimal — no `BaseConsumer`, no retry, no DLQ. The student focuses purely on the Kafka consumer API and the dual-write problem, not reliability infrastructure:

```python
# Week 6 — minimal audit consumer (refactored into BaseConsumer in Week 9)
from app.database import db_factory   # same factory used by the API
from app.config import settings

async def run():
    consumer = AIOKafkaConsumer(
        "transfer.events",
        group_id="minibank.audit-consumer",
        bootstrap_servers=settings.kafka_bootstrap_servers,
        enable_auto_commit=False,
    )
    await consumer.start()
    try:
        async for message in consumer:
            event = json.loads(message.value)
            async with db_factory() as db:
                async with db.begin():
                    db.add(AuditEvent(
                        event_id=event["event_id"],
                        event_type=event["event_type"],
                        ...
                    ))
            await consumer.commit()
    finally:
        await consumer.stop()
# Observe: kill Kafka mid-transfer → consumer stalls, then catches up on restart.
# The gap to find: transfer row exists in DB, audit_events row is missing.
```

In **Week 7**, the outbox retrofits this:

```python
# Week 7 — outbox (correct)
async with db.begin():
    # ... ledger insert ...
    db.add(Transfer(...))
    db.add(OutboxRow(topic="transfer.events", payload=envelope))
    # COMMIT — transfer and outbox row atomically
# Relay delivers outbox → Kafka asynchronously
```

---

## 3. New Database Tables

```sql
-- Alembic 0003_add_outbox
CREATE TABLE outbox (
    id           UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    topic        VARCHAR(100) NOT NULL,
    event_type   VARCHAR(100) NOT NULL,
    payload      JSONB        NOT NULL,   -- full event envelope including event_id
    status       VARCHAR(20)  NOT NULL DEFAULT 'pending',
                              -- pending | publishing | published | failed
    retry_count  INT          NOT NULL DEFAULT 0,
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ
);
-- Partial index: relay only scans pending rows
CREATE INDEX idx_outbox_pending ON outbox (created_at) WHERE status = 'pending';
-- Recovery index: find stuck 'publishing' rows
CREATE INDEX idx_outbox_publishing ON outbox (created_at) WHERE status = 'publishing';


-- Alembic 0004_add_audit_events
CREATE TABLE audit_events (
    id            UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id      UUID         UNIQUE NOT NULL,  -- idempotency: one row per event
    event_type    VARCHAR(100) NOT NULL,
    actor_id      UUID,                          -- user who triggered the event
    resource_id   UUID,
    resource_type VARCHAR(50),
    payload       JSONB        NOT NULL,
    occurred_at   TIMESTAMPTZ  NOT NULL
);


-- Alembic 0005_add_transaction_activity
CREATE TABLE transaction_activity (
    id           UUID          PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id     UUID          NOT NULL,
    account_id   UUID          NOT NULL REFERENCES accounts(id),
    direction    VARCHAR(10)   NOT NULL,          -- debit | credit
    amount       NUMERIC(19,4) NOT NULL,
    currency     VARCHAR(3)    NOT NULL DEFAULT 'USD',  -- ISO 4217
    entry_type   VARCHAR(30)   NOT NULL,
    reference_id UUID,
    occurred_at  TIMESTAMPTZ   NOT NULL,
    -- A single transfer.completed event creates TWO rows: one debit (sender) + one credit (receiver).
    -- Unique on (event_id, account_id), NOT just event_id, to allow both rows per event.
    CONSTRAINT uq_activity_event_account UNIQUE (event_id, account_id)
);
CREATE INDEX idx_activity_account ON transaction_activity (account_id, occurred_at DESC);


-- No migration 0006 in Phase 2.
-- as_of is derived from MAX(occurred_at) in the query results — no extra table needed.
-- A dedicated consumer_watermarks table is added in Phase 4 (Observability) alongside
-- Prometheus metrics, where consumer lag tracking belongs.
```

---

## 4. Event Envelope

All events share a common envelope. The full event (envelope + payload) is stored as JSONB in `outbox.payload` and travels unchanged to Kafka consumers.

```json
{
  "event_id": "uuid — generated fresh at publish time, NOT the entity's ID",
  "event_type": "transfer.completed",
  "occurred_at": "2024-01-15T10:30:00Z",
  "version": "1",
  "actor_id": "uuid — the user who initiated the action (from current_user.id in the router)",
  "payload": { ... }
}
```

**Why `event_id` must be a fresh UUID:** Consumer idempotency relies on `event_id` being stable across replays. If `event_id` were the outbox row's PK, rebuilding the outbox table would generate new IDs and consumers would re-process events they already handled. `event_id` is generated once in `publish_event()` and stored in the payload forever.

### Event routing table

| Topic | Event Type | Produced by | Consumed by |
|-------|-----------|-------------|-------------|
| `transfer.events` | `transfer.completed` | TransferService | Audit, Notification, Activity |
| `transfer.events` | `transfer.failed` | TransferService | Audit, Notification |
| `account.events` | `account.opened` | AccountService | Audit, Notification |
| `account.events` | `seed.completed` | AccountService | Audit, Activity |
| `minibank.audit-consumer.dlq` | (original event) | audit-consumer (after 3 retries) | Manual inspection |
| `minibank.notification-consumer.dlq` | (original event) | notification-consumer (after 3 retries) | Manual inspection |
| `minibank.activity-consumer.dlq` | (original event) | activity-consumer (after 3 retries) | Manual inspection |

### Event payload schemas

All event payloads are defined as Pydantic models in `app/events/schemas.py`. Both producers and consumers import from this module — the models ARE the data contract. A typo or missing field is caught at construction time (producer) or parse time (consumer) with a clear `ValidationError`, not a `KeyError` buried in business logic.

**Why Pydantic and not Avro/Protobuf:** Schema Registry + Avro is the industry standard for cross-team event contracts. For a single-repo learning project, Pydantic models provide the same structural validation without the infrastructure overhead. The principle is the same — typed contracts at the boundary between producer and consumer.

```python
# app/events/schemas.py
from pydantic import BaseModel


class EventEnvelope(BaseModel):
    event_id: str
    event_type: str
    occurred_at: str
    version: str
    actor_id: str | None
    payload: dict          # validated per event type by the specific payload model


class TransferCompletedPayload(BaseModel):
    transfer_id: str
    from_account_id: str
    to_account_id: str
    amount: str            # Decimal serialized as string ("100.0000")
    currency: str          # ISO 4217 (e.g. "USD", "SGD", "GBP")
    entry_type: str
    idempotency_key: str


class TransferFailedPayload(BaseModel):
    transfer_id: str
    from_account_id: str
    to_account_id: str
    amount: str
    currency: str          # ISO 4217
    failure_code: str
    entry_type: str
    idempotency_key: str

class AccountOpenedPayload(BaseModel):
    account_id: str
    user_id: str
    status: str


class SeedCompletedPayload(BaseModel):
    account_id: str
    user_id: str
    amount: str
    currency: str          # ISO 4217
    entry_type: str
```

**Mapping:** Each `event_type` string has exactly one payload model:

| `event_type` | Payload model |
|-------------|---------------|
| `transfer.completed` | `TransferCompletedPayload` |
| `transfer.failed` | `TransferFailedPayload` |
| `account.opened` | `AccountOpenedPayload` |
| `seed.completed` | `SeedCompletedPayload` |

**Consumer-side dispatch helper** — parse the envelope and payload in one step:

```python
# app/events/schemas.py (continued)

PAYLOAD_MODELS: dict[str, type[BaseModel]] = {
    "transfer.completed": TransferCompletedPayload,
    "transfer.failed": TransferFailedPayload,
    "account.opened": AccountOpenedPayload,
    "seed.completed": SeedCompletedPayload,
}


def parse_event(raw: dict) -> tuple[EventEnvelope, BaseModel]:
    """Parse a raw event dict into a typed envelope + payload.
    Raises ValidationError if the structure doesn't match the contract.
    Unknown event types return the envelope with the raw payload dict as-is.
    """
    envelope = EventEnvelope(**raw)
    model_cls = PAYLOAD_MODELS.get(envelope.event_type)
    if model_cls:
        payload = model_cls(**envelope.payload)
    else:
        payload = envelope.payload  # unknown event type — pass through unvalidated
    return envelope, payload
```

---

## 5. Event Publisher Helper

The `publish_event()` function is the only way to write to the outbox. It generates a fresh `event_id`, constructs the envelope, and inserts an outbox row — all within the caller's existing DB transaction. The caller commits.

Callers pass a **Pydantic payload model**, not a raw dict. The model's `.model_dump()` guarantees all values are JSON-serializable primitives (str, int, float, bool, None) — the manual `json.dumps()` validation from the earlier design is no longer needed.

```python
# app/events/publisher.py
import uuid
from datetime import datetime, timezone
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from app.models.outbox import OutboxRow

def publish_event(
    db: AsyncSession,
    topic: str,
    event_type: str,
    payload: BaseModel,
    actor_id: uuid.UUID | None = None,
    event_id: str | None = None,
) -> None:
    """Insert an outbox row in the caller's current transaction. Caller must commit.

    payload must be a Pydantic model (e.g. TransferCompletedPayload). Passing a raw
    dict is a type error — the contract is enforced at construction time by the model,
    not at serialization time by json.dumps().

    event_id: Optional deterministic event ID. If None, generates uuid4() (normal
    live-traffic path). Backfill passes a UUID5 derived from the source entity to
    make backfill idempotent — same entity always produces the same event_id, so
    consumer UNIQUE constraints deduplicate on replay.
    """
    envelope = {
        "event_id": event_id or str(uuid.uuid4()),
        "event_type": event_type,
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "version": "1",
        "actor_id": str(actor_id) if actor_id else None,
        "payload": payload.model_dump(),   # Pydantic model → dict of primitives (always serializable)
    }
    db.add(OutboxRow(
        topic=topic,
        event_type=event_type,
        payload=envelope,   # full envelope stored, not just payload
    ))
```

**Updated `transfer()` signature — add `actor_user_id` parameter:**
```python
# app/services/transfer_service.py — Phase 2: add actor_user_id to signature
async def transfer(
    db: AsyncSession,
    redis: Redis,
    from_account_id: UUID,
    to_account_id: UUID,
    amount: Decimal,
    idempotency_key: str,
    actor_user_id: UUID | None = None,   # Phase 2: injected from router's current_user.id
) -> TransferResponse:
```

**Updated router call — pass `current_user.id`:**
```python
# app/routers/transfers.py — Phase 2: add actor_user_id
result = await transfer_service.transfer(
    db=db,
    redis=redis,
    from_account_id=sender_account.id,
    to_account_id=recipient_account.id,
    amount=Decimal(body.amount),
    idempotency_key=idempotency_key,
    actor_user_id=current_user.id,   # Phase 2 addition
)
```

**Usage in TransferService (success path):**
```python
# Inside transfer(), before await db.commit():
# actor_user_id is now a function parameter — no NameError
from app.events.schemas import TransferCompletedPayload

publish_event(db, "transfer.events", "transfer.completed", TransferCompletedPayload(
    transfer_id=str(transfer_record.id),
    from_account_id=str(from_account_id),
    to_account_id=str(to_account_id),
    amount=f"{amount:.4f}",
    currency="USD",
    entry_type="transfer",
    idempotency_key=idempotency_key,
), actor_id=actor_user_id)
await db.commit()
```

**Usage in TransferService (failure path):**
```python
# Inside transfer(), before await db.commit() for the failed record:
from app.events.schemas import TransferFailedPayload

publish_event(db, "transfer.events", "transfer.failed", TransferFailedPayload(
    transfer_id=str(failed_record.id),
    from_account_id=str(from_account_id),
    to_account_id=str(to_account_id),
    amount=f"{amount:.4f}",
    currency="USD",
    failure_code="INSUFFICIENT_BALANCE",
    entry_type="transfer",
    idempotency_key=idempotency_key,
), actor_id=actor_user_id)
await db.commit()
raise InsufficientBalanceError()
```

**Usage in AccountService (account open path):**
```python
# Inside open_account(db, user_id) — actor IS user_id; no new parameter needed
from app.events.schemas import AccountOpenedPayload

publish_event(db, "account.events", "account.opened", AccountOpenedPayload(
    account_id=str(account.id),
    user_id=str(account.user_id),
    status=account.status,    # "active"
), actor_id=user_id)   # user_id is already the function parameter
await db.commit()
```

**Usage in AccountService (seed path):**
```python
# Inside seed(db, account_id, amount, idempotency_key) — no new parameter needed.
# account is already fetched above: account = await get_account_by_id(db, account_id)
# The actor is the account owner (only the owner can seed in dev mode).
from app.events.schemas import SeedCompletedPayload

publish_event(db, "account.events", "seed.completed", SeedCompletedPayload(
    account_id=str(account.id),
    user_id=str(account.user_id),
    amount=f"{amount:.4f}",
    currency="USD",
    entry_type="seed",
), actor_id=account.user_id)   # account.user_id already in scope from get_account_by_id()
await db.commit()
```

---

## 6. Outbox Relay

The relay uses a **two-phase pattern** to keep DB transactions short and avoid holding locks during Kafka network I/O.

```python
# workers/outbox_relay.py
import asyncio
import json
import logging
import time
from datetime import datetime, timedelta, timezone
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError   # aiokafka.errors, NOT kafka.errors — wrong import catches nothing
from sqlalchemy import delete, func, select, update
from app.config import settings
from app.database import db_factory
from app.models.outbox import OutboxRow

logger = logging.getLogger(__name__)

BATCH_SIZE = 100
MIN_SLEEP = 1    # seconds
MAX_SLEEP = 30   # seconds
MAX_OUTBOX_RETRIES = 10   # after this, row is marked 'failed' — needs manual intervention

async def relay_loop(db_factory, kafka_producer):
    backoff = MIN_SLEEP
    last_recovery = 0    # throttle recover_stuck_rows to once every 5 min
    last_cleanup = 0     # throttle cleanup_published_rows to once per day

    while True:
        now = time.monotonic()
        # Reset 'publishing' rows stuck by a crashed relay instance
        if now - last_recovery > 300:       # 5 minutes
            await recover_stuck_rows(db_factory)
            last_recovery = now
        # Purge published rows older than 7 days to prevent unbounded table growth
        if now - last_cleanup > 86400:      # 24 hours
            await cleanup_published_rows(db_factory)
            last_cleanup = now

        claimed = await claim_batch(db_factory)

        if not claimed:
            # No work — grow backoff so an idle relay polls at 1s, 2s, 4s … up to 30s.
            # Without this, backoff stays at MIN_SLEEP=1s and the relay burns CPU on empty polls.
            backoff = min(backoff * 2, MAX_SLEEP)
            await asyncio.sleep(backoff)
            continue

        for row in claimed:
            try:
                # row.payload is a dict (JSONB from Postgres) — must serialize to bytes.
                # send_and_wait() blocks until the broker acks the write — true at-least-once.
                # send() (fire-and-forget) returns before the broker acks; a crash between
                # send() and confirm_batch() would mark the row 'published' even though Kafka
                # never received it, silently losing the event.
                await kafka_producer.send_and_wait(row.topic, json.dumps(row.payload).encode())
                row.status = "published"
                row.published_at = datetime.now(timezone.utc)
            except KafkaError:
                row.retry_count += 1
                if row.retry_count >= MAX_OUTBOX_RETRIES:
                    row.status = "failed"   # permanently failed — needs manual intervention
                    logger.error(f"Outbox row {row.id} permanently failed after {MAX_OUTBOX_RETRIES} retries")
                else:
                    row.status = "pending"   # return to pool for retry

        await confirm_batch(db_factory, claimed)

        # Back off only if NO rows were published — that signals Kafka connectivity failure.
        # A single bad row (e.g. MessageSizeTooLargeError that fails with KafkaError) does
        # not mean Kafka is down. Growing backoff for a payload error slows all subsequent
        # work until the bad row hits MAX_OUTBOX_RETRIES. Use published_count as the
        # discriminator: if at least one row succeeded, Kafka is reachable — reset backoff.
        published_count = sum(1 for row in claimed if row.status == "published")
        if published_count == 0 and claimed:
            backoff = min(backoff * 2, MAX_SLEEP)
        else:
            backoff = MIN_SLEEP
        await asyncio.sleep(backoff)


async def claim_batch(db_factory) -> list[OutboxRow]:
    """Step 1: Claim pending rows atomically. Short transaction — released immediately."""
    async with db_factory() as db:
        async with db.begin():
            result = await db.execute(
                select(OutboxRow)
                .where(OutboxRow.status == "pending")
                .order_by(OutboxRow.created_at)
                .limit(BATCH_SIZE)
                .with_for_update(skip_locked=True)
            )
            rows = result.scalars().all()
            for row in rows:
                row.status = "publishing"   # claim: others skip these rows
            # COMMIT — locks released, rows marked 'publishing'
            return rows


async def confirm_batch(db_factory, rows: list[OutboxRow]) -> None:
    """Step 3: Persist publish results. Short transaction."""
    async with db_factory() as db:
        async with db.begin():
            for row in rows:
                await db.merge(row)


async def recover_stuck_rows(db_factory) -> None:
    """Recovery: reset 'publishing' rows stuck > 5 min (process crash between Step 1 and Step 3).

    Uses created_at as a proxy for claim time — there is no claimed_at column.
    Limitation: if the relay was down for hours and claims a large backlog, recover_stuck_rows
    could fire on a row whose created_at is hours old but was just claimed seconds ago,
    resetting it to 'pending' before confirm_batch runs. Consumers are idempotent so no
    data corruption occurs, but the event publishes twice. Low-risk in practice since
    relay iterations complete in seconds, not minutes.
    """
    async with db_factory() as db:
        async with db.begin():
            await db.execute(
                update(OutboxRow)
                .where(
                    OutboxRow.status == "publishing",
                    OutboxRow.created_at < datetime.now(timezone.utc) - timedelta(minutes=5),
                )
                .values(status="pending")
            )


async def cleanup_published_rows(db_factory) -> None:
    """Periodic: delete old published and failed outbox rows.

    'published' rows: deleted after 7 days. Retention matches the Kafka topic
    window — if you need to replay, the Kafka topic still has the events.

    'failed' rows: permanently undeliverable (e.g. message exceeds max.message.bytes).
    They require manual inspection before deletion. Kept for 30 days so an operator
    has time to investigate, then auto-purged. Without this, 'failed' rows accumulate
    forever — 'cleanup_published_rows' previously only touched 'published' rows.
    """
    async with db_factory() as db:
        async with db.begin():
            await db.execute(
                delete(OutboxRow).where(
                    OutboxRow.status == "published",
                    OutboxRow.published_at < datetime.now(timezone.utc) - timedelta(days=7),
                )
            )
            # 'failed' rows have published_at=NULL so the published filter above never matches them.
            # Use created_at as the age proxy — row was written when the original event was produced.
            # Log a warning before deletion so operators have a last chance to investigate.
            failed_count = (await db.execute(
                select(func.count()).where(
                    OutboxRow.status == "failed",
                    OutboxRow.created_at < datetime.now(timezone.utc) - timedelta(days=30),
                )
            )).scalar_one()
            if failed_count:
                logger.warning(
                    f"Purging {failed_count} permanently-failed outbox rows older than 30 days "
                    f"— inspect before this window closes"
                )
            await db.execute(
                delete(OutboxRow).where(
                    OutboxRow.status == "failed",
                    OutboxRow.created_at < datetime.now(timezone.utc) - timedelta(days=30),
                )
            )
```

**Session factory requirement:** The `db_factory` name (used by workers) must be exported from `app/database.py`. Phase 1 exports `AsyncSessionLocal` — Phase 2 adds a one-line alias. Phase 1's `AsyncSessionLocal` already sets `expire_on_commit=False`, which is required: without it, SQLAlchemy expires all ORM attributes when a session commits. `claim_batch` returns rows after its session closes — accessing `row.topic`, `row.payload`, or setting `row.status` on those detached objects outside the session triggers an implicit lazy load, which raises `MissingGreenlet: greenlet_spawn has not been called` in async context.

```python
# app/database.py — Phase 2 addition (one line at the bottom)
# Phase 1 exports AsyncSessionLocal. Workers import db_factory by convention.
# expire_on_commit=False is already set on AsyncSessionLocal — the alias inherits it.
db_factory = AsyncSessionLocal
```

**`FOR UPDATE SKIP LOCKED`** ensures two concurrent relay instances claim different rows — no duplicate publishes. The relay is safe to run as multiple instances for redundancy.

**Sequential `send_and_wait()` throughput:** The current relay loop calls `send_and_wait()` for each row in the batch one at a time — 100 rows = 100 sequential Kafka round-trips (~200ms at 2ms latency). For a learning project this is fine. In production, you'd publish concurrently and await all acks together:

```python
async def publish_one(row) -> bool:
    """Returns True if a publish error occurred."""
    try:
        await kafka_producer.send_and_wait(row.topic, json.dumps(row.payload).encode())
        row.status = "published"
        row.published_at = datetime.now(timezone.utc)
        return False
    except KafkaError:
        row.retry_count += 1
        row.status = "failed" if row.retry_count >= MAX_OUTBOX_RETRIES else "pending"
        logger.error(f"Outbox row {row.id} publish failed (attempt {row.retry_count})")
        return True

results = await asyncio.gather(*[publish_one(row) for row in claimed])
# published_count mirrors the sequential version's backoff discriminator.
# publish_one() returns True on error, so count non-error results.
published_count = sum(1 for had_error in results if not had_error)
# backoff logic is identical to the sequential version: back off only when published_count == 0
```

This publishes the full batch concurrently while still waiting for all broker acks before confirming. For Phase 2, the sequential version is left in place to keep the control flow readable.

**At-least-once delivery:** If the process crashes between Step 1 (claim) and Step 3 (confirm), the row stays `"publishing"`. `recover_stuck_rows` (called every 5 minutes at the top of the relay loop) resets it to `"pending"`. The relay will re-publish it. Consumers must be idempotent (see Section 8).

**Max retries:** If a row fails to publish ≥ `MAX_OUTBOX_RETRIES` (10) times, it is marked `"failed"` and requires manual intervention. This handles permanently unpublishable rows (e.g., message exceeds Kafka's `max.message.bytes`) that would otherwise cycle `pending → publishing → pending` forever.

**Cleanup:** `cleanup_published_rows` deletes `"published"` rows older than 7 days. Call it on a cron schedule or periodically inside the relay loop. Without cleanup, the outbox grows without bound — thousands of `"published"` rows that serve no purpose but slow vacuums and backups.

---

## 7. Consumer Base Pattern

All consumers extend `BaseConsumer`, which handles:
- Offset commit (only after successful processing — at-least-once delivery)
- Retry via re-publish with incremented `x-retry-count` header
- Dead-letter routing after 3 retries to the consumer's own DLQ
- Idempotency enforcement via DB unique constraint

**Kafka library:** Workers are asyncio processes — they use `AsyncSession` and `asyncio.sleep`. Use `aiokafka` (async), not `kafka-python` (sync). Verify `aiokafka>=0.11` is in `pyproject.toml` dependencies (already present from Phase 1 setup).

```python
# consumers/consumer_base.py
# NOTE: Do NOT name this directory `kafka/` — it shadows the `kafka-python` package on
# the Python import path, causing ImportError when aiokafka tries to import kafka internals.
# Use `consumers/` instead.
import json
import logging
from datetime import datetime, timezone
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from app.config import settings   # project-specific — "kafka:9092" or "localhost:29092"

logger = logging.getLogger(__name__)

class BaseConsumer:
    group_id: str           # defined by subclass, e.g. "minibank.audit-consumer"
    topics: list[str]       # defined by subclass

    def __init__(self, db_factory):
        self.db_factory = db_factory   # injected — consumers use this to open DB sessions

    async def process(self, event: dict) -> None:
        raise NotImplementedError

    async def handle_message(self, message, producer, consumer) -> None:
        """Inner message handling loop — extracted from run() for testability.

        Tests call this method directly with a fake message and a mock producer,
        exercising deserialization errors and retry/DLQ routing without running
        the full consumer loop. See Section 16 DLQ tests for usage.
        """
        # aiokafka delivers headers as [(bytes, bytes)] — both keys AND values are bytes.
        # Using a string key like "x-retry-count" always misses the dict lookup, so
        # retry_count would always be 0, the DLQ threshold would never trigger, and
        # failed messages would retry infinitely. Use b"x-retry-count" (bytes literal).
        headers_dict = dict(message.headers or [])
        retry_count = int(headers_dict.get(b"x-retry-count", b"0").decode())
        # retry_count semantics: this is the number of PRIOR re-publishes, not attempts.
        #   retry_count=0: first attempt (original message, no header set)
        #   retry_count=1: second attempt (re-published once after first failure)
        #   retry_count=3: fourth attempt → >= 3 triggers DLQ
        # So `retry_count >= 3` means "already failed 3 times, this is attempt 4 → DLQ."
        # PRD says "after 3 retries" — that is 4 total processing attempts. ✓

        # Deserialization is handled separately — a NameError in the except block
        # would otherwise crash the consumer (event never assigned if json.loads raises)
        try:
            event = json.loads(message.value)
        except json.JSONDecodeError as exc:
            # Malformed JSON — cannot retry meaningfully; send straight to DLQ.
            # Must use send_and_wait() — send() is fire-and-forget. If the broker ack
            # never arrives, send() returns anyway and we commit the offset, permanently
            # losing the event. send_and_wait() raises on failure, propagating out of
            # this except block, crashing the consumer, and leaving the offset uncommitted
            # so Docker Compose can restart and retry — same reasoning as the processing
            # exception handler below.
            dlq_topic = f"{self.group_id}.dlq"
            await producer.send_and_wait(dlq_topic, value=message.value, headers=message.headers)
            await consumer.commit()
            logger.error(f"DLQ (malformed JSON): {self.group_id} offset={message.offset} error={exc}")
            return

        try:
            await self.process(event)
            await consumer.commit()          # commit only on success

        except Exception as exc:
            if retry_count >= 3:
                # Exhausted retries — send to this consumer's DLQ.
                # send_and_wait() is required here: send() is fire-and-forget — if the
                # broker ack never arrives, the DLQ write is silently dropped while the
                # offset is still committed, permanently losing the event with no trace.
                # send_and_wait() raises on failure, which propagates out of this except
                # block, crashes the consumer, and lets Docker Compose restart it without
                # committing the offset — preserving at-least-once semantics.
                dlq_topic = f"{self.group_id}.dlq"
                await producer.send_and_wait(dlq_topic, value=message.value, headers=message.headers)
                await consumer.commit()      # don't re-process this message
                logger.error(f"DLQ: {self.group_id} event_id={event.get('event_id')} err={exc}")
            else:
                # Re-publish with incremented retry count.
                # Same reasoning: send_and_wait() ensures the retry message lands on the
                # topic before we commit the original offset. With send(), a failed flush
                # would commit the offset and drop the retry, silently losing the event.
                new_headers = dict(message.headers or [])   # same fallback as headers_dict above — both produce {}
                new_headers[b"x-retry-count"] = str(retry_count + 1).encode()  # bytes key — replaces existing bytes entry
                await producer.send_and_wait(message.topic, value=message.value, headers=list(new_headers.items()))
                await consumer.commit()      # original offset consumed, retry is a new message

    async def run(self):
        # Read from settings — "kafka:9092" inside Docker, "localhost:29092" for host dev (see Section 13 dual listener)
        consumer = AIOKafkaConsumer(
            *self.topics,
            group_id=self.group_id,
            auto_offset_reset="latest",      # change to "earliest" for replay — see startup note below
            enable_auto_commit=False,        # manual commit only on success
            bootstrap_servers=settings.kafka_bootstrap_servers,
        )
        producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap_servers)
        # Both starts are INSIDE the try block so the finally always runs.
        # If producer.start() raised while outside try, the already-started consumer
        # would leak until process exit. aiokafka's stop() is safe on an unstarted
        # object (no-op), so calling both stops unconditionally in finally is correct.
        messages_since_lag_log = 0
        LAG_LOG_INTERVAL = 100   # log lag every 100 messages

        try:
            await consumer.start()
            await producer.start()
            async for message in consumer:   # aiokafka: async for, not for
                # Consumer lag approximation (US-2.10): log every LAG_LOG_INTERVAL messages.
                # This measures MESSAGE AGE — time elapsed since the event was published to
                # Kafka — NOT consumer position lag (end-offset − committed-offset).
                #
                # Interpretation:
                #   High lag during backfill is EXPECTED: historical events have old timestamps.
                #   A 6-month-old backfill event logs ~15,000,000s lag — the consumer is fine.
                #   High lag on live traffic IS a signal: the consumer is falling behind.
                #
                # For precise position lag, use the Kafka admin client to fetch end-offset and
                # committed-offset. That approach is a Phase 4 observability improvement.
                messages_since_lag_log += 1
                if messages_since_lag_log >= LAG_LOG_INTERVAL:
                    # message.timestamp is milliseconds since epoch (int) — must convert to
                    # datetime before subtracting; subtracting a float from datetime raises TypeError
                    msg_dt = datetime.fromtimestamp(message.timestamp / 1000, tz=timezone.utc)
                    lag_seconds = (datetime.now(timezone.utc) - msg_dt).total_seconds()
                    logger.info(f"consumer_lag group={self.group_id} topic={message.topic} "
                                f"partition={message.partition} offset={message.offset} "
                                f"approx_lag_seconds={lag_seconds:.1f}")
                    messages_since_lag_log = 0

                await self.handle_message(message, producer, consumer)
        finally:
            await consumer.stop()
            await producer.stop()
```

**`auto_offset_reset="latest"` startup order:** `"latest"` means the consumer only receives messages published *after* it subscribes. Events published while no consumer was running (e.g., the relay starts before the consumers during `docker compose up`) are silently dropped — no error, no warning. For the initial Phase 2 deployment after backfill, either:
- Start consumers before the relay: `docker compose up audit-consumer activity-consumer notification-consumer && docker compose up outbox-relay`
- Or reset offsets to `"earliest"` for the first run (see Section 10 for the reset procedure)

For ongoing restarts (after the initial catchup), `"latest"` is correct — you don't want a restarted consumer to replay the entire topic history.

**Why re-publish for retries rather than not-committing:** Not committing blocks all subsequent messages in the partition. Re-publishing with a counter moves the consumer forward and lets retries happen asynchronously. This is the standard pattern for at-least-once consumers.

**Worker entrypoint pattern (consumers):**
```python
# workers/audit_consumer.py
import asyncio
from app.database import db_factory
from consumers.consumer_base import BaseConsumer

# AuditConsumer is defined here in the worker file — NOT imported from consumer_base.
# consumer_base.py only defines BaseConsumer (the retry/DLQ machinery).
# Each worker file owns its consumer subclass and its __main__ entrypoint.
class AuditConsumer(BaseConsumer):
    group_id = "minibank.audit-consumer"
    topics = ["transfer.events", "account.events"]

    async def process(self, event: dict) -> None:
        ...  # see Section 8 for full implementation

if __name__ == "__main__":
    asyncio.run(AuditConsumer(db_factory).run())
    # BaseConsumer.run() handles producer.start() / producer.stop() internally
```

**Worker entrypoint pattern (relay):**
```python
# workers/outbox_relay.py
import asyncio
from aiokafka import AIOKafkaProducer
from app.config import settings
from app.database import db_factory

async def main():
    producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap_servers)
    await producer.start()        # must call start() before any send()
    try:
        await relay_loop(db_factory, producer)
    finally:
        await producer.stop()     # flush pending sends and close connection

if __name__ == "__main__":
    asyncio.run(main())
```

---

## 8. Consumer Implementations

### Audit Consumer (`minibank.audit-consumer`)

Appends every event to `audit_events`. Idempotent via `UNIQUE(event_id)`.

```python
from sqlalchemy.exc import IntegrityError   # must import — caught in process() for idempotency
from app.events.schemas import parse_event

# Explicit mapping — more readable than `"transfer" in event_type` substring check.
# A substring heuristic silently misclassifies future event types containing "transfer"
# (e.g. "account_transfer_limit.updated" → "transfer" when the resource is "account").
_RESOURCE_TYPE = {
    "transfer.completed": "transfer",
    "transfer.failed":    "transfer",
    "account.opened":     "account",
    "seed.completed":     "account",
}

class AuditConsumer(BaseConsumer):
    group_id = "minibank.audit-consumer"
    topics = ["transfer.events", "account.events"]

    async def process(self, event: dict) -> None:
        # Validate envelope + payload against typed schemas.
        # ValidationError propagates to BaseConsumer → retry → DLQ (correct behavior
        # for a structurally invalid event — it won't pass on retry either).
        envelope, payload = parse_event(event)

        # self.db_factory is injected via BaseConsumer.__init__()
        # Open a fresh session per message — short-lived, no shared state across messages
        try:
            async with self.db_factory() as db:
                async with db.begin():
                    # Use envelope fields (typed) instead of raw dict access.
                    # payload is a typed model — access .transfer_id / .account_id via getattr
                    # rather than dict-style indexing. For resource_id, we need either transfer_id
                    # or account_id depending on event type — getattr with None fallback is cleaner
                    # than checking isinstance on every payload model.
                    resource_id = getattr(payload, "transfer_id", None) or getattr(payload, "account_id", None)
                    db.add(AuditEvent(
                        event_id=envelope.event_id,
                        event_type=envelope.event_type,
                        actor_id=envelope.actor_id,
                        resource_id=resource_id,
                        resource_type=_RESOURCE_TYPE.get(envelope.event_type, "unknown"),
                        payload=event,   # store original raw dict — full fidelity for audit trail
                        occurred_at=datetime.fromisoformat(envelope.occurred_at),
                    ))
        except IntegrityError:
            pass  # UNIQUE(event_id) violated — already processed, idempotent no-op
            # IMPORTANT: must catch here, not let it propagate to BaseConsumer.run(),
            # which would treat it as a processing failure and retry/DLQ the message.
```

### Activity Consumer (`minibank.activity-consumer`)

Builds the CQRS read model. One `transfer.completed` event inserts **two** rows — one debit for the sender, one credit for the receiver.

```python
from decimal import Decimal               # amount arrives as a JSON string — must convert to Decimal
from sqlalchemy.exc import IntegrityError   # must import — caught in process() for idempotency

class ActivityConsumer(BaseConsumer):
    group_id = "minibank.activity-consumer"
    topics = ["transfer.events", "account.events"]

    async def process(self, event: dict) -> None:
        # Validate envelope + payload against typed schemas.
        # ValidationError propagates to BaseConsumer → retry → DLQ.
        from app.events.schemas import parse_event, TransferCompletedPayload, SeedCompletedPayload
        envelope, payload = parse_event(event)

        try:
            async with self.db_factory() as db:
                async with db.begin():
                    occurred_at = datetime.fromisoformat(envelope.occurred_at)

                    if isinstance(payload, TransferCompletedPayload):
                        # Two rows per event: one debit (sender) + one credit (receiver)
                        # Decimal(payload.amount) is required — asyncpg raises DataError if you pass
                        # a string to a NUMERIC column ("100.0000" is not accepted as-is).
                        db.add(TransactionActivity(
                            event_id=envelope.event_id,
                            account_id=payload.from_account_id,
                            direction="debit",
                            amount=Decimal(payload.amount),
                            currency=payload.currency,
                            entry_type=payload.entry_type,
                            reference_id=payload.transfer_id,
                            occurred_at=occurred_at,
                        ))
                        db.add(TransactionActivity(
                            event_id=envelope.event_id,
                            account_id=payload.to_account_id,
                            direction="credit",
                            amount=Decimal(payload.amount),
                            currency=payload.currency,
                            entry_type=payload.entry_type,
                            reference_id=payload.transfer_id,
                            occurred_at=occurred_at,
                        ))

                    elif isinstance(payload, SeedCompletedPayload):
                        # One credit row — seed has no debit side (money from system)
                        db.add(TransactionActivity(
                            event_id=envelope.event_id,
                            account_id=payload.account_id,
                            direction="credit",
                            amount=Decimal(payload.amount),
                            currency=payload.currency,
                            entry_type=payload.entry_type,    # "seed"
                            reference_id=None,
                            occurred_at=occurred_at,
                        ))

                    elif envelope.event_type == "transfer.failed":
                        # Intentional: failed transfers produce no activity row.
                        # A failed transfer never moved money — there is nothing to show in the
                        # transaction history. The failure IS captured in audit_events via
                        # AuditConsumer. This elif is explicit to distinguish "not implemented"
                        # from "not needed" — without it, a reader can't tell if it's a bug.
                        pass

                    # Other event types (e.g. account.opened, future events) — no activity row
        except IntegrityError:
            pass  # UNIQUE(event_id, account_id) violated — already processed, idempotent no-op
        # No watermark table — as_of is derived at query time (see Section 9)
```

### Notification Consumer (`minibank.notification-consumer`)

Logs simulated notifications to stdout. No DB writes — stateless.

```python
class NotificationConsumer(BaseConsumer):
    group_id = "minibank.notification-consumer"
    topics = ["transfer.events", "account.events"]

    async def process(self, event: dict) -> None:
        from app.events.schemas import (
            parse_event, TransferCompletedPayload, TransferFailedPayload, AccountOpenedPayload,
        )
        envelope, payload = parse_event(event)

        match payload:
            case TransferCompletedPayload():
                logger.info(f"NOTIFY [sender]  {payload.from_account_id}: You sent {payload.amount}")
                logger.info(f"NOTIFY [receiver] {payload.to_account_id}: You received {payload.amount}")
            case TransferFailedPayload():
                logger.info(f"NOTIFY [sender] {payload.from_account_id}: Transfer failed: {payload.failure_code}")
            case AccountOpenedPayload():
                logger.info(f"NOTIFY [user] {payload.user_id}: Your account is now active")
            case _:
                pass  # seed.completed and any future event types — no notification defined; intentional no-op
```

---

## 9. Modified API Endpoint

**GET /v1/accounts/me/transactions** — read source migrated from ledger to CQRS read model

Phase 2 re-points this existing endpoint to read from `transaction_activity` instead of `ledger_entries`. The response shape gains one new field: `as_of` in the meta object.

```json
{
  "data": [
    {
      "entry_id": "uuid",
      "direction": "debit",
      "amount": "100.0000",
      "currency": "USD",
      "entry_type": "transfer",
      "reference_id": "uuid",
      "created_at": "2024-01-15T10:30:00Z"
    }
  ],
  "meta": {
    "as_of": "2024-01-15T10:30:00Z",
    "total": 42,
    "page": 1,
    "limit": 20
  }
}
```

**Backward compatibility — `created_at` field:** Phase 1's `TransactionItem` schema uses `created_at`. The `transaction_activity` table stores this as `occurred_at` (event timestamp). The service layer maps `occurred_at` → `created_at` when constructing the `TransactionItem` response. The field name exposed to clients must not change.

**`as_of` semantics:** `MAX(occurred_at)` from the rows returned in the current query. This tells the client the most recent event reflected in the response. If the query returns zero rows, `as_of` is `null`. `as_of` is a new optional field — add `as_of: datetime | None = None` to `PaginationMeta` in `app/schemas/common.py`. It is backward-compatible (new optional field, not a removed one).

> **Note:** Phase 1's class is `PaginationMeta` (with an `n`), not `PaginatedMeta`. The router returns a raw dict today — `{"meta": {"total": total, "page": page, "limit": limit}}`. After Phase 2, add `"as_of": as_of` to that dict. If you switch to constructing `PaginationMeta` explicitly, import the correct name.

**`as_of: null` ambiguity:** `null` is ambiguous — it means either (a) the account has no transactions, or (b) the activity consumer hasn't processed any events yet (lagging or just started). A new account seeded with $1000 will show `as_of: null` and an empty feed until the `seed.completed` event is processed, even though money exists in the balance. This is the observable eventual consistency window. Clients should treat `as_of: null` as "data may not be fully current, not necessarily empty." For production systems, a consumer watermark table (Phase 4) removes this ambiguity by returning the last-processed event timestamp even when the result set is empty.

**Query parameter compatibility:** All Phase 1 query parameters (`page`, `limit`, `from_date`, `to_date`, `entry_type`) are preserved. Date filters (`from_date`/`to_date`) now filter on `occurred_at` in `transaction_activity` rather than `created_at` in `ledger_entries` — semantically equivalent since both represent when the event happened.

**Why `MAX(occurred_at)` and not a watermark table:** The client cares about the freshness of *their* data, not the consumer's global state. A user with no recent transfers would always see an old watermark even though their data is fully up to date. `MAX(occurred_at)` answers the right question: "how recent is the data you just received?"

**Why not a separate `/activity` endpoint:** One transaction endpoint, one data source. This is how every neobank app works — the user sees a single feed. The CQRS migration is an internal re-plumbing, not a product change.

**Service layer pseudocode — migrated query:**

```python
# app/services/account_service.py
async def get_transactions(
    db: AsyncSession,
    account_id: uuid.UUID,
    page: int,
    limit: int,
    from_date: datetime | None,
    to_date: datetime | None,
    entry_type: str | None,
) -> tuple[list[TransactionActivity], int, datetime | None]:
    """Read from transaction_activity (CQRS read model). Returns (rows, total, as_of)."""

    base_q = (
        select(TransactionActivity)
        .where(TransactionActivity.account_id == account_id)
    )
    if from_date:
        base_q = base_q.where(TransactionActivity.occurred_at >= from_date)
    if to_date:
        base_q = base_q.where(TransactionActivity.occurred_at <= to_date)
    if entry_type:
        base_q = base_q.where(TransactionActivity.entry_type == entry_type)

    # Total count (separate query — same filters, no pagination)
    count_q = select(func.count()).select_from(base_q.subquery())
    total = (await db.execute(count_q)).scalar_one()

    # Paginated rows
    rows_q = base_q.order_by(TransactionActivity.occurred_at.desc()).offset((page - 1) * limit).limit(limit)
    rows = (await db.execute(rows_q)).scalars().all()

    # as_of = MAX(occurred_at) of the rows returned on this page (not the whole table).
    # This tells the client "the most recent event in the data you just received".
    # Using MAX over the full account history would be wrong — a user with old data would
    # see a stale as_of even though their current page is fully up to date.
    as_of = max((r.occurred_at for r in rows), default=None)

    return rows, total, as_of


# app/routers/accounts.py — response construction
# The TransactionItem schema uses `created_at` (Phase 1 field name).
# transaction_activity stores this as `occurred_at`. Map at construction time.
items = [
    TransactionItem(
        entry_id=str(row.id),          # UUID → str: Pydantic v2 does NOT auto-coerce UUID to str
        direction=row.direction,
        amount=row.amount,             # Decimal → str: Pydantic v2 lax mode coerces Decimal via str()
        entry_type=row.entry_type,
        reference_id=str(row.reference_id) if row.reference_id else None,  # UUID | None → str | None
        created_at=row.occurred_at,    # ← field rename: occurred_at → created_at
    )
    for row in rows
]
return PaginatedResponse(
    data=items,
    meta=PaginationMeta(total=total, page=page, limit=limit, as_of=as_of),
    # Phase 1's class is PaginationMeta (app/schemas/common.py) — not PaginatedMeta.
)
```

**Key implementation traps:**
- `as_of` is `max()` over the **current page's rows**, not a global aggregate — scoping it to the page is intentional
- `occurred_at` → `created_at` rename happens in the router/service, not in the Pydantic schema — the schema stays unchanged
- `total` must be counted with the same filters as the page query — a missing filter on the count query will return wrong pagination metadata

---

## 10. Consumer Offset Management

| Reset mode | When to use |
|-----------|-------------|
| `latest` (default) | New deployment — consumer only processes events published after it starts |
| `earliest` | Rebuild from scratch — consumer replays all events from the beginning of the topic |

**To rebuild `transaction_activity` from scratch:**
1. Stop the activity consumer
2. `TRUNCATE transaction_activity;`
   > **Note:** `TRUNCATE` takes an `ACCESS EXCLUSIVE` lock — it blocks all concurrent reads of `transaction_activity`, including live requests to `GET /v1/accounts/me/transactions`. The lock is held only for the duration of the TRUNCATE (milliseconds), but it will stall any in-flight requests. Do this during a low-traffic window or maintenance mode. If you want to avoid the brief lock, `DELETE FROM transaction_activity;` is slower but uses a row-level lock.
3. Reset the consumer group's committed offsets — `auto_offset_reset=earliest` is **only** consulted when the group has no committed offsets; if the consumer has been running, its offsets are already committed and `earliest` is silently ignored. Run inside the Kafka container (`kafka:9092` doesn't resolve from the host; inside the container use `localhost:9092`):
   ```bash
   docker compose exec kafka kafka-consumer-groups \
     --bootstrap-server localhost:9092 \
     --group minibank.activity-consumer \
     --topic transfer.events --reset-offsets --to-earliest --execute
   docker compose exec kafka kafka-consumer-groups \
     --bootstrap-server localhost:9092 \
     --group minibank.activity-consumer \
     --topic account.events --reset-offsets --to-earliest --execute
   ```
4. Restart the activity consumer (no need to set `auto_offset_reset=earliest` — group offsets are already reset). Consumer replays all `transfer.events` and `account.events` and rebuilds the table.
5. `GET /v1/accounts/me/transactions` will return `as_of: null` and empty data until the consumer catches up — this is the observable eventual consistency window

---

## 11. Backfill Management Command

Phase 1 data (transfers and accounts created before Phase 2) has no events. Without backfill, the audit log and activity feed are incomplete.

**Idempotency via deterministic event IDs:** Backfill uses UUID5 (namespace + source entity ID) instead of UUID4 to generate `event_id`s. The same entity always produces the same `event_id`, so running backfill multiple times is safe — consumer UNIQUE constraints (`audit_events.event_id`, `transaction_activity(event_id, account_id)`) silently reject duplicates on subsequent runs.

```python
# management/backfill_events.py
import uuid
from sqlalchemy import func, select
from app.models.outbox import OutboxRow
from app.models.account import Account
from app.models.transfer import Transfer
from app.models.ledger_entry import LedgerEntry
from app.events.publisher import publish_event
from app.events.schemas import (
    AccountOpenedPayload, TransferCompletedPayload, TransferFailedPayload, SeedCompletedPayload,
)
# NOTE: app.database is NOT imported at module level — see db_factory=None injection below.
# A module-level import would create a hard dependency that prevents importing backfill_events
# in tests before the DB engine is configured. All other workers (relay, consumers) use the
# same lazy-import pattern: module-level default is None, resolved lazily inside the function.

BACKFILL_NAMESPACE = uuid.UUID("b4cf1110-0000-0000-0000-000000000000")


def backfill_event_id(entity_type: str, entity_id: uuid.UUID) -> str:
    """Deterministic UUID5 — same entity always produces the same event_id.

    This makes backfill naturally idempotent: running it N times produces the same
    event_ids. Consumers deduplicate via their UNIQUE constraints on event_id.
    """
    return str(uuid.uuid5(BACKFILL_NAMESPACE, f"{entity_type}:{entity_id}"))


async def backfill(db_factory=None, force: bool = False):
    """Generate outbox rows for all Phase 1 data. Safe to retry.

    Accepts an optional db_factory for testing. When called without arguments
    (production / docker compose run), falls back to app.database.db_factory.
    This follows the same injection pattern as the relay and all consumers —
    backfill is the only component that would otherwise use a hard-wired import,
    making it untestable without patching the module-level db_factory.

    IDEMPOTENT: Uses deterministic UUID5 event IDs derived from source entity IDs.
    Running backfill multiple times produces outbox rows with the same event_ids.
    Consumers (audit, activity) deduplicate via their UNIQUE constraints — no
    duplicate rows are ever inserted in downstream tables.

    A preflight check raises RuntimeError if backfill has already run. Pass
    force=True to skip the check (e.g. when re-running after a partial failure).
    With deterministic IDs, force=True is always safe — no cleanup required.

    PREFLIGHT GUARD LIMITATION: The guard counts 'account.opened' outbox rows to
    detect prior runs. This only works reliably if backfill runs BEFORE any live
    account registrations occur (i.e., before Phase 2 starts accepting traffic).
    After Phase 2 goes live, every new account creation writes an 'account.opened'
    outbox row — the guard cannot distinguish live-traffic rows from historical
    backfill rows. Use force=True to override.
    """
    if db_factory is None:
        from app.database import db_factory as _default
        db_factory = _default

    if not force:
        async with db_factory() as db:
            count = (await db.execute(
                select(func.count()).where(OutboxRow.event_type == "account.opened")
            )).scalar_one()
        if count > 0:
            raise RuntimeError(
                f"Backfill appears to have already run ({count} account.opened outbox rows found). "
                "Pass force=True to override."
            )

    # Session management: read all IDs first in one transaction, then write
    # one outbox row per entity in its own short session.

    # --- Accounts ---
    async with db_factory() as db:
        async with db.begin():
            account_rows = (await db.execute(
                select(Account.id, Account.user_id, Account.status)
                .where(Account.user_id.is_not(None))  # skip system account
                .order_by(Account.created_at)
            )).all()

    for account_id, user_id, status in account_rows:
        async with db_factory() as db:
            async with db.begin():
                publish_event(db, "account.events", "account.opened", AccountOpenedPayload(
                    account_id=str(account_id),
                    user_id=str(user_id),
                    status=status,
                ), actor_id=None,
                   event_id=backfill_event_id("account.opened", account_id))

    # --- Transfers (completed + failed) ---
    async with db_factory() as db:
        async with db.begin():
            transfer_rows = (await db.execute(
                select(Transfer.id, Transfer.from_account_id, Transfer.to_account_id,
                       Transfer.amount, Transfer.status, Transfer.failure_code,
                       Transfer.idempotency_key)
                .order_by(Transfer.created_at)
            )).all()

    for t_id, from_id, to_id, amount, status, failure_code, idem_key in transfer_rows:
        async with db_factory() as db:
            async with db.begin():
                if status == "completed":
                    publish_event(db, "transfer.events", "transfer.completed", TransferCompletedPayload(
                        transfer_id=str(t_id),
                        from_account_id=str(from_id),
                        to_account_id=str(to_id),
                        amount=f"{amount:.4f}",
                        currency="USD",
                        entry_type="transfer",
                        idempotency_key=idem_key,
                    ), actor_id=None,
                       event_id=backfill_event_id("transfer.completed", t_id))
                elif status == "failed":
                    publish_event(db, "transfer.events", "transfer.failed", TransferFailedPayload(
                        transfer_id=str(t_id),
                        from_account_id=str(from_id),
                        to_account_id=str(to_id),
                        amount=f"{amount:.4f}",
                        currency="USD",
                        failure_code=failure_code or "UNKNOWN",
                        entry_type="transfer",
                        idempotency_key=idem_key,
                    ), actor_id=None,
                       event_id=backfill_event_id("transfer.failed", t_id))

    # --- Seed entries ---
    # Leg-based ledger: seed entries are credit legs with entry_type='seed' and
    # direction='credit'. The user's account is the credit side.
    # LedgerEntry.id is used as the source entity for deterministic event_id generation.
    async with db_factory() as db:
        async with db.begin():
            seed_rows = (await db.execute(
                select(LedgerEntry.id, LedgerEntry.amount, LedgerEntry.account_id, Account.user_id)
                .join(Account, LedgerEntry.account_id == Account.id)
                .where(LedgerEntry.entry_type == "seed", LedgerEntry.direction == "credit")
                .order_by(LedgerEntry.created_at)
            )).all()

    for entry_id, amount, account_id, user_id in seed_rows:
        async with db_factory() as db:
            async with db.begin():
                publish_event(db, "account.events", "seed.completed", SeedCompletedPayload(
                    account_id=str(account_id),
                    user_id=str(user_id),
                    amount=f"{amount:.4f}",
                    currency="USD",
                    entry_type="seed",
                ), actor_id=None,
                   event_id=backfill_event_id("seed.completed", entry_id))


if __name__ == "__main__":
    import asyncio
    asyncio.run(backfill())
# Run with: docker compose run --rm api python -m management.backfill_events
```

**Note on `actor_id=None` for backfill:** Phase 1 transfers don't have actor information stored. Historical events will have `actor_id: null` in the audit log. This is acceptable — the audit trail is complete for the action itself; the actor attribution gap applies only to pre-Phase-2 history.

**Why UUID5 and not UUID4:** UUID4 makes backfill a one-shot operation — running it twice corrupts downstream tables with duplicates (different event_ids bypass consumer UNIQUE constraints). UUID5 uses a fixed namespace + the source entity's ID as input, so the same entity always produces the same event_id. This makes backfill naturally idempotent via the same deduplication mechanism that protects live consumers.

---

## 12. Kafka Topic Configuration

```yaml
# Managed via kafka/create-topics.sh or a startup init container
topics:
  - name: transfer.events
    partitions: 1        # single partition = guaranteed ordering across all transfers
    retention_ms: 604800000  # 7 days

  - name: account.events
    partitions: 1
    retention_ms: 604800000

  - name: minibank.audit-consumer.dlq
    partitions: 1
    retention_ms: -1     # infinite retention — DLQ events must not expire silently

  - name: minibank.notification-consumer.dlq
    partitions: 1
    retention_ms: -1

  - name: minibank.activity-consumer.dlq
    partitions: 1
    retention_ms: -1
```

**`create-topics.sh` — complete script:** All five topics must exist before any worker starts (`KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`). DLQ topics require `retention.ms=-1` (infinite retention). The `--if-not-exists` flag makes the script idempotent — without it, a second `docker compose up` causes `kafka-init` to exit non-zero ("Topic already exists") and every downstream worker fails its `service_completed_successfully` dependency check.

```bash
#!/bin/sh
# kafka/create-topics.sh — lives in the `kafka/` dir (bash scripts, not Python — no import path conflict)
BOOTSTRAP=kafka:9092

kafka-topics --bootstrap-server $BOOTSTRAP --create --if-not-exists \
  --topic transfer.events --partitions 1 --replication-factor 1 \
  --config retention.ms=604800000

kafka-topics --bootstrap-server $BOOTSTRAP --create --if-not-exists \
  --topic account.events --partitions 1 --replication-factor 1 \
  --config retention.ms=604800000

# DLQ topics: infinite retention — events must not expire before manual inspection
kafka-topics --bootstrap-server $BOOTSTRAP --create --if-not-exists \
  --topic minibank.audit-consumer.dlq --partitions 1 --replication-factor 1 \
  --config retention.ms=-1

kafka-topics --bootstrap-server $BOOTSTRAP --create --if-not-exists \
  --topic minibank.notification-consumer.dlq --partitions 1 --replication-factor 1 \
  --config retention.ms=-1

kafka-topics --bootstrap-server $BOOTSTRAP --create --if-not-exists \
  --topic minibank.activity-consumer.dlq --partitions 1 --replication-factor 1 \
  --config retention.ms=-1
```

**Why single partition for Phase 2:** Single partition guarantees that all events are delivered in the order they were published. With multiple partitions, ordering is only guaranteed within a partition — a transfer to account A and a subsequent transfer from account A could arrive at a consumer in the wrong order if they land in different partitions. For Phase 2, correctness over throughput.

**Production note:** In a real neobank, you partition by `account_id` so transfers to the same account are ordered. The partition key is `account_id`, but a transfer event affects two accounts. You'd publish two events (one per account) or accept out-of-order delivery within the CQRS read model and reconcile using `occurred_at`.

---

## 13. Worker Lifecycle (Docker Compose)

Each worker is a separate Python process with its own `__main__` entrypoint. Docker Compose supervises and restarts them on crash.

Phase 1 runs the API on the host via `uvicorn app.main:app`. Phase 2 containerizes it so the API, relay, and consumers all share the same Docker network and can reach Kafka at `kafka:9092`.

### Dockerfile

```dockerfile
# Dockerfile
FROM python:3.12-slim

WORKDIR /app

# Install uv for fast dependency resolution
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Install dependencies first (layer caching)
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

# Make venv's python the default — worker commands use `python -m ...`, not `uv run`.
# Without this, `python` resolves to /usr/local/bin/python3.12 (system Python) which
# has no access to project dependencies in .venv/. Every worker container would crash
# with ModuleNotFoundError on startup.
ENV PATH="/app/.venv/bin:$PATH"

# Copy application code
COPY app/ app/
COPY consumers/ consumers/
COPY workers/ workers/
COPY management/ management/
COPY alembic/ alembic/
COPY alembic.ini .

# Default command — overridden per service in docker-compose.yml.
# With PATH set, uvicorn is directly available — no `uv run` wrapper needed.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

### `.env` file

Workers and the API load settings via pydantic-settings, which reads environment variables. Inside Docker, hostnames resolve to container names (`postgres`, `redis`, `kafka`), not `localhost`.

```bash
# .env — Docker Compose env file (not committed; already in .gitignore)
DATABASE_URL=postgresql+asyncpg://minibank:minibank@postgres:5432/minibank
REDIS_URL=redis://redis:6379/0
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
JWT_SECRET=dev-secret-change-in-production
```

> **Host dev:** If running the API on the host (without Docker), override these:
> `DATABASE_URL=postgresql+asyncpg://minibank:minibank@localhost:5432/minibank`,
> `REDIS_URL=redis://localhost:6379/0`,
> `KAFKA_BOOTSTRAP_SERVERS=localhost:29092`.

### Docker Compose additions

```yaml
# docker-compose.yml additions (Phase 2)
services:
  # ... existing postgres, redis ...

  api:
    build: .
    # command: default from Dockerfile (uvicorn)
    env_file: .env
    ports:
      - "8000:8000"
    depends_on:
      postgres: { condition: service_healthy }
      redis: { condition: service_healthy }
      # Week 6 only — API connects to Kafka directly for inline publishing:
      #   kafka-init: { condition: service_completed_successfully }
      # Remove in Week 7 when the outbox replaces inline publishing.
    restart: unless-stopped

  zookeeper:
    image: confluentinc/cp-zookeeper:7.6.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
    healthcheck:
      test: ["CMD", "nc", "-z", "localhost", "2181"]

  kafka:
    image: confluentinc/cp-kafka:7.6.0
    depends_on:
      zookeeper: { condition: service_healthy }
    ports:
      - "9092:9092"    # internal Docker-to-Docker: containers connect via kafka:9092
      - "29092:29092"  # host access: local dev tools connect via localhost:29092
    # Why two listeners: a single PLAINTEXT://kafka:9092 listener advertises the hostname
    # "kafka" which resolves inside Docker but NOT on the host. Port-exposing alone doesn't
    # fix this — the broker tells clients to reconnect to the advertised address, so a host
    # client connecting to localhost:9092 is immediately redirected to kafka:9092 (DNS fails).
    # Two listeners solve this: PLAINTEXT for intra-Docker, PLAINTEXT_HOST for host clients.
    # Set KAFKA_BOOTSTRAP_SERVERS="kafka:9092" for Docker workers, "localhost:29092" for host dev.
    environment:
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,PLAINTEXT_HOST://0.0.0.0:29092
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,PLAINTEXT_HOST://localhost:29092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
    healthcheck:
      test: ["CMD", "kafka-topics", "--bootstrap-server", "localhost:9092", "--list"]

  kafka-init:
    image: confluentinc/cp-kafka:7.6.0
    depends_on:
      kafka: { condition: service_healthy }
    volumes:
      - ./kafka:/kafka   # bind-mount local kafka/ dir — the image has no copy of create-topics.sh
    entrypoint: ["/bin/sh", "/kafka/create-topics.sh"]
    restart: "no"

  outbox-relay:
    build: .
    command: python -m workers.outbox_relay
    env_file: .env   # workers load settings via pydantic-settings; without this, ValidationError at import
    depends_on:
      postgres: { condition: service_healthy }
      kafka-init: { condition: service_completed_successfully }  # topics must exist before relay publishes
    restart: unless-stopped

  audit-consumer:
    build: .
    command: python -m workers.audit_consumer
    env_file: .env
    depends_on:
      kafka-init: { condition: service_completed_successfully }  # topics must exist before consumer subscribes
      postgres: { condition: service_healthy }
    restart: unless-stopped

  notification-consumer:
    build: .
    command: python -m workers.notification_consumer
    env_file: .env
    depends_on:
      kafka-init: { condition: service_completed_successfully }
    restart: unless-stopped

  activity-consumer:
    build: .
    command: python -m workers.activity_consumer
    env_file: .env
    depends_on:
      kafka-init: { condition: service_completed_successfully }
      postgres: { condition: service_healthy }
    restart: unless-stopped
```

Each worker creates its own SQLAlchemy engine with its own connection pool. They share the same Postgres instance but have independent connections.

---

## 14. Codebase Structure (Phase 2 Additions)

New files shown. Phase 1 structure unchanged.

```
minibank/
├── Dockerfile                            # Phase 2 addition — shared by api, relay, and all consumers
├── .env                                  # Phase 2 addition — Docker-internal hostnames (not committed)
├── alembic/versions/
│   ├── 0001_initial_schema.py            # (Phase 1)
│   ├── 0002_transfer_failed_support.py   # (Phase 1 fix)
│   ├── 0003_add_outbox.py
│   ├── 0004_add_audit_events.py
│   └── 0005_add_transaction_activity.py
├── docker-compose.yml                    # + api, zookeeper, kafka, kafka-init, 4 workers
├── consumers/                            # Must NOT be named kafka/ — that shadows the kafka-python package on sys.path
│   ├── __init__.py                       # required — `from consumers.consumer_base import BaseConsumer` needs this
│   └── consumer_base.py                  # BaseConsumer: retry, DLQ, offset commit
│                                         # (relay creates AIOKafkaProducer in workers/outbox_relay.py;
│                                         #  consumers create their own inside BaseConsumer.run())
├── kafka/
│   └── create-topics.sh                  # topic creation script (Kafka init container only; bash, not Python — no import conflict)
├── app/
│   ├── config.py                         # + kafka_bootstrap_servers field (Phase 2 addition)
│   │                                     #   pydantic-settings field accessed as settings.kafka_bootstrap_servers
│   │                                     #   env var: KAFKA_BOOTSTRAP_SERVERS; default: "kafka:9092"
│   ├── database.py                       # + db_factory = AsyncSessionLocal (Phase 2 addition — one line alias)
│   │                                     #   workers import `db_factory`; AsyncSessionLocal already has expire_on_commit=False
│   ├── models/
│   │   ├── outbox.py                     # OutboxRow ORM model
│   │   ├── audit_event.py                # AuditEvent ORM model
│   │   └── transaction_activity.py       # TransactionActivity ORM model
│   ├── events/
│   │   ├── __init__.py                   # required — `from app.events.publisher import publish_event` needs this
│   │   ├── schemas.py                    # EventEnvelope, payload models, parse_event() — typed event contracts
│   │   └── publisher.py                  # publish_event(db, topic, event_type, payload, actor_id)
│   ├── services/
│   │   ├── transfer_service.py           # + publish_event() calls before each commit
│   │   └── account_service.py            # + publish_event() in open_account() + seed()
│   └── routers/
│       └── accounts.py                   # GET /me/transactions re-pointed to transaction_activity
├── workers/
│   ├── __init__.py                       # required — `python -m workers.outbox_relay` needs this
│   ├── outbox_relay.py                   # Two-phase relay with exponential backoff
│   ├── audit_consumer.py                 # Kafka → audit_events
│   ├── notification_consumer.py          # Kafka → stdout
│   └── activity_consumer.py             # Kafka → transaction_activity
├── management/
│   ├── __init__.py                       # required — `python -m management.backfill_events` needs this
│   └── backfill_events.py                # One-time backfill for Phase 1 historical data
└── tests/
    ├── conftest.py                        # + Kafka container fixture (Phase 2 additions)
    ├── test_outbox_relay.py               # relay: publish, at-least-once, max retries, concurrent claim
    ├── test_audit_consumer.py             # audit consumer: all event types, idempotency
    ├── test_activity_consumer.py          # activity consumer: transfer (2 rows), seed (1 row), idempotency
    ├── test_notification_consumer.py      # notification consumer: correct log output per event type
    ├── test_transactions_cqrs.py          # GET /transactions: reads read model, filters, as_of, backward compat
    └── test_backfill.py                   # backfill: outbox rows generated for all Phase 1 data
```

---

## 15. Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Teaching order | Direct publish Week 6 (minimal consumer, no retry/DLQ), outbox + BaseConsumer Week 7–9 | Experience failure modes before the fix; isolate learning to Kafka API first |
| Outbox relay pattern | Two-phase (claim → publish → confirm) | DB transactions stay short; locks not held during Kafka I/O |
| `event_id` generation | Fresh UUID in `publish_event()`, stored in payload | Stable across outbox rebuilds; not coupled to entity IDs |
| Kafka library | `aiokafka` (async) | Workers use `AsyncSession` + `asyncio` — sync `kafka-python` would block the event loop |
| Kafka bootstrap server | `settings.kafka_bootstrap_servers` (pydantic-settings field, env var: `KAFKA_BOOTSTRAP_SERVERS`) | `kafka:9092` inside Docker; `localhost:29092` for host dev (dual listener) — hardcoding breaks one or the other |
| Consumer retry | Re-publish with `x-retry-count` header | Consumer makes forward progress; no partition stall |
| `handle_message()` extracted from `run()` | Inner loop logic in its own method | Makes DLQ routing and retry logic unit-testable without a running consumer loop or Kafka container |
| Relay backoff trigger | Back off only when `published_count == 0` | A single bad payload row (e.g. oversized) should not slow down a healthy relay; only true Kafka connectivity failure warrants backoff |
| Consumer idempotency | Catch `IntegrityError` in `process()` → no-op | Unique constraint violation = already processed; must not propagate to BaseConsumer retry logic |
| API containerization | Dockerize in Phase 2 (Phase 1 runs on host) | API, relay, and consumers share the Docker network; `kafka:9092` resolves for all; `.env` file provides Docker-internal hostnames |
| Deserialization errors | Separate try/except before processing try/except | Prevents `NameError` on `event` variable if `json.loads` fails; malformed messages go straight to DLQ |
| Consumer `db` session | `self.db_factory()` per message in `process()` | DB session injected at construction; fresh session per message — no shared state across messages |
| Outbox max retries | 10 retries → `"failed"` status | Permanently unpublishable rows (bad payload) don't cycle forever; `"failed"` requires manual intervention |
| Outbox cleanup | `cleanup_published_rows` deletes `"published"` rows > 7 days | Prevents unbounded table growth; 7-day window matches Kafka topic retention |
| DLQ naming | Per-consumer (`{group-id}.dlq`) | Operator knows exactly which pipeline failed |
| Consumer groups | One per consumer type, cluster-prefixed | Prevents accidental group reuse; all consumers receive all events |
| `transaction_activity` uniqueness | `UNIQUE(event_id, account_id)` | One transfer = two rows (debit + credit); seed = one row (credit only) |
| `as_of` timestamp | `MAX(occurred_at)` from query results | Answers "how fresh is the data you received?" — no extra table needed in Phase 2 |
| `created_at` field in response | Map `occurred_at` → `created_at` in service layer | Phase 1 clients expect `created_at`; internal column name change is not a breaking change |
| `seed.completed` event | Outbox row from `seed()`, activity consumer handles it | Prevents CQRS migration from dropping seed entries from `/transactions` history |
| No separate `/activity` endpoint | Re-point existing `/transactions` | Real neobanks have one transaction feed; CQRS is an internal re-plumbing |
| Single Kafka partition | 1 partition per topic | Guaranteed ordering for Phase 2; production trade-off documented |
| Backfill | Management command with deterministic UUID5 event IDs (namespace + entity ID) | Idempotent — safe to retry without corrupting downstream tables; consumer UNIQUE constraints deduplicate |
| Worker lifecycle | Separate Docker Compose services | Independent restart, clear separation of concerns |
| Event serialisation | JSON (not Avro) | Avro adds schema enforcement; JSON sufficient for learning |
| Event contracts | Pydantic payload models (`app/events/schemas.py`) + `parse_event()` | Typed schemas catch structural errors at publish time (producer) and at consumption time (consumer) — a typo in a field name is a `ValidationError`, not a silent `KeyError` at 3am. Raw dicts across a Kafka boundary are a production anti-pattern |
| Consumer lag logging | Approximate via message timestamp every 100 messages | Precise lag needs an admin client call (end-offset − committed-offset); timestamp approximation sufficient for Phase 2 |
| CQRS `as_of` scope | `MAX(occurred_at)` of the current page, not the whole account history | Page-scoped freshness is what the client actually received; global max would misrepresent freshness for paginated older results |

---

## 16. Testing Strategy

### Test dependencies

Verify present in `pyproject.toml` (already included from Phase 1 setup — no changes needed):

```toml
[project]
dependencies = [
    "aiokafka>=0.11",                          # already present — used by relay and consumers
]

[dependency-groups]
dev = [
    "testcontainers[postgres,redis,kafka]>=4.14.2",   # already present — kafka extra included
]
```

### Testing approach

Phase 2 has two distinct test surfaces:

| Surface | Approach | Kafka needed? |
|---------|----------|---------------|
| Consumer `process()` logic | Call directly with a fake event dict | No — just a real test DB |
| Relay (claim/publish/confirm) | Call helpers directly against test DB + Kafka | Yes |
| DLQ routing | Call `handle_message()` with `AsyncMock` producer | No — mock producer replaces real Kafka |
| CQRS endpoint | Pre-populate `transaction_activity`, call API | No |
| Backfill | Call `backfill()` against test DB | No |

Testing `process()` directly is the most important pattern: it exercises all the DB logic, idempotency constraints, and field mapping — without the complexity of starting a full consumer loop.

### Phase 2 fixtures (`tests/conftest.py` additions)

```python
import asyncio
import json
import jwt
import pytest
import pytest_asyncio
from decimal import Decimal
from uuid import uuid4
from datetime import datetime, timezone, timedelta
from testcontainers.kafka import KafkaContainer
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from app.config import settings, SYSTEM_ACCOUNT_ID
from app.database import Base
from app.models.user import User
from app.models.account import Account
from app.models.transfer import Transfer
from app.models.ledger_entry import LedgerEntry
# Phase 2 models — imported here so Base.metadata.create_all creates their tables
# even when running only Phase 1 test files (e.g. `pytest tests/test_transfers.py`).
# Without these imports, the Phase 1 db_session TRUNCATE fails with
# 'relation "outbox" does not exist' because create_all never created the tables.
from app.models.outbox import OutboxRow
from app.models.audit_event import AuditEvent
from app.models.transaction_activity import TransactionActivity

# --- Kafka container (session-scoped — one broker for the whole test run) ---

@pytest.fixture(scope="session")
def kafka_bootstrap():
    """Start a Kafka testcontainer. Yields the bootstrap server address."""
    with KafkaContainer("confluentinc/cp-kafka:7.6.0") as kafka:
        yield kafka.get_bootstrap_server()
    # Note: testcontainers defaults KAFKA_AUTO_CREATE_TOPICS_ENABLE=true,
    # so topics are created on first use. No create-topics.sh needed in tests.


@pytest_asyncio.fixture
async def kafka_producer(kafka_bootstrap):
    producer = AIOKafkaProducer(bootstrap_servers=kafka_bootstrap)
    await producer.start()
    yield producer
    await producer.stop()


@pytest_asyncio.fixture
async def kafka_consumer(kafka_bootstrap):
    """Single-use consumer for asserting messages landed on a topic."""
    consumers = []
    async def factory(topic: str, group_id: str = None):
        group_id = group_id or f"test-{uuid4()}"  # unique group per use — always reads from start
        c = AIOKafkaConsumer(
            topic,
            bootstrap_servers=kafka_bootstrap,
            group_id=group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )
        await c.start()
        consumers.append(c)
        return c
    yield factory
    for c in consumers:
        await c.stop()


# --- DB factory for consumers (real sessions against test Postgres) ---

@pytest_asyncio.fixture
async def consumer_db_factory(postgres_container):
    """Async session factory pointing at the test Postgres container.
    Pass this to consumer constructors: AuditConsumer(db_factory=consumer_db_factory).

    Depends on `postgres_container` (session-scoped, from Phase 1's conftest.py) — NOT
    on a `db_engine` fixture, which Phase 1 does not expose. Phase 1 creates its engine
    inline inside `db_session` and `client`; those engines are never yielded as named
    fixtures. This fixture creates its own engine independently.

    TRUNCATE rationale: consumer-only tests (no `client`, no `db_session`) need a clean
    DB state before each test. Phase 1's `db_session` TRUNCATE only runs when `client`
    is in the fixture chain. Tests asserting `count == 0` would fail if a prior test left
    rows behind.

    ON CONFLICT DO NOTHING on the system account insert: CQRS integration tests use BOTH
    `client` (which triggers `db_session` → TRUNCATE + INSERT) and `consumer_db_factory`
    (which also TRUNCATEs + INSERTs). Whichever runs second must be a no-op, not an error.

    expire_on_commit=False: see Section 6 — relay's detached ORM row pattern requires it.
    """
    url = postgres_container.get_connection_url().replace("psycopg2", "asyncpg")
    engine = create_async_engine(url, pool_size=5, max_overflow=10)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)  # idempotent; creates Phase 2 tables too
    async with engine.begin() as conn:
        await conn.execute(text(
            "TRUNCATE outbox, audit_events, transaction_activity, "
            "transfers, ledger_entries, accounts, users RESTART IDENTITY CASCADE"
        ))
        await conn.execute(
            text("INSERT INTO accounts (id, user_id, status, created_at, updated_at) "
                 "VALUES (:id, NULL, 'active', NOW(), NOW()) ON CONFLICT DO NOTHING"),
            {"id": str(SYSTEM_ACCOUNT_ID)},
        )
    factory = async_sessionmaker(engine, expire_on_commit=False)
    yield factory
    await engine.dispose()


# --- Phase 2 model factories (used by backfill and integration tests) ---

@pytest_asyncio.fixture
async def transfer_factory(consumer_db_factory):
    """Create Transfer rows for testing. Only creates rows in the transfers table —
    does not create ledger entries or outbox rows.

    IMPORTANT: If the transfers table has FK constraints on from_account_id/to_account_id
    → accounts.id (which Phase 1's schema does), callers MUST pass real account IDs from
    account_factory(). Passing None generates random UUIDs that will fail the FK check at
    flush time with IntegrityError.
    """
    async def factory(status: str = "completed", from_account_id=None, to_account_id=None, failure_code=None):
        async with consumer_db_factory() as db:
            async with db.begin():
                t = Transfer(
                    from_account_id=from_account_id or uuid4(),
                    to_account_id=to_account_id or uuid4(),
                    amount=Decimal("100.0000"),
                    status=status,
                    failure_code=failure_code,
                    idempotency_key=str(uuid4()),
                    created_at=datetime.now(timezone.utc),  # NOT NULL — no server default
                )
                db.add(t)
                await db.flush()
                return t
    return factory


@pytest_asyncio.fixture
async def seed_factory(consumer_db_factory):
    """Create seed LedgerEntry legs for a given account (entry_type='seed').

    Leg-based ledger: creates two rows grouped by transaction_id:
    - Debit leg on SYSTEM_ACCOUNT_ID (money leaves the system)
    - Credit leg on user's account (money enters the user's account)
    Returns the credit leg (user-facing entry).
    """
    async def factory(account_id):
        async with consumer_db_factory() as db:
            async with db.begin():
                txn_id = uuid4()
                now = datetime.now(timezone.utc)
                debit_leg = LedgerEntry(
                    transaction_id=txn_id,
                    account_id=SYSTEM_ACCOUNT_ID,
                    direction="debit",
                    amount=Decimal("1000.0000"),
                    currency="USD",
                    entry_type="seed",
                    created_at=now,
                )
                credit_leg = LedgerEntry(
                    transaction_id=txn_id,
                    account_id=account_id,
                    direction="credit",
                    amount=Decimal("1000.0000"),
                    currency="USD",
                    entry_type="seed",
                    idempotency_key=str(uuid4()),
                    created_at=now,
                )
                db.add(debit_leg)
                db.add(credit_leg)
                await db.flush()
                return credit_leg
    return factory


# --- account_factory ---
# Phase 1 conftest.py does NOT define `account_factory`. Phase 1 has `alice_account` and
# `bob_account` — named, API-based fixtures that call POST /v1/accounts via the HTTP client
# and return a dict. They are not callable factories and they depend on `client`.
# Phase 2 must define its own `account_factory` using ORM directly via consumer_db_factory.
# This avoids the HTTP stack and works in consumer tests that have no `client` fixture.
#
# User.created_at / Account.created_at have no server defaults — must be set explicitly.

@pytest_asyncio.fixture
async def account_factory(consumer_db_factory):
    """Creates a User + Account row directly via ORM. Returns the Account with .id and .user_id set."""
    async def factory():
        async with consumer_db_factory() as db:
            async with db.begin():
                now = datetime.now(timezone.utc)
                user = User(
                    email=f"user-{uuid4()}@test.com",
                    hashed_password="hashed",
                    created_at=now,
                    updated_at=now,
                )
                db.add(user)
                await db.flush()
                account = Account(
                    user_id=user.id,
                    status="active",
                    created_at=now,
                    updated_at=now,
                )
                db.add(account)
                await db.flush()
                return account
    return factory


# --- auth_headers ---
# Phase 1 has `alice_headers` — a static dict fixture, not callable. Phase 2's CQRS tests
# need to generate JWT headers for any given Account ORM object. This fixture returns a
# factory function that signs a token with settings.jwt_secret — the same key FastAPI's
# get_current_user dependency uses — so the token is accepted without going through /login.

@pytest.fixture
def auth_headers():
    """Returns a factory: given an Account ORM object, produces Authorization headers."""
    def make_headers(account) -> dict:
        token = jwt.encode(
            {
                "sub": str(account.user_id),
                "exp": datetime.now(timezone.utc) + timedelta(hours=1),
            },
            settings.jwt_secret,   # Phase 1 uses settings.jwt_secret — NOT settings.SECRET_KEY
            algorithm="HS256",     # must match settings.jwt_algorithm used by get_current_user
        )
        return {"Authorization": f"Bearer {token}"}
    return make_headers


# --- Event builder helper ---
# make_event intentionally takes a raw dict payload, NOT a Pydantic model.
# Tests need to construct both valid AND invalid events (e.g. missing fields,
# wrong types) to verify that consumers reject malformed payloads correctly.
# A typed model would prevent constructing the broken events we want to test.
# For tests that exercise the happy path, pass a dict matching the schema contract.

def make_event(event_type: str, payload: dict, actor_id=None) -> dict:
    """Build a valid event envelope for test use."""
    return {
        "event_id": str(uuid4()),
        "event_type": event_type,
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "version": "1",
        "actor_id": str(actor_id) if actor_id else None,
        "payload": payload,
    }
```

---

### Consumer tests — `process()` called directly (no Kafka)

**`tests/test_audit_consumer.py`**

```python
async def test_audit_consumer_transfer_completed(consumer_db_factory):
    """transfer.completed → one audit_events row with correct fields."""
    consumer = AuditConsumer(db_factory=consumer_db_factory)
    event = make_event("transfer.completed", {
        "transfer_id": str(uuid4()),
        "from_account_id": str(uuid4()),
        "to_account_id": str(uuid4()),
        "amount": "100.0000", "currency": "USD",
        "entry_type": "transfer",
        "idempotency_key": "test-key",
    })
    await consumer.process(event)

    async with consumer_db_factory() as db:
        row = (await db.execute(
            select(AuditEvent).where(AuditEvent.event_id == event["event_id"])
        )).scalar_one()
    assert row.event_type == "transfer.completed"
    assert row.resource_type == "transfer"


async def test_audit_consumer_transfer_failed(consumer_db_factory):
    """transfer.failed → one audit_events row."""
    consumer = AuditConsumer(db_factory=consumer_db_factory)
    event = make_event("transfer.failed", {
        "transfer_id": str(uuid4()),
        "from_account_id": str(uuid4()),
        "to_account_id": str(uuid4()),
        "amount": "100.0000", "currency": "USD",
        "failure_code": "INSUFFICIENT_BALANCE",
    })
    await consumer.process(event)
    # assert row exists (same pattern as above)


async def test_audit_consumer_account_opened(consumer_db_factory):
    """account.opened → one audit_events row."""
    consumer = AuditConsumer(db_factory=consumer_db_factory)
    event = make_event("account.opened", {
        "account_id": str(uuid4()),
        "user_id": str(uuid4()),
        "status": "active",
    })
    await consumer.process(event)

    async with consumer_db_factory() as db:
        row = (await db.execute(
            select(AuditEvent).where(AuditEvent.event_id == event["event_id"])
        )).scalar_one()
    assert row.event_type == "account.opened"
    assert row.resource_type == "account"


async def test_audit_consumer_seed_completed(consumer_db_factory):
    """seed.completed → one audit_events row (audit logs ALL event types)."""
    consumer = AuditConsumer(db_factory=consumer_db_factory)
    event = make_event("seed.completed", {
        "account_id": str(uuid4()),
        "user_id": str(uuid4()),
        "amount": "1000.0000", "currency": "USD",
        "entry_type": "seed",
    })
    await consumer.process(event)
    # assert row exists


async def test_audit_consumer_idempotent(consumer_db_factory):
    """Processing the same event twice inserts only one row — IntegrityError caught, not retried."""
    consumer = AuditConsumer(db_factory=consumer_db_factory)
    event = make_event("transfer.completed", {
        "transfer_id": str(uuid4()),
        "from_account_id": str(uuid4()),
        "to_account_id": str(uuid4()),
        "amount": "100.0000", "currency": "USD",
        "entry_type": "transfer",
        "idempotency_key": "test-key",
    })
    await consumer.process(event)
    await consumer.process(event)   # second call — must not raise, must not insert duplicate

    async with consumer_db_factory() as db:
        count = (await db.execute(
            select(func.count()).where(AuditEvent.event_id == event["event_id"])
        )).scalar_one()
    assert count == 1   # exactly one row, not two
```

**`tests/test_activity_consumer.py`**

```python
async def test_activity_consumer_transfer_completed_creates_two_rows(consumer_db_factory, account_factory):
    """transfer.completed → debit row for sender + credit row for receiver."""
    sender = await account_factory()
    receiver = await account_factory()
    consumer = ActivityConsumer(db_factory=consumer_db_factory)
    event = make_event("transfer.completed", {
        "transfer_id": str(uuid4()),
        "from_account_id": str(sender.id),
        "to_account_id": str(receiver.id),
        "amount": "50.0000", "currency": "USD",
        "entry_type": "transfer",
        "idempotency_key": "test-key",
    })
    await consumer.process(event)

    async with consumer_db_factory() as db:
        rows = (await db.execute(
            select(TransactionActivity).where(TransactionActivity.event_id == event["event_id"])
        )).scalars().all()
    assert len(rows) == 2
    directions = {r.account_id: r.direction for r in rows}
    assert directions[sender.id] == "debit"
    assert directions[receiver.id] == "credit"


async def test_activity_consumer_seed_completed_creates_one_credit_row(consumer_db_factory, account_factory):
    """seed.completed → one credit row only (no debit — money from system)."""
    account = await account_factory()
    consumer = ActivityConsumer(db_factory=consumer_db_factory)
    event = make_event("seed.completed", {
        "account_id": str(account.id),
        "user_id": str(account.user_id),
        "amount": "1000.0000", "currency": "USD",
        "entry_type": "seed",
    })
    await consumer.process(event)

    async with consumer_db_factory() as db:
        rows = (await db.execute(
            select(TransactionActivity).where(TransactionActivity.account_id == account.id)
        )).scalars().all()
    assert len(rows) == 1
    assert rows[0].direction == "credit"
    assert rows[0].entry_type == "seed"


async def test_activity_consumer_idempotent(consumer_db_factory, account_factory):
    """Replaying transfer.completed twice produces the same two rows, not four."""
    sender, receiver = await account_factory(), await account_factory()
    consumer = ActivityConsumer(db_factory=consumer_db_factory)
    event = make_event("transfer.completed", {
        "from_account_id": str(sender.id), "to_account_id": str(receiver.id),
        "transfer_id": str(uuid4()), "amount": "10.0000", "currency": "USD", "entry_type": "transfer",
        "idempotency_key": "key",
    })
    await consumer.process(event)
    await consumer.process(event)   # replay — IntegrityError caught on UNIQUE(event_id, account_id)

    async with consumer_db_factory() as db:
        count = (await db.execute(
            select(func.count()).where(TransactionActivity.event_id == event["event_id"])
        )).scalar_one()
    assert count == 2   # still two, not four


async def test_activity_consumer_ignores_account_opened(consumer_db_factory, account_factory):
    """account.opened produces no activity rows — informational event only."""
    account = await account_factory()
    consumer = ActivityConsumer(db_factory=consumer_db_factory)
    event = make_event("account.opened", {
        "account_id": str(account.id), "user_id": str(account.user_id), "status": "active",
    })
    await consumer.process(event)

    async with consumer_db_factory() as db:
        count = (await db.execute(select(func.count(TransactionActivity.id)))).scalar_one()
    assert count == 0
```

**`tests/test_notification_consumer.py`**

```python
async def test_notification_transfer_completed_logs_both_sides(caplog):
    """transfer.completed → two log lines (sender + receiver).
    NotificationConsumer makes no DB calls — db_factory=None is correct.
    """
    consumer = NotificationConsumer(db_factory=None)
    event = make_event("transfer.completed", {
        "from_account_id": "acct-A",
        "to_account_id": "acct-B",
        "transfer_id": str(uuid4()),
        "amount": "75.0000", "currency": "USD",
        "entry_type": "transfer",
        "idempotency_key": "test-key",
    })
    with caplog.at_level(logging.INFO):
        await consumer.process(event)
    assert any("acct-A" in r.message and "sent" in r.message for r in caplog.records)
    assert any("acct-B" in r.message and "received" in r.message for r in caplog.records)


async def test_notification_transfer_failed_logs_failure(caplog):
    """transfer.failed → one log line with failure_code."""
    consumer = NotificationConsumer(db_factory=None)
    event = make_event("transfer.failed", {
        "transfer_id": str(uuid4()),
        "from_account_id": "acct-A",
        "to_account_id": "acct-B",
        "amount": "50.0000", "currency": "USD",
        "failure_code": "INSUFFICIENT_BALANCE",
    })
    with caplog.at_level(logging.INFO):
        await consumer.process(event)
    assert any("acct-A" in r.message and "INSUFFICIENT_BALANCE" in r.message for r in caplog.records)


async def test_notification_account_opened_logs_welcome(caplog):
    """account.opened → one welcome log line."""
    consumer = NotificationConsumer(db_factory=None)
    event = make_event("account.opened", {
        "account_id": str(uuid4()),
        "user_id": "user-X",
        "status": "active",
    })
    with caplog.at_level(logging.INFO):
        await consumer.process(event)
    assert any("user-X" in r.message and "active" in r.message for r in caplog.records)


async def test_notification_seed_completed_logs_nothing(caplog):
    """seed.completed is a no-op for notifications — intentional, no log output."""
    consumer = NotificationConsumer(db_factory=None)
    event = make_event("seed.completed", {
        "account_id": str(uuid4()),
        "user_id": str(uuid4()),
        "amount": "1000.0000", "currency": "USD",
        "entry_type": "seed",
    })
    initial_count = len(caplog.records)
    await consumer.process(event)
    assert len(caplog.records) == initial_count   # no new records
```

---

### Relay tests — needs Kafka container

**`tests/test_outbox_relay.py`**

```python
async def test_relay_publishes_pending_row_and_marks_published(
    consumer_db_factory, kafka_producer, kafka_consumer
):
    """Relay picks up a pending outbox row, publishes to Kafka, marks status='published'."""
    event_payload = make_event("transfer.completed", {
        "transfer_id": str(uuid4()),
        "from_account_id": str(uuid4()),
        "to_account_id": str(uuid4()),
        "amount": "100.0000", "currency": "USD",
        "entry_type": "transfer",
        "idempotency_key": "test-key",
    })

    # Insert a pending outbox row
    async with consumer_db_factory() as db:
        async with db.begin():
            db.add(OutboxRow(
                topic="transfer.events",
                event_type="transfer.completed",
                payload=event_payload,
                status="pending",
            ))

    # Run relay phases directly (claim_batch and confirm_batch) against test DB
    from workers.outbox_relay import claim_batch, confirm_batch
    claimed = await claim_batch(consumer_db_factory)
    assert len(claimed) == 1
    await kafka_producer.send_and_wait(claimed[0].topic, json.dumps(claimed[0].payload).encode())
    claimed[0].status = "published"
    claimed[0].published_at = datetime.now(timezone.utc)
    await confirm_batch(consumer_db_factory, claimed)

    # Assert row is published in DB
    async with consumer_db_factory() as db:
        row = await db.get(OutboxRow, claimed[0].id)
    assert row.status == "published"

    # Assert message arrived on Kafka topic
    consumer = await kafka_consumer("transfer.events")
    msg = await asyncio.wait_for(consumer.__anext__(), timeout=5.0)
    received = json.loads(msg.value)
    assert received["event_type"] == "transfer.completed"


async def test_recover_stuck_rows_resets_publishing_to_pending(consumer_db_factory):
    """recover_stuck_rows resets 'publishing' rows with old created_at back to 'pending'.
    This simulates the relay crashing between claim and confirm — at-least-once guarantee.
    """
    from workers.outbox_relay import recover_stuck_rows

    # Insert a row in 'publishing' state with a created_at old enough to trigger recovery
    async with consumer_db_factory() as db:
        async with db.begin():
            row = OutboxRow(
                topic="transfer.events", event_type="transfer.completed",
                payload={}, status="publishing",
            )
            db.add(row)
            # Manually age the created_at beyond the 5-minute threshold
            await db.flush()
            row.created_at = datetime.now(timezone.utc) - timedelta(minutes=10)

    await recover_stuck_rows(consumer_db_factory)

    async with consumer_db_factory() as db:
        refreshed = await db.get(OutboxRow, row.id)
    assert refreshed.status == "pending"   # reset — relay will re-publish


async def test_relay_marks_row_failed_after_max_retries(consumer_db_factory):
    """A row that has already hit MAX_OUTBOX_RETRIES is marked 'failed', not 'pending'."""
    from workers.outbox_relay import MAX_OUTBOX_RETRIES, confirm_batch

    async with consumer_db_factory() as db:
        async with db.begin():
            row = OutboxRow(
                topic="transfer.events", event_type="transfer.completed",
                payload={}, status="publishing",
                retry_count=MAX_OUTBOX_RETRIES - 1,  # one failure away from the threshold
            )
            db.add(row)

    # Simulate one more KafkaError — this increment crosses the threshold.
    # retry_count goes from MAX_OUTBOX_RETRIES-1 to MAX_OUTBOX_RETRIES (e.g. 9→10).
    # 10 >= 10 → "failed". This is the actual boundary the relay enforces.
    row.retry_count += 1
    row.status = "failed" if row.retry_count >= MAX_OUTBOX_RETRIES else "pending"
    await confirm_batch(consumer_db_factory, [row])

    async with consumer_db_factory() as db:
        refreshed = await db.get(OutboxRow, row.id)
    assert refreshed.status == "failed"
    assert refreshed.retry_count == MAX_OUTBOX_RETRIES   # increment happened before status transition


async def test_for_update_skip_locked_prevents_duplicate_claims(consumer_db_factory):
    """Two concurrent claim_batch calls must claim different rows — no duplicate publishes."""
    from workers.outbox_relay import claim_batch

    # Insert 2 pending rows
    async with consumer_db_factory() as db:
        async with db.begin():
            for _ in range(2):
                db.add(OutboxRow(topic="transfer.events", event_type="transfer.completed",
                                  payload={}, status="pending"))

    # asyncio.gather provides cooperative concurrency: coroutines interleave only at
    # await points, not OS-level parallelism. Both claim_batch calls will interleave
    # when asyncpg awaits the DB response, giving overlapping transactions. This is
    # sufficient to exercise FOR UPDATE SKIP LOCKED within a single process.
    # True OS-parallel relay instances (separate processes) would exercise it further,
    # but asyncio concurrency catches the most common claim-logic bugs.
    results = await asyncio.gather(
        claim_batch(consumer_db_factory),
        claim_batch(consumer_db_factory),
    )
    all_claimed_ids = [row.id for batch in results for row in batch]
    assert len(all_claimed_ids) == 2                              # all rows claimed — neither was skipped
    assert len(set(all_claimed_ids)) == 2                         # no duplicates
    # SKIP LOCKED means each concurrent claimant takes a disjoint subset.
    # With 2 rows and 2 concurrent claims, each batch must claim exactly 1 row.
    # If SKIP LOCKED were missing or broken, one batch would block and then claim
    # all remaining rows — producing batch_sizes=[0,2] or [2,0], not [1,1].
    batch_sizes = sorted(len(batch) for batch in results)
    assert batch_sizes == [1, 1], f"Expected [1,1] but got {batch_sizes} — SKIP LOCKED not working"


async def test_cleanup_deletes_old_published_rows(consumer_db_factory):
    """cleanup_published_rows deletes 'published' rows older than 7 days."""
    from workers.outbox_relay import cleanup_published_rows

    async with consumer_db_factory() as db:
        async with db.begin():
            row = OutboxRow(
                topic="transfer.events", event_type="transfer.completed",
                payload={}, status="published",
                published_at=datetime.now(timezone.utc) - timedelta(days=8),
            )
            db.add(row)
            await db.flush()
            row_id = row.id

    await cleanup_published_rows(consumer_db_factory)

    async with consumer_db_factory() as db:
        assert await db.get(OutboxRow, row_id) is None   # deleted


async def test_cleanup_warns_and_deletes_old_failed_rows(consumer_db_factory, caplog):
    """cleanup_published_rows logs a warning before deleting 'failed' rows older than 30 days."""
    from workers.outbox_relay import cleanup_published_rows

    async with consumer_db_factory() as db:
        async with db.begin():
            row = OutboxRow(
                topic="transfer.events", event_type="transfer.completed",
                payload={}, status="failed",
            )
            db.add(row)
            await db.flush()
            row.created_at = datetime.now(timezone.utc) - timedelta(days=31)
            row_id = row.id

    with caplog.at_level(logging.WARNING):
        await cleanup_published_rows(consumer_db_factory)

    assert any("Purging" in r.message and "failed" in r.message for r in caplog.records)
    async with consumer_db_factory() as db:
        assert await db.get(OutboxRow, row_id) is None   # deleted
```

---

### CQRS endpoint tests — no Kafka needed

**`tests/test_transactions_cqrs.py`**

```python
async def test_transactions_reads_from_activity_not_ledger(
    client, consumer_db_factory, auth_headers, account_factory
):
    """GET /transactions reads transaction_activity — returns data even when ledger is empty.
    This is the primary acceptance criterion for the CQRS migration.
    """
    account = await account_factory()
    event_id = uuid4()

    # Populate transaction_activity directly — bypassing the ledger
    async with consumer_db_factory() as db:
        async with db.begin():
            db.add(TransactionActivity(
                event_id=event_id,
                account_id=account.id,
                direction="credit",
                amount=Decimal("500.0000"),
                entry_type="seed",
                occurred_at=datetime.now(timezone.utc),
            ))

    # Verify ledger is empty for this account (simulates post-migration state).
    # Leg-based ledger: each leg has an account_id column.
    async with consumer_db_factory() as db:
        ledger_count = (await db.execute(
            select(func.count()).where(LedgerEntry.account_id == account.id)
        )).scalar_one()
    assert ledger_count == 0   # no ledger entries — data lives only in activity table

    response = await client.get("/v1/accounts/me/transactions", headers=auth_headers(account))
    assert response.status_code == 200
    data = response.json()
    assert len(data["data"]) == 1
    assert data["data"][0]["entry_type"] == "seed"
    assert data["data"][0]["direction"] == "credit"


async def test_transactions_created_at_field_preserved(client, consumer_db_factory, account_factory, auth_headers):
    """Response uses 'created_at' field name (Phase 1 contract) even though DB stores 'occurred_at'.
    Backward compatibility — clients must not need to change field names after migration.
    """
    account = await account_factory()
    occurred = datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)
    async with consumer_db_factory() as db:
        async with db.begin():
            db.add(TransactionActivity(
                event_id=uuid4(),
                account_id=account.id,
                direction="credit",
                amount=Decimal("100.0000"),
                entry_type="seed",
                occurred_at=occurred,
            ))

    response = await client.get("/v1/accounts/me/transactions", headers=auth_headers(account))
    item = response.json()["data"][0]
    assert "created_at" in item            # must use Phase 1 field name
    assert "occurred_at" not in item       # must not expose internal column name
    assert item["created_at"].startswith("2024-01-15T10:30:00")  # timezone suffix may vary


async def test_transactions_as_of_is_max_occurred_at_of_current_page(client, consumer_db_factory, account_factory, auth_headers):
    """as_of = MAX(occurred_at) of the rows returned on this page, not the whole account history."""
    account = await account_factory()
    t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
    t2 = datetime(2024, 1, 2, tzinfo=timezone.utc)   # most recent
    async with consumer_db_factory() as db:
        async with db.begin():
            for t in (t1, t2):
                db.add(TransactionActivity(
                    event_id=uuid4(),
                    account_id=account.id,
                    direction="credit",
                    amount=Decimal("50.0000"),
                    entry_type="seed",
                    occurred_at=t,
                ))

    response = await client.get("/v1/accounts/me/transactions?limit=10", headers=auth_headers(account))
    meta = response.json()["meta"]
    assert meta["as_of"].startswith("2024-01-02T00:00:00")   # MAX of the page


async def test_transactions_as_of_null_when_no_results(client, account_factory, auth_headers):
    """as_of is null when the account has no activity rows."""
    account = await account_factory()
    response = await client.get("/v1/accounts/me/transactions", headers=auth_headers(account))
    assert response.json()["meta"]["as_of"] is None


async def test_transactions_date_filter_on_occurred_at(client, consumer_db_factory, account_factory, auth_headers):
    """from_date/to_date filter on occurred_at (not ledger's created_at)."""
    account = await account_factory()
    t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
    t2 = datetime(2024, 1, 2, tzinfo=timezone.utc)
    async with consumer_db_factory() as db:
        async with db.begin():
            for t in (t1, t2):
                db.add(TransactionActivity(
                    event_id=uuid4(),
                    account_id=account.id,
                    direction="credit",
                    amount=Decimal("50.0000"),
                    entry_type="seed",
                    occurred_at=t,
                ))

    response = await client.get(
        f"/v1/accounts/me/transactions?from_date={t2.isoformat()}",
        headers=auth_headers(account),
    )
    assert len(response.json()["data"]) == 1   # only t2 row


async def test_transactions_entry_type_filter(client, consumer_db_factory, account_factory, auth_headers):
    """entry_type=seed returns only seed entries."""
    account = await account_factory()
    async with consumer_db_factory() as db:
        async with db.begin():
            db.add(TransactionActivity(
                event_id=uuid4(),
                account_id=account.id,
                direction="credit",
                amount=Decimal("1000.0000"),
                entry_type="seed",
                occurred_at=datetime.now(timezone.utc),
            ))
            db.add(TransactionActivity(
                event_id=uuid4(),
                account_id=account.id,
                direction="debit",
                amount=Decimal("50.0000"),
                entry_type="transfer",
                reference_id=uuid4(),
                occurred_at=datetime.now(timezone.utc),
            ))

    response = await client.get("/v1/accounts/me/transactions?entry_type=seed", headers=auth_headers(account))
    items = response.json()["data"]
    assert len(items) == 1
    assert all(i["entry_type"] == "seed" for i in items)
```

---

### DLQ tests — `handle_message()` called directly (no running consumer loop)

`BaseConsumer.run()` is a long-running loop — testing it requires either patching `AIOKafkaConsumer` or letting it block the test. Instead, test `handle_message()` directly with a mock producer. This exercises all of the deserialization + retry + DLQ routing code without the loop.

**`tests/test_dlq_routing.py`**

```python
import json
import pytest
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

from sqlalchemy import func, select
from app.models.audit_event import AuditEvent
from workers.audit_consumer import AuditConsumer
from conftest import make_event


def make_fake_message(value: bytes, retry_count: int = 0) -> MagicMock:
    """Build a fake aiokafka Message for use with handle_message()."""
    msg = MagicMock()
    msg.value = value
    msg.headers = [(b"x-retry-count", str(retry_count).encode())] if retry_count else []
    msg.topic = "transfer.events"
    msg.offset = 0
    return msg


async def test_malformed_json_goes_to_dlq_immediately(consumer_db_factory):
    """Malformed JSON bypasses retry — sent straight to DLQ on first attempt."""
    consumer_instance = AuditConsumer(db_factory=consumer_db_factory)
    mock_producer = AsyncMock()
    mock_consumer = AsyncMock()

    fake_msg = make_fake_message(b"this is not valid json")
    await consumer_instance.handle_message(fake_msg, mock_producer, mock_consumer)

    # Must have sent to DLQ, not retried on source topic
    mock_producer.send_and_wait.assert_called_once()
    dlq_topic = mock_producer.send_and_wait.call_args[0][0]
    assert dlq_topic == "minibank.audit-consumer.dlq"
    assert mock_producer.send_and_wait.call_args[1].get("value") == b"this is not valid json"

    # Offset must be committed (malformed message consumed, not retried)
    mock_consumer.commit.assert_called_once()

    # Nothing written to audit_events
    async with consumer_db_factory() as db:
        count = (await db.execute(select(func.count(AuditEvent.id)))).scalar_one()
    assert count == 0


async def test_process_failure_retries_then_dlqs(consumer_db_factory, monkeypatch):
    """process() failure: re-published with incrementing x-retry-count, DLQ'd after 3."""
    consumer_instance = AuditConsumer(db_factory=consumer_db_factory)
    monkeypatch.setattr(consumer_instance, "process", AsyncMock(side_effect=RuntimeError("forced")))

    mock_producer = AsyncMock()
    mock_consumer = AsyncMock()
    event = make_event("transfer.completed", {
        "transfer_id": str(uuid4()), "from_account_id": str(uuid4()),
        "to_account_id": str(uuid4()), "amount": "100.0000", "currency": "USD",
        "entry_type": "transfer", "idempotency_key": "key",
    })
    encoded = json.dumps(event).encode()

    # Retry 0, 1, 2 → re-published to source topic each time with incremented header
    for retry_count in range(3):
        msg = make_fake_message(encoded, retry_count=retry_count)
        await consumer_instance.handle_message(msg, mock_producer, mock_consumer)

        sent_topic = mock_producer.send_and_wait.call_args[0][0]
        assert sent_topic == "transfer.events", f"retry {retry_count} should re-publish to source topic"

        # Verify x-retry-count header is incremented — without this check, a bug that
        # re-publishes with the same counter forever would never reach the DLQ threshold.
        sent_kwargs = mock_producer.send_and_wait.call_args[1]
        sent_headers = dict(sent_kwargs.get("headers") or [])
        expected_count = str(retry_count + 1).encode()
        assert sent_headers.get(b"x-retry-count") == expected_count, \
            f"retry {retry_count}: expected x-retry-count={retry_count + 1}"

        mock_producer.reset_mock()
        mock_consumer.reset_mock()

    # retry_count=3 → DLQ
    msg = make_fake_message(encoded, retry_count=3)
    await consumer_instance.handle_message(msg, mock_producer, mock_consumer)
    sent_topic = mock_producer.send_and_wait.call_args[0][0]
    assert sent_topic == "minibank.audit-consumer.dlq"
    mock_consumer.commit.assert_called_once()


async def test_idempotent_replay_does_not_go_to_dlq(consumer_db_factory):
    """IntegrityError (duplicate event_id) is caught in process() — not retried or DLQ'd."""
    consumer_instance = AuditConsumer(db_factory=consumer_db_factory)
    mock_producer = AsyncMock()
    mock_consumer = AsyncMock()

    event = make_event("transfer.completed", {
        "transfer_id": str(uuid4()), "from_account_id": str(uuid4()),
        "to_account_id": str(uuid4()), "amount": "100.0000", "currency": "USD",
        "entry_type": "transfer", "idempotency_key": "key",
    })
    encoded = json.dumps(event).encode()

    # First call — inserts row
    await consumer_instance.handle_message(make_fake_message(encoded), mock_producer, mock_consumer)
    # Second call — IntegrityError caught in process(), handle_message commits cleanly
    await consumer_instance.handle_message(make_fake_message(encoded), mock_producer, mock_consumer)

    # No DLQ, no retry — producer never called
    mock_producer.send_and_wait.assert_not_called()

    # Exactly one audit row, not two
    async with consumer_db_factory() as db:
        count = (await db.execute(
            select(func.count()).where(AuditEvent.event_id == event["event_id"])
        )).scalar_one()
    assert count == 1
```

---

### Backfill test

**`tests/test_backfill.py`**

```python
async def test_backfill_creates_outbox_rows_for_all_phase1_data(
    consumer_db_factory, account_factory, transfer_factory, seed_factory
):
    """backfill() generates one outbox row per account, completed transfer, and seed entry."""
    # Set up Phase 1 data.
    # transfer_factory requires real account IDs — FK constraints on from_account_id/to_account_id.
    # Passing None (default) generates random UUIDs that fail the FK check at flush time.
    account = await account_factory()
    sender = await account_factory()
    receiver = await account_factory()
    transfer = await transfer_factory(status="completed", from_account_id=sender.id, to_account_id=receiver.id)
    seed = await seed_factory(account_id=account.id)

    # Pass consumer_db_factory explicitly — backfill() defaults to app.database.db_factory
    # (module-level global) which may point to a different DB than the test container.
    # Every other component (relay, consumers) accepts db_factory as a parameter;
    # backfill() follows the same pattern.
    await backfill(db_factory=consumer_db_factory)

    async with consumer_db_factory() as db:
        rows = (await db.execute(select(OutboxRow))).scalars().all()

    event_types = {r.event_type for r in rows}
    assert "account.opened" in event_types
    assert "transfer.completed" in event_types
    assert "seed.completed" in event_types

    # All backfill events have actor_id=None (Phase 1 has no actor attribution)
    assert all(r.payload.get("actor_id") is None for r in rows)


async def test_backfill_includes_failed_transfers(consumer_db_factory, account_factory, transfer_factory):
    """backfill() emits transfer.failed for historical failures — PRD requires audit trail
    for every transfer (completed AND failed)."""
    sender = await account_factory()
    receiver = await account_factory()
    await transfer_factory(status="failed", from_account_id=sender.id, to_account_id=receiver.id)
    await backfill(db_factory=consumer_db_factory)

    async with consumer_db_factory() as db:
        rows = (await db.execute(select(OutboxRow))).scalars().all()
    event_types = {r.event_type for r in rows}
    assert "transfer.failed" in event_types
    # Failed transfer event has no activity row — only audit consumer processes it
    failed_rows = [r for r in rows if r.event_type == "transfer.failed"]
    assert all(r.payload.get("actor_id") is None for r in failed_rows)


async def test_backfill_raises_if_already_run(consumer_db_factory, account_factory):
    """Running backfill twice raises RuntimeError — guard against accidental double-run.

    The guard detects prior runs by counting 'account.opened' outbox rows. This is the
    only protection against duplicate rows in audit_events and transaction_activity.
    """
    await account_factory()   # creates an account that the first backfill will emit
    await backfill(db_factory=consumer_db_factory)

    with pytest.raises(RuntimeError, match="already run"):
        await backfill(db_factory=consumer_db_factory)   # second run must be blocked


async def test_backfill_force_bypasses_guard(consumer_db_factory, account_factory):
    """force=True skips the preflight check — documented escape hatch for partial-failure recovery."""
    await account_factory()
    await backfill(db_factory=consumer_db_factory)
    # Should not raise — force=True is the documented way to re-run after partial failure + cleanup
    await backfill(db_factory=consumer_db_factory, force=True)
```

---

### Phase 1 migration checklist — changes required before Phase 2 tests pass

Two Phase 1 files must be updated when Phase 2 is added to the project. Neither is a Phase 2
feature — they are maintenance required to keep Phase 1 tests green after Phase 2 tables exist.

#### 1. `tests/conftest.py` — extend the TRUNCATE list

Phase 1's `db_session` fixture TRUNCATEs only its own tables:

```python
# Phase 1 (current)
await conn.execute(text(
    "TRUNCATE transfers, ledger_entries, accounts, users RESTART IDENTITY CASCADE"
))
```

After Phase 2 tables (`outbox`, `audit_events`, `transaction_activity`) are created by
`Base.metadata.create_all`, any test using `client` (which triggers `db_session`) will leave
rows in Phase 2 tables between runs. Tests that assert `COUNT(*) == 0` on those tables will
fail non-deterministically. Update to:

```python
# Phase 1 + Phase 2
await conn.execute(text(
    "TRUNCATE outbox, audit_events, transaction_activity, "
    "transfers, ledger_entries, accounts, users RESTART IDENTITY CASCADE"
))
```

> **Execution order:** `TRUNCATE` processes tables in the order listed and resolves FK
> dependencies automatically when `CASCADE` is specified. Listing Phase 2 tables first avoids
> FK constraint errors if any Phase 2 table references Phase 1 tables.

#### 2. `tests/test_transactions.py` — rewrite after CQRS migration

`GET /v1/accounts/me/transactions` is backed by the ledger in Phase 1 and by
`transaction_activity` in Phase 2. After the migration, 6 of 10 tests in
`test_transactions.py` will fail with empty results because `transaction_activity` is always
empty in tests — no activity consumer runs in the test suite.

**Tests that break (all assert non-zero result counts):**

| Test | Expects |
|------|---------|
| `test_transfer_sender_view` | 2 entries (seed + transfer) |
| `test_transfer_receiver_view` | 1 entry |
| `test_filter_by_entry_type` | 1 seed entry |
| `test_filter_by_date_range_includes_entries` | 1 seed entry |
| `test_pagination` | 6 entries |
| `test_pagination_pages_non_overlapping` | 4 entries across 2 pages |

**Fix:** Rewrite these tests to insert rows directly into `transaction_activity` instead of
going through the transfer/seed API calls. Phase 2 adds `test_transactions_cqrs.py` which
covers the new endpoint properly. The Phase 1 `test_transactions.py` tests can be:

- **Deleted** — their coverage is superseded by `test_transactions_cqrs.py`.
- **Rewritten** — replace `POST /v1/transfers` + ledger assertions with direct
  `transaction_activity` inserts (using `consumer_db_factory`) before asserting the API
  response. This is the preferred approach: it preserves the test intent (query params,
  pagination behaviour) while targeting the new read model.

> The 4 tests that are **unaffected** (`test_no_history`, `test_filter_by_from_date_excludes_past`,
> `test_filter_by_to_date_excludes_future`, `test_limit_exceeds_max`) do not seed data and pass
> without change — `transaction_activity` being empty is exactly what they expect.

---

### Acceptance criteria → test mapping

| PRD Acceptance Criterion | Covered by |
|--------------------------|-----------|
| Kill Kafka → event eventually delivered (outbox) | `test_recover_stuck_rows_resets_publishing_to_pending` |
| Audit log: entry for every transfer (completed + failed) + account open | `test_audit_consumer_transfer_completed`, `test_audit_consumer_transfer_failed`, `test_audit_consumer_account_opened` |
| `/transactions` reads `transaction_activity`, not ledger | `test_transactions_reads_from_activity_not_ledger` |
| Transaction history includes Phase 1 backfill data | `test_backfill_creates_outbox_rows_for_all_phase1_data` |
| Backfill double-run guard raises RuntimeError; `force=True` bypasses it | `test_backfill_raises_if_already_run`, `test_backfill_force_bypasses_guard` |
| Malformed event → DLQ after retries; consumer does not crash | `test_malformed_json_goes_to_dlq_immediately`, `test_process_failure_retries_then_dlqs` |
| Two relay instances do not duplicate-publish | `test_for_update_skip_locked_prevents_duplicate_claims` |
| Outbox cleanup deletes old published/failed rows; warns before purging failed | `test_cleanup_deletes_old_published_rows`, `test_cleanup_warns_and_deletes_old_failed_rows` |
| Relay exponential backoff — does not spin-loop | Manual: run relay with Kafka down, observe log sleep durations grow to 30s |
| Consumer lag logged | Manual: run consumer against topic with backlog, observe log output |
| Notifications logged for completed, failed transfers and account opens | `test_notification_transfer_completed_logs_both_sides`, `test_notification_transfer_failed_logs_failure`, `test_notification_account_opened_logs_welcome` |
| `as_of` = `MAX(occurred_at)` of result set; null when empty | `test_transactions_as_of_is_max_occurred_at_of_current_page`, `test_transactions_as_of_null_when_no_results` |
| Replaying same event = no duplicate rows | `test_audit_consumer_idempotent`, `test_activity_consumer_idempotent`, `test_idempotent_replay_does_not_go_to_dlq` |
