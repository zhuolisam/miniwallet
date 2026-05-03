"""Minimal audit consumer — Week 6 (intentionally fragile).

Subscribes to `transfer.events`, deserialises each message with json.loads,
and INSERTs a row into the `audit_events` table.

No retry, no DLQ, no BaseConsumer abstraction — this is the bare Kafka consumer
API so you can focus on two things:
  1. How AIOKafkaConsumer works (start, async-for loop, manual commit).
  2. The dual-write problem: if this process crashes between the DB INSERT and
     the Kafka offset commit, the message will be redelivered — and if it crashes
     between the producer's db.commit() and send(), the event is lost entirely.

Week 9 will replace this with BaseConsumer (retry + DLQ). For now, keep it minimal.

Run inside Docker:
    (handled by docker-compose.yml — service `audit-consumer`)

Run on host (for local testing):
    KAFKA_BOOTSTRAP_SERVERS=localhost:29092 uv run python -m workers.audit_consumer
"""

import asyncio
import json
import logging
import uuid
from datetime import datetime

from aiokafka import AIOKafkaConsumer
from app.config import settings
from app.database import db_factory
from app.events.schemas import parse_event
from sqlalchemy.exc import IntegrityError

from app.models.audit_event import AuditEvent  # noqa: F401 — imported so the model is registered

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("audit_consumer")

TOPIC = "transfer.events"
GROUP_ID = "minibank.audit-consumer"

# Explicit resource_type mapping — more readable than a substring heuristic.
# A substring check like `"transfer" in event_type` would misclassify future
# types such as "account_transfer_limit.updated".
_RESOURCE_TYPE: dict[str, str] = {
    "transfer.completed": "transfer",
    "transfer.failed":    "transfer",
    "account.opened":     "account",
    "seed.completed":     "account",
}


async def process(event: dict, session_factory=None) -> None:
    """Persist a single event to the audit_events table.

    Separated from the consumer loop so tests can call this directly
    without needing a running Kafka broker.

    Args:
        event: Event envelope dict (raw JSON-deserialized).
        session_factory: Optional async session factory. Defaults to the
            module-level db_factory (app.database). Tests pass in a factory
            bound to the testcontainer Postgres so they don't hit the
            Docker-internal ``postgres`` hostname.

    Idempotency: audit_events.event_id has a UNIQUE constraint.
    A duplicate delivery (e.g. consumer restart before offset commit)
    raises IntegrityError — catch it and treat as a no-op.

    Validation: parse_event() validates the envelope and payload against
    typed Pydantic models. A structurally invalid event raises ValidationError,
    which propagates to the caller (Week 9: BaseConsumer retries → DLQ).
    """
    _factory = session_factory or db_factory

    # Validate envelope + payload against typed schemas.
    # ValidationError propagates — a structurally broken event won't pass on retry.
    envelope, payload = parse_event(event)

    # Extract resource_id from the typed payload. Transfer events have transfer_id,
    # account/seed events have account_id. getattr with None fallback handles all cases
    # without fragile substring checks on event_type.
    resource_id_str = getattr(payload, "transfer_id", None) or getattr(payload, "account_id", None)
    resource_id = uuid.UUID(resource_id_str) if resource_id_str else None

    try:
        async with _factory() as db:
            async with db.begin():
                db.add(AuditEvent(
                    id=uuid.uuid4(),
                    event_id=uuid.UUID(envelope.event_id),
                    event_type=envelope.event_type,
                    actor_id=uuid.UUID(envelope.actor_id) if envelope.actor_id else None,
                    resource_id=resource_id,
                    resource_type=_RESOURCE_TYPE.get(envelope.event_type),
                    payload=event,
                    occurred_at=datetime.fromisoformat(envelope.occurred_at),
                ))
    except IntegrityError:
        # UNIQUE(event_id) violated — already processed, idempotent no-op.
        # Must catch here, not let it propagate to BaseConsumer (Week 9),
        # which would treat it as a processing failure and retry/DLQ.
        logger.warning("Duplicate event_id=%s detected. Skipping.", envelope.event_id)


async def run() -> None:
    """Main consumer loop.

    Creates an AIOKafkaConsumer, subscribes to TOPIC, and processes messages
    in a loop until the process is killed. Each message is processed by
    `process()`, then the offset is manually committed.

    Manual offset commit (enable_auto_commit=False) is critical for at-least-once
    delivery: we only advance the offset after the DB write succeeds. If the
    process crashes between the INSERT and this commit(), Kafka will redeliver
    the message on restart — and the UNIQUE constraint catches the duplicate.
    """

    # 1. Create an AIOKafkaConsumer:
    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=GROUP_ID,
        enable_auto_commit=False,   # manual commit only — see docstring above
        auto_offset_reset="earliest",  # replay from start if no committed offset yet

        )
    # 2. Start the consumer:
    await consumer.start()
    logger.info("Audit consumer started. Listening on topic=%s group=%s", TOPIC, GROUP_ID)

    # 3. Wrap the loop in try/finally to ensure consumer.stop() is always called:
    try:
        async for msg in consumer:
            event = json.loads(msg.value)
            await process(event)
            logger.info("Received %s  event_id=%s  offset=%s",
                            event.get("event_type"), event.get("event_id"), msg.offset)
            await consumer.commit()
            logger.info("Committed offset=%s  event_id=%s", msg.offset, event.get("event_id"))
    finally:
        await consumer.stop()
        logger.info("Audit consumer stopped.")

if __name__ == "__main__":
    asyncio.run(run())
