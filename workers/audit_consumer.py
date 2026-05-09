"""Audit consumer — persists every event to audit_events for compliance.

Upgraded from Week 6's minimal consumer to use BaseConsumer (retry + DLQ).
Subscribes to BOTH transfer.events and account.events — the audit trail
must capture every state change in the system.

Idempotent via UNIQUE(event_id) on audit_events. Replaying an event
produces no duplicate rows — IntegrityError is caught and silenced.

Run inside Docker:
    (handled by docker-compose.yml — service `audit-consumer`)

Run on host:
    KAFKA_BOOTSTRAP_SERVERS=localhost:29092 uv run python -m workers.audit_consumer
"""

import asyncio
import logging
import uuid
from datetime import datetime

from sqlalchemy.exc import IntegrityError

from consumers.consumer_base import BaseConsumer
from app.database import db_factory
from app.events.schemas import parse_event
from app.models.audit_event import AuditEvent

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("audit_consumer")

_RESOURCE_TYPE: dict[str, str] = {
    "transfer.completed": "transfer",
    "transfer.failed":    "transfer",
    "account.opened":     "account",
    "seed.completed":     "account",
}


class AuditConsumer(BaseConsumer):
    group_id = "minibank.audit-consumer"
    topics = ["transfer.events", "account.events"]

    async def process(self, event: dict) -> None:
        """Persist a single event to the audit_events table.

        Idempotency: audit_events.event_id has a UNIQUE constraint.
        A duplicate raises IntegrityError — caught here as a no-op.
        Must NOT propagate to BaseConsumer, which would treat it as failure.
        """
        envelope, payload = parse_event(event)

        resource_id_str = getattr(payload, "transfer_id", None) or getattr(payload, "account_id", None)
        resource_id = uuid.UUID(resource_id_str) if resource_id_str else None

        try:
            async with self.db_factory() as db:
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
            logger.warning("Duplicate event_id=%s detected. Skipping.", envelope.event_id)


if __name__ == "__main__":
    asyncio.run(AuditConsumer(db_factory).run())
