"""Event publisher — the ONLY way to write to the outbox.

Generates a fresh event_id, constructs the full event envelope, and inserts
an OutboxRow in the caller's current DB transaction. The caller commits.

Callers pass a Pydantic payload model, not a raw dict. The model's .model_dump()
guarantees all values are JSON-serializable primitives — no manual json.dumps()
validation needed.

Usage (inside a service function's existing transaction):
    from app.events.publisher import publish_event
    from app.events.schemas import TransferCompletedPayload

    publish_event(db, "transfer.events", "transfer.completed", TransferCompletedPayload(
        transfer_id=str(transfer_record.id),
        ...
    ), actor_id=actor_user_id)
    await db.commit()  # outbox row + domain row committed atomically
"""

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

    Args:
        db: The async session from the caller's transaction.
        topic: Kafka topic name (e.g. "transfer.events", "account.events").
        event_type: Domain event type string (e.g. "transfer.completed").
        payload: A Pydantic model instance — NOT a raw dict.
        actor_id: The user who initiated the action (from current_user.id).
        event_id: Optional deterministic event ID. If None, generates uuid4().
                  Used by backfill to produce idempotent events (uuid5 from source entity).
    """
    envelope = {
        "event_id": event_id or str(uuid.uuid4()),
        "event_type": event_type,
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "version": "1",
        "actor_id": str(actor_id) if actor_id else None,
        "payload": payload.model_dump(),
    }
    db.add(OutboxRow(
        topic=topic,
        event_type=event_type,
        payload=envelope,
    ))
