"""Activity consumer — builds the CQRS read model (transaction_activity).

Subscribes to transfer.events and account.events. For each event:
- transfer.completed → TWO rows: debit (sender) + credit (receiver)
- seed.completed    → ONE row: credit (account owner)
- transfer.failed   → no row (failed transfers never moved money)
- account.opened    → no row (informational, not a transaction)

Idempotent via UNIQUE(event_id, account_id). Replaying an event
produces no duplicates — IntegrityError is caught and silenced.

Run inside Docker:
    (handled by docker-compose.yml — service `activity-consumer`)

Run on host:
    KAFKA_BOOTSTRAP_SERVERS=localhost:29092 uv run python -m workers.activity_consumer
"""

import asyncio
import logging
from datetime import datetime
from decimal import Decimal
import uuid

from sqlalchemy.exc import IntegrityError

from consumers.consumer_base import BaseConsumer
from app.database import db_factory
from app.events.schemas import (
    parse_event,
    TransferCompletedPayload,
    SeedCompletedPayload,
)
from app.models.transaction_activity import TransactionActivity

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("activity_consumer")


class ActivityConsumer(BaseConsumer):
    group_id = "minibank.activity-consumer"
    topics = ["transfer.events", "account.events"]

    async def process(self, event: dict) -> None:
        """Build transaction_activity rows from a single event.

        Args:
            event: Raw event envelope dict (deserialized from Kafka message).

        Idempotency: UNIQUE(event_id, account_id) constraint. On duplicate,
        IntegrityError is caught here — NOT propagated to BaseConsumer.
        """
        # TODO:student — Implement the activity consumer process() logic:
        #
        # 1. Parse the event using parse_event(event) → (envelope, payload)
        #    This validates structure against typed schemas.
        envelope, payload = parse_event(event)


        #
        # 2. Open a DB session: async with self.db_factory() as db:
        #                            async with db.begin():
        #
        # 3. Dispatch based on payload type:
        try:
            async with self.db_factory() as db:
                async with db.begin():
                    if isinstance(payload, TransferCompletedPayload):
                        # Debit row for sender
                        debit_activity = TransactionActivity(
                            event_id=uuid.UUID(envelope.event_id),
                            account_id=uuid.UUID(payload.from_account_id),
                            direction="debit",
                            amount=Decimal(payload.amount),
                            currency=getattr(payload, "currency", "USD"),
                            entry_type=payload.entry_type,
                            reference_id=uuid.UUID(payload.transfer_id),
                            occurred_at=datetime.fromisoformat(envelope.occurred_at),
                        )
                        db.add(debit_activity)

                        # Credit row for receiver
                        credit_activity = TransactionActivity(
                            event_id=uuid.UUID(envelope.event_id),
                            account_id=uuid.UUID(payload.to_account_id),
                            direction="credit",
                            amount=Decimal(payload.amount),
                            currency=getattr(payload, "currency", "USD"),
                            entry_type=payload.entry_type,
                            reference_id=uuid.UUID(payload.transfer_id),
                            occurred_at=datetime.fromisoformat(envelope.occurred_at),
                        )
                        db.add(credit_activity)

                    elif isinstance(payload, SeedCompletedPayload):
                        # Credit row for seed
                        seed_activity = TransactionActivity(
                            event_id=uuid.UUID(envelope.event_id),
                            account_id=uuid.UUID(payload.account_id),
                            direction="credit",
                            amount=Decimal(payload.amount),
                            currency=getattr(payload, "currency", "USD"),
                            entry_type=payload.entry_type,
                            reference_id=None,
                            occurred_at=datetime.fromisoformat(envelope.occurred_at),
                        )
                        db.add(seed_activity)

                    elif envelope.event_type == "transfer.failed":
                        # No activity row for failed transfers
                        pass

                    else:
                        # No activity row for other events (e.g., account.opened)
                        pass
        except IntegrityError:
            # Duplicate event (same event_id, account_id) — ignore
            logger.warning(
                f"Duplicate event detected (event_id={envelope.event_id}) — ignoring."
            )

if __name__ == "__main__":
    asyncio.run(ActivityConsumer(db_factory).run())
