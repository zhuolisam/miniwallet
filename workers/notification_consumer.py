"""Notification consumer — logs simulated notifications to stdout.

Subscribes to transfer.events and account.events. No DB writes — stateless.
In production this would dispatch to push notification / email services.
For Phase 2, it simply logs what would be sent.

Expected output per event type:
- transfer.completed → two log lines (sender + receiver)
- transfer.failed   → one log line (sender, with failure_code)
- account.opened    → one log line (welcome message)
- seed.completed    → no log (no notification defined)

Run inside Docker:
    (handled by docker-compose.yml — service `notification-consumer`)

Run on host:
    KAFKA_BOOTSTRAP_SERVERS=localhost:29092 uv run python -m workers.notification_consumer
"""

import asyncio
import logging

from consumers.consumer_base import BaseConsumer
from app.database import db_factory
from app.events.schemas import (
    SeedCompletedPayload,
    parse_event,
    TransferCompletedPayload,
    TransferFailedPayload,
    AccountOpenedPayload,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("notification_consumer")


class NotificationConsumer(BaseConsumer):
    group_id = "minibank.notification-consumer"
    topics = ["transfer.events", "account.events"]

    async def process(self, event: dict) -> None:
        """Log a simulated notification based on event type.

        Args:
            event: Raw event envelope dict (deserialized from Kafka message).

        No DB interaction — this consumer is stateless. db_factory is unused
        but accepted via BaseConsumer.__init__ for interface consistency.
        """
        # Notification consumer process() logic:
        #
        # 1. Parse the event: envelope, payload = parse_event(event)
        envelope, payload = parse_event(event)
        #
        # 2. Use match/case (or isinstance checks) to dispatch:

        match payload:
            case TransferCompletedPayload():
                logger.info("NOTIFY [sender] %s: You sent %s to %s", payload.from_account_id, payload.amount, payload.to_account_id)
                logger.info("NOTIFY [receiver] %s: You received %s from %s", payload.to_account_id, payload.amount, payload.from_account_id)
        
            case TransferFailedPayload():
                logger.info("NOTIFY [sender] %s: Your transfer of %s to %s failed: %s", payload.from_account_id, payload.amount, payload.to_account_id, payload.failure_code)

            case AccountOpenedPayload():
               logger.info("NOTIFY [user] %s: Your account is now active", payload.user_id)

            case SeedCompletedPayload():
                pass  # no notification defined — intentional no-op
            
            case _:
                logger.warning("Received unrecognized event type: %s", envelope.event_type)

        # Key details:
        #   - This consumer makes NO database calls. It only logs.
        #   - Tests verify log output via caplog — use logger.info(), not print()
        #   - The exact log format matters for tests: include account_id/user_id and amount/failure_code


if __name__ == "__main__":
    asyncio.run(NotificationConsumer(db_factory).run())
