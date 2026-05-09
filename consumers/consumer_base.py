"""BaseConsumer — shared consumer infrastructure with retry and DLQ routing.

All Kafka consumers extend this class. It handles:
- Manual offset commit (at-least-once delivery)
- Retry via re-publish with incremented x-retry-count header
- Dead-letter routing after 3 retries to the consumer's own DLQ topic
- Consumer lag logging (approximate, based on message timestamp)

Subclasses define:
- group_id: str — Kafka consumer group (e.g. "minibank.audit-consumer")
- topics: list[str] — topics to subscribe to
- process(event: dict) — business logic for one event

The retry/DLQ machinery lives in handle_message(), which is extracted from
run() for testability. Tests call handle_message() directly with a mock
producer and a fake message object — no running consumer loop needed.
"""

import json
import logging
from datetime import datetime, timezone

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from app.config import settings

logger = logging.getLogger(__name__)

MAX_RETRIES = 3
LAG_LOG_INTERVAL = 100


class BaseConsumer:
    """Base class for all Kafka event consumers.

    Subclasses MUST define:
        group_id: str
        topics: list[str]
        async def process(self, event: dict) -> None
    """

    group_id: str
    topics: list[str]

    def __init__(self, db_factory):
        self.db_factory = db_factory

    async def process(self, event: dict) -> None:
        """Process a single deserialized event. Subclass must implement.

        Raise any exception to trigger retry/DLQ routing.
        For idempotent consumers: catch IntegrityError internally and return
        normally — do NOT let it propagate, or BaseConsumer will retry/DLQ it.
        """
        raise NotImplementedError

    async def handle_message(self, message, producer, consumer) -> None:
        """Process one Kafka message with retry and DLQ routing.

        Extracted from run() so tests can exercise deserialization errors,
        retry logic, and DLQ routing without a running consumer loop.

        Args:
            message: aiokafka ConsumerRecord (or mock with .value, .headers, .topic, .offset)
            producer: AIOKafkaProducer (or AsyncMock) for retry/DLQ publishing
            consumer: AIOKafkaConsumer (or AsyncMock) for offset commit
        """
        # TODO:student — Implement the full handle_message logic:
        #
        # 1. Extract retry count from message headers.
        #    Headers are [(bytes, bytes)]. Key is b"x-retry-count" (bytes!).
        #    Default to 0 if header is missing.
        retry_count = 0
        for key, value in message.headers:
            if key == b"x-retry-count":
                retry_count = int(value.decode())
                break
        # 2. Attempt JSON deserialization (json.loads(message.value)).
        try:
            event = json.loads(message.value)
        except json.JSONDecodeError:
            dlq_topic = f"{self.group_id}.dlq"
            await producer.send_and_wait(dlq_topic, message.value)
            await consumer.commit()
            logger.error(
                "JSON deserialization failed for message at offset %s in topic %s. "
                "Sent to DLQ topic %s.",
                message.offset, message.topic, dlq_topic,
            )
            return

        # 3. Call await self.process(event) inside a try/except Exception.
        #    On success: await consumer.commit()
        #    On failure:
        #      - If retry_count >= MAX_RETRIES (3): send to DLQ, commit
        #      - Else: re-publish to message.topic with incremented x-retry-count
        #        header, then commit (original offset consumed, retry is new message)
    
        try:
            await self.process(event)
            await consumer.commit()
        except Exception as e:
            logger.error(
                "Error processing message at offset %s in topic %s: %s",
                message.offset, message.topic, str(e),
            )
            if retry_count >= MAX_RETRIES:
                dlq_topic = f"{self.group_id}.dlq"
                await producer.send_and_wait(dlq_topic, message.value)
                await consumer.commit()
                logger.error(
                    "Max retries exceeded for message at offset %s in topic %s. "
                    "Sent to DLQ topic %s.",
                    message.offset, message.topic, dlq_topic,
                )
            else:
                new_headers = [
                    (key, value) for key, value in message.headers if key != b"x-retry-count"
                ] + [(b"x-retry-count", str(retry_count + 1).encode())]
                await producer.send_and_wait(
                    message.topic, message.value, headers=new_headers
                )
                await consumer.commit()
                logger.info(
                    "Scheduled retry #%d for message at offset %s in topic %s.",
                    retry_count + 1, message.offset, message.topic,
                )

        # Key details:
        #   - Headers are bytes: b"x-retry-count", not "x-retry-count"
        #   - Use send_and_wait() for both DLQ and retry (at-least-once guarantee)
        #   - Always commit after DLQ or retry publish (consumer makes forward progress)

    async def run(self) -> None:
        """Main consumer loop — start consumer + producer, process messages forever.

        This method is complete (teacher-provided). It relies on handle_message()
        which you implement above.
        """
        consumer = AIOKafkaConsumer(
            *self.topics,
            group_id=self.group_id,
            auto_offset_reset="latest",
            enable_auto_commit=False,
            bootstrap_servers=settings.kafka_bootstrap_servers,
        )
        producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap_servers)
        messages_since_lag_log = 0

        try:
            await consumer.start()
            await producer.start()
            logger.info(
                "Consumer started: group=%s topics=%s",
                self.group_id, self.topics,
            )
            # Infinite async iterator — polls Kafka continuously, no while True needed
            async for message in consumer:
                messages_since_lag_log += 1
                if messages_since_lag_log >= LAG_LOG_INTERVAL:
                    msg_dt = datetime.fromtimestamp(
                        message.timestamp / 1000, tz=timezone.utc
                    )
                    lag_seconds = (
                        datetime.now(timezone.utc) - msg_dt
                    ).total_seconds()
                    logger.info(
                        "consumer_lag group=%s topic=%s partition=%s "
                        "offset=%s approx_lag_seconds=%.1f",
                        self.group_id,
                        message.topic,
                        message.partition,
                        message.offset,
                        lag_seconds,
                    )
                    messages_since_lag_log = 0

                await self.handle_message(message, producer, consumer)
        finally:
            await consumer.stop()
            await producer.stop()
            logger.info("Consumer stopped: group=%s", self.group_id)
