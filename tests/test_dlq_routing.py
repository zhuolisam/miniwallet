"""Tests for BaseConsumer DLQ routing — handle_message() called directly.

Tests exercise deserialization errors, retry logic, and DLQ routing without
a running consumer loop. Uses AsyncMock for producer and consumer.
"""

import json
import uuid

import pytest
from unittest.mock import AsyncMock, MagicMock

from sqlalchemy import func, select
from app.models.audit_event import AuditEvent
from workers.audit_consumer import AuditConsumer
from tests.conftest import make_event


def make_fake_message(value: bytes, retry_count: int = 0) -> MagicMock:
    """Build a fake aiokafka Message for use with handle_message()."""
    msg = MagicMock()
    msg.value = value
    msg.headers = [(b"x-retry-count", str(retry_count).encode())] if retry_count else []
    msg.topic = "transfer.events"
    msg.offset = 0
    return msg


@pytest.mark.asyncio
async def test_malformed_json_goes_to_dlq_immediately(consumer_db_factory):
    """Malformed JSON bypasses retry — sent straight to DLQ on first attempt."""
    consumer_instance = AuditConsumer(db_factory=consumer_db_factory)
    mock_producer = AsyncMock()
    mock_consumer = AsyncMock()

    fake_msg = make_fake_message(b"this is not valid json")

    # Call consumer_instance.handle_message(fake_msg, mock_producer, mock_consumer)

    await consumer_instance.handle_message(fake_msg, mock_producer, mock_consumer)

    # Verify:
    #   - producer.send_and_wait called once with DLQ topic "minibank.audit-consumer.dlq"
    #   - value is the original malformed bytes
    #   - consumer.commit() called once
    #   - No rows in audit_events
    assert mock_producer.send_and_wait.call_count == 1
    args, kwargs = mock_producer.send_and_wait.call_args
    assert args[0] == "minibank.audit-consumer.dlq"
    assert args[1] == b"this is not valid json"
    assert mock_consumer.commit.call_count == 1

    async with consumer_db_factory() as db:
        result = await db.execute(select(func.count()).select_from(AuditEvent))
        count = result.scalar_one()
        assert count == 0



@pytest.mark.asyncio
async def test_process_failure_retries_then_dlqs(consumer_db_factory, monkeypatch):
    """process() failure: re-published with incrementing x-retry-count, DLQ'd after 3."""
    consumer_instance = AuditConsumer(db_factory=consumer_db_factory)
    monkeypatch.setattr(consumer_instance, "process", AsyncMock(side_effect=RuntimeError("forced")))

    mock_producer = AsyncMock()
    mock_consumer = AsyncMock()
    event = make_event("transfer.completed", {
        "transfer_id": str(uuid.uuid4()),
        "from_account_id": str(uuid.uuid4()),
        "to_account_id": str(uuid.uuid4()),
        "amount": "100.0000",
        "currency": "USD",
        "entry_type": "transfer",
        "idempotency_key": "key",
    })
    encoded = json.dumps(event).encode()

    # TODO:student — For retry_count in 0, 1, 2:
    #   - Call handle_message with that retry_count
    #   - Verify producer.send_and_wait sends to SOURCE topic (not DLQ)
    #   - Verify x-retry-count header is incremented
    #   - Reset mocks between iterations

    for retry_count in range(3):
        fake_msg = make_fake_message(encoded, retry_count=retry_count)
        await consumer_instance.handle_message(fake_msg, mock_producer, mock_consumer)

        assert mock_producer.send_and_wait.call_count == 1
        args, kwargs = mock_producer.send_and_wait.call_args
        assert args[0] == "transfer.events"
        assert args[1] == encoded
        headers = dict(kwargs["headers"])
        assert headers[b"x-retry-count"] == str(retry_count + 1).encode()
        assert mock_consumer.commit.call_count == 1

        mock_producer.reset_mock()
        mock_consumer.reset_mock()
    
    # Then for retry_count=3:
    #   - Call handle_message
    #   - Verify producer.send_and_wait sends to DLQ topic
    #   - Verify consumer.commit() called
    await consumer_instance.handle_message(make_fake_message(encoded, retry_count=3), mock_producer, mock_consumer)
    assert mock_producer.send_and_wait.call_count == 1
    args, kwargs = mock_producer.send_and_wait.call_args
    assert args[0] == "minibank.audit-consumer.dlq"
    assert args[1] == encoded
    assert mock_consumer.commit.call_count == 1


@pytest.mark.asyncio
async def test_idempotent_replay_does_not_go_to_dlq(consumer_db_factory):
    """IntegrityError (duplicate event_id) is caught in process() — not retried or DLQ'd."""
    consumer_instance = AuditConsumer(db_factory=consumer_db_factory)
    mock_producer = AsyncMock()
    mock_consumer = AsyncMock()

    event = make_event("transfer.completed", {
        "transfer_id": str(uuid.uuid4()),
        "from_account_id": str(uuid.uuid4()),
        "to_account_id": str(uuid.uuid4()),
        "amount": "100.0000",
        "currency": "USD",
        "entry_type": "transfer",
        "idempotency_key": "key",
    })
    encoded = json.dumps(event).encode()

    # Call handle_message twice with the same event.
    fake_msg = make_fake_message(encoded)
    await consumer_instance.handle_message(fake_msg, mock_producer, mock_consumer)
    await consumer_instance.handle_message(fake_msg, mock_producer, mock_consumer)

    # Verify:
    #   - producer.send_and_wait NEVER called (no DLQ, no retry)
    assert mock_producer.send_and_wait.call_count == 0
    #   - consumer.commit() called twice (both attempts consumed)
    assert mock_consumer.commit.call_count == 2
    #   - No exceptions raised (IntegrityError handled in process())
    #   - Only one row in audit_events (idempotent replay doesn't create duplicate)
    async with consumer_db_factory() as db:
        result = await db.execute(select(func.count()).select_from(AuditEvent))
        count = result.scalar_one()
        assert count == 1   
