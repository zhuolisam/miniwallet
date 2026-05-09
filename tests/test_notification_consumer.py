"""Tests for the notification consumer — verifies correct log output per event type.

NotificationConsumer makes no DB calls — db_factory=None is correct in tests.
Tests verify log output via caplog.
"""

import logging
import uuid

import pytest

from workers.notification_consumer import NotificationConsumer
from tests.conftest import make_event


@pytest.mark.asyncio
async def test_notification_transfer_completed_logs_both_sides(caplog):
    """transfer.completed → two log lines (sender + receiver)."""
    consumer = NotificationConsumer(db_factory=None)
    event = make_event("transfer.completed", {
        "transfer_id": str(uuid.uuid4()),
        "from_account_id": "acct-A",
        "to_account_id": "acct-B",
        "amount": "75.0000",
        "currency": "USD",
        "entry_type": "transfer",
        "idempotency_key": "test-key",
    })

    # Call consumer.process(event) with caplog.at_level(logging.INFO)
    with caplog.at_level(logging.INFO):
        await consumer.process(event)
        assert len(caplog.records) == 2, f"Expected 2 log records, got {len(caplog.records)}: {[r.message for r in caplog.records]}"
        assert any("acct-A" in r.message and "75.0000" in r.message and "acct-B" in r.message for r in caplog.records), "Sender log missing or incorrect"
        assert any("acct-B" in r.message and "75.0000" in r.message and "acct-A" in r.message for r in caplog.records), "Receiver log missing or incorrect"

@pytest.mark.asyncio
async def test_notification_transfer_failed_logs_failure(caplog):
    """transfer.failed → one log line with failure_code."""
    consumer = NotificationConsumer(db_factory=None)
    event = make_event("transfer.failed", {
        "transfer_id": str(uuid.uuid4()),
        "from_account_id": "acct-A",
        "to_account_id": "acct-B",
        "amount": "50.0000",
        "currency": "USD",
        "failure_code": "INSUFFICIENT_BALANCE",
        "entry_type": "transfer",
        "idempotency_key": "key",
    })

    with caplog.at_level(logging.INFO):
        await consumer.process(event)
        assert len(caplog.records) == 1, f"Expected 1 log record, got {len(caplog.records)}: {[r.message for r in caplog.records]}"
        assert "acct-A" in caplog.records[0].message and "INSUFFICIENT_BALANCE" in caplog.records[0].message, "Failure log missing or incorrect"


@pytest.mark.asyncio
async def test_notification_account_opened_logs_welcome(caplog):
    """account.opened → one welcome log line."""
    consumer = NotificationConsumer(db_factory=None)
    event = make_event("account.opened", {
        "account_id": str(uuid.uuid4()),
        "user_id": "user-X",
        "status": "active",
    })

    with caplog.at_level(logging.INFO):
        await consumer.process(event)
        assert len(caplog.records) == 1, f"Expected 1 log record, got {len(caplog.records)}: {[r.message for r in caplog.records]}"
        assert "user-X" in caplog.records[0].message and "active" in caplog.records[0].message, "Welcome log missing or incorrect"


@pytest.mark.asyncio
async def test_notification_seed_completed_logs_nothing(caplog):
    """seed.completed is a no-op for notifications — no log output."""
    consumer = NotificationConsumer(db_factory=None)
    event = make_event("seed.completed", {
        "account_id": str(uuid.uuid4()),
        "user_id": str(uuid.uuid4()),
        "amount": "1000.0000",
        "currency": "USD",
        "entry_type": "seed",
    })

    with caplog.at_level(logging.INFO):
        await consumer.process(event)
        assert len(caplog.records) == 0, f"Expected 0 log records, got {len(caplog.records)}: {[r.message for r in caplog.records]}"
