"""Tests for the audit consumer.

Testing strategy (from SYSTEM-DESIGN.md §16):
  - Call `process()` directly with a real DB — no Kafka container needed.
  - This tests all DB logic, field mapping, and idempotency without the
    complexity of a full consumer loop.
  - The consumer loop itself (run()) is tested manually via docker-compose.

Each test uses `consumer_db_factory` (a session factory against the test
Postgres container) and the `make_event()` helper from conftest.py.
"""

import uuid

from sqlalchemy import select

from app.models.audit_event import AuditEvent
from tests.conftest import make_event
from workers.audit_consumer import AuditConsumer


async def test_audit_persists_transfer_completed(consumer_db_factory):
    """transfer.completed → one audit_events row with correct fields."""
    consumer = AuditConsumer(db_factory=consumer_db_factory)
    event = make_event("transfer.completed", {
        "transfer_id":     str(uuid.uuid4()),
        "from_account_id": str(uuid.uuid4()),
        "to_account_id":   str(uuid.uuid4()),
        "amount":          "100.0000",
        "currency":        "USD",
        "entry_type":      "transfer",
        "idempotency_key": "test-key",
    })

    await consumer.process(event)

    async with consumer_db_factory() as db:
        row = (await db.execute(select(AuditEvent).where(AuditEvent.event_id == uuid.UUID(event["event_id"])))).scalar_one()

    assert row is not None, "Expected one audit_events row, got zero"
    assert row.event_type == "transfer.completed"
    assert row.resource_type == "transfer"
    assert row.actor_id is not None
    assert row.payload["payload"]["transfer_id"] == event["payload"]["transfer_id"]


async def test_audit_persists_transfer_failed(consumer_db_factory):
    """transfer.failed → one audit_events row with event_type and resource_type set."""
    consumer = AuditConsumer(db_factory=consumer_db_factory)
    event = make_event("transfer.failed", {
        "transfer_id":     str(uuid.uuid4()),
        "from_account_id": str(uuid.uuid4()),
        "to_account_id":   str(uuid.uuid4()),
        "amount":          "100.0000",
        "currency":        "USD",
        "failure_code":    "INSUFFICIENT_BALANCE",
        "entry_type":      "transfer",
        "idempotency_key": "test-key-1",
    })

    await consumer.process(event)

    async with consumer_db_factory() as db:
        row = (await db.execute(select(AuditEvent).where(AuditEvent.event_id == uuid.UUID(event["event_id"])))).scalar_one()

    assert row is not None, "Expected one audit_events row, got zero"
    assert row.event_type == "transfer.failed"
    assert row.resource_type == "transfer"
    assert row.actor_id is not None
    assert row.payload["payload"]["transfer_id"] == event["payload"]["transfer_id"]


async def test_audit_persists_account_opened(consumer_db_factory):
    """account.opened → one audit_events row with resource_type == 'account'."""
    consumer = AuditConsumer(db_factory=consumer_db_factory)
    account_id = str(uuid.uuid4())
    event = make_event("account.opened", {
        "account_id": account_id,
        "user_id":    str(uuid.uuid4()),
        "status":     "active",
    })

    await consumer.process(event)

    async with consumer_db_factory() as db:
        row = (await db.execute(select(AuditEvent).where(AuditEvent.event_id == uuid.UUID(event["event_id"])))).scalar_one()

    assert row.event_type == "account.opened"
    assert row.resource_type == "account"
    assert row.actor_id is not None
    assert row.payload["payload"]["account_id"] == event["payload"]["account_id"]


async def test_audit_persists_seed_completed(consumer_db_factory):
    """seed.completed → one audit_events row (the audit log captures ALL event types)."""
    consumer = AuditConsumer(db_factory=consumer_db_factory)
    account_id = str(uuid.uuid4())
    event = make_event("seed.completed", {
        "account_id": account_id,
        "user_id":    str(uuid.uuid4()),
        "amount":     "1000.0000",
        "currency":   "USD",
        "entry_type": "seed",
    })

    await consumer.process(event)

    async with consumer_db_factory() as db:
        row = (await db.execute(select(AuditEvent).where(AuditEvent.event_id == uuid.UUID(event["event_id"])))).scalar_one()

    assert row.event_type == "seed.completed"
    assert row.resource_type == "account"
    assert row.actor_id is not None
    assert row.payload["payload"]["account_id"] == event["payload"]["account_id"]


async def test_audit_idempotent_on_duplicate_event_id(consumer_db_factory):
    """Processing the same event twice inserts only one row.

    The UNIQUE constraint on audit_events.event_id enforces this at the DB level.
    process() must catch IntegrityError and treat it as a no-op — NOT re-raise it,
    which would be misinterpreted as a processing failure by BaseConsumer.
    """
    consumer = AuditConsumer(db_factory=consumer_db_factory)
    event = make_event("transfer.completed", {
        "transfer_id":     str(uuid.uuid4()),
        "from_account_id": str(uuid.uuid4()),
        "to_account_id":   str(uuid.uuid4()),
        "amount":          "50.0000",
        "currency":        "USD",
        "entry_type":      "transfer",
        "idempotency_key": "dup-key",
    })

    await consumer.process(event)
    await consumer.process(event)  # duplicate — should NOT raise

    async with consumer_db_factory() as db:
        result = await db.execute(
            select(AuditEvent).where(AuditEvent.event_id == uuid.UUID(event["event_id"]))
        )
        rows = result.scalars().all()
    assert len(rows) == 1


async def test_audit_stores_correct_resource_type_for_all_event_types(consumer_db_factory):
    """Verify the _RESOURCE_TYPE mapping produces correct resource_type for each event."""
    consumer = AuditConsumer(db_factory=consumer_db_factory)
    cases = [
        ("transfer.completed", {"transfer_id": str(uuid.uuid4()), "from_account_id": str(uuid.uuid4()),
                                 "to_account_id": str(uuid.uuid4()), "amount": "10.0000", "currency": "USD",
                                 "entry_type": "transfer", "idempotency_key": "k1"}, "transfer"),
        ("transfer.failed",    {"transfer_id": str(uuid.uuid4()), "from_account_id": str(uuid.uuid4()),
                                 "to_account_id": str(uuid.uuid4()), "amount": "10.0000", "currency": "USD",
                                 "failure_code": "INSUFFICIENT_BALANCE",
                                 "entry_type": "transfer", "idempotency_key": "test-key"}, "transfer"),
        ("account.opened",     {"account_id": str(uuid.uuid4()), "user_id": str(uuid.uuid4()),
                                 "status": "active"}, "account"),
        ("seed.completed",     {"account_id": str(uuid.uuid4()), "user_id": str(uuid.uuid4()),
                                 "amount": "1000.0000", "currency": "USD", "entry_type": "seed"}, "account"),
    ]

    for event_type, payload, expected_resource_type in cases:
        event = make_event(event_type, payload)
        await consumer.process(event)
        async with consumer_db_factory() as db:
            row = (await db.execute(select(AuditEvent).where(AuditEvent.event_id == uuid.UUID(event["event_id"])))).scalar_one()
        assert row.resource_type == expected_resource_type, f"Expected resource_type='{expected_resource_type}' for event_type='{event_type}', got '{row.resource_type}'"