"""Tests for the Week 6 minimal audit consumer.

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
from workers.audit_consumer import process


async def test_audit_persists_transfer_completed(consumer_db_factory):
    """transfer.completed → one audit_events row with correct fields."""
    event = make_event("transfer.completed", {
        "transfer_id":     str(uuid.uuid4()),
        "from_account_id": str(uuid.uuid4()),
        "to_account_id":   str(uuid.uuid4()),
        "amount":          "100.00000000",
        "entry_type":      "transfer",
        "idempotency_key": "test-key",
    })

    # 1. Call await process(event)
    await process(event, session_factory=consumer_db_factory)

    # 2. Open a session via consumer_db_factory and query audit_events:
    async with consumer_db_factory() as db:
        row = (await db.execute(select(AuditEvent).where(AuditEvent.event_id == uuid.UUID(event["event_id"])))).scalar_one()

    # 3. Assert:
    assert row is not None, "Expected one audit_events row, got zero"
    assert row.event_type == "transfer.completed"
    assert row.resource_type == "transfer"
    assert row.actor_id is not None
    assert row.payload["payload"]["transfer_id"] == event["payload"]["transfer_id"]

async def test_audit_persists_transfer_failed(consumer_db_factory):
    """transfer.failed → one audit_events row with event_type and resource_type set."""
    event = make_event("transfer.failed", {
        "transfer_id":     str(uuid.uuid4()),
        "from_account_id": str(uuid.uuid4()),
        "to_account_id":   str(uuid.uuid4()),
        "amount":          "100.00000000",
        "failure_code":    "INSUFFICIENT_BALANCE",
    })

    # 1. Call await process(event)
    await process(event, session_factory=consumer_db_factory)
    # 2. Query audit_events for event["event_id"]
    async with consumer_db_factory() as db:
        row = (await db.execute(select(AuditEvent).where(AuditEvent.event_id == uuid.UUID(event["event_id"])))).scalar_one()
    # 3. Assert:
    assert row is not None, "Expected one audit_events row, got zero"
    assert row.event_type == "transfer.failed"
    assert row.resource_type == "transfer"
    assert row.actor_id is not None
    assert row.payload["payload"]["transfer_id"] == event["payload"]["transfer_id"]


async def test_audit_persists_account_opened(consumer_db_factory):
    """account.opened → one audit_events row with resource_type == 'account'."""
    account_id = str(uuid.uuid4())
    event = make_event("account.opened", {
        "account_id": account_id,
        "user_id":    str(uuid.uuid4()),
        "status":     "active",
    })

    # 1. Call await process(event)
    await process(event, session_factory=consumer_db_factory)
    # 2. Query audit_events for event["event_id"]
    async with consumer_db_factory() as db:
        row = (await db.execute(select(AuditEvent).where(AuditEvent.event_id == uuid.UUID(event["event_id"])))).scalar_one()
    # 3. Assert:
    assert row.event_type == "account.opened"
    assert row.resource_type == "account"
    assert row.actor_id is not None
    assert row.payload["payload"]["account_id"] == event["payload"]["account_id"]

async def test_audit_persists_seed_completed(consumer_db_factory):
    """seed.completed → one audit_events row (the audit log captures ALL event types)."""
    account_id = str(uuid.uuid4())
    event = make_event("seed.completed", {
        "account_id": account_id,
        "user_id":    str(uuid.uuid4()),
        "amount":     "1000.00000000",
        "entry_type": "seed",
    })

    # 1. Call await process(event)
    await process(event, session_factory=consumer_db_factory)
    # 2. Query audit_events for event["event_id"]
    async with consumer_db_factory() as db:
        row = (await db.execute(select(AuditEvent).where(AuditEvent.event_id == uuid.UUID(event["event_id"])))).scalar_one()
    # 3. Assert:
    assert row.event_type == "seed.completed"
    assert row.resource_type == "account"
    assert row.actor_id is not None
    assert row.payload["payload"]["account_id"] == event["payload"]["account_id"]

async def test_audit_idempotent_on_duplicate_event_id(consumer_db_factory):
    """Processing the same event twice inserts only one row.

    The UNIQUE constraint on audit_events.event_id enforces this at the DB level.
    process() must catch IntegrityError and treat it as a no-op — NOT re-raise it,
    which would be misinterpreted as a processing failure by BaseConsumer (Week 9).
    """
    event = make_event("transfer.completed", {
        "transfer_id":     str(uuid.uuid4()),
        "from_account_id": str(uuid.uuid4()),
        "to_account_id":   str(uuid.uuid4()),
        "amount":          "50.00000000",
        "entry_type":      "transfer",
        "idempotency_key": "dup-key",
    })

    # 1. Call await process(event)  — first insert, should succeed
    await process(event, session_factory=consumer_db_factory)
    # 2. Call await process(event)  — duplicate event_id, should NOT raise
    try:
        await process(event, session_factory=consumer_db_factory)
    except Exception as e:
        assert False, f"Expected no exception on duplicate event_id, got {type(e).__name__}: {e}"
    #
    # 3. Count rows in audit_events for this event_id:
    async with consumer_db_factory() as db:
        result = await db.execute(
            select(AuditEvent).where(AuditEvent.event_id == uuid.UUID(event["event_id"]))
        )
        rows = result.scalars().all()
    assert len(rows) == 1   # exactly one row, not two


async def test_audit_stores_correct_resource_type_for_all_event_types(consumer_db_factory):
    """Verify the _RESOURCE_TYPE mapping produces correct resource_type for each event."""
    cases = [
        ("transfer.completed", {"transfer_id": str(uuid.uuid4()), "from_account_id": str(uuid.uuid4()),
                                 "to_account_id": str(uuid.uuid4()), "amount": "10.00", "entry_type": "transfer",
                                 "idempotency_key": "k1"}, "transfer"),
        ("transfer.failed",    {"transfer_id": str(uuid.uuid4()), "from_account_id": str(uuid.uuid4()),
                                 "to_account_id": str(uuid.uuid4()), "amount": "10.00",
                                 "failure_code": "INSUFFICIENT_BALANCE"}, "transfer"),
        ("account.opened",     {"account_id": str(uuid.uuid4()), "user_id": str(uuid.uuid4()),
                                 "status": "active"}, "account"),
        ("seed.completed",     {"account_id": str(uuid.uuid4()), "user_id": str(uuid.uuid4()),
                                 "amount": "1000.00", "entry_type": "seed"}, "account"),
    ]

    # For each (event_type, payload, expected_resource_type) in cases:
    #   1. event = make_event(event_type, payload)
    #   2. await process(event)
    #   3. Query audit_events for event["event_id"]
    #   4. assert row.resource_type == expected_resource_type

    for event_type, payload, expected_resource_type in cases:
        event = make_event(event_type, payload)
        await process(event, session_factory=consumer_db_factory)
        async with consumer_db_factory() as db:
            row = (await db.execute(select(AuditEvent).where(AuditEvent.event_id == uuid.UUID(event["event_id"])))).scalar_one()
        assert row.resource_type == expected_resource_type, f"Expected resource_type='{expected_resource_type}' for event_type='{event_type}', got '{row.resource_type}'"