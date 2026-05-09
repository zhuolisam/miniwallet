"""Tests for the activity consumer — CQRS read model builder.

All tests call process() directly (no Kafka needed). They verify that:
- transfer.completed → 2 rows (debit + credit)
- seed.completed → 1 row (credit)
- Replay (duplicate event_id) → no extra rows (idempotent)
- account.opened → 0 rows (informational only)
"""

import uuid
from decimal import Decimal

import pytest
from sqlalchemy import func, select

from app.models.transaction_activity import TransactionActivity
from workers.activity_consumer import ActivityConsumer
from tests.conftest import make_event


@pytest.mark.asyncio
async def test_activity_consumer_transfer_completed_creates_two_rows(consumer_db_factory, account_factory):
    """transfer.completed → one debit row for sender + one credit row for receiver."""
    sender = await account_factory()
    receiver = await account_factory()
    consumer = ActivityConsumer(db_factory=consumer_db_factory)
    event = make_event("transfer.completed", {
        "transfer_id": str(uuid.uuid4()),
        "from_account_id": str(sender.id),
        "to_account_id": str(receiver.id),
        "amount": "50.0000",
        "currency": "USD",
        "entry_type": "transfer",
        "idempotency_key": "test-key",
    })

    # Call consumer.process(event), then verify:

    await consumer.process(event)
    async with consumer.db_factory() as db:
        # - 2 rows in transaction_activity for this event_id
        result = await db.execute(
            select(func.count()).where(TransactionActivity.event_id == uuid.UUID(event["event_id"]))
        )
        count = result.scalar()
        assert count == 2, f"Expected 2 activity rows, got {count}"

        # - sender's row has direction="debit"
        result = await db.execute(
            select(TransactionActivity).where(
                TransactionActivity.event_id == uuid.UUID(event["event_id"]),
                TransactionActivity.account_id == sender.id,
            )
        )
        debit_row = result.scalar_one_or_none()
        assert debit_row is not None, "Debit row for sender not found"
        assert debit_row.direction == "debit", f"Expected direction='debit', got {debit_row.direction}"
        assert debit_row.amount == Decimal("50.0000"), f"Expected amount=50.00, got {debit_row.amount}"
        assert debit_row.entry_type == "transfer", f"Expected entry_type='transfer', got {debit_row.entry_type}"
        assert debit_row.reference_id == uuid.UUID(event["payload"]["transfer_id"]), (
            f"Expected reference_id={event['payload']['transfer_id']}, got {debit_row.reference_id}"
        )

        # - receiver's row has direction="credit"
        result = await db.execute(
            select(TransactionActivity).where(
                TransactionActivity.event_id == uuid.UUID(event["event_id"]),
                TransactionActivity.account_id == receiver.id,
            )
        )
        credit_row = result.scalar_one_or_none()
        assert credit_row is not None, "Credit row for receiver not found"
        assert credit_row.direction == "credit", f"Expected direction='credit', got {credit_row.direction}"
        assert credit_row.amount == Decimal("50.0000"), f"Expected amount=50.00, got {credit_row.amount}"
        assert credit_row.entry_type == "transfer", f"Expected entry_type='transfer', got {credit_row.entry_type}"
        assert credit_row.reference_id == uuid.UUID(event["payload"]["transfer_id"]), (
            f"Expected reference_id={event['payload']['transfer_id']}, got {credit_row.reference_id}"
        )


@pytest.mark.asyncio
async def test_activity_consumer_seed_completed_creates_one_credit_row(consumer_db_factory, account_factory):
    """seed.completed → one credit row (no debit — money from system)."""
    account = await account_factory()
    consumer = ActivityConsumer(db_factory=consumer_db_factory)
    event = make_event("seed.completed", {
        "account_id": str(account.id),
        "user_id": str(account.user_id),
        "amount": "1000.0000",
        "currency": "USD",
        "entry_type": "seed",
    })

    await consumer.process(event)
    async with consumer.db_factory() as db:
        # - 1 row in transaction_activity for this account
        result = await db.execute(
            select(func.count()).where(
                TransactionActivity.event_id == uuid.UUID(event["event_id"]),
                TransactionActivity.account_id == account.id,
            )
        )
        count = result.scalar()
        assert count == 1, f"Expected 1 activity row, got {count}"

        # - direction="credit", entry_type="seed", reference_id is None
        result = await db.execute(
            select(TransactionActivity).where(
                TransactionActivity.event_id == uuid.UUID(event["event_id"]),
                TransactionActivity.account_id == account.id,
            )
        )
        row = result.scalar_one_or_none()
        assert row is not None, "Activity row not found"
        assert row.direction == "credit", f"Expected direction='credit', got {row.direction}"
        assert row.amount == Decimal("1000.0000"), f"Expected amount=1000.00, got {row.amount}"
        assert row.entry_type == "seed", f"Expected entry_type='seed', got {row.entry_type}"
        assert row.reference_id is None, f"Expected reference_id=None, got {row.reference_id}"


@pytest.mark.asyncio
async def test_activity_consumer_idempotent(consumer_db_factory, account_factory):
    """Replaying transfer.completed twice produces 2 rows, not 4."""
    sender = await account_factory()
    receiver = await account_factory()
    consumer = ActivityConsumer(db_factory=consumer_db_factory)
    event = make_event("transfer.completed", {
        "transfer_id": str(uuid.uuid4()),
        "from_account_id": str(sender.id),
        "to_account_id": str(receiver.id),
        "amount": "10.0000",
        "currency": "USD",
        "entry_type": "transfer",
        "idempotency_key": "key",
    })

    await consumer.process(event)
    await consumer.process(event)  # replay same event again

    async with consumer.db_factory() as db:
        # - Still only 2 rows in transaction_activity for this event_id (not 4)
        result = await db.execute(
            select(func.count()).where(TransactionActivity.event_id == uuid.UUID(event["event_id"]))
        )
        count = result.scalar()
        assert count == 2, f"Expected 2 activity rows after replay, got {count}"


@pytest.mark.asyncio
async def test_activity_consumer_ignores_account_opened(consumer_db_factory, account_factory):
    """account.opened produces no activity rows — informational event only."""
    account = await account_factory()
    consumer = ActivityConsumer(db_factory=consumer_db_factory)
    event = make_event("account.opened", {
        "account_id": str(account.id),
        "user_id": str(account.user_id),
        "status": "active",
    })

    await consumer.process(event)
    async with consumer.db_factory() as db:
        result = await db.execute(
            select(func.count()).where(TransactionActivity.event_id == uuid.UUID(event["event_id"]))
        )
        count = result.scalar()
        assert count == 0, f"Expected 0 activity rows for account.opened, got {count}"


@pytest.mark.asyncio
async def test_activity_consumer_ignores_transfer_failed(consumer_db_factory, account_factory):
    """transfer.failed produces no activity rows — failed transfers never moved money."""
    sender = await account_factory()
    receiver = await account_factory()
    consumer = ActivityConsumer(db_factory=consumer_db_factory)
    event = make_event("transfer.failed", {
        "transfer_id": str(uuid.uuid4()),
        "from_account_id": str(sender.id),
        "to_account_id": str(receiver.id),
        "amount": "50.0000",
        "currency": "USD",
        "failure_code": "INSUFFICIENT_BALANCE",
        "entry_type": "transfer",
        "idempotency_key": "key",
    })

    await consumer.process(event)
    async with consumer.db_factory() as db:
        result = await db.execute(
            select(func.count()).where(TransactionActivity.event_id == uuid.UUID(event["event_id"]))
        )
        count = result.scalar()
        assert count == 0, f"Expected 0 activity rows for transfer.failed, got {count}"
