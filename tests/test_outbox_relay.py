"""Tests for the outbox relay worker.

These tests validate the relay's core functions without a running Kafka broker.
The relay's job is:
  1. claim_batch: SELECT pending rows with FOR UPDATE SKIP LOCKED
  2. publish to Kafka (mocked in these tests)
  3. confirm_batch: persist publish results back to the DB

Tests call the relay functions directly with a real test Postgres (via
consumer_db_factory) and verify DB state before and after each step.
"""

import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import select, func

from app.models.outbox import OutboxRow
from workers.outbox_relay import (
    claim_batch,
    confirm_batch,
    recover_stuck_rows,
    cleanup_published_rows,
    BATCH_SIZE,
)


async def _insert_outbox_row(
    session_factory,
    topic: str = "transfer.events",
    event_type: str = "transfer.completed",
    status: str = "pending",
) -> OutboxRow:
    """Helper: insert an outbox row and return it."""
    row = OutboxRow(
        id=uuid.uuid4(),
        topic=topic,
        event_type=event_type,
        payload={
            "event_id": str(uuid.uuid4()),
            "event_type": event_type,
            "occurred_at": datetime.now(timezone.utc).isoformat(),
            "version": "1",
            "actor_id": None,
            "payload": {"test": True},
        },
        status=status,
    )
    async with session_factory() as db:
        async with db.begin():
            db.add(row)
    return row


async def _count_rows(session_factory, status: str | None = None) -> int:
    """Helper: count outbox rows, optionally filtered by status."""
    async with session_factory() as db:
        q = select(func.count()).select_from(OutboxRow)
        if status:
            q = q.where(OutboxRow.status == status)
        return (await db.execute(q)).scalar_one()


# ---------------------------------------------------------------------------
# Test: claim_batch
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_claim_batch_returns_pending_rows(consumer_db_factory):
    """claim_batch should return pending rows and mark them 'publishing'."""
    # 1. Insert 3 pending outbox rows using _insert_outbox_row()
    for _ in range(3):
        await _insert_outbox_row(consumer_db_factory, status="pending")

    # 2. Call claim_batch(consumer_db_factory)
    batch = await claim_batch(consumer_db_factory)

    # 3. Assert the returned list has 3 rows
    assert len(batch) == 3
    # 4. Assert each row's status is "publishing"
    for row in batch:
        assert row.status == "publishing"
    # 5. Verify DB: count of pending rows is now 0, count of publishing rows is 3
    pending_count = await _count_rows(consumer_db_factory, status="pending")
    publishing_count = await _count_rows(consumer_db_factory, status="publishing")
    assert pending_count == 0
    assert publishing_count == 3


@pytest.mark.asyncio
async def test_claim_batch_skips_non_pending_rows(consumer_db_factory):
    """claim_batch should only claim 'pending' rows, not 'publishing' or 'published'."""
    # 1. Insert 1 pending row, 1 publishing row, 1 published row
    await _insert_outbox_row(consumer_db_factory, status="pending")
    await _insert_outbox_row(consumer_db_factory, status="publishing")
    await _insert_outbox_row(consumer_db_factory, status="published")

    # 2. Call claim_batch(consumer_db_factory)
    batch = await claim_batch(consumer_db_factory)

    # 3. Assert only 1 row returned (the pending one)
    assert len(batch) == 1
    assert batch[0].status == "publishing"  # status should be updated to
    
    # 4. Verify DB: count of pending rows is now 0, count of publishing rows is 2
    pending_count = await _count_rows(consumer_db_factory, status="pending")
    publishing_count = await _count_rows(consumer_db_factory, status="publishing")
    assert pending_count == 0
    assert publishing_count == 2


@pytest.mark.asyncio
async def test_claim_batch_respects_batch_size(consumer_db_factory):
    """claim_batch should return at most BATCH_SIZE rows."""
    # TODO: student — implement this test:
    #
    # 1. Insert BATCH_SIZE + 5 pending rows
    for _ in range(BATCH_SIZE + 5):
        await _insert_outbox_row(consumer_db_factory, status="pending")

    # 2. Call claim_batch(consumer_db_factory)
    batch = await claim_batch(consumer_db_factory)
    # 3. Assert exactly BATCH_SIZE rows returned
    assert len(batch) == BATCH_SIZE
    # 4. Call claim_batch again — remaining 5 rows returned
    batch2 = await claim_batch(consumer_db_factory)
    assert len(batch2) == 5


# ---------------------------------------------------------------------------
# Test: confirm_batch
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_confirm_batch_persists_published_status(consumer_db_factory):
    """After relay sets row.status = 'published', confirm_batch persists it."""
    # 1. Insert a pending row
    row = await _insert_outbox_row(consumer_db_factory, status="pending")

    # 2. claim_batch to get the row (now 'publishing')
    batch = await claim_batch(consumer_db_factory)
    assert len(batch) == 1
    # 3. Simulate successful publish: set row.status = "published",

    row = batch[0]
    row.status = "published"
    row.published_at = datetime.now(timezone.utc)

    # 4. Call confirm_batch(consumer_db_factory, [row])
    await confirm_batch(consumer_db_factory, batch)

    # 5. Query DB: row status should be "published", published_at should be set
    async with consumer_db_factory() as db:
        q = select(OutboxRow).where(OutboxRow.id == row.id)
        result = await db.execute(q)
        updated_row = result.scalar_one()
        assert updated_row.status == "published"
        assert updated_row.published_at is not None


@pytest.mark.asyncio
async def test_confirm_batch_persists_retry(consumer_db_factory):
    """When publish fails, relay sets status back to 'pending' and increments retry_count."""
    # 1. Insert a pending row
    row = await _insert_outbox_row(consumer_db_factory, status="pending")
    # 2. claim_batch to get the row
    batch = await claim_batch(consumer_db_factory)
    assert len(batch) == 1
    # 3. Simulate failed publish: set row.status = "pending", row.retry_count += 1
    row = batch[0]
    row.status = "pending"
    row.retry_count += 1

    # 4. Call confirm_batch(consumer_db_factory, [row])
    await confirm_batch(consumer_db_factory, batch)

    # 5. Query DB: row status should be "pending", retry_count should be 1
    async with consumer_db_factory() as db:
        q = select(OutboxRow).where(OutboxRow.id == row.id)
        result = await db.execute(q)
        updated_row = result.scalar_one()
        assert updated_row.status == "pending"
        assert updated_row.retry_count == 1

# ---------------------------------------------------------------------------
# Test: recover_stuck_rows
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_recover_stuck_rows_resets_old_publishing(consumer_db_factory):
    """Rows stuck in 'publishing' for > 5 min should be reset to 'pending'."""

    row = None
    # 1. Insert a row with status='publishing' and created_at = 10 minutes ago
    async with consumer_db_factory() as db:
        row = OutboxRow(
            id=uuid.uuid4(),
            topic="test.topic",
            event_type="test.event",
            payload={"test": True},
            status="publishing",
            created_at=datetime.now(timezone.utc) - timedelta(minutes=10),
        )
        async with db.begin():
            db.add(row)
    # 2. Call recover_stuck_rows(consumer_db_factory)
    await recover_stuck_rows(consumer_db_factory)
    
    # 3. Query DB: row status should now be "pending"
    async with consumer_db_factory() as db:
        q = select(OutboxRow).where(OutboxRow.id == row.id)
        result = await db.execute(q)
        updated_row = result.scalar_one()
        assert updated_row.status == "pending"


@pytest.mark.asyncio
async def test_recover_stuck_rows_ignores_recent_publishing(consumer_db_factory):
    """Rows in 'publishing' for < 5 min should NOT be reset — they're actively being processed."""

    row = None
    # 1. Insert a row with status='publishing' and created_at = just now
    async with consumer_db_factory() as db:
        row = OutboxRow(
            id=uuid.uuid4(),
            topic="test.topic",
            event_type="test.event",
            payload={"test": True},
            status="publishing",
            created_at=datetime.now(timezone.utc),
        )
        async with db.begin():
            db.add(row)
    # 2. Call recover_stuck_rows(consumer_db_factory)
    await recover_stuck_rows(consumer_db_factory)
    # 3. Query DB: row status should still be "publishing"
    async with consumer_db_factory() as db:
        q = select(OutboxRow).where(OutboxRow.id == row.id)
        result = await db.execute(q)
        updated_row = result.scalar_one()
        assert updated_row.status == "publishing"


# ---------------------------------------------------------------------------
# Test: cleanup_published_rows
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cleanup_deletes_old_published_rows(consumer_db_factory):
    """Published rows older than 7 days should be deleted."""

    async with consumer_db_factory() as db:
            # 1. Insert a row with status='published' and published_at = 10 days ago
            old_row = OutboxRow(
                id=uuid.uuid4(),
                topic="test.topic",
                event_type="test.event",
                payload={"test": True},
                status="published",
                published_at=datetime.now(timezone.utc) - timedelta(days=10),
            )
            # 2. Insert a row with status='published' and published_at = 1 day ago (should survive)
            recent_row = OutboxRow(
                id=uuid.uuid4(),
                topic="test.topic",
                event_type="test.event",
                payload={"test": True},
                status="published",
                published_at=datetime.now(timezone.utc) - timedelta(days=1),
            )
            async with db.begin():
                db.add_all([old_row, recent_row])
    
    # 3. Call cleanup_published_rows(consumer_db_factory)

    await cleanup_published_rows(consumer_db_factory)
    # 4. Assert only 1 row remains (the recent one)

    async with consumer_db_factory() as db:
        q = select(OutboxRow)
        result = await db.execute(q)
        rows = result.scalars().all()
        assert len(rows) == 1
        assert rows[0].id == recent_row.id
