"""Integration tests — outbox rows are created when domain operations execute.

These tests verify the outbox pattern: after a transfer, account open, or seed,
an outbox row exists in the same DB with the correct event envelope.

Unlike test_outbox_relay.py (which tests the relay in isolation), these tests
use the full HTTP stack (client fixture) to make API calls and then check
that the outbox table has the expected rows.

No Kafka needed — we're testing the write side (API → outbox), not the
delivery side (relay → Kafka → consumer).
"""

import uuid

import pytest
from sqlalchemy import select, func

from app.events.schemas import AccountOpenedPayload, SeedCompletedPayload, TransferCompletedPayload, TransferFailedPayload
from app.models.outbox import OutboxRow


# ---------------------------------------------------------------------------
# Test: transfer.completed outbox row
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_transfer_creates_outbox_row(
    client, seeded_alice_account, alice_headers, bob_account, db_session
):
    """A successful transfer should create an outbox row with event_type='transfer.completed'."""
    resp = await client.post(
        "/v1/transfers",
        json={
            "to_account_id": str(bob_account["account_id"]),
            "amount": "50.00",
        },
        headers={**alice_headers, "Idempotency-Key": str(uuid.uuid4())},
    )

    assert resp.status_code == 201

    # Query outbox table
    result = await db_session.execute(
        select(OutboxRow).where(OutboxRow.event_type == "transfer.completed")
    )
    rows = result.scalars().all()

    assert len(rows) == 1
    row = rows[0]
    assert row.topic == "transfer.events"
    assert row.status == "pending"
    payload = TransferCompletedPayload.model_validate(row.payload["payload"])
    assert payload.transfer_id == resp.json()["data"]["transfer_id"]
    assert payload.from_account_id == str(seeded_alice_account["account_id"])
    assert payload.to_account_id == str(bob_account["account_id"])
    assert payload.amount == "50.00000000"
    assert payload.entry_type == "transfer"



# ---------------------------------------------------------------------------
# Test: transfer.failed outbox row
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_failed_transfer_creates_outbox_row(
    client, alice_account, alice_headers, bob_account, db_session
):
    """A failed transfer (insufficient balance, no seed) should create a transfer.failed outbox row."""
    resp = await client.post(
        "/v1/transfers",
        json={
            "to_account_id": str(bob_account["account_id"]),
            "amount": "999999.00",
        },
        headers={**alice_headers, "Idempotency-Key": str(uuid.uuid4())},
    )

    assert resp.status_code == 422

    # Query outbox table
    result = await db_session.execute(
        select(OutboxRow).where(OutboxRow.event_type == "transfer.failed")
    )
    rows = result.scalars().all()

    assert len(rows) == 1
    row = rows[0]
    assert row.topic == "transfer.events"
    assert row.status == "pending"
    payload = TransferFailedPayload.model_validate(row.payload["payload"])
    assert payload.from_account_id == str(alice_account["account_id"])
    assert payload.to_account_id == str(bob_account["account_id"])
    assert payload.amount == "999999.00000000"
    assert payload.failure_code == "INSUFFICIENT_BALANCE"


# ---------------------------------------------------------------------------
# Test: account.opened outbox row
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_open_account_creates_outbox_row(client, alice_headers, db_session):
    """Opening an account should create an outbox row with event_type='account.opened'."""
    resp = await client.post("/v1/accounts", headers={**alice_headers, "Idempotency-Key": str(uuid.uuid4())})

    assert resp.status_code == 201

    # Query outbox table
    result = await db_session.execute(
        select(OutboxRow).where(OutboxRow.event_type == "account.opened")
    )
    rows = result.scalars().all()

    assert len(rows) == 1
    row = rows[0]
    assert row.topic == "account.events"
    assert row.status == "pending"
    assert row.payload["actor_id"] == resp.json()["data"]["user_id"]
    payload = AccountOpenedPayload.model_validate(row.payload["payload"])
    assert payload.account_id == resp.json()["data"]["account_id"]
    assert payload.user_id == resp.json()["data"]["user_id"]



# ---------------------------------------------------------------------------
# Test: seed.completed outbox row
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_seed_creates_outbox_row(
    client, alice_headers, alice_account, db_session
):
    """Seeding an account should create an outbox row with event_type='seed.completed'."""
    resp = await client.post(
        "/v1/dev/seed",
        json={
            "account_id": str(alice_account["account_id"]),
            "amount": "1000.00",
        },
        headers={**alice_headers, "Idempotency-Key": str(uuid.uuid4())},
    )

    assert resp.status_code == 201

    # Query outbox table
    result = await db_session.execute(
        select(OutboxRow).where(OutboxRow.event_type == "seed.completed")
    )
    rows = result.scalars().all()

    assert len(rows) == 1
    row = rows[0]
    assert row.topic == "account.events"
    assert row.status == "pending"
    payload = SeedCompletedPayload.model_validate(row.payload["payload"])
    assert payload.account_id == resp.json()["data"]["account_id"]
    assert payload.amount == "1000.00000000"


# ---------------------------------------------------------------------------
# Test: atomicity — outbox row and domain row are committed together
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_outbox_row_absent_when_transfer_rolled_back(
    client, alice_headers, db_session
):
    """If the domain write rolls back, the outbox row should also not exist.

    This is the core guarantee of the outbox pattern: atomicity.
    Test by attempting a transfer to a non-existent account (404),
    which should not create any outbox rows.
    """
    resp = await client.post(
        "/v1/transfers",
        json={
            "to_account_id": str(uuid.uuid4()),
            "amount": "50.00",
        },
        headers={**alice_headers, "Idempotency-Key": str(uuid.uuid4())},
    )

    assert resp.status_code == 404

    result = await db_session.execute(select(func.count()).select_from(OutboxRow))
    count = result.scalar()
    assert count == 0
