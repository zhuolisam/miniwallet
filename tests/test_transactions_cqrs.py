"""Tests for GET /v1/accounts/me/transactions — CQRS read model.

These tests verify that the endpoint reads from transaction_activity (not the
ledger), preserves backward compatibility (created_at field name), and correctly
computes as_of from the result set.

Pre-populate transaction_activity directly — no consumer needed.
"""

import uuid
from datetime import datetime, timezone, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import func, select

from app.models.transaction_activity import TransactionActivity
from app.models.ledger_entry import LedgerEntry



@pytest.mark.asyncio
async def test_transactions_created_at_field_preserved(
    client, consumer_db_factory, account_factory, auth_headers
):
    """Response uses 'created_at' (Phase 1 contract) even though DB stores 'occurred_at'."""
    account = await account_factory()
    occurred = datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)

    async with consumer_db_factory() as db:
        async with db.begin():
            db.add(TransactionActivity(
                event_id=uuid.uuid4(),
                account_id=account.id,
                direction="credit",
                amount=Decimal("100.0000"),
                entry_type="seed",
                occurred_at=occurred,
            ))


    resp = await client.get("/v1/accounts/me/transactions", headers=auth_headers(account))
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["data"]) == 1
    item = data["data"][0]
    assert item["entry_type"] == "seed"
    assert item["amount"] == "100.0000"
    assert item["created_at"].startswith("2024-01-15T10:30:00")

@pytest.mark.asyncio
async def test_transactions_as_of_is_max_occurred_at_of_current_page(
    client, consumer_db_factory, account_factory, auth_headers
):
    """as_of = MAX(occurred_at) of the rows on this page."""
    account = await account_factory()
    t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
    t2 = datetime(2024, 1, 2, tzinfo=timezone.utc)

    async with consumer_db_factory() as db:
        async with db.begin():
            for t in (t1, t2):
                db.add(TransactionActivity(
                    event_id=uuid.uuid4(),
                    account_id=account.id,
                    direction="credit",
                    amount=Decimal("50.0000"),
                    entry_type="seed",
                    occurred_at=t,
                ))

    resp = await client.get("/v1/accounts/me/transactions?limit=10", headers=auth_headers(account))
    assert resp.status_code == 200
    data = resp.json()
    assert data["meta"]["as_of"].startswith("2024-01-02T00:00:00")


@pytest.mark.asyncio
async def test_transactions_as_of_null_when_no_results(
    client, account_factory, auth_headers
):
    """as_of is null when the account has no activity rows."""
    account = await account_factory()

    resp = await client.get("/v1/accounts/me/transactions", headers=auth_headers(account))
    assert resp.status_code == 200
    data = resp.json()
    assert data["meta"]["as_of"] is None


@pytest.mark.asyncio
async def test_transactions_date_filter_on_occurred_at(
    client, consumer_db_factory, account_factory, auth_headers
):
    """from_date/to_date filter on occurred_at."""
    account = await account_factory()
    t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
    t2 = datetime(2024, 1, 2, tzinfo=timezone.utc)

    async with consumer_db_factory() as db:
        async with db.begin():
            for t in (t1, t2):
                db.add(TransactionActivity(
                    event_id=uuid.uuid4(),
                    account_id=account.id,
                    direction="credit",
                    amount=Decimal("50.0000"),
                    entry_type="seed",
                    occurred_at=t,
                ))

    # FastAPI datetime parser expects format without colon in timezone offset
    from_date_str = t2.strftime("%Y-%m-%dT%H:%M:%S")
    resp = await client.get(f"/v1/accounts/me/transactions?from_date={from_date_str}", headers=auth_headers(account))
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["data"]) == 1
    item = data["data"][0]
    assert item["created_at"].startswith("2024-01-02T00:00:00")

@pytest.mark.asyncio
async def test_transactions_entry_type_filter(
    client, consumer_db_factory, account_factory, auth_headers
):
    """entry_type=seed returns only seed entries."""
    account = await account_factory()

    async with consumer_db_factory() as db:
        async with db.begin():
            db.add(TransactionActivity(
                event_id=uuid.uuid4(),
                account_id=account.id,
                direction="credit",
                amount=Decimal("1000.0000"),
                entry_type="seed",
                occurred_at=datetime.now(timezone.utc),
            ))
            db.add(TransactionActivity(
                event_id=uuid.uuid4(),
                account_id=account.id,
                direction="debit",
                amount=Decimal("50.0000"),
                entry_type="transfer",
                reference_id=uuid.uuid4(),
                occurred_at=datetime.now(timezone.utc),
            ))

    resp = await client.get("/v1/accounts/me/transactions?entry_type=seed", headers=auth_headers(account))
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["data"]) == 1
    item = data["data"][0]
    assert item["entry_type"] == "seed"
