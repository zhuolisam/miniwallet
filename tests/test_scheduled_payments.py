"""Tests for scheduled payments — CRUD + scheduler execution.

Test scenarios:
1. Create scheduled payment (happy path)
2. Validation: start_at in past, self-payment, invalid target account
3. List scheduled payments
4. Cancel scheduled payment
5. Scheduler executes due payment → transfer created, schedule advanced
6. Scheduler skips on insufficient balance → event published, schedule advances
7. Scheduler concurrent safety (FOR UPDATE SKIP LOCKED)
8. Scheduler idempotency (crash after transfer, before advancing schedule)
"""

import uuid
from datetime import datetime, timezone, timedelta
from decimal import Decimal

import pytest
import pytest_asyncio
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.config import SYSTEM_ACCOUNT_ID
from app.database import Base
from app.models.scheduled_payment import ScheduledPayment
from app.models.scheduled_payment_execution import ScheduledPaymentExecution
from app.models.ledger_entry import LedgerEntry
from app.models.transfer import Transfer
from workers.payment_scheduler import advance_schedule, _execute_payment, _poll_and_execute


# ---------------------------------------------------------------------------
# CRUD Tests (via HTTP client)
# ---------------------------------------------------------------------------

class TestScheduledPaymentCRUD:
    """Test the API endpoints for scheduled payment management."""

    async def test_create_scheduled_payment(
        self, client, alice_headers, seeded_alice_account, bob_account
    ):
        # TODO: student — test creating a scheduled payment
        #
        # 1. POST /v1/scheduled-payments with:
        #    {
        #        "to_account_id": bob_account["account_id"],
        #        "amount": "25.00",
        #        "frequency": "monthly",
        #        "start_at": (now + 1 day).isoformat()
        #    }
        # 2. Assert 201 response
        # 3. Assert response has: id, from_account_id, to_account_id, amount,
        #    currency, frequency, next_run_at, status="active", created_at
        pass

    async def test_create_rejects_past_start_at(
        self, client, alice_headers, seeded_alice_account, bob_account
    ):
        # TODO: student — test that start_at in the past returns 400 INVALID_START_TIME
        #
        # 1. POST with start_at = yesterday
        # 2. Assert 400, error_code == "INVALID_START_TIME"
        pass

    async def test_create_rejects_self_payment(
        self, client, alice_headers, seeded_alice_account
    ):
        # TODO: student — test that paying yourself returns 400 CANNOT_PAY_SELF
        #
        # 1. POST with to_account_id = alice's own account_id
        # 2. Assert 400, error_code == "CANNOT_PAY_SELF"
        pass

    async def test_create_rejects_nonexistent_target(
        self, client, alice_headers, seeded_alice_account
    ):
        # TODO: student — test that invalid target returns 404 ACCOUNT_NOT_FOUND
        #
        # 1. POST with to_account_id = random UUID
        # 2. Assert 404
        pass

    async def test_create_rejects_invalid_frequency(
        self, client, alice_headers, seeded_alice_account, bob_account
    ):
        # TODO: student — test that invalid frequency returns 422
        #
        # 1. POST with frequency = "biweekly"
        # 2. Assert 422 (Pydantic validation error)
        pass

    async def test_list_scheduled_payments(
        self, client, alice_headers, seeded_alice_account, bob_account
    ):
        # TODO: student — test listing scheduled payments
        #
        # 1. Create two scheduled payments
        # 2. GET /v1/scheduled-payments
        # 3. Assert response contains both payments
        pass

    async def test_cancel_scheduled_payment(
        self, client, alice_headers, seeded_alice_account, bob_account
    ):
        # TODO: student — test cancelling a scheduled payment
        #
        # 1. Create a scheduled payment
        # 2. DELETE /v1/scheduled-payments/{id}
        # 3. Assert 204
        # 4. GET /v1/scheduled-payments — assert the payment has status="cancelled"
        pass

    async def test_cancel_nonexistent_returns_404(
        self, client, alice_headers, seeded_alice_account
    ):
        # TODO: student — test that cancelling a nonexistent payment returns 404
        #
        # 1. DELETE /v1/scheduled-payments/{random_uuid}
        # 2. Assert 404
        pass


# ---------------------------------------------------------------------------
# advance_schedule() unit tests
# ---------------------------------------------------------------------------

class TestAdvanceSchedule:
    """Test the schedule advancement logic."""

    def test_daily(self):
        base = datetime(2025, 3, 15, 10, 0, 0, tzinfo=timezone.utc)
        result = advance_schedule(base, "daily")
        assert result == datetime(2025, 3, 16, 10, 0, 0, tzinfo=timezone.utc)

    def test_weekly(self):
        base = datetime(2025, 3, 15, 10, 0, 0, tzinfo=timezone.utc)
        result = advance_schedule(base, "weekly")
        assert result == datetime(2025, 3, 22, 10, 0, 0, tzinfo=timezone.utc)

    def test_monthly(self):
        base = datetime(2025, 1, 31, 10, 0, 0, tzinfo=timezone.utc)
        result = advance_schedule(base, "monthly")
        # Jan 31 + 1 month = Feb 28 (relativedelta handles month-end correctly)
        assert result.month == 2
        assert result.day == 28

    def test_monthly_preserves_time(self):
        base = datetime(2025, 3, 15, 14, 30, 0, tzinfo=timezone.utc)
        result = advance_schedule(base, "monthly")
        assert result == datetime(2025, 4, 15, 14, 30, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Scheduler execution tests (unit-level, calling worker functions directly)
# ---------------------------------------------------------------------------

class TestSchedulerExecution:
    """Test the scheduler worker executing due payments."""

    async def test_executes_due_payment(
        self, consumer_db_factory, redis_client, account_factory, seed_factory
    ):
        # TODO: student — test that a due payment gets executed
        #
        # Setup:
        # 1. Create two accounts (sender + receiver) via account_factory
        # 2. Seed sender with funds via seed_factory
        # 3. Create a ScheduledPayment row with next_run_at = 1 minute ago
        #
        # Act:
        # 4. Call _poll_and_execute(consumer_db_factory, redis_client)
        #
        # Assert:
        # 5. A Transfer record exists for the payment amount
        # 6. ScheduledPaymentExecution row exists with result="executed"
        # 7. ScheduledPayment.next_run_at has advanced by one period
        pass

    async def test_skips_on_insufficient_balance(
        self, consumer_db_factory, redis_client, account_factory
    ):
        # TODO: student — test that insufficient balance results in a skip
        #
        # Setup:
        # 1. Create sender (no seed — zero balance) and receiver
        # 2. Create due ScheduledPayment
        #
        # Act:
        # 3. Call _poll_and_execute(consumer_db_factory, redis_client)
        #
        # Assert:
        # 4. No Transfer created
        # 5. ScheduledPaymentExecution with result="skipped", skip_reason="INSUFFICIENT_BALANCE"
        # 6. next_run_at still advanced (schedule continues)
        pass

    async def test_does_not_execute_future_payment(
        self, consumer_db_factory, redis_client, account_factory, seed_factory
    ):
        # TODO: student — test that payments with next_run_at in the future are not claimed
        #
        # Setup:
        # 1. Create funded sender + receiver
        # 2. Create ScheduledPayment with next_run_at = 1 hour from now
        #
        # Act:
        # 3. Call _poll_and_execute(consumer_db_factory, redis_client)
        #
        # Assert:
        # 4. No ScheduledPaymentExecution rows
        # 5. next_run_at unchanged
        pass

    async def test_does_not_execute_cancelled_payment(
        self, consumer_db_factory, redis_client, account_factory, seed_factory
    ):
        # TODO: student — test that cancelled payments are skipped
        #
        # Setup:
        # 1. Create funded sender + receiver
        # 2. Create ScheduledPayment with status="cancelled" and next_run_at in the past
        #
        # Act & Assert:
        # 3. After _poll_and_execute, no execution occurred
        pass

    async def test_idempotent_on_crash_retry(
        self, consumer_db_factory, redis_client, account_factory, seed_factory
    ):
        # TODO: student — test crash between transfer and schedule advance
        #
        # This simulates the crash scenario:
        # 1. Create a due payment
        # 2. Call _execute_payment once (transfer succeeds, schedule advances)
        # 3. Manually reset next_run_at back to the original value (simulating
        #    "schedule didn't advance because we crashed")
        # 4. Call _execute_payment again with the same payment
        # 5. Assert: transfer idempotency prevents double-debit
        #    - Only one Transfer exists (or the second call is a no-op)
        #    - Sender balance only debited once
        pass
