"""Scheduled payment worker — poll-based executor for recurring payments.

Runs as a background loop in the app's lifespan. Every 10 seconds, it:
1. Claims due payments (FOR UPDATE SKIP LOCKED — safe with concurrent instances)
2. Executes each via transfer() (reuses Phase 1 transfer logic)
3. Advances next_run_at + writes execution log + outbox event

Two-phase design per payment:
    Phase 1 (Claim): SELECT due payments in a short TX, then commit (release locks).
    Phase 2 (Execute): For each payment, call transfer() in its own session,
        then advance the schedule in a separate TX.

Why two phases? transfer() calls db.commit() internally. Wrapping it in an
outer db.begin() would attempt a nested commit — which either fails or commits
prematurely, releasing FOR UPDATE locks before we advance next_run_at.

Idempotency: key = "scheduled:{payment_id}:{next_run_at_iso}". If the scheduler
crashes after transfer() commits but before advancing next_run_at, the retry
hits transfer()'s idempotency check → no double-debit.

Dependencies:
    - python-dateutil (for relativedelta — correct "+1 month" advancement)
    - app.services.transfer_service.transfer (the Phase 1 transfer function)
"""

import asyncio
import logging
from datetime import datetime, timezone
from uuid import UUID

from dateutil.relativedelta import relativedelta
from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.config import SYSTEM_ACCOUNT_ID
from app.events.publisher import publish_event
from app.events.schemas import PaymentExecutedPayload, PaymentSkippedPayload
from app.exceptions import AccountNotFoundError, InsufficientBalanceError
from app.models.scheduled_payment import ScheduledPayment
from app.models.scheduled_payment_execution import ScheduledPaymentExecution
from app.services.transfer_service import transfer

logger = logging.getLogger(__name__)

POLL_INTERVAL_SECONDS = 10
CLAIM_BATCH_SIZE = 50


def advance_schedule(current: datetime, frequency: str) -> datetime:
    """Advance next_run_at by one period.

    Uses relativedelta for 'monthly' — timedelta cannot do "+1 calendar month"
    correctly (months have variable day counts). For daily/weekly, relativedelta
    and timedelta produce the same result, but we use relativedelta for consistency.
    """
    if frequency == "daily":
        return current + relativedelta(days=1)
    elif frequency == "weekly":
        return current + relativedelta(weeks=1)
    elif frequency == "monthly":
        return current + relativedelta(months=1)
    raise ValueError(f"Unknown frequency: {frequency}")


async def scheduler_loop(
    db_session_factory: async_sessionmaker,
    redis: Redis,
) -> None:
    """Main scheduler loop. Runs until cancelled.

    The entire loop body is wrapped in try/except. A single DB hiccup or
    network blip must NOT kill the scheduler — that would silently leave
    scheduled payments un-executed until app restart.
    """
    while True:
        try:
            await _poll_and_execute(db_session_factory, redis)
        except Exception:
            logger.exception("Scheduler loop error")

        await asyncio.sleep(POLL_INTERVAL_SECONDS)


async def _poll_and_execute(
    db_session_factory: async_sessionmaker,
    redis: Redis,
) -> None:
    """One poll cycle: claim due payments, then execute each."""
    # --- Phase 1: Claim due payments (short TX) ---
    # TODO: student — implement the claim query
    #
    # Open a session via db_session_factory(). Inside a transaction (db.begin()):
    #   1. SELECT ScheduledPayment WHERE status == 'active'
    #      AND next_run_at <= NOW()
    #      WITH FOR UPDATE SKIP LOCKED
    #      LIMIT CLAIM_BATCH_SIZE
    #   2. Collect all results into due_payments list.
    #   3. Transaction commits → locks released.
    #
    # NOTE: After commit, the ORM objects in due_payments are detached. Accessing
    # their attributes still works because db_factory has expire_on_commit=False.
    # If that setting ever changes, this will break with lazy-load errors.
    #
    # Hint:
    #   async with db_session_factory() as db:
    #       async with db.begin():
    #           result = await db.execute(
    #               select(ScheduledPayment)
    #               .where(ScheduledPayment.status == "active")
    #               .where(ScheduledPayment.next_run_at <= datetime.now(timezone.utc))
    #               .with_for_update(skip_locked=True)
    #               .limit(CLAIM_BATCH_SIZE)
    #           )
    #           due_payments = result.scalars().all()
    due_payments: list[ScheduledPayment] = []
    # TODO: student — replace the empty list above with the claim query result

    # --- Phase 2: Execute each payment ---
    for payment in due_payments:
        try:
            await _execute_payment(db_session_factory, redis, payment)
        except Exception:
            logger.exception("Failed to execute scheduled payment %s", payment.id)


async def _execute_payment(
    db_session_factory: async_sessionmaker,
    redis: Redis,
    payment: ScheduledPayment,
) -> None:
    """Execute a single scheduled payment.

    Step A: Call transfer() (manages its own DB commit + idempotency).
    Step B: Advance schedule + write execution log + publish event (own TX).

    Crash between A and B: next poll re-claims the same payment (next_run_at
    not yet advanced). transfer() idempotency key prevents double-debit.
    """
    idempotency_key = f"scheduled:{payment.id}:{payment.next_run_at.isoformat()}"
    skip_reason: str | None = None
    transfer_id: UUID | None = None

    # --- Step A: Execute the transfer ---
    # TODO: student — implement the transfer execution
    #
    # Try calling transfer() with the payment parameters:
    #   async with db_session_factory() as db:
    #       result = await transfer(
    #           db=db,
    #           redis=redis,
    #           from_account_id=payment.from_account_id,
    #           to_account_id=payment.to_account_id,
    #           amount=payment.amount,
    #           idempotency_key=idempotency_key,
    #       )
    #       transfer_id = UUID(result.transfer_id)
    #
    # Catch specific exceptions:
    #   - InsufficientBalanceError → skip_reason = "INSUFFICIENT_BALANCE"
    #   - AccountNotFoundError → skip_reason = "ACCOUNT_INACTIVE"
    #   - Any other Exception → return early (do NOT advance schedule; retry next cycle)
    #
    # The idempotency key guarantees no double-execution on retry.

    # --- Step B: Advance schedule + write execution log + outbox ---
    # TODO: student — implement the schedule advancement
    #
    # Open a session. Inside a transaction:
    #   1. Re-load the payment row with FOR UPDATE:
    #      fresh = await db.execute(
    #          select(ScheduledPayment).where(ScheduledPayment.id == payment.id).with_for_update()
    #      )
    #      fresh_payment = fresh.scalar_one()
    #
    #   2. Idempotency guard: if next_run_at already advanced, another instance handled it:
    #      if fresh_payment.next_run_at != payment.next_run_at:
    #          return
    #
    #   3. Write execution log:
    #      db.add(ScheduledPaymentExecution(
    #          scheduled_payment_id=payment.id,
    #          scheduled_for=payment.next_run_at,
    #          result="executed" if skip_reason is None else "skipped",
    #          skip_reason=skip_reason,
    #          transfer_id=transfer_id,
    #          executed_at=datetime.now(timezone.utc),
    #      ))
    #
    #   4. Advance schedule:
    #      fresh_payment.next_run_at = advance_schedule(fresh_payment.next_run_at, fresh_payment.frequency)
    #      fresh_payment.updated_at = datetime.now(timezone.utc)
    #
    #   5. Publish event:
    #      if skip_reason:
    #          publish_event(db, "payment.events", "payment.skipped", PaymentSkippedPayload(...))
    #      else:
    #          publish_event(db, "payment.events", "payment.executed", PaymentExecutedPayload(...))
    pass
