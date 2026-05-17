"""Scheduled payment service — CRUD for recurring payments.

This service handles creation, listing, and cancellation of scheduled payments.
The actual execution is handled by workers/payment_scheduler.py — this service
only manages the schedule definition.

Scheduled payments reuse Phase 1's transfer() for execution. No new banking
primitives here — it's pure orchestration of existing tools.
"""

from datetime import datetime, timezone
from decimal import Decimal
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.exceptions import (
    AccountNotFoundError,
    CannotPaySelfError,
    InvalidStartTimeError,
    ScheduledPaymentNotFoundError,
)
from app.models.account import Account
from app.models.scheduled_payment import ScheduledPayment
from app.schemas.scheduled_payment import ScheduledPaymentResponse


async def create_scheduled_payment(
    db: AsyncSession,
    from_account_id: UUID,
    to_account_id: UUID,
    amount: Decimal,
    frequency: str,
    start_at: datetime,
) -> ScheduledPaymentResponse:
    """Create a new recurring payment schedule.

    Validation:
    - start_at must be in the future
    - to_account_id must not equal from_account_id (CANNOT_PAY_SELF)
    - to_account_id must exist and be active (ACCOUNT_NOT_FOUND)

    Does NOT check sender's balance at creation time — balance is checked at
    execution time by transfer(). A user can set up a payment even if they
    currently lack funds; they just need funds when the payment fires.
    """
    # TODO: student — implement scheduled payment creation
    #
    # 1. Validate start_at is in the future:
    #    if start_at <= datetime.now(timezone.utc):
    #        raise InvalidStartTimeError()
    #
    # 2. Validate not paying self:
    #    if from_account_id == to_account_id:
    #        raise CannotPaySelfError()
    #
    # 3. Validate target account exists and is active:
    #    target = await db.execute(
    #        select(Account).where(Account.id == to_account_id)
    #    )
    #    target_account = target.scalar_one_or_none()
    #    if target_account is None or target_account.status != "active":
    #        raise AccountNotFoundError()
    #
    # 4. Create the ScheduledPayment row:
    #    now = datetime.now(timezone.utc)
    #    payment = ScheduledPayment(
    #        from_account_id=from_account_id,
    #        to_account_id=to_account_id,
    #        amount=amount,
    #        currency="USD",
    #        frequency=frequency,
    #        next_run_at=start_at,
    #        status="active",
    #        created_at=now,
    #        updated_at=now,
    #    )
    #    db.add(payment)
    #    await db.commit()
    #
    # 5. Return the response:
    #    return _payment_to_response(payment)
    pass


async def list_scheduled_payments(
    db: AsyncSession,
    from_account_id: UUID,
) -> list[ScheduledPaymentResponse]:
    """List all scheduled payments for an account (active and cancelled)."""
    # TODO: student — implement listing
    #
    # 1. Query all ScheduledPayment rows where from_account_id matches:
    #    result = await db.execute(
    #        select(ScheduledPayment)
    #        .where(ScheduledPayment.from_account_id == from_account_id)
    #        .order_by(ScheduledPayment.created_at.desc())
    #    )
    #    payments = result.scalars().all()
    #
    # 2. Return mapped responses:
    #    return [_payment_to_response(p) for p in payments]
    pass


async def cancel_scheduled_payment(
    db: AsyncSession,
    payment_id: UUID,
    from_account_id: UUID,
) -> None:
    """Cancel a scheduled payment (soft-delete — sets status='cancelled').

    Returns None on success. Raises ScheduledPaymentNotFoundError if the payment
    doesn't exist or doesn't belong to the caller.
    """
    # TODO: student — implement cancellation
    #
    # 1. Load the payment:
    #    result = await db.execute(
    #        select(ScheduledPayment).where(ScheduledPayment.id == payment_id)
    #    )
    #    payment = result.scalar_one_or_none()
    #
    # 2. Check ownership and existence:
    #    if payment is None or payment.from_account_id != from_account_id:
    #        raise ScheduledPaymentNotFoundError()
    #
    # 3. Set status to cancelled (idempotent — cancelling twice is fine):
    #    payment.status = "cancelled"
    #    payment.updated_at = datetime.now(timezone.utc)
    #    await db.commit()
    pass


def _payment_to_response(p: ScheduledPayment) -> ScheduledPaymentResponse:
    """Shape a ScheduledPayment ORM row into the API response model."""
    return ScheduledPaymentResponse(
        id=str(p.id),
        from_account_id=str(p.from_account_id),
        to_account_id=str(p.to_account_id),
        amount=f"{p.amount:.4f}",
        currency=p.currency,
        frequency=p.frequency,
        next_run_at=p.next_run_at,
        status=p.status,
        created_at=p.created_at,
    )
