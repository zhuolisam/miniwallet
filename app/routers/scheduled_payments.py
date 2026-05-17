"""Scheduled payments router — CRUD for recurring payments.

POST /v1/scheduled-payments — create a recurring payment
GET  /v1/scheduled-payments — list user's scheduled payments
DELETE /v1/scheduled-payments/{id} — cancel (soft-delete)

All endpoints are JWT-protected. from_account_id is derived from the JWT
(same one-account-per-user assumption as the rest of the API).
"""

from uuid import UUID

from fastapi import APIRouter, Depends
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.dependencies import get_current_user, get_redis
from app.exceptions import AccountNotFoundError
from app.models.user import User
from app.schemas.common import DataResponse
from app.schemas.scheduled_payment import (
    ScheduledPaymentRequest,
    ScheduledPaymentResponse,
)
import app.services.account_service as account_service
import app.services.scheduled_payment_service as scheduled_payment_service

router = APIRouter()


@router.post("", status_code=201, response_model=DataResponse[ScheduledPaymentResponse])
async def create_scheduled_payment(
    body: ScheduledPaymentRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Create a recurring payment schedule."""
    sender_account = await account_service.get_account_by_user(db, current_user.id)
    if sender_account is None:
        raise AccountNotFoundError()

    result = await scheduled_payment_service.create_scheduled_payment(
        db=db,
        from_account_id=sender_account.id,
        to_account_id=UUID(body.to_account_id),
        amount=body.validated_amount,
        frequency=body.frequency,
        start_at=body.start_at,
    )
    return {"data": result.model_dump()}


@router.get("", response_model=DataResponse[list[ScheduledPaymentResponse]])
async def list_scheduled_payments(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """List the caller's scheduled payments (active and cancelled)."""
    sender_account = await account_service.get_account_by_user(db, current_user.id)
    if sender_account is None:
        raise AccountNotFoundError()

    results = await scheduled_payment_service.list_scheduled_payments(
        db=db,
        from_account_id=sender_account.id,
    )
    return {"data": [r.model_dump() for r in results]}


@router.delete("/{payment_id}", status_code=204)
async def cancel_scheduled_payment(
    payment_id: str,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Cancel a scheduled payment (soft-delete). Returns 204 on success."""
    sender_account = await account_service.get_account_by_user(db, current_user.id)
    if sender_account is None:
        raise AccountNotFoundError()

    await scheduled_payment_service.cancel_scheduled_payment(
        db=db,
        payment_id=UUID(payment_id),
        from_account_id=sender_account.id,
    )
