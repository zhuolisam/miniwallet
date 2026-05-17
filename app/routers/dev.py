from decimal import Decimal
from uuid import UUID

from fastapi import APIRouter, Depends, Header
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import get_db
from app.dependencies import get_current_user
from app.models.user import User
from app.schemas.account import SeedRequest, SeedResponse
from app.schemas.common import DataResponse
from app.schemas.deposit import DepositResponse, SimulateDepositRequest
from app.exceptions import ForbiddenError
import app.services.account_service as account_service
import app.services.deposit_service as deposit_service

router = APIRouter()


@router.post("/seed", status_code=201, response_model=DataResponse[SeedResponse])
async def seed(
    body: SeedRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    idempotency_key: str = Header(alias="Idempotency-Key"),
):
    if settings.app_env != "development":
        raise ForbiddenError()
    result: SeedResponse = await account_service.seed(db, UUID(body.account_id), Decimal(body.amount), idempotency_key, current_user.id)
    return {"data": result.model_dump()}


@router.post("/simulate-deposit", status_code=201, response_model=DataResponse[DepositResponse])
async def simulate_deposit(
    body: SimulateDepositRequest,
    db: AsyncSession = Depends(get_db),
):
    """Simulate an inbound bank webhook.

    Dev-only (guarded by APP_ENV). In production, the equivalent endpoint would
    be a webhook handler invoked by the bank partner (ClearBank, Modulr) with
    an HMAC signature header — the caller is NOT an authenticated user.
    Accordingly, this endpoint does NOT require JWT auth (a bank partner has no
    JWT) but it IS guarded by APP_ENV='development' so it can't be hit in prod.

    Idempotency: the request body's `external_reference` is the idempotency key.
    A repeated webhook with the same reference returns the original record with
    its current status (completed or rejected), status 201 either way to match
    real webhook retry semantics.
    """
    #   1. If settings.app_env != "development" → raise ForbiddenError()
    if settings.app_env != "development":
        raise ForbiddenError()
    #   2. Call deposit_service.simulate_deposit
    result = await deposit_service.simulate_deposit(
        db=db,
        account_id=UUID(body.account_id),
        amount=Decimal(body.amount),
        currency=body.currency,
        source_type=body.source_type,
        external_reference=body.external_reference,
    )
    #   3. Return
    return {"data": result.model_dump()}
