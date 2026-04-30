from uuid import UUID
from decimal import Decimal

from fastapi import APIRouter, Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession
from redis.asyncio import Redis

from app.database import get_db
from app.dependencies import get_current_user, get_redis
from app.models.user import User
from app.schemas.transfer import TransferRequest, TransferResponse
from app.schemas.common import DataResponse
from app.exceptions import (
    AccountNotFoundError,
    MissingIdempotencyKeyError,
    SameAccountError,
    UserNotFoundError,
)
import app.services.account_service as account_service
import app.services.user_service as user_service
import app.services.transfer_service as transfer_service

router = APIRouter()


@router.post("", status_code=201, response_model=DataResponse[TransferResponse])
async def create_transfer(
    body: TransferRequest,
    request: Request,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    redis: Redis = Depends(get_redis),
):
    idempotency_key = request.headers.get("Idempotency-Key")
    if idempotency_key is None:
        raise MissingIdempotencyKeyError()

    sender_account = await account_service.get_account_by_user(db, current_user.id)
    if sender_account is None:
        raise AccountNotFoundError()

    if body.to_email:
        recipient_user = await user_service.get_by_email(db, body.to_email)
        if recipient_user is None:
            raise UserNotFoundError()
        recipient_account = await account_service.get_account_by_user(db, recipient_user.id)
    elif body.to_account_id:
        recipient_account = await account_service.get_account_by_id(db, UUID(body.to_account_id))
    else:
        from fastapi import HTTPException
        raise HTTPException(status_code=422, detail={"code": "VALIDATION_ERROR", "message": "to_email or to_account_id required"})

    if recipient_account is None:
        raise AccountNotFoundError()

    if sender_account.id == recipient_account.id:
        raise SameAccountError()

    result: TransferResponse = await transfer_service.transfer(
        db=db,
        redis=redis,
        from_account_id=sender_account.id,
        to_account_id=recipient_account.id,
        amount=Decimal(body.amount),
        idempotency_key=idempotency_key,
    )
    return {"data": result.model_dump()}


@router.get("/{transfer_id}", response_model=DataResponse[TransferResponse])
async def get_transfer(
    transfer_id: str,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    account = await account_service.get_account_by_user(db, current_user.id)
    if account is None:
        raise AccountNotFoundError()
    result: TransferResponse = await transfer_service.get_transfer(db, UUID(transfer_id), account.id)
    return {"data": result.model_dump()}
