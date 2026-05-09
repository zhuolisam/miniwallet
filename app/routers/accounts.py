from datetime import datetime, timezone

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.dependencies import get_current_user
from app.models.user import User
from app.exceptions import AccountNotFoundError
from app.schemas.account import (
    AccountResponse,
    AccountSummaryResponse,
    BalanceResponse,
    TransactionItem,
)
from app.schemas.common import DataResponse, PaginatedResponse
import app.services.account_service as account_service

router = APIRouter()


@router.post("", status_code=201, response_model=DataResponse[AccountResponse])
async def open_account(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    account = await account_service.open_account(db, current_user.id)
    balance = await account_service.get_balance(db, account.id)
    return {"data": {
        "account_id": str(account.id),
        "user_id": str(account.user_id),
        "status": account.status,
        "balance": f"{balance:.4f}",
        "created_at": account.created_at.isoformat(),
    }}


@router.get("/me", response_model=DataResponse[AccountSummaryResponse])
async def get_account(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    account = await account_service.get_account_by_user(db, current_user.id)
    if account is None:
        raise AccountNotFoundError()
    balance = await account_service.get_balance(db, account.id)
    return {"data": {
        "account_id": str(account.id),
        "status": account.status,
        "balance": f"{balance:.4f}",
        "created_at": account.created_at.isoformat(),
    }}


@router.get("/me/balance", response_model=DataResponse[BalanceResponse])
async def get_balance(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    account = await account_service.get_account_by_user(db, current_user.id)
    if account is None:
        raise AccountNotFoundError()
    balance = await account_service.get_balance(db, account.id)
    return {"data": {
        "account_id": str(account.id),
        "balance": f"{balance:.4f}",
        "as_of": datetime.now(timezone.utc).isoformat(),
    }}


@router.get("/me/transactions", response_model=PaginatedResponse[TransactionItem])
async def get_transactions(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    from_date: datetime | None = Query(None),
    to_date: datetime | None = Query(None),
    entry_type: str | None = Query(None),
):
    account = await account_service.get_account_by_user(db, current_user.id)
    if account is None:
        raise AccountNotFoundError()
    items, total, as_of = await account_service.get_transactions(
        db, account.id, page, limit, from_date, to_date, entry_type
    )
    return {
        "data": [item.model_dump() for item in items],
        "meta": {
            "total": total,
            "page": page,
            "limit": limit,
            "as_of": as_of.isoformat() if as_of else None,
        },
    }
