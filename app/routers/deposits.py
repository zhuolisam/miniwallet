"""Deposit router — GET /v1/deposits/{id} only.

POST /v1/dev/simulate-deposit lives in routers/dev.py because it is a
dev-only endpoint (guarded by APP_ENV='development'), mirroring /v1/dev/seed.
The production equivalent of simulate-deposit would be a webhook handler
validated by partner HMAC signature, which is out of scope here.
"""

from uuid import UUID

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.dependencies import get_current_user
from app.exceptions import AccountNotFoundError
from app.models.user import User
from app.schemas.common import DataResponse
from app.schemas.deposit import DepositResponse
import app.services.account_service as account_service
import app.services.deposit_service as deposit_service


router = APIRouter()


@router.get("/{deposit_id}", response_model=DataResponse[DepositResponse])
async def get_deposit(
    deposit_id: str,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Return the caller's own deposit. 404 if not found OR not owned by caller."""
    account = await account_service.get_account_by_user(db, current_user.id)
    if account is None:
        raise AccountNotFoundError()
    result = await deposit_service.get_deposit(db, UUID(deposit_id), account.id)
    return {"data": result.model_dump()}
