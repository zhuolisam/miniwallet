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
from app.exceptions import ForbiddenError
import app.services.account_service as account_service

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
