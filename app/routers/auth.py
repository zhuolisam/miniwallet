from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from redis.asyncio import Redis

from app.database import get_db
from app.dependencies import get_redis
from app.schemas.auth import RegisterRequest, LoginRequest, RefreshRequest, RegisterResponse, TokenResponse
from app.schemas.common import DataResponse
import app.services.auth_service as auth_service

router = APIRouter()


@router.post("/register", status_code=201, response_model=DataResponse[RegisterResponse])
async def register(body: RegisterRequest, db: AsyncSession = Depends(get_db)):
    user = await auth_service.register(db, body.email, body.password)
    return {"data": {"user_id": str(user.id), "email": user.email}}


@router.post("/login", response_model=DataResponse[TokenResponse])
async def login(body: LoginRequest, db: AsyncSession = Depends(get_db), redis: Redis = Depends(get_redis)):
    tokens: TokenResponse = await auth_service.login(db, redis, body.email, body.password)
    return {"data": tokens.model_dump()}


@router.post("/refresh", response_model=DataResponse[TokenResponse])
async def refresh(body: RefreshRequest, redis: Redis = Depends(get_redis)):
    tokens: TokenResponse = await auth_service.refresh_token(redis, body.refresh_token)
    return {"data": tokens.model_dump()}
