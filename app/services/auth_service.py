import uuid
import secrets
from datetime import datetime, timezone, timedelta
import bcrypt
import jwt
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from redis.asyncio import Redis

from app.config import settings
from app.models.user import User
from app.schemas.auth import TokenResponse
from app.exceptions import (
    EmailAlreadyExistsError,
    InvalidCredentialsError,
    InvalidRefreshTokenError,
)


async def register(db: AsyncSession, email: str, password: str) -> User:
    hashed = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
    now = datetime.now(timezone.utc)
    user = User(
        id=uuid.uuid4(),
        email=email,
        hashed_password=hashed,
        created_at=now,
        updated_at=now,
    )
    db.add(user)
    try:
        await db.commit()
    except IntegrityError:
        await db.rollback()
        raise EmailAlreadyExistsError()
    await db.refresh(user)
    return user


async def login(db: AsyncSession, redis: Redis, email: str, password: str) -> TokenResponse:
    result = await db.execute(select(User).where(User.email == email))
    user = result.scalar_one_or_none()
    if user is None or not bcrypt.checkpw(password.encode(), user.hashed_password.encode()):
        raise InvalidCredentialsError()

    access_token = _create_access_token(str(user.id))
    refresh_token = secrets.token_urlsafe(32)
    ttl = settings.refresh_token_expire_days * 86400
    await redis.setex(f"refresh:{refresh_token}", ttl, str(user.id))

    return TokenResponse(
        access_token=access_token,
        refresh_token=refresh_token,
        token_type="bearer",
        expires_in=settings.access_token_expire_minutes * 60,
    )


async def refresh_token(redis: Redis, old_token: str) -> TokenResponse:
    user_id = await redis.get(f"refresh:{old_token}")
    if user_id is None:
        raise InvalidRefreshTokenError()

    await redis.delete(f"refresh:{old_token}")

    new_access = _create_access_token(user_id)
    new_refresh = secrets.token_urlsafe(32)
    ttl = settings.refresh_token_expire_days * 86400
    await redis.setex(f"refresh:{new_refresh}", ttl, user_id)

    return TokenResponse(
        access_token=new_access,
        refresh_token=new_refresh,
        token_type="bearer",
        expires_in=settings.access_token_expire_minutes * 60,
    )


def _create_access_token(user_id: str) -> str:
    expire = datetime.now(timezone.utc) + timedelta(minutes=settings.access_token_expire_minutes)
    return jwt.encode(
        {"sub": user_id, "exp": expire},
        settings.jwt_secret,
        algorithm=settings.jwt_algorithm,
    )
