from typing import Optional

import jwt
from fastapi import Depends
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jwt.exceptions import InvalidTokenError
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import get_db
from app.exceptions import UnauthorizedError
from app.models.user import User

# auto_error=False so that missing/malformed Authorization header returns None instead of 403.
# We raise our own UnauthorizedError (→ 401) rather than letting FastAPI return 403.
bearer_scheme = HTTPBearer(auto_error=False)

# --- Redis ---

_redis_client: Redis | None = None


async def get_redis() -> Redis:
    """FastAPI dependency — returns a shared Redis client (one connection pool per process)."""
    global _redis_client
    if _redis_client is None:
        _redis_client = Redis.from_url(settings.redis_url, decode_responses=True)
    return _redis_client


# --- Auth ---

async def get_current_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(bearer_scheme),
    db: AsyncSession = Depends(get_db),
) -> User:
    """
    FastAPI dependency for protected routes.
    Decodes JWT, loads the user from DB, raises 401 on any failure.
    Usage: current_user: User = Depends(get_current_user)
    """
    if credentials is None:
        raise UnauthorizedError()
    try:
        payload = jwt.decode(
            credentials.credentials,
            settings.jwt_secret,
            algorithms=[settings.jwt_algorithm],
        )
        user_id: str | None = payload.get("sub")
        if user_id is None:
            raise UnauthorizedError()
    except InvalidTokenError:
        raise UnauthorizedError()

    user = await db.get(User, user_id)
    if user is None:
        raise UnauthorizedError()
    return user
