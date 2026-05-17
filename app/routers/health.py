"""Health endpoint — system liveness + circuit breaker state.

GET /v1/health returns:
- Overall status: "ok" (all checks pass) or "degraded" (one or more failed)
- Individual checks: database connectivity, Redis connectivity
- Circuit breaker state: CLOSED/OPEN/HALF_OPEN + failure count

Not JWT-protected — load balancers and monitoring tools call this.
"""

from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from redis.asyncio import Redis

from app.database import get_db
from app.dependencies import get_redis, get_circuit_breaker

router = APIRouter()


async def _check_db(db: AsyncSession) -> bool:
    try:
        await db.execute(text("SELECT 1"))
        return True
    except Exception:
        return False


async def _check_redis(redis: Redis) -> bool:
    try:
        await redis.ping()
        return True
    except Exception:
        return False


@router.get("/health")
async def health(
    db: AsyncSession = Depends(get_db),
    redis: Redis = Depends(get_redis),
    circuit_breaker=Depends(get_circuit_breaker),
):
    db_ok = await _check_db(db)
    redis_ok = await _check_redis(redis)

    return {
        "status": "ok" if (db_ok and redis_ok) else "degraded",
        "checks": {
            "database": "ok" if db_ok else "unavailable",
            "redis": "ok" if redis_ok else "unavailable",
        },
        "circuit_breaker": await circuit_breaker.get_status(),
    }
