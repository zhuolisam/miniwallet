import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from app.exceptions import MiniBankError
from app.middleware.correlation_id import CorrelationIDMiddleware
from app.routers import (
    accounts, auth, deposits, dev, health, scheduled_payments, transfers, users, withdrawals,
)

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    from app.circuit_breaker import CircuitBreaker
    from app.database import db_factory
    from app.dependencies import get_redis
    from rail.simulator import BankRailSimulator
    from workers.saga_recovery import recover_stuck_withdrawals
    from workers.payment_scheduler import scheduler_loop

    # Initialize singletons
    rail = BankRailSimulator()
    app.state.rail = rail

    # Redis client for background workers
    redis = await get_redis()

    # Run recovery on startup (fire-and-forget — catches crashes from last downtime)
    circuit_breaker = CircuitBreaker(redis=redis)
    startup_recovery = asyncio.create_task(
        recover_stuck_withdrawals(db_factory, circuit_breaker, rail)
    )

    # Start background loops
    recovery_task = asyncio.create_task(
        _recovery_loop(db_factory, redis, rail)
    )
    scheduler_task = asyncio.create_task(
        scheduler_loop(db_factory, redis)
    )

    yield

    # Shutdown: cancel background tasks
    for task in (startup_recovery, recovery_task, scheduler_task):
        task.cancel()


async def _recovery_loop(db_session_factory, redis, rail):
    """Run saga recovery every 5 minutes."""
    from app.circuit_breaker import CircuitBreaker
    from workers.saga_recovery import recover_stuck_withdrawals

    while True:
        await asyncio.sleep(300)
        try:
            circuit_breaker = CircuitBreaker(redis=redis)
            await recover_stuck_withdrawals(db_session_factory, circuit_breaker, rail)
        except Exception:
            logger.exception("Saga recovery loop error")


async def _minibank_error_handler(request: Request, exc: MiniBankError) -> JSONResponse:
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": {"code": exc.error_code, "message": exc.message}},
    )


async def _validation_error_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    details = [
        {"field": ".".join(str(loc) for loc in err["loc"] if loc != "body"), "message": err["msg"]}
        for err in exc.errors()
    ]
    return JSONResponse(
        status_code=422,
        content={"error": {"code": "VALIDATION_ERROR", "message": "Request validation failed", "details": details}},
    )


def create_app() -> FastAPI:
    app = FastAPI(title="MiniBank", version="1.0.0", lifespan=lifespan)

    app.add_middleware(CorrelationIDMiddleware)

    app.add_exception_handler(MiniBankError, _minibank_error_handler)
    app.add_exception_handler(RequestValidationError, _validation_error_handler)

    app.include_router(auth.router,               prefix="/v1/auth",               tags=["auth"])
    app.include_router(users.router,              prefix="/v1/users",              tags=["users"])
    app.include_router(accounts.router,           prefix="/v1/accounts",           tags=["accounts"])
    app.include_router(transfers.router,          prefix="/v1/transfers",          tags=["transfers"])
    app.include_router(deposits.router,           prefix="/v1/deposits",           tags=["deposits"])
    app.include_router(withdrawals.router,        prefix="/v1/withdrawals",        tags=["withdrawals"])
    app.include_router(scheduled_payments.router, prefix="/v1/scheduled-payments", tags=["scheduled-payments"])
    app.include_router(health.router,             prefix="/v1",                    tags=["health"])
    app.include_router(dev.router,                prefix="/v1/dev",                tags=["dev"])

    return app


app = create_app()
