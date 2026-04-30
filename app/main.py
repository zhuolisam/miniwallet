from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from app.exceptions import MiniBankError
from app.middleware.correlation_id import CorrelationIDMiddleware
from app.routers import accounts, auth, dev, transfers, users


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield


async def _minibank_error_handler(request: Request, exc: MiniBankError) -> JSONResponse:
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": {"code": exc.error_code, "message": exc.message}},
    )


async def _validation_error_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    return JSONResponse(
        status_code=422,
        content={"error": {"code": "VALIDATION_ERROR", "message": "Request validation failed"}},
    )


def create_app() -> FastAPI:
    app = FastAPI(title="MiniBank", version="1.0.0", lifespan=lifespan)

    app.add_middleware(CorrelationIDMiddleware)

    # Single handler for all domain errors — HTTP metadata lives on the exception class
    app.add_exception_handler(MiniBankError, _minibank_error_handler)
    # Override FastAPI's default validation error format to match our error envelope
    app.add_exception_handler(RequestValidationError, _validation_error_handler)

    app.include_router(auth.router,      prefix="/v1/auth",      tags=["auth"])
    app.include_router(users.router,     prefix="/v1/users",     tags=["users"])
    app.include_router(accounts.router,  prefix="/v1/accounts",  tags=["accounts"])
    app.include_router(transfers.router, prefix="/v1/transfers",  tags=["transfers"])
    app.include_router(dev.router,       prefix="/v1/dev",        tags=["dev"])

    return app


app = create_app()
