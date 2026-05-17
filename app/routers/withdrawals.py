"""Withdrawal router — POST /v1/withdrawals and GET /v1/withdrawals/{id}.

Both endpoints are JWT-protected. The caller's account is derived from
`current_user.id` (Phase 1 assumes one account per user) — `account_id` is
NOT in the request body for the POST, preventing "send from someone else's
account" bugs at the router layer.
"""

from decimal import Decimal
from uuid import UUID

from fastapi import APIRouter, Depends, Header
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.dependencies import get_circuit_breaker, get_current_user, get_rail, get_redis
from app.exceptions import AccountNotFoundError, BankRailUnavailableError
from app.models.user import User
from app.schemas.common import DataResponse
from app.schemas.withdrawal import WithdrawalRequest, WithdrawalResponse
from rail.simulator import BankRailSimulator
import app.services.account_service as account_service
import app.services.withdrawal_service as withdrawal_service


router = APIRouter()


@router.post("", status_code=201, response_model=DataResponse[WithdrawalResponse])
async def create_withdrawal(
    body: WithdrawalRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    redis: Redis = Depends(get_redis),
    rail: BankRailSimulator = Depends(get_rail),
    circuit_breaker = Depends(get_circuit_breaker),
    idempotency_key: str = Header(alias="Idempotency-Key"),
):
    """Initiate a withdrawal. Blocks until the rail responds (saga runs inline).

    Pre-flight check: if the circuit breaker is OPEN, return 503 immediately.
    Don't debit the user just to compensate them — that's pointless churn.
    """
    # Circuit breaker pre-flight: fail fast if rail is known-down
    if not await circuit_breaker.is_call_allowed():
        raise BankRailUnavailableError()

    sender_account = await account_service.get_account_by_user(db, current_user.id)
    if sender_account is None:
        raise AccountNotFoundError()
    result = await withdrawal_service.create_withdrawal(
        db=db,
        redis=redis,
        rail=rail,
        account_id=sender_account.id,
        amount=Decimal(body.amount),
        currency=body.currency,
        destination_type=body.destination_type,
        destination_details=body.destination_details,
        idempotency_key=idempotency_key,
        actor_user_id=current_user.id,
        circuit_breaker=circuit_breaker,
    )
    return {"data": result.model_dump()}

@router.get("/{withdrawal_id}", response_model=DataResponse[WithdrawalResponse])
async def get_withdrawal(
    withdrawal_id: str,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Return the caller's own withdrawal. 404 if missing or not owned."""
    sender_account = await account_service.get_account_by_user(db, current_user.id)
    if sender_account is None:
        raise AccountNotFoundError()
    result = await withdrawal_service.get_withdrawal(
        db=db,
        withdrawal_id=UUID(withdrawal_id),
        requesting_account_id=sender_account.id,
    )
    return {"data": result.model_dump()}
