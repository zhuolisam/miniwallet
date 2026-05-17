from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, field_validator


_ALLOWED_DESTINATION_TYPES = {"bank_transfer", "card_withdrawal"}


class WithdrawalRequest(BaseModel):
    """Body for POST /v1/withdrawals.

    `destination_details` is stored as-is — different rails require different
    fields (sort_code+account_number for UK Faster Payments, iban for SEPA,
    routing_number+account for US ACH, etc.). We are not the authority on
    what's valid; the rail validates and returns a failure_code if not.
    """
    amount: str
    currency: str = "USD"
    destination_type: str
    destination_details: dict

    @field_validator("amount")
    @classmethod
    def validate_amount(cls, v: str) -> str:
        if Decimal(v) <= 0:
            raise ValueError("amount must be positive")
        return v

    @field_validator("destination_type")
    @classmethod
    def validate_destination_type(cls, v: str) -> str:
        if v not in _ALLOWED_DESTINATION_TYPES:
            raise ValueError(
                f"destination_type must be one of: {sorted(_ALLOWED_DESTINATION_TYPES)}"
            )
        return v


class WithdrawalResponse(BaseModel):
    """Shape returned by POST /v1/withdrawals and GET /v1/withdrawals/{id}.

    Note the response is a point-in-time snapshot. Clients MUST poll
    GET /v1/withdrawals/{id} to observe terminal state (completed | failed)
    — same semantics as Stripe's idempotency caching.
    """
    withdrawal_id: str
    account_id: str
    amount: str
    currency: str
    status: str
    destination_type: str
    failure_code: str | None = None
    external_reference: str | None = None
    created_at: datetime
    submitted_at: datetime | None = None
    completed_at: datetime | None = None
