from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, field_validator


# Supported source types (matches SYSTEM-DESIGN Section 3).
_ALLOWED_SOURCE_TYPES = {"bank_transfer", "card_topup", "direct_debit"}


class SimulateDepositRequest(BaseModel):
    """Body for POST /v1/dev/simulate-deposit.

    Mimics a real bank partner webhook (ClearBank / Modulr / Railsr). The rail
    supplies `external_reference`; our service uses it as the idempotency key.
    """
    account_id: str
    amount: str
    currency: str = "USD"
    source_type: str
    external_reference: str

    @field_validator("amount")
    @classmethod
    def validate_amount(cls, v: str) -> str:
        if Decimal(v) <= 0:
            raise ValueError("amount must be positive")
        return v

    @field_validator("source_type")
    @classmethod
    def validate_source_type(cls, v: str) -> str:
        if v not in _ALLOWED_SOURCE_TYPES:
            raise ValueError(
                f"source_type must be one of: {sorted(_ALLOWED_SOURCE_TYPES)}"
            )
        return v


class DepositResponse(BaseModel):
    """Returned by POST /v1/dev/simulate-deposit and GET /v1/deposits/{id}.

    On a duplicate webhook (same `external_reference`), the service returns
    the *original* record with status='completed' or 'rejected' — NOT a fresh
    pending record. Clients observing pending status should poll GET /v1/deposits/{id}.
    """
    deposit_id: str
    account_id: str
    amount: str
    currency: str
    status: str
    source_type: str
    external_reference: str
    rejection_reason: str | None = None
    created_at: datetime
    completed_at: datetime | None = None
