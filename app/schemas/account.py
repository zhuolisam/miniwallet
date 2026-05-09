from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, field_validator


class AccountResponse(BaseModel):
    account_id: str
    user_id: str
    status: str
    balance: str
    created_at: datetime


class AccountSummaryResponse(BaseModel):
    """GET /accounts/me — same as AccountResponse but without user_id (spec omits it)."""
    account_id: str
    status: str
    balance: str
    created_at: datetime

class BalanceResponse(BaseModel):
    account_id: str
    balance: str
    as_of: datetime

class TransactionItem(BaseModel):
    entry_id: str
    direction: str  # "credit" | "debit"
    amount: str
    currency: str
    entry_type: str
    reference_id: str | None
    created_at: datetime

class SeedRequest(BaseModel):
    account_id: str
    amount: str

    @field_validator("amount")
    @classmethod
    def validate_amount(cls, v: str) -> str:
        d = Decimal(v)
        if d <= 0:
            raise ValueError("amount must be positive")
        return v

class SeedResponse(BaseModel):
    entry_id: str
    account_id: str
    amount: str
    new_balance: str
