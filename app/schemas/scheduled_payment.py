from datetime import datetime
from decimal import Decimal, InvalidOperation

from pydantic import BaseModel, field_validator


class ScheduledPaymentRequest(BaseModel):
    to_account_id: str
    amount: str
    frequency: str  # daily | weekly | monthly
    start_at: datetime

    @field_validator("amount")
    @classmethod
    def amount_must_be_positive(cls, v: str) -> str:
        try:
            d = Decimal(v)
        except InvalidOperation:
            raise ValueError("amount must be a valid decimal number")
        if d <= 0:
            raise ValueError("amount must be greater than zero")
        return v

    @field_validator("frequency")
    @classmethod
    def frequency_must_be_valid(cls, v: str) -> str:
        if v not in ("daily", "weekly", "monthly"):
            raise ValueError("frequency must be one of: daily, weekly, monthly")
        return v

    @property
    def validated_amount(self) -> Decimal:
        return Decimal(self.amount)


class ScheduledPaymentResponse(BaseModel):
    id: str
    from_account_id: str
    to_account_id: str
    amount: str
    currency: str
    frequency: str
    next_run_at: datetime
    status: str
    created_at: datetime
