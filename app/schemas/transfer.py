from datetime import datetime
from pydantic import BaseModel, field_validator
from decimal import Decimal

class TransferRequest(BaseModel):
    to_email: str | None = None
    to_account_id: str | None = None
    amount: str

    @field_validator("amount")
    @classmethod
    def validate_amount(cls, v: str) -> str:
        d = Decimal(v)
        if d <= 0:
            raise ValueError("amount must be positive")
        return v

class TransferResponse(BaseModel):
    transfer_id: str
    from_account_id: str
    to_account_id: str
    amount: str
    status: str
    failure_code: str | None = None
    created_at: datetime
