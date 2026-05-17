import uuid
from datetime import datetime
from decimal import Decimal

from sqlalchemy import String, DateTime, Numeric
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.dialects.postgresql import UUID as PG_UUID

from app.database import Base


class Deposit(Base):
    __tablename__ = "deposits"

    id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    account_id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    amount: Mapped[Decimal] = mapped_column(Numeric(19, 4), nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False, default="USD")
    # pending → completed | rejected
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="pending")
    # bank_transfer | card_topup | direct_debit
    source_type: Mapped[str] = mapped_column(String(30), nullable=False)
    # Rail's transaction ID. UNIQUE = natural idempotency guarantee — duplicate
    # webhooks from the partner land as IntegrityError at INSERT time, not business logic.
    external_reference: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    # Populated only when status='rejected'. Matches the exhaustive set documented
    # in SYSTEM-DESIGN Section 3: ACCOUNT_NOT_FOUND | ACCOUNT_NOT_ACTIVE | INVALID_AMOUNT | UNSUPPORTED_CURRENCY.
    rejection_reason: Mapped[str | None] = mapped_column(String(100), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
