"""TransactionActivity — CQRS read model for GET /v1/accounts/me/transactions.

Built exclusively by the activity-consumer from Kafka events. The API reads
from this table; it never writes to it directly. One transfer.completed event
creates TWO rows (debit for sender, credit for receiver). One seed.completed
event creates ONE row (credit for the account owner).

Unique constraint on (event_id, account_id) ensures consumer idempotency:
replaying an event produces no duplicate rows.
"""

import uuid
from datetime import datetime
from decimal import Decimal

from sqlalchemy import String, DateTime, Numeric, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.dialects.postgresql import UUID as PG_UUID

from app.database import Base


class TransactionActivity(Base):
    __tablename__ = "transaction_activity"

    id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    event_id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    account_id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    direction: Mapped[str] = mapped_column(String(10), nullable=False)  # "debit" | "credit"
    amount: Mapped[Decimal] = mapped_column(Numeric(19, 4), nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False, default="USD")
    entry_type: Mapped[str] = mapped_column(String(30), nullable=False)  # "transfer" | "seed" | "deposit" | "withdrawal" | "withdrawal_reversal"
    reference_id: Mapped[uuid.UUID | None] = mapped_column(PG_UUID(as_uuid=True), nullable=True)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        UniqueConstraint("event_id", "account_id", name="uq_activity_event_account"),
    )
