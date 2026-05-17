import uuid
from datetime import datetime

from sqlalchemy import String, DateTime, ForeignKey
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class ScheduledPaymentExecution(Base):
    __tablename__ = "scheduled_payment_executions"

    id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    scheduled_payment_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("scheduled_payments.id"), nullable=False
    )
    # The next_run_at value that triggered this execution
    scheduled_for: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    # executed | skipped
    result: Mapped[str] = mapped_column(String(20), nullable=False)
    # NULL if executed. e.g. "INSUFFICIENT_BALANCE", "ACCOUNT_INACTIVE"
    skip_reason: Mapped[str | None] = mapped_column(String(100), nullable=True)
    # References transfers.id if result='executed'. NULL if skipped.
    transfer_id: Mapped[uuid.UUID | None] = mapped_column(PG_UUID(as_uuid=True), nullable=True)
    executed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
