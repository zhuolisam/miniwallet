import uuid
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import String, DateTime, ForeignKey, Numeric, event
from sqlalchemy.dialects.postgresql import JSONB, UUID as PG_UUID
from sqlalchemy.orm import Mapped, Session, mapped_column

from app.database import Base


class Withdrawal(Base):
    __tablename__ = "withdrawals"

    id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    account_id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), ForeignKey("accounts.id"), nullable=False)
    amount: Mapped[Decimal] = mapped_column(Numeric(19, 4), nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False, default="USD")
    # pending → submitted → completed | failed
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="pending")
    # Populated only on status='failed'. Valid values:
    # INVALID_ACCOUNT | BENEFICIARY_CLOSED | TIMEOUT | NETWORK_ERROR | CIRCUIT_OPEN
    failure_code: Mapped[str | None] = mapped_column(String(50), nullable=True)
    # bank_transfer | card_withdrawal
    destination_type: Mapped[str] = mapped_column(String(30), nullable=False)
    # Opaque per-rail details. { "sort_code": "...", "account_number": "..." } for UK,
    # { "iban": "..." } for SEPA, etc. We do NOT schema-validate — the rail does.
    destination_details: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    # Rail's transaction ID. NULL until the rail accepts the submission and returns its ref.
    # Populated in TX 3a on success.
    external_reference: Mapped[str | None] = mapped_column(String(255), nullable=True)
    # Client-supplied via Idempotency-Key header. UNIQUE is the hard safety net.
    idempotency_key: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    submitted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


# ---------------------------------------------------------------------------
# updated_at enforcement — critical for the Week 12 saga-recovery job.
#
# The recovery sweeper finds stuck rows via `WHERE updated_at < cutoff`. If a
# new code path transitions a Withdrawal row but forgets to refresh updated_at,
# that row becomes invisible to recovery until `created_at` ages past the
# cutoff — potentially much longer than the 5-minute recovery window.
#
# This before_flush listener auto-refreshes updated_at on ANY modified
# Withdrawal instance, so correctness does not depend on the developer
# remembering to set it in every branch of the saga.
#
# This is a session-level event (not a mapper event) — `session.dirty` is
# the set of pending modifications. See SYSTEM-DESIGN.md Section 4 "Why
# updated_at matters for recovery".
# ---------------------------------------------------------------------------
@event.listens_for(Session, "before_flush")
def _set_withdrawal_updated_at(session, flush_context, instances):
    for obj in session.dirty:
        if isinstance(obj, Withdrawal):
            obj.updated_at = datetime.now(timezone.utc)
