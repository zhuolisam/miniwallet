"""Outbox table — transactional event guarantee.

Every domain event is written as an OutboxRow in the same DB transaction
as the state change it represents. A separate relay process (outbox_relay)
claims pending rows, publishes them to Kafka, and marks them delivered.

This eliminates the dual-write problem from Week 6: if the process crashes
after the DB commit, the outbox row is already persisted. The relay will
pick it up and deliver it when it restarts.
"""

import uuid

from sqlalchemy import Column, DateTime, Index, Integer, String, text
from sqlalchemy.dialects.postgresql import JSONB, UUID

from app.database import Base


class OutboxRow(Base):
    __tablename__ = "outbox"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    topic = Column(String(100), nullable=False)
    event_type = Column(String(100), nullable=False)
    payload = Column(JSONB, nullable=False)  # full event envelope (including event_id)
    status = Column(String(20), nullable=False, server_default=text("'pending'"))
    retry_count = Column(Integer, nullable=False, server_default=text("0"))
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=text("NOW()"))
    published_at = Column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        # Partial index: relay only scans pending rows — full table scan avoided
        Index("idx_outbox_pending", "created_at", postgresql_where=text("status = 'pending'")),
        # Recovery index: find stuck 'publishing' rows from crashed relay instances
        Index("idx_outbox_publishing", "created_at", postgresql_where=text("status = 'publishing'")),
    )
