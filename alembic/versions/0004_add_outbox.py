"""Add outbox table for transactional event delivery

Revision ID: 0004
Revises: 0003
Create Date: 2026-05-03 00:00:00.000000

"""
from alembic import op

revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE outbox (
            id           UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
            topic        VARCHAR(100) NOT NULL,
            event_type   VARCHAR(100) NOT NULL,
            payload      JSONB        NOT NULL,
            status       VARCHAR(20)  NOT NULL DEFAULT 'pending',
            retry_count  INT          NOT NULL DEFAULT 0,
            created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
            published_at TIMESTAMPTZ
        )
    """)
    # Partial index: relay only scans pending rows
    op.execute("""
        CREATE INDEX idx_outbox_pending
        ON outbox (created_at)
        WHERE status = 'pending'
    """)
    # Recovery index: find stuck 'publishing' rows
    op.execute("""
        CREATE INDEX idx_outbox_publishing
        ON outbox (created_at)
        WHERE status = 'publishing'
    """)


def downgrade() -> None:
    op.execute("DROP TABLE outbox")
