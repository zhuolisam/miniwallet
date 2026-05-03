"""Add audit_events table

Revision ID: 0003
Revises: 0002
Create Date: 2026-04-30 00:00:00.000000

"""
from alembic import op

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE audit_events (
            id           UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
            event_id     UUID         NOT NULL UNIQUE,
            event_type   VARCHAR(100) NOT NULL,
            actor_id     UUID,
            resource_id  UUID,
            resource_type VARCHAR(50),
            payload      JSONB        NOT NULL,
            occurred_at  TIMESTAMPTZ  NOT NULL
        )
    """)


def downgrade() -> None:
    op.execute("DROP TABLE audit_events")
