"""Add failure_code and updated_at to transfers

Revision ID: 0002
Revises: 0001
Create Date: 2026-04-29 00:00:00.000000

"""
from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Record why a transfer failed — NULL for successful transfers
    op.execute("ALTER TABLE transfers ADD COLUMN failure_code VARCHAR(50)")

    # Track state changes — useful for Phase 3 sagas
    op.execute("ALTER TABLE transfers ADD COLUMN updated_at TIMESTAMPTZ")


def downgrade() -> None:
    op.execute("ALTER TABLE transfers DROP COLUMN failure_code")
    op.execute("ALTER TABLE transfers DROP COLUMN updated_at")
