"""Add reference_id column to ledger_entries.

Revision ID: 0006
Revises: 0005
"""

from alembic import op

revision = "0006"
down_revision = "0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE ledger_entries ADD COLUMN reference_id UUID;
    """)
    op.execute("""
        CREATE INDEX idx_ledger_entries_reference_id
        ON ledger_entries (reference_id) WHERE reference_id IS NOT NULL;
    """)


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_ledger_entries_reference_id;")
    op.execute("ALTER TABLE ledger_entries DROP COLUMN IF EXISTS reference_id;")
