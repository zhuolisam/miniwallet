"""Add deposits table (Phase 3 / Week 10).

Revision ID: 0007
Revises: 0006
"""

from alembic import op


revision = "0007"
down_revision = "0006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE deposits (
            id                  UUID           PRIMARY KEY DEFAULT gen_random_uuid(),
            account_id          UUID           NOT NULL,
            amount              NUMERIC(19,4)  NOT NULL CHECK (amount > 0),
            currency            VARCHAR(3)     NOT NULL DEFAULT 'USD',
            status              VARCHAR(20)    NOT NULL DEFAULT 'pending',
            source_type         VARCHAR(30)    NOT NULL,
            external_reference  VARCHAR(255)   NOT NULL UNIQUE,
            rejection_reason    VARCHAR(100),
            created_at          TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
            completed_at        TIMESTAMPTZ,
            updated_at          TIMESTAMPTZ    NOT NULL DEFAULT NOW()
        )
    """)
    # Supports future GET /v1/deposits list endpoint and audit queries scoped to an account.
    op.execute("""
        CREATE INDEX idx_deposits_account_id
        ON deposits (account_id, created_at DESC)
    """)


def downgrade() -> None:
    op.execute("DROP TABLE deposits")
