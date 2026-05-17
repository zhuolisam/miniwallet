"""Add withdrawals table (Phase 3 / Week 11).

Revision ID: 0008
Revises: 0007
"""

from alembic import op


revision = "0008"
down_revision = "0007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE withdrawals (
            id                   UUID          PRIMARY KEY DEFAULT gen_random_uuid(),
            account_id           UUID          NOT NULL REFERENCES accounts(id),
            amount               NUMERIC(19,4) NOT NULL CHECK (amount > 0),
            currency             VARCHAR(3)    NOT NULL DEFAULT 'USD',
            status               VARCHAR(20)   NOT NULL DEFAULT 'pending',
            failure_code         VARCHAR(50),
            destination_type     VARCHAR(30)   NOT NULL,
            destination_details  JSONB         NOT NULL DEFAULT '{}'::jsonb,
            external_reference   VARCHAR(255),
            idempotency_key      VARCHAR(255)  UNIQUE NOT NULL,
            created_at           TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
            submitted_at         TIMESTAMPTZ,
            completed_at         TIMESTAMPTZ,
            updated_at           TIMESTAMPTZ   NOT NULL DEFAULT NOW()
        )
    """)
    # Partial index powering the Week 12 saga-recovery sweeper: find stuck rows
    # without a full-table scan. Created now so the Week 12 job "just works" on
    # day one — no schema change required later.
    op.execute("""
        CREATE INDEX idx_withdrawals_recovery
        ON withdrawals (updated_at)
        WHERE status IN ('pending', 'submitted')
    """)
    # Supports the user-facing GET /v1/withdrawals list (deferred to a later week
    # but the index is cheap to add alongside the table).
    op.execute("""
        CREATE INDEX idx_withdrawals_account_id
        ON withdrawals (account_id, created_at DESC)
    """)


def downgrade() -> None:
    op.execute("DROP TABLE withdrawals")
