"""Add scheduled_payments and scheduled_payment_executions tables.

Revision ID: 0009
Revises: 0008
"""

from alembic import op


revision = "0009"
down_revision = "0008"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE scheduled_payments (
            id              UUID          PRIMARY KEY DEFAULT gen_random_uuid(),
            from_account_id UUID          NOT NULL REFERENCES accounts(id),
            to_account_id   UUID          NOT NULL REFERENCES accounts(id),
            amount          NUMERIC(19,4) NOT NULL CHECK (amount > 0),
            currency        VARCHAR(3)    NOT NULL DEFAULT 'USD',
            frequency       VARCHAR(20)   NOT NULL CHECK (frequency IN ('daily', 'weekly', 'monthly')),
            next_run_at     TIMESTAMPTZ   NOT NULL,
            status          VARCHAR(20)   NOT NULL DEFAULT 'active',
            created_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
            updated_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW()
        );

        CREATE INDEX idx_scheduled_payments_due
            ON scheduled_payments (next_run_at)
            WHERE status = 'active';

        CREATE TABLE scheduled_payment_executions (
            id                    UUID          PRIMARY KEY DEFAULT gen_random_uuid(),
            scheduled_payment_id  UUID          NOT NULL REFERENCES scheduled_payments(id),
            scheduled_for         TIMESTAMPTZ   NOT NULL,
            result                VARCHAR(20)   NOT NULL,
            skip_reason           VARCHAR(100),
            transfer_id           UUID,
            executed_at           TIMESTAMPTZ   NOT NULL DEFAULT NOW()
        );

        CREATE INDEX idx_spe_payment_id
            ON scheduled_payment_executions (scheduled_payment_id, executed_at DESC);
    """)


def downgrade() -> None:
    op.execute("""
        DROP TABLE IF EXISTS scheduled_payment_executions;
        DROP TABLE IF EXISTS scheduled_payments;
    """)
