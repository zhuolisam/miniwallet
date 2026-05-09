"""Add transaction_activity table (CQRS read model).

Revision ID: 0005
Revises: 0004
"""

from alembic import op

revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE transaction_activity (
            id           UUID           PRIMARY KEY DEFAULT gen_random_uuid(),
            event_id     UUID           NOT NULL,
            account_id   UUID           NOT NULL REFERENCES accounts(id),
            direction    VARCHAR(10)    NOT NULL,
            amount       NUMERIC(19,4)  NOT NULL,
            currency     VARCHAR(3)     NOT NULL DEFAULT 'USD',
            entry_type   VARCHAR(30)    NOT NULL,
            reference_id UUID,
            occurred_at  TIMESTAMPTZ    NOT NULL,
            CONSTRAINT uq_activity_event_account UNIQUE (event_id, account_id)
        );
    """)
    op.execute("""
        CREATE INDEX idx_activity_account
        ON transaction_activity (account_id, occurred_at DESC);
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS transaction_activity;")
