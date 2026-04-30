"""Initial schema

Revision ID: 0001
Revises:
Create Date: 2024-01-01 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE users (
            id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
            email           VARCHAR(255) UNIQUE NOT NULL,
            hashed_password VARCHAR(255) NOT NULL,
            created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
            updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
        )
    """)
    op.execute("CREATE INDEX idx_users_email ON users (email)")

    op.execute("""
        CREATE TABLE accounts (
            id         UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id    UUID        REFERENCES users(id) ON DELETE RESTRICT,
            status     VARCHAR(20) NOT NULL DEFAULT 'active',
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CONSTRAINT uq_one_account_per_user UNIQUE (user_id)
        )
    """)
    op.execute("CREATE INDEX idx_accounts_user_id ON accounts (user_id)")
    op.execute("CREATE UNIQUE INDEX uq_one_system_account ON accounts ((user_id IS NULL)) WHERE user_id IS NULL")

    op.execute("""
        CREATE TABLE ledger_entries (
            id                UUID          PRIMARY KEY DEFAULT gen_random_uuid(),
            debit_account_id  UUID          NOT NULL REFERENCES accounts(id),
            credit_account_id UUID          NOT NULL REFERENCES accounts(id),
            amount            NUMERIC(20,8) NOT NULL CHECK (amount > 0),
            entry_type        VARCHAR(30)   NOT NULL,
            reference_id      UUID,
            idempotency_key   VARCHAR(255)  UNIQUE NOT NULL,
            created_at        TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
            CONSTRAINT chk_different_accounts CHECK (debit_account_id != credit_account_id)
        )
    """)
    op.execute("CREATE INDEX idx_ledger_debit  ON ledger_entries (debit_account_id,  created_at DESC)")
    op.execute("CREATE INDEX idx_ledger_credit ON ledger_entries (credit_account_id, created_at DESC)")
    op.execute("CREATE INDEX idx_ledger_ref    ON ledger_entries (reference_id)")

    op.execute("""
        CREATE TABLE transfers (
            id               UUID          PRIMARY KEY DEFAULT gen_random_uuid(),
            from_account_id  UUID          NOT NULL REFERENCES accounts(id),
            to_account_id    UUID          NOT NULL REFERENCES accounts(id),
            amount           NUMERIC(20,8) NOT NULL CHECK (amount > 0),
            status           VARCHAR(20)   NOT NULL DEFAULT 'completed',
            idempotency_key  VARCHAR(255)  UNIQUE NOT NULL,
            created_at       TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
            CONSTRAINT chk_different_transfer_accounts CHECK (from_account_id != to_account_id)
        )
    """)
    op.execute("CREATE INDEX idx_transfers_from ON transfers (from_account_id, created_at DESC)")
    op.execute("CREATE INDEX idx_transfers_to   ON transfers (to_account_id,   created_at DESC)")

    # System account seed
    op.execute("""
        INSERT INTO accounts (id, user_id, status, created_at, updated_at)
        VALUES ('00000000-0000-0000-0000-000000000000', NULL, 'active', NOW(), NOW())
    """)


def downgrade() -> None:
    op.execute("DROP TABLE transfers")
    op.execute("DROP TABLE ledger_entries")
    op.execute("DROP TABLE accounts")
    op.execute("DROP TABLE users")
