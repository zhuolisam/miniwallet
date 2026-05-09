"""Initial schema

Revision ID: 0001
Revises:
Create Date: 2024-01-01 00:00:00.000000

"""
from alembic import op

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

    # Proper double-entry ledger: one row per leg (debit or credit)
    # grouped by transaction_id. Invariant: SUM(all entries) = 0 when
    # credits are positive and debits are negative.
    op.execute("""
        CREATE TABLE ledger_entries (
            id              UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
            transaction_id  UUID         NOT NULL,
            account_id      UUID         NOT NULL REFERENCES accounts(id),
            direction       VARCHAR(6)   NOT NULL CHECK (direction IN ('debit', 'credit')),
            amount          NUMERIC(19,4) NOT NULL CHECK (amount > 0),
            currency        VARCHAR(3)   NOT NULL DEFAULT 'USD',
            entry_type      VARCHAR(30)  NOT NULL,
            idempotency_key VARCHAR(255) UNIQUE,
            created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
        )
    """)
    op.execute("CREATE INDEX idx_ledger_account ON ledger_entries (account_id, created_at DESC)")
    op.execute("CREATE INDEX idx_ledger_txn ON ledger_entries (transaction_id)")

    op.execute("""
        CREATE TABLE transfers (
            id               UUID           PRIMARY KEY DEFAULT gen_random_uuid(),
            from_account_id  UUID           NOT NULL REFERENCES accounts(id),
            to_account_id    UUID           NOT NULL REFERENCES accounts(id),
            amount           NUMERIC(19,4)  NOT NULL CHECK (amount > 0),
            currency         VARCHAR(3)     NOT NULL DEFAULT 'USD',
            status           VARCHAR(20)    NOT NULL DEFAULT 'completed',
            failure_code     VARCHAR(50),
            idempotency_key  VARCHAR(255)   UNIQUE NOT NULL,
            created_at       TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
            updated_at       TIMESTAMPTZ,
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
