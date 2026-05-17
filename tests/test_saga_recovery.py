"""Tests for the saga recovery worker.

These tests simulate crash scenarios by manually creating withdrawals stuck
in 'pending' or 'submitted' state, then running the recovery function and
asserting it resolves them correctly.

Test scenarios:
1. Crash at 'pending' → recovery retries rail → completes
2. Crash at 'pending' + circuit OPEN → recovery compensates immediately
3. Crash at 'pending' + rail failure → recovery compensates
4. Crash at 'submitted' + has external_reference → queries rail → completes
5. Crash at 'submitted' + no external_reference + past timeout → compensates
6. Crash at 'submitted' + no external_reference + within timeout → leaves it
7. Recovery is idempotent: running twice on same withdrawal → one compensation
"""

import uuid
from datetime import datetime, timezone, timedelta
from decimal import Decimal

import pytest
import pytest_asyncio
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from redis.asyncio import Redis
from testcontainers.redis import RedisContainer

from app.circuit_breaker import CircuitBreaker, _KEY_STATE, _KEY_LAST_FAILURE_AT, _KEY_FAILURE_COUNT
from app.config import SYSTEM_ACCOUNT_ID
from app.database import Base
from app.models.ledger_entry import LedgerEntry
from app.models.withdrawal import Withdrawal
from app.models.account import Account
from app.models.user import User
from rail.simulator import BankRailSimulator
from workers.saga_recovery import recover_stuck_withdrawals


@pytest_asyncio.fixture
async def recovery_db_factory(postgres_container):
    """Session factory for saga recovery tests."""
    url = postgres_container.get_connection_url().replace("psycopg2", "asyncpg")
    engine = create_async_engine(url, pool_size=5, max_overflow=10)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    from sqlalchemy import text
    async with engine.begin() as conn:
        await conn.execute(text(
            "TRUNCATE outbox, audit_events, scheduled_payment_executions, scheduled_payments, "
            "deposits, withdrawals, transfers, ledger_entries, accounts, users RESTART IDENTITY CASCADE"
        ))
        await conn.execute(text(
            "INSERT INTO accounts (id, user_id, status, created_at, updated_at) "
            "VALUES (:id, NULL, 'active', NOW(), NOW()) ON CONFLICT DO NOTHING"
        ), {"id": str(SYSTEM_ACCOUNT_ID)})
    factory = async_sessionmaker(engine, expire_on_commit=False)
    yield factory
    await engine.dispose()


@pytest_asyncio.fixture
async def recovery_redis(redis_container) -> Redis:
    """Dedicated Redis client for recovery tests."""
    host = redis_container.get_container_host_ip()
    port = redis_container.get_exposed_port(6379)
    client = Redis.from_url(f"redis://{host}:{port}/0", decode_responses=True)
    await client.flushdb()
    yield client
    await client.flushdb()
    await client.aclose()


@pytest_asyncio.fixture
async def test_account(recovery_db_factory):
    """Create a user + account for recovery tests."""
    async with recovery_db_factory() as db:
        async with db.begin():
            user = User(
                email=f"recovery-{uuid.uuid4()}@test.com",
                hashed_password="hashed",
                created_at=datetime.now(timezone.utc),
                updated_at=datetime.now(timezone.utc),
            )
            db.add(user)
            await db.flush()
            account = Account(
                user_id=user.id,
                status="active",
                created_at=datetime.now(timezone.utc),
                updated_at=datetime.now(timezone.utc),
            )
            db.add(account)
            await db.flush()
            return account


async def _create_stuck_withdrawal(
    db_factory, account_id, status="pending", external_reference=None, minutes_ago=10
):
    """Helper: create a withdrawal stuck in a given state with debit ledger entries."""
    stuck_time = datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)
    withdrawal_id = uuid.uuid4()
    async with db_factory() as db:
        async with db.begin():
            txn_id = uuid.uuid4()
            db.add(LedgerEntry(
                transaction_id=txn_id,
                account_id=account_id,
                direction="debit",
                amount=Decimal("50.0000"),
                currency="USD",
                entry_type="withdrawal",
                reference_id=withdrawal_id,
                created_at=stuck_time,
            ))
            db.add(LedgerEntry(
                transaction_id=txn_id,
                account_id=SYSTEM_ACCOUNT_ID,
                direction="credit",
                amount=Decimal("50.0000"),
                currency="USD",
                entry_type="withdrawal",
                reference_id=withdrawal_id,
                created_at=stuck_time,
            ))

            w = Withdrawal(
                id=withdrawal_id,
                account_id=account_id,
                amount=Decimal("50.0000"),
                currency="USD",
                status=status,
                destination_type="bank_transfer",
                destination_details={"sort_code": "12-34-56", "account_number": "12345678"},
                external_reference=external_reference,
                idempotency_key=str(uuid.uuid4()),
                created_at=stuck_time,
                submitted_at=stuck_time if status == "submitted" else None,
                updated_at=stuck_time,
            )
            db.add(w)
            await db.flush()
            return w.id


class TestSagaRecoveryPending:
    """Tests for recovering withdrawals stuck at 'pending'."""

    @pytest.mark.asyncio
    async def test_recover_pending_retries_rail_and_completes(
        self, recovery_db_factory, test_account, recovery_redis
    ):
        withdrawal_id = await _create_stuck_withdrawal(
            recovery_db_factory, test_account.id, status="pending", minutes_ago=10
        )

        rail = BankRailSimulator()
        rail.force_next_outcome("success")
        cb = CircuitBreaker(redis=recovery_redis)

        await recover_stuck_withdrawals(recovery_db_factory, cb, rail)

        async with recovery_db_factory() as db:
            w = (await db.execute(
                select(Withdrawal).where(Withdrawal.id == withdrawal_id)
            )).scalar_one()
            assert w.status == "completed"
            assert w.external_reference is not None
            assert w.external_reference.startswith("RAIL-")

    @pytest.mark.asyncio
    async def test_recover_pending_compensates_when_circuit_open(
        self, recovery_db_factory, test_account, recovery_redis
    ):
        withdrawal_id = await _create_stuck_withdrawal(
            recovery_db_factory, test_account.id, status="pending", minutes_ago=10
        )

        await recovery_redis.set(_KEY_STATE, "OPEN")
        await recovery_redis.set(_KEY_LAST_FAILURE_AT, datetime.now(timezone.utc).isoformat())
        await recovery_redis.set(_KEY_FAILURE_COUNT, "3")

        rail = BankRailSimulator()
        cb = CircuitBreaker(redis=recovery_redis)

        await recover_stuck_withdrawals(recovery_db_factory, cb, rail)

        async with recovery_db_factory() as db:
            w = (await db.execute(
                select(Withdrawal).where(Withdrawal.id == withdrawal_id)
            )).scalar_one()
            assert w.status == "failed"
            assert w.failure_code == "CIRCUIT_OPEN"

            reversals = (await db.execute(
                select(LedgerEntry)
                .where(LedgerEntry.entry_type == "withdrawal_reversal")
                .where(LedgerEntry.reference_id == withdrawal_id)
            )).scalars().all()
            assert len(reversals) == 2

    @pytest.mark.asyncio
    async def test_recover_pending_compensates_on_rail_failure(
        self, recovery_db_factory, test_account, recovery_redis
    ):
        withdrawal_id = await _create_stuck_withdrawal(
            recovery_db_factory, test_account.id, status="pending", minutes_ago=10
        )

        rail = BankRailSimulator()
        rail.force_next_outcome("fail:NETWORK_ERROR")
        cb = CircuitBreaker(redis=recovery_redis)

        await recover_stuck_withdrawals(recovery_db_factory, cb, rail)

        async with recovery_db_factory() as db:
            w = (await db.execute(
                select(Withdrawal).where(Withdrawal.id == withdrawal_id)
            )).scalar_one()
            assert w.status == "failed"
            assert w.failure_code == "NETWORK_ERROR"


class TestSagaRecoverySubmitted:
    """Tests for recovering withdrawals stuck at 'submitted'."""

    @pytest.mark.asyncio
    async def test_recover_submitted_with_reference_queries_rail(
        self, recovery_db_factory, test_account, recovery_redis
    ):
        withdrawal_id = await _create_stuck_withdrawal(
            recovery_db_factory, test_account.id,
            status="submitted",
            external_reference="RAIL-abc123",
            minutes_ago=10,
        )

        rail = BankRailSimulator()
        cb = CircuitBreaker(redis=recovery_redis)

        await recover_stuck_withdrawals(recovery_db_factory, cb, rail)

        async with recovery_db_factory() as db:
            w = (await db.execute(
                select(Withdrawal).where(Withdrawal.id == withdrawal_id)
            )).scalar_one()
            assert w.status == "completed"
            assert w.external_reference == "RAIL-abc123"

    @pytest.mark.asyncio
    async def test_recover_submitted_no_reference_compensates_after_timeout(
        self, recovery_db_factory, test_account, recovery_redis
    ):
        withdrawal_id = await _create_stuck_withdrawal(
            recovery_db_factory, test_account.id,
            status="submitted",
            external_reference=None,
            minutes_ago=35,
        )

        rail = BankRailSimulator()
        cb = CircuitBreaker(redis=recovery_redis)

        await recover_stuck_withdrawals(recovery_db_factory, cb, rail)

        async with recovery_db_factory() as db:
            w = (await db.execute(
                select(Withdrawal).where(Withdrawal.id == withdrawal_id)
            )).scalar_one()
            assert w.status == "failed"
            assert w.failure_code == "TIMEOUT"

    @pytest.mark.asyncio
    async def test_recover_submitted_no_reference_within_timeout_leaves_it(
        self, recovery_db_factory, test_account, recovery_redis
    ):
        withdrawal_id = await _create_stuck_withdrawal(
            recovery_db_factory, test_account.id,
            status="submitted",
            external_reference=None,
            minutes_ago=10,
        )

        rail = BankRailSimulator()
        cb = CircuitBreaker(redis=recovery_redis)

        await recover_stuck_withdrawals(recovery_db_factory, cb, rail)

        async with recovery_db_factory() as db:
            w = (await db.execute(
                select(Withdrawal).where(Withdrawal.id == withdrawal_id)
            )).scalar_one()
            assert w.status == "submitted"


class TestSagaRecoveryIdempotency:
    """Test that recovery is idempotent (running twice doesn't double-compensate)."""

    @pytest.mark.asyncio
    async def test_recovery_idempotent_compensation(
        self, recovery_db_factory, test_account, recovery_redis
    ):
        withdrawal_id = await _create_stuck_withdrawal(
            recovery_db_factory, test_account.id, status="pending", minutes_ago=10
        )

        await recovery_redis.set(_KEY_STATE, "OPEN")
        await recovery_redis.set(_KEY_LAST_FAILURE_AT, datetime.now(timezone.utc).isoformat())
        await recovery_redis.set(_KEY_FAILURE_COUNT, "3")

        rail = BankRailSimulator()
        cb = CircuitBreaker(redis=recovery_redis)

        await recover_stuck_withdrawals(recovery_db_factory, cb, rail)
        await recover_stuck_withdrawals(recovery_db_factory, cb, rail)

        async with recovery_db_factory() as db:
            reversals = (await db.execute(
                select(LedgerEntry)
                .where(LedgerEntry.entry_type == "withdrawal_reversal")
                .where(LedgerEntry.reference_id == withdrawal_id)
            )).scalars().all()
            # Exactly 2 reversal entries (one debit + one credit), NOT 4
            assert len(reversals) == 2

            w = (await db.execute(
                select(Withdrawal).where(Withdrawal.id == withdrawal_id)
            )).scalar_one()
            assert w.status == "failed"
