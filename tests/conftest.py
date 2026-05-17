import uuid
from datetime import datetime, timezone, timedelta

import jwt
import pytest
import pytest_asyncio
from collections.abc import AsyncGenerator
from httpx import AsyncClient, ASGITransport
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from testcontainers.postgres import PostgresContainer
from testcontainers.redis import RedisContainer
from testcontainers.kafka import KafkaContainer
from redis.asyncio import Redis

from app.main import create_app
from app.database import Base, get_db
from app.dependencies import get_redis, get_rail, get_circuit_breaker
from app.circuit_breaker import CircuitBreaker
from app.config import settings, SYSTEM_ACCOUNT_ID
from rail.simulator import BankRailSimulator
from app.models.user import User
from app.models.account import Account
# Phase 2 models — imported here so Base.metadata.create_all creates their tables
# when running any test file, including Phase 1 tests. Without these imports,
# create_all never creates the audit_events table and the TRUNCATE in db_session fails.
from app.models.audit_event import AuditEvent  # noqa: F401
from app.models.outbox import OutboxRow  # noqa: F401
from app.models.transaction_activity import TransactionActivity  # noqa: F401
# Phase 3 models — imported so Base.metadata.create_all builds the deposit and
# withdrawal tables when tests spin up a fresh schema. Without these imports,
# integration tests that touch POST /v1/dev/simulate-deposit or /v1/withdrawals
# would fail with UndefinedTable.
from app.models.deposit import Deposit  # noqa: F401
from app.models.withdrawal import Withdrawal  # noqa: F401
from app.models.scheduled_payment import ScheduledPayment  # noqa: F401
from app.models.scheduled_payment_execution import ScheduledPaymentExecution  # noqa: F401


@pytest.fixture(scope="session")
def postgres_container():
    with PostgresContainer("postgres:16") as pg:
        yield pg


@pytest.fixture(scope="session")
def redis_container():
    with RedisContainer("redis:7") as r:
        yield r


@pytest_asyncio.fixture
async def db_session(postgres_container) -> AsyncGenerator[AsyncSession, None]:
    """
    Create a fresh session for each test with TRUNCATE CASCADE cleanup.
    Each test gets its own engine and session bound to the shared postgres_container.
    """
    url = postgres_container.get_connection_url().replace("psycopg2", "asyncpg")
    engine = create_async_engine(url, pool_size=5, max_overflow=10)

    # Create tables on first use (idempotent via run_sync)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # Clean up non-system data before each test
    async with engine.begin() as conn:
        await conn.execute(text("TRUNCATE outbox, audit_events, transaction_activity, scheduled_payment_executions, scheduled_payments, deposits, withdrawals, transfers, ledger_entries, accounts, users RESTART IDENTITY CASCADE"))
        await conn.execute(
            text("""
                INSERT INTO accounts (id, user_id, status, created_at, updated_at)
                VALUES (:id, NULL, 'active', NOW(), NOW())
            """),
            {"id": str(SYSTEM_ACCOUNT_ID)},
        )

    session_factory = async_sessionmaker(bind=engine, expire_on_commit=False)
    async with session_factory() as session:
        yield session

    await engine.dispose()


@pytest_asyncio.fixture
async def redis_client(redis_container) -> AsyncGenerator[Redis, None]:
    host = redis_container.get_container_host_ip()
    port = redis_container.get_exposed_port(6379)
    client = Redis.from_url(f"redis://{host}:{port}/0", decode_responses=True)
    yield client
    await client.flushdb()
    await client.aclose()


@pytest_asyncio.fixture
async def client(postgres_container, db_session, redis_client) -> AsyncGenerator[AsyncClient, None]:
    """
    HTTP test client with DB and Redis overridden to use test containers.
    Uses the same postgres_container as db_session so data is immediately visible.
    """
    app = create_app()
    url = postgres_container.get_connection_url().replace("psycopg2", "asyncpg")

    # One engine per test — reused for all requests, not recreated per request
    test_engine = create_async_engine(url, pool_size=5, max_overflow=10)

    async def override_get_db() -> AsyncGenerator[AsyncSession, None]:
        session_factory = async_sessionmaker(bind=test_engine, expire_on_commit=False)
        async with session_factory() as session:
            try:
                yield session
            except Exception:
                await session.rollback()
                raise

    async def override_get_redis() -> Redis:
        return redis_client

    test_rail = BankRailSimulator()
    test_circuit_breaker = CircuitBreaker(redis=redis_client)

    def override_get_rail() -> BankRailSimulator:
        return test_rail

    def override_get_circuit_breaker() -> CircuitBreaker:
        return test_circuit_breaker

    app.dependency_overrides[get_db] = override_get_db
    app.dependency_overrides[get_redis] = override_get_redis
    app.dependency_overrides[get_rail] = override_get_rail
    app.dependency_overrides[get_circuit_breaker] = override_get_circuit_breaker

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        yield ac

    await test_engine.dispose()


@pytest_asyncio.fixture
async def alice_registered(client) -> dict:
    resp = await client.post("/v1/auth/register", json={
        "email": "alice@example.com", "password": "password123"
    })
    return resp.json()["data"]


@pytest_asyncio.fixture
async def alice_headers(client, alice_registered) -> dict:
    resp = await client.post("/v1/auth/login", json={
        "email": "alice@example.com", "password": "password123"
    })
    token = resp.json()["data"]["access_token"]
    return {"Authorization": f"Bearer {token}"}


@pytest_asyncio.fixture
async def alice_account(client, alice_headers) -> dict:
    resp = await client.post("/v1/accounts", headers=alice_headers)
    return resp.json()["data"]


@pytest_asyncio.fixture
async def seeded_alice_account(client, alice_headers, alice_account) -> dict:
    headers = {"Idempotency-Key": "seed-fixture"} | alice_headers
    await client.post(
        "/v1/dev/seed",
        headers=headers,
        json={"account_id": alice_account["account_id"], "amount": "1000.00"},
    )
    return alice_account


@pytest_asyncio.fixture
async def bob_registered(client) -> dict:
    resp = await client.post("/v1/auth/register", json={
        "email": "bob@example.com", "password": "password123"
    })
    return resp.json()["data"]


@pytest_asyncio.fixture
async def bob_headers(client, bob_registered) -> dict:
    resp = await client.post("/v1/auth/login", json={
        "email": "bob@example.com", "password": "password123"
    })
    token = resp.json()["data"]["access_token"]
    return {"Authorization": f"Bearer {token}"}


@pytest_asyncio.fixture
async def bob_account(client, bob_headers) -> dict:
    resp = await client.post("/v1/accounts", headers=bob_headers)
    return resp.json()["data"]


# ---------------------------------------------------------------------------
# Phase 2 fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def kafka_container():
    """Session-scoped Kafka testcontainer — one broker for the whole test run."""
    with KafkaContainer("confluentinc/cp-kafka:7.6.0") as kafka:
        yield kafka


@pytest.fixture(scope="session")
def kafka_bootstrap(kafka_container) -> str:
    """Bootstrap server address for the test Kafka container.

    testcontainers maps Kafka's port to a random host port (e.g. localhost:32789).
    Always use this fixture in tests — never hardcode settings.kafka_bootstrap_servers,
    which points to the Docker-internal address (kafka:9092) unreachable from tests.
    Also note: testcontainers defaults KAFKA_AUTO_CREATE_TOPICS_ENABLE=true, so topics
    are created on first use — no create-topics.sh needed in tests.
    """
    return kafka_container.get_bootstrap_server()


@pytest_asyncio.fixture
async def consumer_db_factory(postgres_container):
    """Async session factory pointing at the test Postgres container.

    Pass this to consumer constructors: e.g. AuditConsumer(db_factory=consumer_db_factory).
    Creates Phase 2 tables (audit_events) if they don't exist, and TRUNCATEs them
    before each test so assertions like `count == 0` start clean.

    Why not reuse db_session: Phase 1's db_session yields a single AsyncSession object,
    not a factory. Consumers need a *factory* (callable that returns a new session each
    call) to open a fresh session per message — the same pattern used by workers.
    """
    url = postgres_container.get_connection_url().replace("psycopg2", "asyncpg")
    engine = create_async_engine(url, pool_size=5, max_overflow=10)

    # Idempotently create all tables (including Phase 2 ones)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # Clean slate before each test
    async with engine.begin() as conn:
        await conn.execute(text(
            "TRUNCATE outbox, audit_events, scheduled_payment_executions, scheduled_payments, "
            "deposits, withdrawals, transfers, ledger_entries, accounts, users RESTART IDENTITY CASCADE"
        ))
        # ON CONFLICT DO NOTHING: if db_session (from the `client` fixture) already
        # inserted the system account in this test, this is a safe no-op.
        await conn.execute(
            text("INSERT INTO accounts (id, user_id, status, created_at, updated_at) "
                 "VALUES (:id, NULL, 'active', NOW(), NOW()) ON CONFLICT DO NOTHING"),
            {"id": str(SYSTEM_ACCOUNT_ID)},
        )

    factory = async_sessionmaker(engine, expire_on_commit=False)
    yield factory
    await engine.dispose()


@pytest_asyncio.fixture
async def account_factory(consumer_db_factory):
    """Creates a User + Account row directly via ORM (no HTTP stack).

    Phase 1's `alice_account` and `bob_account` are named fixtures tied to
    the HTTP client. Phase 2 consumer tests need accounts without going through
    the API — this factory creates them directly via the ORM.

    Returns the Account ORM object with .id and .user_id populated.
    """
    async def factory():
        now = datetime.now(timezone.utc)
        async with consumer_db_factory() as db:
            async with db.begin():
                user = User(
                    email=f"user-{uuid.uuid4()}@test.com",
                    hashed_password="hashed",
                    created_at=now,
                    updated_at=now,
                )
                db.add(user)
                await db.flush()
                account = Account(
                    user_id=user.id,
                    status="active",
                    created_at=now,
                    updated_at=now,
                )
                db.add(account)
                await db.flush()
                return account
    return factory


@pytest.fixture
def auth_headers():
    """Returns a factory: given an Account ORM object, produces Authorization headers.

    Signs a JWT with the same secret key used by get_current_user, so the token
    is accepted by the API without going through POST /v1/auth/login.
    Useful in CQRS integration tests that need both a real account and an HTTP client.
    """
    def make_headers(account) -> dict:
        token = jwt.encode(
            {
                "sub": str(account.user_id),
                "exp": datetime.now(timezone.utc) + timedelta(hours=1),
            },
            settings.jwt_secret,
            algorithm=settings.jwt_algorithm,
        )
        return {"Authorization": f"Bearer {token}"}
    return make_headers


@pytest_asyncio.fixture
async def transfer_factory(consumer_db_factory):
    """Create Transfer rows for testing backfill.

    Callers MUST pass real account IDs (from account_factory) — the transfers
    table has FK constraints on from_account_id/to_account_id → accounts.id.
    """
    from app.models.transfer import Transfer
    from decimal import Decimal as D

    async def factory(status="completed", from_account_id=None, to_account_id=None, failure_code=None):
        async with consumer_db_factory() as db:
            async with db.begin():
                t = Transfer(
                    from_account_id=from_account_id or uuid.uuid4(),
                    to_account_id=to_account_id or uuid.uuid4(),
                    amount=D("100.0000"),
                    status=status,
                    failure_code=failure_code,
                    idempotency_key=str(uuid.uuid4()),
                    created_at=datetime.now(timezone.utc),
                )
                db.add(t)
                await db.flush()
                return t
    return factory


@pytest_asyncio.fixture
async def seed_factory(consumer_db_factory):
    """Create seed LedgerEntry legs for a given account (entry_type='seed').

    Creates two legs: debit SYSTEM_ACCOUNT_ID, credit user's account.
    Returns the credit leg (user-facing entry).
    """
    from app.models.ledger_entry import LedgerEntry
    from decimal import Decimal as D

    async def factory(account_id):
        async with consumer_db_factory() as db:
            async with db.begin():
                txn_id = uuid.uuid4()
                now = datetime.now(timezone.utc)
                debit_leg = LedgerEntry(
                    transaction_id=txn_id,
                    account_id=SYSTEM_ACCOUNT_ID,
                    direction="debit",
                    amount=D("1000.0000"),
                    currency="USD",
                    entry_type="seed",
                    created_at=now,
                )
                credit_leg = LedgerEntry(
                    transaction_id=txn_id,
                    account_id=account_id,
                    direction="credit",
                    amount=D("1000.0000"),
                    currency="USD",
                    entry_type="seed",
                    idempotency_key=str(uuid.uuid4()),
                    created_at=now,
                )
                db.add(debit_leg)
                db.add(credit_leg)
                await db.flush()
                return credit_leg
    return factory


@pytest_asyncio.fixture
async def flush_outbox_to_activity(db_session, postgres_container):
    """Drain pending outbox rows through the ActivityConsumer to populate transaction_activity.

    Call this after HTTP operations (transfers, seeds) in integration tests that
    assert on GET /me/transactions. In production, the relay + Kafka + activity-consumer
    pipeline does this asynchronously. In tests, we short-circuit by feeding outbox
    payloads directly into the consumer's process() method — no Kafka needed.
    """
    from sqlalchemy import select
    from workers.activity_consumer import ActivityConsumer

    url = postgres_container.get_connection_url().replace("psycopg2", "asyncpg")
    engine = create_async_engine(url, pool_size=5, max_overflow=10)
    factory = async_sessionmaker(engine, expire_on_commit=False)

    async def flush():
        consumer = ActivityConsumer(db_factory=factory)
        async with factory() as db:
            rows = (await db.execute(
                select(OutboxRow).order_by(OutboxRow.created_at)
            )).scalars().all()
        for row in rows:
            await consumer.process(row.payload)

    yield flush
    await engine.dispose()


def make_event(event_type: str, payload: dict, actor_id: str | None = None) -> dict:
    """Build a valid event envelope for use in tests.

    Matches the Phase 2 event contract (PRD Section 'Event Contracts').
    Call directly in test files — not a pytest fixture, so it can be imported or
    called without fixture injection overhead.

    Example:
        event = make_event("transfer.completed", {
            "transfer_id": str(uuid.uuid4()),
            "from_account_id": str(uuid.uuid4()),
            "to_account_id": str(uuid.uuid4()),
            "amount": "100.00000000",
            "entry_type": "transfer",
            "idempotency_key": "test-key",
        })
    """
    return {
        "event_id":    str(uuid.uuid4()),
        "event_type":  event_type,
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "version":     "1",
        "actor_id":    actor_id or str(uuid.uuid4()),
        "payload":     payload,
    }
