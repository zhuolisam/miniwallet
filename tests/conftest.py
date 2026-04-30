import pytest
import pytest_asyncio
from collections.abc import AsyncGenerator
from httpx import AsyncClient, ASGITransport
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from testcontainers.postgres import PostgresContainer
from testcontainers.redis import RedisContainer
from redis.asyncio import Redis

from app.main import create_app
from app.database import Base, get_db
from app.dependencies import get_redis
from app.config import SYSTEM_ACCOUNT_ID


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
        await conn.execute(text("TRUNCATE transfers, ledger_entries, accounts, users RESTART IDENTITY CASCADE"))
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

    app.dependency_overrides[get_db] = override_get_db
    app.dependency_overrides[get_redis] = override_get_redis

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
