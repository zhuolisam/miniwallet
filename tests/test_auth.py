from datetime import datetime, timezone, timedelta

import jwt

from app.config import settings


async def test_register_new_user(client):
    resp = await client.post("/v1/auth/register", json={
        "email": "user@example.com", "password": "password123"
    })
    assert resp.status_code == 201
    data = resp.json()["data"]
    assert data["email"] == "user@example.com"
    assert "user_id" in data


async def test_register_duplicate_email(client):
    await client.post("/v1/auth/register", json={
        "email": "dup@example.com", "password": "password123"
    })
    resp = await client.post("/v1/auth/register", json={
        "email": "dup@example.com", "password": "password456"
    })
    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "EMAIL_ALREADY_EXISTS"


async def test_register_short_password(client):
    resp = await client.post("/v1/auth/register", json={
        "email": "user@example.com", "password": "short"
    })
    assert resp.status_code == 422
    assert resp.json()["error"]["code"] == "VALIDATION_ERROR"


async def test_login_correct_credentials(client):
    await client.post("/v1/auth/register", json={
        "email": "user@example.com", "password": "password123"
    })
    resp = await client.post("/v1/auth/login", json={
        "email": "user@example.com", "password": "password123"
    })
    assert resp.status_code == 200
    data = resp.json()["data"]
    assert "access_token" in data
    assert "refresh_token" in data
    assert data["token_type"] == "bearer"
    assert data["expires_in"] > 0


async def test_login_wrong_password(client):
    await client.post("/v1/auth/register", json={
        "email": "user@example.com", "password": "password123"
    })
    resp = await client.post("/v1/auth/login", json={
        "email": "user@example.com", "password": "wrongpassword"
    })
    assert resp.status_code == 401
    assert resp.json()["error"]["code"] == "INVALID_CREDENTIALS"


async def test_login_unknown_email(client):
    resp = await client.post("/v1/auth/login", json={
        "email": "unknown@example.com", "password": "password123"
    })
    assert resp.status_code == 401
    assert resp.json()["error"]["code"] == "INVALID_CREDENTIALS"


async def test_refresh_valid_token(client):
    await client.post("/v1/auth/register", json={
        "email": "user@example.com", "password": "password123"
    })
    login_resp = await client.post("/v1/auth/login", json={
        "email": "user@example.com", "password": "password123"
    })
    old_token = login_resp.json()["data"]["refresh_token"]
    resp = await client.post("/v1/auth/refresh", json={"refresh_token": old_token})
    assert resp.status_code == 200
    data = resp.json()["data"]
    assert "access_token" in data
    assert "refresh_token" in data


async def test_refresh_already_rotated_token(client):
    await client.post("/v1/auth/register", json={
        "email": "user@example.com", "password": "password123"
    })
    login_resp = await client.post("/v1/auth/login", json={
        "email": "user@example.com", "password": "password123"
    })
    token = login_resp.json()["data"]["refresh_token"]
    await client.post("/v1/auth/refresh", json={"refresh_token": token})
    # Second use of the same token must fail
    resp = await client.post("/v1/auth/refresh", json={"refresh_token": token})
    assert resp.status_code == 401
    assert resp.json()["error"]["code"] == "INVALID_REFRESH_TOKEN"


async def test_refresh_expired_token(client, redis_client):
    """Redis TTL expiry and key deletion are equivalent — the service only checks existence."""
    await client.post("/v1/auth/register", json={
        "email": "user@example.com", "password": "password123"
    })
    login_resp = await client.post("/v1/auth/login", json={
        "email": "user@example.com", "password": "password123"
    })
    token = login_resp.json()["data"]["refresh_token"]

    # Simulate expiry by removing the key from Redis
    await redis_client.delete(f"refresh:{token}")

    resp = await client.post("/v1/auth/refresh", json={"refresh_token": token})
    assert resp.status_code == 401
    assert resp.json()["error"]["code"] == "INVALID_REFRESH_TOKEN"


async def test_access_protected_route_with_valid_token(client, alice_headers):
    resp = await client.get("/v1/users/me", headers=alice_headers)
    assert resp.status_code == 200
    assert "user_id" in resp.json()["data"]


async def test_access_protected_route_without_token(client):
    resp = await client.get("/v1/users/me")
    assert resp.status_code == 401


async def test_access_protected_route_with_expired_access_token(client, alice_registered):
    """JWT exp claim must be validated — expired tokens must be rejected."""
    expired_token = jwt.encode(
        {"sub": alice_registered["user_id"], "exp": datetime.now(timezone.utc) - timedelta(minutes=1)},
        settings.jwt_secret,
        algorithm=settings.jwt_algorithm,
    )
    resp = await client.get("/v1/users/me", headers={"Authorization": f"Bearer {expired_token}"})
    assert resp.status_code == 401
    assert resp.json()["error"]["code"] == "UNAUTHORIZED"
