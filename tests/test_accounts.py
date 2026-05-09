from unittest.mock import patch


async def test_open_account(client, alice_headers):
    resp = await client.post("/v1/accounts", headers=alice_headers)
    assert resp.status_code == 201
    data = resp.json()["data"]
    assert data["balance"] == "0.0000"
    assert data["status"] == "active"
    assert "account_id" in data


async def test_open_account_twice(client, alice_headers):
    await client.post("/v1/accounts", headers=alice_headers)
    resp = await client.post("/v1/accounts", headers=alice_headers)
    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "ACCOUNT_ALREADY_EXISTS"


async def test_get_account_before_opening(client):
    # Register new user without account
    resp = await client.post("/v1/auth/register", json={
        "email": "newuser@example.com", "password": "password123"
    })

    # Login new user
    login_resp = await client.post("/v1/auth/login", json={
        "email": "newuser@example.com", "password": "password123"
    })
    token = login_resp.json()["data"]["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    resp = await client.get("/v1/accounts/me", headers=headers)
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "NOT_FOUND"


async def test_get_account_after_opening(client, alice_headers, alice_account):
    resp = await client.get("/v1/accounts/me", headers=alice_headers)
    assert resp.status_code == 200
    data = resp.json()["data"]
    assert data["account_id"] == alice_account["account_id"]
    assert data["balance"] == "0.0000"


async def test_seed_account(client, alice_headers, alice_account):
    headers = {"Idempotency-Key": "seed-test"} | alice_headers
    resp = await client.post(
        "/v1/dev/seed",
        headers=headers,
        json={"account_id": alice_account["account_id"], "amount": "500.00"}
    )
    assert resp.status_code == 201
    data = resp.json()["data"]
    assert data["new_balance"] == "500.0000"
    assert data["amount"] == "500.0000"


async def test_seed_nonexistent_account(client, alice_headers):
    headers = {"Idempotency-Key": "seed-nonexistent"} | alice_headers
    resp = await client.post(
        "/v1/dev/seed",
        headers=headers,
        json={"account_id": "00000000-0000-0000-0000-111111111111", "amount": "100.00"}
    )
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "NOT_FOUND"


async def test_get_balance_endpoint(client, alice_headers, seeded_alice_account):
    resp = await client.get("/v1/accounts/me/balance", headers=alice_headers)
    assert resp.status_code == 200
    data = resp.json()["data"]
    assert data["balance"] == "1000.0000"
    assert "account_id" in data


async def test_get_balance_no_account(client):
    resp = await client.post("/v1/auth/register", json={
        "email": "nobal@example.com", "password": "password123"
    })
    login = await client.post("/v1/auth/login", json={
        "email": "nobal@example.com", "password": "password123"
    })
    headers = {"Authorization": f"Bearer {login.json()['data']['access_token']}"}
    resp = await client.get("/v1/accounts/me/balance", headers=headers)
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "NOT_FOUND"


async def test_seed_endpoint_forbidden_in_production(client, alice_headers, alice_account):
    headers = {"Idempotency-Key": "seed-prod"} | alice_headers
    with patch("app.routers.dev.settings.app_env", "production"):
        resp = await client.post(
            "/v1/dev/seed",
            headers=headers,
            json={"account_id": alice_account["account_id"], "amount": "100.00"}
        )
        assert resp.status_code == 403
        assert resp.json()["error"]["code"] == "FORBIDDEN"
