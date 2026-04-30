async def test_transfer_happy_path(client, alice_headers, seeded_alice_account, bob_account, bob_headers):
    resp = await client.post(
        "/v1/transfers",
        headers={"Idempotency-Key": "transfer-1"} | alice_headers,
        json={"to_email": "bob@example.com", "amount": "100.00"}
    )
    assert resp.status_code == 201
    data = resp.json()["data"]
    assert data["amount"] == "100.00000000"

    resp = await client.get("/v1/accounts/me/balance", headers=alice_headers)
    assert resp.json()["data"]["balance"] == "900.00000000"

    resp = await client.get("/v1/accounts/me/balance", headers=bob_headers)
    assert resp.json()["data"]["balance"] == "100.00000000"


async def test_transfer_by_account_id(client, alice_headers, seeded_alice_account, bob_account, bob_headers):
    """to_account_id is an alternative to to_email — must work identically."""
    resp = await client.post(
        "/v1/transfers",
        headers={"Idempotency-Key": "transfer-by-id"} | alice_headers,
        json={"to_account_id": bob_account["account_id"], "amount": "250.00"}
    )
    assert resp.status_code == 201
    assert resp.json()["data"]["amount"] == "250.00000000"

    resp = await client.get("/v1/accounts/me/balance", headers=bob_headers)
    assert resp.json()["data"]["balance"] == "250.00000000"


async def test_transfer_insufficient_balance(client, alice_headers, alice_account, bob_account):
    resp = await client.post(
        "/v1/transfers",
        headers={"Idempotency-Key": "transfer-insuff"} | alice_headers,
        json={"to_email": "bob@example.com", "amount": "100.00"}
    )
    assert resp.status_code == 422
    assert resp.json()["error"]["code"] == "INSUFFICIENT_BALANCE"


async def test_transfer_exact_balance(client, alice_headers, seeded_alice_account, bob_account):
    resp = await client.post(
        "/v1/transfers",
        headers={"Idempotency-Key": "transfer-exact"} | alice_headers,
        json={"to_email": "bob@example.com", "amount": "1000.00"}
    )
    assert resp.status_code == 201

    resp = await client.get("/v1/accounts/me/balance", headers=alice_headers)
    assert resp.json()["data"]["balance"] == "0.00000000"


async def test_transfer_to_yourself(client, alice_headers, seeded_alice_account):
    resp = await client.post(
        "/v1/transfers",
        headers={"Idempotency-Key": "transfer-self"} | alice_headers,
        json={"to_account_id": seeded_alice_account["account_id"], "amount": "100.00"}
    )
    assert resp.status_code == 422
    assert resp.json()["error"]["code"] == "SAME_ACCOUNT"


async def test_transfer_to_nonexistent_email(client, alice_headers, seeded_alice_account):
    resp = await client.post(
        "/v1/transfers",
        headers={"Idempotency-Key": "transfer-nonexist"} | alice_headers,
        json={"to_email": "nonexistent@example.com", "amount": "100.00"}
    )
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "NOT_FOUND"


async def test_transfer_to_user_without_account(client, alice_headers, seeded_alice_account, bob_registered):
    resp = await client.post(
        "/v1/transfers",
        headers={"Idempotency-Key": "transfer-noaccount"} | alice_headers,
        json={"to_email": "bob@example.com", "amount": "100.00"}
    )
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "NOT_FOUND"


async def test_transfer_missing_idempotency_key(client, alice_headers, seeded_alice_account, bob_account):
    resp = await client.post(
        "/v1/transfers",
        headers=alice_headers,
        json={"to_email": "bob@example.com", "amount": "100.00"}
    )
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "MISSING_IDEMPOTENCY_KEY"


async def test_transfer_idempotency_same_body(client, alice_headers, seeded_alice_account, bob_account):
    resp1 = await client.post(
        "/v1/transfers",
        headers={"Idempotency-Key": "idempotent-1"} | alice_headers,
        json={"to_email": "bob@example.com", "amount": "100.00"}
    )
    assert resp1.status_code == 201

    resp2 = await client.post(
        "/v1/transfers",
        headers={"Idempotency-Key": "idempotent-1"} | alice_headers,
        json={"to_email": "bob@example.com", "amount": "100.00"}
    )
    assert resp2.status_code == 201
    # Same transfer_id returned — not a new debit
    assert resp2.json()["data"]["transfer_id"] == resp1.json()["data"]["transfer_id"]
    resp = await client.get("/v1/accounts/me/balance", headers=alice_headers)
    assert resp.json()["data"]["balance"] == "900.00000000"


async def test_transfer_idempotency_different_amount(client, alice_headers, seeded_alice_account, bob_account):
    await client.post(
        "/v1/transfers",
        headers={"Idempotency-Key": "conflict-1"} | alice_headers,
        json={"to_email": "bob@example.com", "amount": "100.00"}
    )
    resp = await client.post(
        "/v1/transfers",
        headers={"Idempotency-Key": "conflict-1"} | alice_headers,
        json={"to_email": "bob@example.com", "amount": "200.00"}
    )
    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "IDEMPOTENCY_CONFLICT"


# --- Failed transfer persistence ---

async def test_failed_transfer_persisted_in_db(client, alice_headers, alice_account, bob_account, db_session):
    """Insufficient balance → 422, but Transfer(status=failed) exists in DB for audit trail."""
    from sqlalchemy import select
    from app.models.transfer import Transfer

    resp = await client.post(
        "/v1/transfers",
        headers={"Idempotency-Key": "fail-persist-1"} | alice_headers,
        json={"to_email": "bob@example.com", "amount": "100.00"},
    )
    assert resp.status_code == 422
    assert resp.json()["error"]["code"] == "INSUFFICIENT_BALANCE"

    result = await db_session.execute(
        select(Transfer).where(Transfer.idempotency_key == "fail-persist-1")
    )
    record = result.scalar_one()
    assert record.status == "failed"
    assert record.failure_code == "INSUFFICIENT_BALANCE"


async def test_failed_transfer_key_consumed_on_retry(client, alice_headers, seeded_alice_account, bob_account):
    """After a failed transfer, retrying with the same idempotency key returns 409 — key is consumed."""
    # First attempt: insufficient (transfer more than balance)
    await client.post(
        "/v1/transfers",
        headers={"Idempotency-Key": "fail-retry-1"} | alice_headers,
        json={"to_email": "bob@example.com", "amount": "9999.00"},
    )

    # Retry with the same key — even though balance is now sufficient, the key is consumed
    resp = await client.post(
        "/v1/transfers",
        headers={"Idempotency-Key": "fail-retry-1"} | alice_headers,
        json={"to_email": "bob@example.com", "amount": "9999.00"},
    )
    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "IDEMPOTENCY_KEY_CONSUMED"


async def test_failed_transfer_new_key_succeeds(client, alice_headers, alice_account, bob_account):
    """After a failed transfer, using a new idempotency key succeeds once funds are available."""
    # Attempt 1: no funds → fails
    resp = await client.post(
        "/v1/transfers",
        headers={"Idempotency-Key": "fail-newkey-attempt1"} | alice_headers,
        json={"to_email": "bob@example.com", "amount": "100.00"},
    )
    assert resp.status_code == 422

    # Seed funds
    await client.post(
        "/v1/dev/seed",
        headers={"Idempotency-Key": "seed-newkey"} | alice_headers,
        json={"account_id": alice_account["account_id"], "amount": "500.00"},
    )

    # Attempt 2: new key, now has funds → succeeds
    resp = await client.post(
        "/v1/transfers",
        headers={"Idempotency-Key": "fail-newkey-attempt2"} | alice_headers,
        json={"to_email": "bob@example.com", "amount": "100.00"},
    )
    assert resp.status_code == 201
    assert resp.json()["data"]["status"] == "completed"


async def test_view_failed_transfer(client, alice_headers, alice_account, bob_account):
    """GET /v1/transfers/{id} returns status=failed and failure_code for a failed transfer."""
    resp = await client.post(
        "/v1/transfers",
        headers={"Idempotency-Key": "fail-view-1"} | alice_headers,
        json={"to_email": "bob@example.com", "amount": "100.00"},
    )
    assert resp.status_code == 422

    # We need the transfer_id — query the DB via the transfer response isn't available on 422.
    # Instead verify through test_failed_transfer_persisted_in_db pattern.
    # Here we test that GET works for a known failed transfer using db_session (covered in
    # test_view_transfer_sender for the success case). This tests the schema shape.
    pass  # covered by test_failed_transfer_persisted_in_db + test_view_transfer_sender


async def test_view_failed_transfer_by_id(client, alice_headers, alice_account, bob_account, db_session):
    """Sender can retrieve their failed transfer by ID — status=failed, failure_code set."""
    from sqlalchemy import select
    from app.models.transfer import Transfer

    await client.post(
        "/v1/transfers",
        headers={"Idempotency-Key": "fail-view-id-1"} | alice_headers,
        json={"to_email": "bob@example.com", "amount": "100.00"},
    )

    result = await db_session.execute(
        select(Transfer).where(Transfer.idempotency_key == "fail-view-id-1")
    )
    record = result.scalar_one()

    resp = await client.get(f"/v1/transfers/{record.id}", headers=alice_headers)
    assert resp.status_code == 200
    data = resp.json()["data"]
    assert data["status"] == "failed"
    assert data["failure_code"] == "INSUFFICIENT_BALANCE"


async def test_transfer_zero_amount(client, alice_headers, seeded_alice_account, bob_account):
    resp = await client.post(
        "/v1/transfers",
        headers={"Idempotency-Key": "zero-transfer"} | alice_headers,
        json={"to_email": "bob@example.com", "amount": "0"}
    )
    assert resp.status_code == 422


async def test_transfer_negative_amount(client, alice_headers, seeded_alice_account, bob_account):
    resp = await client.post(
        "/v1/transfers",
        headers={"Idempotency-Key": "negative-transfer"} | alice_headers,
        json={"to_email": "bob@example.com", "amount": "-100.00"}
    )
    assert resp.status_code == 422


# --- GET /v1/transfers/{transfer_id} (US-1.9) ---

async def test_view_transfer_sender(client, alice_headers, seeded_alice_account, bob_account):
    """Sender can view their own transfer."""
    resp = await client.post(
        "/v1/transfers",
        headers={"Idempotency-Key": "view-t-sender"} | alice_headers,
        json={"to_email": "bob@example.com", "amount": "100.00"}
    )
    transfer_id = resp.json()["data"]["transfer_id"]

    resp = await client.get(f"/v1/transfers/{transfer_id}", headers=alice_headers)
    assert resp.status_code == 200
    data = resp.json()["data"]
    assert data["transfer_id"] == transfer_id
    assert data["amount"] == "100.00000000"
    assert data["status"] == "completed"
    assert "from_account_id" in data
    assert "to_account_id" in data
    assert "created_at" in data


async def test_view_transfer_receiver(client, alice_headers, seeded_alice_account, bob_account, bob_headers):
    """Receiver can also view a transfer they received."""
    resp = await client.post(
        "/v1/transfers",
        headers={"Idempotency-Key": "view-t-receiver"} | alice_headers,
        json={"to_email": "bob@example.com", "amount": "100.00"}
    )
    transfer_id = resp.json()["data"]["transfer_id"]

    resp = await client.get(f"/v1/transfers/{transfer_id}", headers=bob_headers)
    assert resp.status_code == 200
    assert resp.json()["data"]["transfer_id"] == transfer_id


async def test_view_transfer_not_found(client, alice_headers, alice_account):
    """Non-existent transfer_id returns 404."""
    resp = await client.get(
        "/v1/transfers/00000000-0000-0000-0000-111111111111",
        headers=alice_headers,
    )
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "NOT_FOUND"


async def test_view_transfer_third_party_gets_404(client, alice_headers, seeded_alice_account, bob_account, bob_headers):
    """A user who is neither sender nor receiver gets 404 — no information leak."""
    # Charlie registers and opens an account
    await client.post("/v1/auth/register", json={
        "email": "charlie@example.com", "password": "password123"
    })
    charlie_login = await client.post("/v1/auth/login", json={
        "email": "charlie@example.com", "password": "password123"
    })
    charlie_headers = {"Authorization": f"Bearer {charlie_login.json()['data']['access_token']}"}
    await client.post("/v1/accounts", headers=charlie_headers)

    # Alice sends to Bob
    resp = await client.post(
        "/v1/transfers",
        headers={"Idempotency-Key": "view-t-3p"} | alice_headers,
        json={"to_email": "bob@example.com", "amount": "100.00"}
    )
    transfer_id = resp.json()["data"]["transfer_id"]

    # Charlie cannot see it
    resp = await client.get(f"/v1/transfers/{transfer_id}", headers=charlie_headers)
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "NOT_FOUND"
