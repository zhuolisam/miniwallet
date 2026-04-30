from datetime import datetime, timezone, timedelta


async def test_no_history(client, alice_headers, alice_account):
    resp = await client.get("/v1/accounts/me/transactions", headers=alice_headers)
    assert resp.status_code == 200
    data = resp.json()
    assert data["data"] == []
    assert data["meta"]["total"] == 0


async def test_transfer_sender_view(client, alice_headers, seeded_alice_account, bob_account):
    await client.post(
        "/v1/transfers",
        headers={"Idempotency-Key": "transfer-tx"} | alice_headers,
        json={"to_email": "bob@example.com", "amount": "100.00"}
    )

    resp = await client.get("/v1/accounts/me/transactions", headers=alice_headers)
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["data"]) == 2  # seed + transfer
    transfer_item = [t for t in data["data"] if t["entry_type"] == "transfer"][0]
    assert transfer_item["direction"] == "debit"


async def test_transfer_receiver_view(client, alice_headers, seeded_alice_account, bob_account, bob_headers):
    await client.post(
        "/v1/transfers",
        headers={"Idempotency-Key": "transfer-rx"} | alice_headers,
        json={"to_email": "bob@example.com", "amount": "100.00"}
    )

    resp = await client.get("/v1/accounts/me/transactions", headers=bob_headers)
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["data"]) == 1
    assert data["data"][0]["direction"] == "credit"


async def test_filter_by_entry_type(client, alice_headers, seeded_alice_account):
    resp = await client.get(
        "/v1/accounts/me/transactions",
        headers=alice_headers,
        params={"entry_type": "seed"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["data"]) == 1
    assert data["data"][0]["entry_type"] == "seed"


async def test_filter_by_from_date_excludes_past(client, alice_headers, seeded_alice_account):
    """from_date in the future returns no entries."""
    future = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()
    resp = await client.get(
        "/v1/accounts/me/transactions",
        headers=alice_headers,
        params={"from_date": future},
    )
    assert resp.status_code == 200
    assert resp.json()["data"] == []
    assert resp.json()["meta"]["total"] == 0


async def test_filter_by_to_date_excludes_future(client, alice_headers, seeded_alice_account):
    """to_date in the past returns no entries."""
    past = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
    resp = await client.get(
        "/v1/accounts/me/transactions",
        headers=alice_headers,
        params={"to_date": past},
    )
    assert resp.status_code == 200
    assert resp.json()["data"] == []
    assert resp.json()["meta"]["total"] == 0


async def test_filter_by_date_range_includes_entries(client, alice_headers, seeded_alice_account):
    """Entries created now must appear when the range brackets the present."""
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
    tomorrow = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()
    resp = await client.get(
        "/v1/accounts/me/transactions",
        headers=alice_headers,
        params={"from_date": yesterday, "to_date": tomorrow},
    )
    assert resp.status_code == 200
    assert resp.json()["meta"]["total"] == 1  # the seed entry


async def test_pagination(client, alice_headers, seeded_alice_account):
    for i in range(5):
        await client.post(
            "/v1/dev/seed",
            headers={"Idempotency-Key": f"seed-{i}"} | alice_headers,
            json={"account_id": seeded_alice_account["account_id"], "amount": "100.00"}
        )

    resp = await client.get(
        "/v1/accounts/me/transactions", headers=alice_headers, params={"page": 1, "limit": 2}
    )
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["data"]) == 2
    assert data["meta"]["total"] == 6
    assert data["meta"]["page"] == 1
    assert data["meta"]["limit"] == 2

    resp2 = await client.get(
        "/v1/accounts/me/transactions", headers=alice_headers, params={"page": 2, "limit": 2}
    )
    assert len(resp2.json()["data"]) == 2


async def test_pagination_pages_non_overlapping(client, alice_headers, seeded_alice_account):
    """Page 1 and page 2 must contain distinct entries — no duplicates across pages."""
    for i in range(3):
        await client.post(
            "/v1/dev/seed",
            headers={"Idempotency-Key": f"overlap-seed-{i}"} | alice_headers,
            json={"account_id": seeded_alice_account["account_id"], "amount": "10.00"}
        )

    page1 = await client.get(
        "/v1/accounts/me/transactions", headers=alice_headers, params={"page": 1, "limit": 2}
    )
    page2 = await client.get(
        "/v1/accounts/me/transactions", headers=alice_headers, params={"page": 2, "limit": 2}
    )

    ids1 = {e["entry_id"] for e in page1.json()["data"]}
    ids2 = {e["entry_id"] for e in page2.json()["data"]}
    assert ids1.isdisjoint(ids2), f"Pages share entries: {ids1 & ids2}"


async def test_limit_exceeds_max(client, alice_headers, alice_account):
    resp = await client.get(
        "/v1/accounts/me/transactions", headers=alice_headers, params={"limit": 101}
    )
    assert resp.status_code == 422
