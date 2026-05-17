"""Tests for the deposit flow (Phase 3 / Week 10 — US-3.1).

All tests use the standard `client` + `alice_*` + `db_session` fixtures. The
simulate-deposit endpoint is dev-only and unauthenticated (see routers/dev.py
for the rationale — bank partner webhooks have no JWT), so tests hit it
directly without an Authorization header.
"""

import uuid

import pytest
from sqlalchemy import text


pytestmark = pytest.mark.asyncio


def _simulate_deposit_body(account_id: str, **kwargs) -> dict:
    return {
        "account_id": account_id,
        "amount": kwargs.get("amount", "100.00"),
        "currency": kwargs.get("currency", "USD"),
        "source_type": kwargs.get("source_type", "bank_transfer"),
        "external_reference": kwargs.get("external_reference", str(uuid.uuid4())),
    }


async def test_deposit_happy_path(client, alice_account):
    body = _simulate_deposit_body(alice_account["account_id"])

    resp = await client.post("/v1/dev/simulate-deposit", json=body)

    assert resp.status_code == 201
    data = resp.json()["data"]
    assert data["status"] == "completed"
    assert data["external_reference"] == body["external_reference"]
    assert data["amount"] == "100.0000"

    balance_resp = await client.get(
        "/v1/accounts/me/balance",
        headers={"Authorization": f"Bearer {alice_account.get('_token', '')}"},
    )
    # alice_account fixture doesn't carry the token — fetch via alice_headers instead
    # We verify the credit by re-reading alice's balance using the alice_headers fixture,
    # but that's not injected here. Instead assert via deposit response alone.
    assert data["account_id"] == alice_account["account_id"]
    assert data["completed_at"] is not None


async def test_deposit_happy_path_credits_balance(client, alice_account, alice_headers):
    body = _simulate_deposit_body(alice_account["account_id"], amount="250.00")

    await client.post("/v1/dev/simulate-deposit", json=body)

    balance_resp = await client.get("/v1/accounts/me/balance", headers=alice_headers)
    assert balance_resp.status_code == 200
    balance = float(balance_resp.json()["data"]["balance"])
    assert balance == pytest.approx(250.0)


async def test_deposit_idempotent_same_reference(client, alice_account):
    ref = str(uuid.uuid4())
    body = _simulate_deposit_body(alice_account["account_id"], external_reference=ref)

    resp1 = await client.post("/v1/dev/simulate-deposit", json=body)
    assert resp1.status_code == 201
    deposit_id = resp1.json()["data"]["deposit_id"]

    resp2 = await client.post("/v1/dev/simulate-deposit", json=body)
    assert resp2.status_code == 201
    assert resp2.json()["data"]["deposit_id"] == deposit_id


async def test_deposit_idempotent_balance_credited_once(client, alice_account, alice_headers):
    ref = str(uuid.uuid4())
    body = _simulate_deposit_body(alice_account["account_id"], amount="100.00", external_reference=ref)

    await client.post("/v1/dev/simulate-deposit", json=body)
    await client.post("/v1/dev/simulate-deposit", json=body)

    balance_resp = await client.get("/v1/accounts/me/balance", headers=alice_headers)
    balance = float(balance_resp.json()["data"]["balance"])
    assert balance == pytest.approx(100.0)


async def test_deposit_rejected_account_not_found(client):
    body = _simulate_deposit_body(str(uuid.uuid4()))

    resp = await client.post("/v1/dev/simulate-deposit", json=body)

    assert resp.status_code == 201
    data = resp.json()["data"]
    assert data["status"] == "rejected"
    assert data["rejection_reason"] == "ACCOUNT_NOT_FOUND"


async def test_deposit_rejected_unsupported_currency(client, alice_account):
    body = _simulate_deposit_body(alice_account["account_id"], currency="EUR")

    resp = await client.post("/v1/dev/simulate-deposit", json=body)

    assert resp.status_code == 201
    data = resp.json()["data"]
    assert data["status"] == "rejected"
    assert data["rejection_reason"] == "UNSUPPORTED_CURRENCY"


async def test_deposit_ledger_invariant(client, alice_account, db_session):
    body = _simulate_deposit_body(alice_account["account_id"], amount="500.00")
    resp = await client.post("/v1/dev/simulate-deposit", json=body)
    assert resp.json()["data"]["status"] == "completed"

    result = await db_session.execute(text("""
        SELECT SUM(
            CASE WHEN direction = 'credit' THEN amount
                 WHEN direction = 'debit'  THEN -amount
            END
        ) AS net
        FROM ledger_entries
        WHERE entry_type = 'deposit'
    """))
    net = result.scalar()
    assert net == 0


async def test_deposit_get_by_id_enforces_ownership(
    client, alice_account, alice_headers, bob_account, bob_headers
):
    body = _simulate_deposit_body(alice_account["account_id"])
    deposit_resp = await client.post("/v1/dev/simulate-deposit", json=body)
    deposit_id = deposit_resp.json()["data"]["deposit_id"]

    bob_resp = await client.get(f"/v1/deposits/{deposit_id}", headers=bob_headers)
    assert bob_resp.status_code == 404

    alice_resp = await client.get(f"/v1/deposits/{deposit_id}", headers=alice_headers)
    assert alice_resp.status_code == 200
    assert alice_resp.json()["data"]["deposit_id"] == deposit_id


async def test_deposit_dev_only_in_non_dev_env(client, alice_account, monkeypatch):
    from app.config import settings
    monkeypatch.setattr(settings, "app_env", "production")

    body = _simulate_deposit_body(alice_account["account_id"])
    resp = await client.post("/v1/dev/simulate-deposit", json=body)

    assert resp.status_code == 403
