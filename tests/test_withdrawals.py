"""Tests for the withdrawal saga (Phase 3 / Week 11 — US-3.2).

The `rail` is injected via FastAPI Depends() and tests override it to control
outcomes deterministically. Use `rail.force_next_outcome("fail:TIMEOUT")` to
queue a specific failure for the next call. For failure tests, override
app.dependency_overrides[get_rail] with a pre-configured simulator in the
test body.
"""

import uuid

import pytest
from sqlalchemy import text

from app.dependencies import get_rail
from rail.simulator import BankRailSimulator


pytestmark = pytest.mark.asyncio


def _withdrawal_body(**kwargs) -> dict:
    return {
        "amount": kwargs.get("amount", "100.00"),
        "currency": kwargs.get("currency", "USD"),
        "destination_type": kwargs.get("destination_type", "bank_transfer"),
        "destination_details": kwargs.get("destination_details", {
            "sort_code": "401111",
            "account_number": "98765432",
        }),
    }


def _withdrawal_headers(alice_headers, key=None) -> dict:
    return {"Idempotency-Key": key or str(uuid.uuid4())} | alice_headers


async def test_withdrawal_happy_path(client, alice_headers, seeded_alice_account):
    """Rail succeeds → status=completed, user balance reduced, external_reference set."""
    headers = _withdrawal_headers(alice_headers)
    body = _withdrawal_body(amount="200.00")

    resp = await client.post("/v1/withdrawals", json=body, headers=headers)

    assert resp.status_code == 201
    data = resp.json()["data"]
    assert data["status"] == "completed"
    assert data["external_reference"] is not None
    assert data["external_reference"].startswith("RAIL-")
    assert data["amount"] == "200.0000"
    assert data["account_id"] == seeded_alice_account["account_id"]

    balance_resp = await client.get("/v1/accounts/me/balance", headers=alice_headers)
    assert balance_resp.status_code == 200
    balance = float(balance_resp.json()["data"]["balance"])
    assert balance == pytest.approx(800.0)


async def test_withdrawal_rail_failure_compensates(client, alice_headers, seeded_alice_account):
    """Rail fails → status=failed, failure_code recorded, balance restored."""
    failing_rail = BankRailSimulator()
    failing_rail.force_next_outcome("fail:TIMEOUT")

    app = client._transport.app
    app.dependency_overrides[get_rail] = lambda: failing_rail

    resp = await client.post(
        "/v1/withdrawals",
        json=_withdrawal_body(amount="100.00"),
        headers=_withdrawal_headers(alice_headers, key="fail-test-1"),
    )

    assert resp.status_code == 201
    data = resp.json()["data"]
    assert data["status"] == "failed"
    assert data["failure_code"] == "TIMEOUT"

    app.dependency_overrides.pop(get_rail, None)  # restore conftest's default rail

    balance_resp = await client.get("/v1/accounts/me/balance", headers=alice_headers)
    balance = float(balance_resp.json()["data"]["balance"])
    assert balance == pytest.approx(1000.0)


async def test_withdrawal_insufficient_balance(client, alice_headers, alice_account):
    """Not enough balance → 422 INSUFFICIENT_BALANCE, no withdrawal row written."""
    headers = _withdrawal_headers(alice_headers)
    body = _withdrawal_body(amount="500.00")

    resp = await client.post("/v1/withdrawals", json=body, headers=headers)

    assert resp.status_code == 422
    assert resp.json()["error"]["code"] == "INSUFFICIENT_BALANCE"


async def test_withdrawal_idempotent_same_key(client, alice_headers, seeded_alice_account):
    """Same Idempotency-Key twice → one withdrawal row, one debit."""
    key = "idempotent-withdrawal-1"
    headers = _withdrawal_headers(alice_headers, key=key)
    body = _withdrawal_body(amount="100.00")

    resp1 = await client.post("/v1/withdrawals", json=body, headers=headers)
    assert resp1.status_code == 201
    withdrawal_id = resp1.json()["data"]["withdrawal_id"]

    resp2 = await client.post("/v1/withdrawals", json=body, headers=headers)
    assert resp2.status_code == 201
    assert resp2.json()["data"]["withdrawal_id"] == withdrawal_id

    balance_resp = await client.get("/v1/accounts/me/balance", headers=alice_headers)
    balance = float(balance_resp.json()["data"]["balance"])
    assert balance == pytest.approx(900.0)


async def test_withdrawal_requires_idempotency_key(client, alice_headers, seeded_alice_account):
    """Missing Idempotency-Key header → 422 (FastAPI validation)."""
    body = _withdrawal_body()

    resp = await client.post("/v1/withdrawals", json=body, headers=alice_headers)

    assert resp.status_code == 422


async def test_withdrawal_get_by_id_enforces_ownership(client, alice_headers, seeded_alice_account, bob_headers, bob_account):
    """Bob cannot GET Alice's withdrawal → 404."""
    headers = _withdrawal_headers(alice_headers)
    resp = await client.post("/v1/withdrawals", json=_withdrawal_body(), headers=headers)
    assert resp.status_code == 201
    withdrawal_id = resp.json()["data"]["withdrawal_id"]

    bob_resp = await client.get(f"/v1/withdrawals/{withdrawal_id}", headers=bob_headers)
    assert bob_resp.status_code == 404

    alice_resp = await client.get(f"/v1/withdrawals/{withdrawal_id}", headers=alice_headers)
    assert alice_resp.status_code == 200
    assert alice_resp.json()["data"]["withdrawal_id"] == withdrawal_id


async def test_withdrawal_idempotency_conflict(client, alice_headers, seeded_alice_account):
    """Same Idempotency-Key with different body → 409 IDEMPOTENCY_CONFLICT."""
    key = "conflict-withdrawal-1"
    headers = _withdrawal_headers(alice_headers, key=key)

    resp1 = await client.post("/v1/withdrawals", json=_withdrawal_body(amount="100.00"), headers=headers)
    assert resp1.status_code == 201

    resp2 = await client.post("/v1/withdrawals", json=_withdrawal_body(amount="200.00"), headers=headers)
    assert resp2.status_code == 409
    assert resp2.json()["error"]["code"] == "IDEMPOTENCY_CONFLICT"


async def test_withdrawal_exact_balance(client, alice_headers, seeded_alice_account):
    """Withdraw full balance ($1000) succeeds — boundary check for < vs <=."""
    headers = _withdrawal_headers(alice_headers)
    body = _withdrawal_body(amount="1000.00")

    resp = await client.post("/v1/withdrawals", json=body, headers=headers)

    assert resp.status_code == 201
    assert resp.json()["data"]["status"] == "completed"

    balance_resp = await client.get("/v1/accounts/me/balance", headers=alice_headers)
    balance = float(balance_resp.json()["data"]["balance"])
    assert balance == pytest.approx(0.0)


async def test_withdrawal_ledger_invariant_after_compensation(client, alice_headers, seeded_alice_account, db_session):
    """After a compensated withdrawal: SUM(all ledger entries, signed) = 0 for withdrawal types."""
    failing_rail = BankRailSimulator()
    failing_rail.force_next_outcome("fail:NETWORK_ERROR")

    app = client._transport.app
    app.dependency_overrides[get_rail] = lambda: failing_rail

    resp = await client.post(
        "/v1/withdrawals",
        json=_withdrawal_body(amount="250.00"),
        headers=_withdrawal_headers(alice_headers, key="ledger-invariant-1"),
    )
    assert resp.status_code == 201
    assert resp.json()["data"]["status"] == "failed"

    app.dependency_overrides.pop(get_rail, None)  # restore conftest's default rail

    result = await db_session.execute(text("""
        SELECT SUM(
            CASE WHEN direction = 'credit' THEN amount
                 WHEN direction = 'debit'  THEN -amount
            END
        ) AS net
        FROM ledger_entries
        WHERE entry_type IN ('withdrawal', 'withdrawal_reversal')
    """))
    net = result.scalar()
    assert net == 0
