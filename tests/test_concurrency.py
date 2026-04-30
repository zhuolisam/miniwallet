import asyncio
from sqlalchemy import select, func

from app.models.transfer import Transfer
from tests.test_ledger_invariant import assert_ledger_sums_to_zero


async def test_no_overdraft_under_concurrency(client, alice_headers, seeded_alice_account, bob_account, bob_headers, db_session):
    """10 concurrent transfers each for the full balance — exactly 1 must succeed.

    After the dust settles:
    - 1 completed Transfer in DB (money moved exactly once)
    - 9 failed Transfers in DB (audit trail of rejected attempts)
    - Ledger sums to zero
    """
    async def do_transfer(i: int):
        return await client.post(
            "/v1/transfers",
            headers={"Idempotency-Key": f"concurrent-{i}"} | alice_headers,
            json={"to_email": "bob@example.com", "amount": "1000.00"},
        )

    results = await asyncio.gather(*[do_transfer(i) for i in range(10)], return_exceptions=True)
    successes = [r for r in results if not isinstance(r, Exception) and r.status_code == 201]
    failures = [r for r in results if not isinstance(r, Exception) and r.status_code != 201]
    assert len(successes) == 1
    for f in failures:
        assert f.status_code == 422
        assert f.json()["error"]["code"] == "INSUFFICIENT_BALANCE"

    alice_bal = await client.get("/v1/accounts/me/balance", headers=alice_headers)
    assert alice_bal.json()["data"]["balance"] == "0.00000000"

    bob_bal = await client.get("/v1/accounts/me/balance", headers=bob_headers)
    assert bob_bal.json()["data"]["balance"] == "1000.00000000"

    # Every failed attempt is persisted — audit trail is complete
    result = await db_session.execute(
        select(func.count()).select_from(Transfer).where(Transfer.status == "failed")
    )
    failed_count = result.scalar_one()
    assert failed_count == 9, f"Expected 9 failed Transfer records, got {failed_count}"

    result = await db_session.execute(
        select(func.count()).select_from(Transfer).where(Transfer.status == "completed")
    )
    completed_count = result.scalar_one()
    assert completed_count == 1


async def test_concurrent_transfers_fractional_balance(client, alice_headers, seeded_alice_account, bob_account, bob_headers):
    """10 concurrent transfers each for balance/10 — all 10 must succeed."""
    async def do_transfer(i: int):
        return await client.post(
            "/v1/transfers",
            headers={"Idempotency-Key": f"fractional-{i}"} | alice_headers,
            json={"to_email": "bob@example.com", "amount": "100.00"},
        )

    results = await asyncio.gather(*[do_transfer(i) for i in range(10)], return_exceptions=True)
    successes = [r for r in results if not isinstance(r, Exception) and r.status_code == 201]
    assert len(successes) == 10

    alice_bal = await client.get("/v1/accounts/me/balance", headers=alice_headers)
    assert alice_bal.json()["data"]["balance"] == "0.00000000"


async def test_no_deadlock_bidirectional_transfers(db_session, client, alice_headers, seeded_alice_account, bob_account, bob_headers):
    """Alice→Bob and Bob→Alice simultaneously across 10 rounds — must not deadlock.

    Bob starts with no funds so his transfers will fail with INSUFFICIENT_BALANCE,
    but they must not cause a deadlock or leave the DB in an inconsistent state.
    Alice starts with 1000 and sends 50 per round; 10 rounds = 500 total if all succeed.
    """
    async def alice_to_bob(i: int):
        return await client.post(
            "/v1/transfers",
            headers={"Idempotency-Key": f"ab-{i}"} | alice_headers,
            json={"to_account_id": bob_account["account_id"], "amount": "50.00"},
        )

    async def bob_to_alice(i: int):
        return await client.post(
            "/v1/transfers",
            headers={"Idempotency-Key": f"ba-{i}"} | bob_headers,
            json={"to_account_id": seeded_alice_account["account_id"], "amount": "50.00"},
        )

    tasks = [f(i) for i in range(10) for f in (alice_to_bob, bob_to_alice)]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # No unhandled exceptions (deadlocks or crashes) — only expected HTTP responses
    for r in results:
        assert not isinstance(r, Exception), f"Unexpected exception: {r}"
        assert r.status_code in (201, 422), f"Unexpected status {r.status_code}: {r.json()}"

    # Ledger must still balance — no money created or destroyed
    alice_bal = (await client.get("/v1/accounts/me/balance", headers=alice_headers)).json()["data"]["balance"]
    bob_bal = (await client.get("/v1/accounts/me/balance", headers=bob_headers)).json()["data"]["balance"]
    from decimal import Decimal
    assert Decimal(alice_bal) + Decimal(bob_bal) == Decimal("1000.00000000"), (
        f"Money was created or destroyed: alice={alice_bal}, bob={bob_bal}"
    )
    
    await assert_ledger_sums_to_zero(db_session)