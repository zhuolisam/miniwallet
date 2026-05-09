from decimal import Decimal

from sqlalchemy import text


async def assert_ledger_sums_to_zero(db):
    """
    The double-entry invariant: sum of all ledger legs must equal zero.

    With the leg-based model: credits are positive, debits are negative.
    SUM(CASE direction WHEN 'credit' THEN amount ELSE -amount END) = 0
    """
    total = await db.scalar(text("""
        SELECT COALESCE(SUM(
            CASE WHEN direction = 'credit' THEN amount
                 WHEN direction = 'debit'  THEN -amount
            END
        ), 0)
        FROM ledger_entries
    """))
    assert Decimal(str(total)) == Decimal("0"), (
        f"Ledger invariant violated: sum of all legs = {total} (expected 0)"
    )


async def test_invariant_after_seed(client, alice_headers, seeded_alice_account, db_session):
    await assert_ledger_sums_to_zero(db_session)


async def test_invariant_after_transfer(client, alice_headers, seeded_alice_account, db_session, bob_account, bob_headers):
    await client.post(
        "/v1/transfers",
        headers={"Idempotency-Key": "invariant-transfer"} | alice_headers,
        json={"to_email": "bob@example.com", "amount": "100.00"},
    )
    await assert_ledger_sums_to_zero(db_session)


async def test_system_account_is_negative_after_seed(client, alice_headers, seeded_alice_account, db_session):
    from app.config import SYSTEM_ACCOUNT_ID
    from app.services.account_service import get_balance

    balance = await get_balance(db_session, SYSTEM_ACCOUNT_ID)
    assert balance < Decimal("0"), "System account should be negative after seeding"


async def test_system_account_exact_balance_after_seed(client, alice_headers, alice_account, db_session):
    """System account balance must equal exactly -(seed amount) after a single seed."""
    from app.config import SYSTEM_ACCOUNT_ID
    from app.services.account_service import get_balance

    await client.post(
        "/v1/dev/seed",
        headers={"Idempotency-Key": "exact-seed"} | alice_headers,
        json={"account_id": alice_account["account_id"], "amount": "1000.00"},
    )
    balance = await get_balance(db_session, SYSTEM_ACCOUNT_ID)
    assert balance == Decimal("-1000.0000"), (
        f"Expected system account balance -1000.0000, got {balance}"
    )


async def test_invariant_after_multiple_operations(
    client, alice_headers, seeded_alice_account, db_session, bob_account, bob_headers
):
    """Invariant holds after a series of mixed operations: seed + transfers in both directions."""
    # Alice seeds an additional 500
    await client.post(
        "/v1/dev/seed",
        headers={"Idempotency-Key": "multi-seed"} | alice_headers,
        json={"account_id": seeded_alice_account["account_id"], "amount": "500.00"},
    )
    # Alice → Bob 200
    await client.post(
        "/v1/transfers",
        headers={"Idempotency-Key": "multi-ab"} | alice_headers,
        json={"to_email": "bob@example.com", "amount": "200.00"},
    )
    # Bob seeds 300
    await client.post(
        "/v1/dev/seed",
        headers={"Idempotency-Key": "multi-bob-seed"} | bob_headers,
        json={"account_id": bob_account["account_id"], "amount": "300.00"},
    )
    # Bob → Alice 50
    await client.post(
        "/v1/transfers",
        headers={"Idempotency-Key": "multi-ba"} | bob_headers,
        json={"to_account_id": seeded_alice_account["account_id"], "amount": "50.00"},
    )
    await assert_ledger_sums_to_zero(db_session)
