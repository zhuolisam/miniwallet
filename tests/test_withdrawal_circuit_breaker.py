"""Tests for withdrawal + circuit breaker integration.

These tests verify the pre-flight circuit breaker check wired into
POST /v1/withdrawals:
1. Circuit OPEN → 503 BANK_RAIL_UNAVAILABLE, no debit
2. Circuit CLOSED → normal flow proceeds
"""

from decimal import Decimal

import pytest
import pytest_asyncio
from datetime import datetime, timezone

from sqlalchemy import select, text

from app.circuit_breaker import (
    CircuitBreaker, _KEY_STATE, _KEY_LAST_FAILURE_AT, _KEY_FAILURE_COUNT,
)
from app.dependencies import get_circuit_breaker
from app.exceptions import BankRailUnavailableError
from app.models.ledger_entry import LedgerEntry
from app.models.withdrawal import Withdrawal
from app.schemas.withdrawal import WithdrawalRequest


@pytest_asyncio.fixture
async def client_with_circuit_breaker(client, redis_client):
    """HTTP client with a circuit breaker backed by the test Redis."""
    cb = CircuitBreaker(redis=redis_client, failure_threshold=3, cooldown_seconds=30)

    def override_get_cb():
        return cb

    app = client._transport.app
    app.dependency_overrides[get_circuit_breaker] = override_get_cb

    yield client, cb, redis_client

    del app.dependency_overrides[get_circuit_breaker]


class TestWithdrawalCircuitBreakerPreFlight:
    """Pre-flight check: circuit OPEN → 503 before any DB work."""

    async def test_circuit_open_returns_503(
        self, client_with_circuit_breaker, alice_headers, seeded_alice_account, db_session
    ):
        # Setup:
        # 1. Get the circuit breaker's redis from the fixture
        client, cb, redis_client = client_with_circuit_breaker

        # 2. Set circuit to OPEN in Redis:
        await redis_client.set(_KEY_STATE, "OPEN")
        await redis_client.set(_KEY_LAST_FAILURE_AT, datetime.now(timezone.utc).isoformat())
        await redis_client.set(_KEY_FAILURE_COUNT, "3")
        #
        # Act:
        # 3. POST /v1/withdrawals with valid body + Idempotency-Key header
        response = await client.post(
            "/v1/withdrawals",
            headers={**alice_headers, "Idempotency-Key": "test-open-circuit"},
            json=WithdrawalRequest(
                    amount="100.00",
                    currency="USD",
                    destination_type="bank_transfer",
                    destination_details={
                        "account_number": "12345678",
                        "sort_code": "87654321",
                    },
                ).model_dump(),
            )
        # Assert:
        assert response.status_code == 503
        response = response.json()
        assert response["error"]["code"] == "BANK_RAIL_UNAVAILABLE"
        assert response["error"]["message"] == "Bank rail is temporarily unavailable. Please retry later."
        # 6. No withdrawal row created (query DB to confirm)
        res = await db_session.execute(select(Withdrawal))
        withdrawals = res.scalars().all()
        assert len(withdrawals) == 0
        # 7. No ledger entries created (balance unchanged)
        result = await db_session.execute(
            text("""
                SELECT COALESCE(SUM(
                    CASE WHEN direction = 'credit' THEN amount
                        WHEN direction = 'debit'  THEN -amount
                    END
                ), 0) AS balance
                FROM ledger_entries
                WHERE account_id = :id
            """),
            {"id": str(seeded_alice_account["account_id"])},
        )
        row = result.scalar()
        balance = Decimal(str(row))
        assert balance == Decimal("1000.00")  # initial seeded balance


    async def test_circuit_closed_allows_withdrawal(
        self, client_with_circuit_breaker, alice_headers, seeded_alice_account, db_session
    ):
        client, cb, redis_client = client_with_circuit_breaker
        # 1. POST /v1/withdrawals with valid body
        response = await client.post(
            "/v1/withdrawals",
            headers={**alice_headers, "Idempotency-Key": "test-closed-circuit"},
            json=WithdrawalRequest(
                    amount="100.00",
                    currency="USD",
                    destination_type="bank_transfer",
                    destination_details={
                        "account_number": "12345678",
                        "sort_code": "87654321",
                    },
                ).model_dump(),
            )

        # 2. Response status == 201
        assert response.status_code == 201
        response_json = response.json()["data"]
        assert response_json["amount"] == "100.0000"
        assert response_json["status"] in ("pending", "completed")
        # 3. Withdrawal record created (status is completed or pending/submitted)
        res = await db_session.execute(select(Withdrawal).where(Withdrawal.id == response_json["withdrawal_id"]))
        withdrawals = res.scalars().all()
        assert len(withdrawals) == 1
        assert withdrawals[0].status in ("pending", "completed")
        # 4. Ledger entry created (balance debited by amount)
        res = await db_session.execute(select(LedgerEntry).where(LedgerEntry.reference_id == response_json["withdrawal_id"]).where(LedgerEntry.account_id == seeded_alice_account["account_id"]))
        ledger_entries = res.scalars().all()
        assert ledger_entries[0].amount == pytest.approx(100)
        assert ledger_entries[0].direction == "debit"
        assert str(ledger_entries[0].account_id) == seeded_alice_account["account_id"]
