"""Saga recovery worker — detects and resolves stuck withdrawals.

If the process crashes between TX 1 (debit committed) and TX 3 (rail result
recorded), the user's money is debited but nothing happened at the rail.
Without recovery, that money is gone forever. This worker closes that gap.

Design (two-phase approach, same as the scheduler):
    Phase 1 — Claim: short TX with FOR UPDATE SKIP LOCKED to find stuck rows.
              Record their IDs. Commit (release locks immediately).
    Phase 2 — Resolve: each withdrawal gets its own session. Rail I/O happens
              here — we do NOT hold row locks during network calls.

Recovery rules:
    - `status='pending'` stuck > 5 min: retry the rail call, or compensate if
      circuit is OPEN.
    - `status='submitted'` stuck > 5 min:
        * If `external_reference` is present: query rail for status, complete or
          compensate accordingly.
        * If `external_reference` is NULL: compensate after 30-min hard timeout.

Runs:
    1. On application startup (catches crashes from last downtime)
    2. Every 5 minutes via background loop (in app/main.py)

Idempotency: compensation's credit leg carries idempotency_key='reversal:{withdrawal_id}'.
The UNIQUE constraint on ledger_entries.idempotency_key prevents double-credit.
"""

import logging
import uuid
from datetime import datetime, timezone, timedelta

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.circuit_breaker import CircuitBreaker, CircuitOpenError
from app.config import SYSTEM_ACCOUNT_ID
from app.events.publisher import publish_event
from app.events.schemas import WithdrawalCompletedPayload, WithdrawalFailedPayload
from app.models.ledger_entry import LedgerEntry
from app.models.withdrawal import Withdrawal
from rail.simulator import BankRailSimulator, RailError

logger = logging.getLogger(__name__)

STUCK_THRESHOLD_MINUTES = 5
HARD_TIMEOUT_MINUTES = 30
BATCH_LIMIT = 20


async def recover_stuck_withdrawals(
    db_session_factory: async_sessionmaker,
    circuit_breaker: CircuitBreaker,
    rail: BankRailSimulator,
) -> None:
    """Main recovery entry point. Called on startup and every 5 minutes.

    Two-phase approach:
    1. Claim: short TX with FOR UPDATE SKIP LOCKED to find stuck withdrawal IDs.
    2. Resolve: each withdrawal in its own session (rail I/O safe).

    Sequential resolution — limits concurrent rail calls when the rail is stressed.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=STUCK_THRESHOLD_MINUTES)

    # --- Phase 1: Claim stuck withdrawal IDs (short TX) ---
    stuck_ids: list[uuid.UUID] = []
    async with db_session_factory() as db:
        async with db.begin():
            result = await db.execute(
                select(Withdrawal.id)
                .where(Withdrawal.status.in_(["pending", "submitted"]))
                .where(Withdrawal.updated_at < cutoff)
                .with_for_update(skip_locked=True)
                .limit(BATCH_LIMIT)
            )
            stuck_ids = [row[0] for row in result.all()]

    # --- Phase 2: Resolve each in its own session ---
    for withdrawal_id in stuck_ids:
        try:
            await _resolve_one(db_session_factory, withdrawal_id, circuit_breaker, rail)
        except Exception:
            logger.exception("Recovery failed for withdrawal %s", withdrawal_id)


async def _resolve_one(
    db_session_factory: async_sessionmaker,
    withdrawal_id: uuid.UUID,
    circuit_breaker: CircuitBreaker,
    rail: BankRailSimulator,
) -> None:
    """Resolve a single stuck withdrawal. Runs in its own session + transaction.

    Re-loads the withdrawal with FOR UPDATE (may have been resolved by another
    instance between Phase 1 and now). Dispatches to _recover_pending or
    _recover_submitted based on current status.
    """
    async with db_session_factory() as db:
        async with db.begin():
            row = await db.execute(
                select(Withdrawal)
                .where(Withdrawal.id == withdrawal_id)
                .with_for_update()
            )
            w = row.scalar_one_or_none()
            if w is None or w.status not in ("pending", "submitted"):
                return
            if w.status == "pending":
                await _recover_pending(db, w, circuit_breaker, rail)
            elif w.status == "submitted":
                await _recover_submitted(db, w, rail)


async def _recover_pending(
    db: AsyncSession,
    withdrawal: Withdrawal,
    circuit_breaker: CircuitBreaker,
    rail: BankRailSimulator,
) -> None:
    """Recovery for withdrawals stuck at 'pending' (crashed before rail call).

    Strategy:
    - If circuit breaker is OPEN → compensate immediately (no point retrying a dead rail)
    - Otherwise → transition to 'submitted', call rail via circuit breaker
      - Success → complete
      - RailError or CircuitOpenError → compensate

    This mirrors the normal withdrawal flow but from a recovery context.
    """
    if not await circuit_breaker.is_call_allowed():
        _compensate(db, withdrawal, "CIRCUIT_OPEN")
        return

    withdrawal.status = "submitted"
    withdrawal.submitted_at = datetime.now(timezone.utc)
    await db.flush()

    try:
        result = await circuit_breaker.call(
            rail.send_withdrawal,
            withdrawal_id=withdrawal.id,
            amount=withdrawal.amount,
            destination=withdrawal.destination_details,
        )
        _complete(db, withdrawal, result.reference)
    except (RailError, CircuitOpenError) as e:
        _compensate(db, withdrawal, e.code)


async def _recover_submitted(
    db: AsyncSession,
    withdrawal: Withdrawal,
    rail: BankRailSimulator,
) -> None:
    """Recovery for withdrawals stuck at 'submitted' (crashed after rail call).

    Strategy depends on whether we have an external_reference:
    - If present: query rail for status → complete or compensate accordingly
    - If NULL: rail may or may not have received it. Conservative: compensate
      after 30-minute hard timeout.
    """
    hard_timeout_cutoff = datetime.now(timezone.utc) - timedelta(minutes=HARD_TIMEOUT_MINUTES)

    if withdrawal.external_reference:
        try:
            status = await rail.query_status(withdrawal.external_reference)
            if status.state == "completed":
                _complete(db, withdrawal, withdrawal.external_reference)
            elif status.state == "failed":
                _compensate(db, withdrawal, status.reason or "RAIL_REJECTED")
        except Exception:
            if withdrawal.updated_at < hard_timeout_cutoff:
                _compensate(db, withdrawal, "TIMEOUT")
    else:
        if withdrawal.updated_at < hard_timeout_cutoff:
            _compensate(db, withdrawal, "TIMEOUT")


def _compensate(db: AsyncSession, withdrawal: Withdrawal, failure_code: str) -> None:
    """Write compensating ledger entries + mark withdrawal as failed.

    Idempotent: the credit leg carries idempotency_key='reversal:{withdrawal_id}'.
    If this runs twice, the second INSERT raises IntegrityError from the UNIQUE
    constraint, and the entire transaction rolls back — no double-credit.
    """
    txn_id = uuid.uuid4()
    now = datetime.now(timezone.utc)
    idem_key = f"reversal:{withdrawal.id}"

    db.add(LedgerEntry(
        transaction_id=txn_id,
        account_id=SYSTEM_ACCOUNT_ID,
        direction="debit",
        amount=withdrawal.amount,
        currency=withdrawal.currency,
        entry_type="withdrawal_reversal",
        reference_id=withdrawal.id,
        created_at=now,
    ))
    db.add(LedgerEntry(
        transaction_id=txn_id,
        account_id=withdrawal.account_id,
        direction="credit",
        amount=withdrawal.amount,
        currency=withdrawal.currency,
        entry_type="withdrawal_reversal",
        reference_id=withdrawal.id,
        idempotency_key=idem_key,
        created_at=now,
    ))

    withdrawal.status = "failed"
    withdrawal.failure_code = failure_code
    withdrawal.completed_at = now

    publish_event(db, "withdrawal.events", "withdrawal.failed",
        WithdrawalFailedPayload(
            withdrawal_id=str(withdrawal.id),
            account_id=str(withdrawal.account_id),
            amount=f"{withdrawal.amount:.4f}",
            currency=withdrawal.currency,
            failure_code=failure_code,
            entry_type="withdrawal_reversal",
        ))


def _complete(db: AsyncSession, withdrawal: Withdrawal, external_reference: str) -> None:
    """Mark withdrawal as completed. Sets submitted_at if not already set (recovery path)."""
    now = datetime.now(timezone.utc)
    withdrawal.status = "completed"
    withdrawal.external_reference = external_reference
    withdrawal.completed_at = now
    if withdrawal.submitted_at is None:
        withdrawal.submitted_at = now

    publish_event(db, "withdrawal.events", "withdrawal.completed",
        WithdrawalCompletedPayload(
            withdrawal_id=str(withdrawal.id),
            account_id=str(withdrawal.account_id),
            amount=f"{withdrawal.amount:.4f}",
            currency=withdrawal.currency,
            external_reference=external_reference,
            entry_type="withdrawal",
        ))
