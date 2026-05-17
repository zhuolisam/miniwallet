"""Withdrawal service — outbound saga orchestrator (Phase 3 / Week 11).

Unlike a transfer (single DB transaction) or a deposit (single DB transaction),
a withdrawal crosses a system boundary: our ledger plus an external bank rail.
Between "user clicks withdraw" and "rail confirms", the user could initiate
other transfers, receive scheduled payments, or attempt a second withdrawal —
so we debit IMMEDIATELY and compensate on failure, rather than waiting for the
rail to confirm before debiting. This is not a design choice; it is the only
correct approach for fiat payments. Every neobank operates this way.

This implementation is an ORCHESTRATION saga: one function owns the entire
lifecycle, and the state is recorded in the `withdrawals.status` column.
Auditable at 2am without event replay. Choreography (reacting to events)
would be harder to debug and harder to recover.

State machine (documented in full in SYSTEM-DESIGN.md Section 4):

    pending → submitted → completed
                       ↓
                       → failed

Transactions:

    TX 1 — reserve funds:
        SELECT account FOR UPDATE
        check balance >= amount
        INSERT withdrawals (status='pending')
        INSERT ledger debit leg (user → system)
        INSERT ledger credit leg (user → system)
        INSERT outbox (withdrawal.initiated)
        COMMIT

    Step 2 — mark submitted + call rail (short TX for the status flip):
        UPDATE withdrawals SET status='submitted', submitted_at=NOW()
        COMMIT
        result = await rail.send_withdrawal(...)
        # ← may raise RailError → go to TX 3b

    TX 3a — complete (on rail success):
        UPDATE withdrawals SET status='completed', external_reference=..., completed_at=NOW()
        INSERT outbox (withdrawal.completed)
        COMMIT

    TX 3b — compensate (on rail failure):
        INSERT ledger debit leg (system, entry_type='withdrawal_reversal')
        INSERT ledger credit leg (user, entry_type='withdrawal_reversal',
                                   idempotency_key='reversal:{withdrawal_id}')
        UPDATE withdrawals SET status='failed', failure_code=..., completed_at=NOW()
        INSERT outbox (withdrawal.failed)
        COMMIT

Ledger invariant: compensation writes TWO NEW ledger rows — never UPDATE the
original debit. Append-only. An auditor reviewing the account sees the debit
AND the reversal — both exist, both are dated, both carry the same reference_id
pointing at the withdrawals row. That's the audit story regulators expect.

Week 11 scope: the circuit breaker pre-flight check and the saga recovery job
land in Week 12. The pre-flight is a best-effort UX optimization (compensating
a debit that never should have happened is avoidable noise); the happy-path
and compensation logic here work without it. Week 11 calls the rail DIRECTLY
(no circuit breaker wrapper) — we add the wrapper in Week 12 and also wire up
the recovery loop in app/main.py::lifespan() at the same time.
"""

import hashlib
import json
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from uuid import UUID

from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.circuit_breaker import CircuitOpenError
from app.config import SYSTEM_ACCOUNT_ID
from app.events.publisher import publish_event
from app.events.schemas import WithdrawalCompletedPayload, WithdrawalFailedPayload, WithdrawalInitiatedPayload
from app.exceptions import AccountNotFoundError, IdempotencyConflictError, InsufficientBalanceError, WithdrawalNotFoundError
from app.models.account import Account
from app.models.ledger_entry import LedgerEntry
from app.models.withdrawal import Withdrawal
from app.schemas.withdrawal import WithdrawalResponse
from app.services import account_service
from rail.simulator import BankRailSimulator, RailError


# Cache TTL for idempotency snapshots. 24h matches the transfer service and
# exceeds any realistic saga completion time, so legitimate retries always
# land on a cache hit before the key expires.
IDEMPOTENCY_TTL_SECONDS = 86400


async def create_withdrawal(
    db: AsyncSession,
    redis: Redis,
    rail: BankRailSimulator,
    account_id: UUID,
    amount: Decimal,
    currency: str,
    destination_type: str,
    destination_details: dict,
    idempotency_key: str,
    actor_user_id: UUID | None = None,
    circuit_breaker=None,
) -> WithdrawalResponse:
    """Run the withdrawal saga end-to-end.

    Returns the final WithdrawalResponse (terminal status for the common case
    where the rail responds within the request lifetime). On idempotent retry
    with the same `idempotency_key`, returns the existing record's current
    state (which may be pending, submitted, completed, or failed depending on
    timing — clients MUST poll GET /v1/withdrawals/{id} for terminal state).

    Raises:
        InsufficientBalanceError — caller had less than `amount` in the account.
        AccountNotFoundError — account_id does not exist (defensive; the router
            resolves account from JWT so this is unusual).
        IdempotencyConflictError — same key, different parameters.
        # Week 12 adds: BankRailUnavailableError when circuit breaker is OPEN.

    Flow (implemented step-by-step by the student):

    1. Idempotency fast path — check Redis BEFORE touching the DB.
       Matches the pattern in /v1/transfers. Two layers serve different goals:

         - Redis (fast path): latency + lock avoidance. A retry that hits
           cache skips TX 1 entirely — no SELECT FOR UPDATE, no ledger
           derivation, no rollback work. This matters because TX 1 takes a
           row lock on the sender account; serializing every retry through
           that lock would bottleneck legitimate concurrent flows on the
           same account (transfers, scheduled payments, etc.).
         - DB UNIQUE on withdrawals.idempotency_key (safety net): correctness.
           If Redis is down, the key was evicted, or TTL expired (24h+),
           the constraint still prevents double-debit.

       Implementation outline (mirror transfer_service.transfer):
           cached_raw = await redis.get(f"idempotency:withdrawal:{idempotency_key}")
           if cached_raw:
               cached = json.loads(cached_raw)
               request_hash = _hash_request(account_id, amount, currency, destination_type, destination_details)
               if cached["request_hash"] != request_hash:
                   raise IdempotencyConflictError()
               return WithdrawalResponse(**json.loads(cached["response"]))

    2. TX 1: Reserve funds.
       See SYSTEM-DESIGN Section 4 for the exact SQL. Key points:
       - SELECT account FOR UPDATE (sender only — we don't touch a receiver)
       - Derive balance via account_service.get_balance(); reject if < amount
       - INSERT Withdrawal (status='pending', idempotency_key=..., ...)
       - INSERT two ledger_entries sharing a transaction_id:
           debit leg:  user → entry_type='withdrawal', reference_id=withdrawal.id
           credit leg: system → entry_type='withdrawal', reference_id=withdrawal.id
         NOTE: withdrawal DEBITS the user (money leaving), CREDITS system.
         (A seed is the opposite direction — don't copy-paste blindly.)
       - publish_event('withdrawal.events', 'withdrawal.initiated', ...)
       - COMMIT

       ⚠️  Handle IntegrityError on flush (DB UNIQUE on idempotency_key):
           try:
               db.add(withdrawal)
               await db.flush()
           except IntegrityError:
               await db.rollback()
               existing = await db.execute(
                   select(Withdrawal).where(Withdrawal.idempotency_key == idempotency_key)
               )
               return _withdrawal_to_response(existing.scalar_one())

       This catches the case where Redis missed (key expired/evicted/Redis
       down) but the withdrawal row already exists in the DB. Without this,
       that scenario is an unhandled 500.

    3. Cache in Redis — IMMEDIATELY after TX 1 commits, before the rail call.
       This is the critical ordering:

           response = _withdrawal_to_response(withdrawal)  # status='pending'
           cache_payload = json.dumps({
               "request_hash": _hash_request(account_id, amount, ...),
               "response": response.model_dump_json(),
           })
           await redis.set(
               f"idempotency:withdrawal:{idempotency_key}",
               cache_payload,
               ex=IDEMPOTENCY_TTL_SECONDS,
           )

       Why here and not after the saga completes:
         - The rail call takes 2-5 seconds. Retries arrive during this window.
         - If we cache only after TX 3, retries during the rail window miss
           Redis and hit the DB UNIQUE → avoidable IntegrityError + rollback.
         - The cached response (status='pending') is intentionally stale.
           Redis is a duplicate-request guard, NOT a status cache. The client
           MUST poll GET /v1/withdrawals/{id} for terminal state.

       Edge case — crash between TX 1 commit and Redis SET:
         - Window is ~microseconds (one await). If it happens, the next retry
           misses Redis → hits DB UNIQUE → IntegrityError handler (step 2)
           returns the existing row. The DB constraint is the safety net.

    4. Step 2: Transition to submitted + call rail.
       Separate short TX for the status flip — we must NOT hold open
       transactions across external network calls. The flow is:

           async with db.begin():
               withdrawal.status = 'submitted'
               withdrawal.submitted_at = now
           # commits here — row lock released

           try:
               result = await rail.send_withdrawal(
                   withdrawal_id=withdrawal.id,
                   amount=withdrawal.amount,
                   destination=withdrawal.destination_details,
               )
           except RailError as e:
               await _compensate(db, withdrawal, e.code)
               return _withdrawal_to_response(withdrawal)

           await _complete(db, withdrawal, result.reference)
           return _withdrawal_to_response(withdrawal)

       `updated_at` is refreshed automatically by the before_flush listener in
       app/models/withdrawal.py — you do NOT need to set it manually in any
       branch. That's deliberate: forgetting to set updated_at is the class
       of bug that makes stuck rows invisible to recovery.

    5. TX 3a `_complete` / TX 3b `_compensate`: implemented as helpers below.
    """
    # Step 1: Idempotency fast path (Redis)
    cached_raw = await redis.get(f"idempotency:withdrawal:{idempotency_key}")
    if cached_raw:
        cached = json.loads(cached_raw)
        request_hash = _hash_request(account_id, amount, currency, destination_type, destination_details)
        if cached["request_hash"] != request_hash:
            raise IdempotencyConflictError()
        return WithdrawalResponse(**json.loads(cached["response"]))

    # Step 2
    if account_id == SYSTEM_ACCOUNT_ID:
        raise AccountNotFoundError()
    account = await db.execute(select(Account).where(Account.id == account_id).with_for_update())
    account = account.scalar_one_or_none()
    if account is None:
        raise AccountNotFoundError()

    balance = await account_service.get_balance(db, account_id)
    if balance < amount:
        raise InsufficientBalanceError()

    now = datetime.now(timezone.utc)
    withdrawal = Withdrawal(
        account_id=account_id,
        amount=amount,
        currency=currency,
        destination_type=destination_type,
        destination_details=destination_details,
        idempotency_key=idempotency_key,
        status="pending",
        created_at=now,
        updated_at=now,
    )

    db.add(withdrawal)
    try:
        await db.flush()
    except IntegrityError:
        await db.rollback()
        existing = await db.execute(
            select(Withdrawal).where(Withdrawal.idempotency_key == idempotency_key)
        )
        return _withdrawal_to_response(existing.scalar_one())

    txn_id = uuid.uuid4()
    debit_leg = LedgerEntry(
        transaction_id=txn_id,
        account_id=account_id,
        direction="debit",
        amount=amount,
        currency=currency,
        entry_type="withdrawal",
        reference_id=withdrawal.id,
        created_at=now,
    )
    credit_leg = LedgerEntry(
        transaction_id=txn_id,
        account_id=SYSTEM_ACCOUNT_ID,
        direction="credit",
        amount=amount,
        currency=currency,
        entry_type="withdrawal",
        reference_id=withdrawal.id,
        created_at=now,
    )

    db.add_all([credit_leg, debit_leg])

    publish_event(
        db=db,
        topic='withdrawal.events',
        event_type='withdrawal.initiated',
        payload=WithdrawalInitiatedPayload(
            withdrawal_id=str(withdrawal.id),
            account_id=str(account_id),
            amount=f"{amount:.4f}",
            currency=currency,
            destination_type=destination_type,
            entry_type="withdrawal",
        ),
        actor_id=actor_user_id,
    )
    await db.commit()

    response = _withdrawal_to_response(withdrawal)

    cache_payload = json.dumps({
        "request_hash": _hash_request(account_id, amount, currency, destination_type, destination_details),
        "response": response.model_dump_json(),
    })
    await redis.set(
        f"idempotency:withdrawal:{idempotency_key}",
        cache_payload,
        ex=IDEMPOTENCY_TTL_SECONDS,
    )

    async with db.begin():
        withdrawal.status = 'submitted'
        withdrawal.submitted_at = datetime.now(timezone.utc)
    
    try:
        if circuit_breaker:
            result = await circuit_breaker.call(
                rail.send_withdrawal,
                withdrawal_id=withdrawal.id,
                amount=withdrawal.amount,
                destination=withdrawal.destination_details,
            )
        else:
            result = await rail.send_withdrawal(
                withdrawal_id=withdrawal.id,
                amount=withdrawal.amount,
                destination=withdrawal.destination_details,
            )
    except (RailError, CircuitOpenError) as e:
        withdrawal = await _compensate(db, withdrawal, e.code, actor_user_id)
        return _withdrawal_to_response(withdrawal)

    withdrawal = await _complete(db, withdrawal, result.reference, actor_user_id)
    return _withdrawal_to_response(withdrawal)

async def _complete(
    db: AsyncSession,
    withdrawal: Withdrawal,
    external_reference: str,
    actor_user_id: UUID | None = None,
) -> Withdrawal:
    """TX 3a — rail accepted the withdrawal. Record the reference and mark completed.

    No new ledger entries here — the debit from TX 1 was correct; we are just
    confirming it.
    """
    async with db.begin():
        withdrawal.status = 'completed'
        withdrawal.external_reference = external_reference
        withdrawal.completed_at = datetime.now(timezone.utc)

        publish_event(
            db=db,
            topic='withdrawal.events',
            event_type='withdrawal.completed',
            payload=WithdrawalCompletedPayload(
                withdrawal_id=str(withdrawal.id),
                account_id=str(withdrawal.account_id),
                amount=f"{withdrawal.amount:.4f}",
                currency=withdrawal.currency,
                external_reference=external_reference,
                entry_type="withdrawal",
            ),
            actor_id=actor_user_id,
        )

    return withdrawal

async def _compensate(
    db: AsyncSession,
    withdrawal: Withdrawal,
    failure_code: str,
    actor_user_id: UUID | None = None,
) -> Withdrawal:
    """TX 3b — rail rejected. Write the reversal ledger entries and mark failed.

    CRITICAL — the idempotency_key on the CREDIT leg is 'reversal:{withdrawal_id}'.
    This guarantees that running compensation twice (e.g., saga retry + Week 12
    recovery both fire) raises IntegrityError on the second run, which aborts
    the second TX — no double-credit is possible.

    NEVER update the original debit row. Always append new rows. The ledger is
    append-only; auditors expect to see debit + reversal as two separate events.
    """

    now = datetime.now(timezone.utc)
    async with db.begin():
        txn_id = uuid.uuid4()
        reversal_debit = LedgerEntry(
            transaction_id=txn_id,
            account_id=SYSTEM_ACCOUNT_ID,
            direction='debit',
            amount=withdrawal.amount,
            currency=withdrawal.currency,
            entry_type='withdrawal_reversal',
            reference_id=withdrawal.id,
            created_at=now,
        )
        reversal_credit = LedgerEntry(
            transaction_id=txn_id,
            account_id=withdrawal.account_id,
            direction='credit',
            amount=withdrawal.amount,
            currency=withdrawal.currency,
            entry_type='withdrawal_reversal',
            reference_id=withdrawal.id,
            idempotency_key=f"reversal:{withdrawal.id}",
            created_at=now,
        )
        db.add_all([reversal_debit, reversal_credit])

        withdrawal.status = 'failed'
        withdrawal.failure_code = failure_code
        withdrawal.completed_at = now

        publish_event(
            db=db,
            topic='withdrawal.events',
            event_type='withdrawal.failed',
            payload=WithdrawalFailedPayload(
                withdrawal_id=str(withdrawal.id),
                account_id=str(withdrawal.account_id),
                amount=f"{withdrawal.amount:.4f}",
                currency=withdrawal.currency,
                failure_code=failure_code,
                entry_type="withdrawal_reversal",
            ),
            actor_id=actor_user_id,
        )
    return withdrawal

async def get_withdrawal(
    db: AsyncSession,
    withdrawal_id: UUID,
    requesting_account_id: UUID,
) -> WithdrawalResponse:
    """Load a withdrawal by ID, enforcing ownership.

    404 if the row doesn't exist OR the caller's account doesn't own it —
    same "conflate missing-vs-not-yours" rule as get_deposit / get_transfer.
    """

    withdrawal = await db.execute(
        select(Withdrawal).where(Withdrawal.id == withdrawal_id)
    )
    withdrawal = withdrawal.scalar_one_or_none()
    if withdrawal is None or withdrawal.account_id != requesting_account_id:
        raise WithdrawalNotFoundError()
    
    return _withdrawal_to_response(withdrawal)

def _withdrawal_to_response(w: Withdrawal) -> WithdrawalResponse:
    """Shape a Withdrawal ORM row into the API response model."""

    return WithdrawalResponse(
        withdrawal_id=str(w.id),
        account_id=str(w.account_id),
        amount=f"{w.amount:.4f}",
        currency=w.currency,
        destination_type=w.destination_type,
        status=w.status,
        created_at=w.created_at,
        submitted_at=w.submitted_at,
        completed_at=w.completed_at,
        failure_code=w.failure_code,
        external_reference=w.external_reference,
    )

def _hash_request(
    account_id: UUID,
    amount: Decimal,
    currency: str,
    destination_type: str,
    destination_details: dict,
) -> str:
    """SHA-256 hash of the request's identity-defining fields.

    Used to detect "same idempotency key, different request body" — an API
    misuse that should raise IdempotencyConflictError rather than silently
    return the cached response for a different request. Mirrors the helper
    in transfer_service.py.
    """

    serialized_details = json.dumps(destination_details, sort_keys=True)
    hash_input = f"{account_id}:{amount}:{currency}:{destination_type}:{serialized_details}"
    return hashlib.sha256(hash_input.encode()).hexdigest()
