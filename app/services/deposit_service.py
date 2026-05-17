"""Deposit service — inbound rail webhook processing (Phase 3 / Week 10).

Deposits are push-driven: the banking partner (ClearBank / Modulr / Railsr in
production) sends us a webhook when funds land in the settlement account. We
mimic that webhook via POST /v1/dev/simulate-deposit.

Unlike withdrawals, the deposit flow is a SINGLE atomic transaction — there is
no external call that can fail between "record the intent" and "move the money".
The rail has already confirmed the money arrived; we just need to record it
against the user's account correctly.

Idempotency lives in the database, not Redis. The UNIQUE constraint on
`deposits.external_reference` is the natural dedup key — the rail's own
transaction ID. This is stronger than Redis (survives restarts, single source
of truth, no TTL to tune) and is the standard pattern across neobanks.

State machine:
    pending → completed  (validation passed, ledger entry written)
    pending → rejected   (validation failed, no ledger entry ever written)

Ledger invariant: a deposit credits the user account and debits the system
account. After any deposit, SUM(all ledger_entries, signed by direction) = 0.
"""

from datetime import datetime, timezone
from decimal import Decimal
from uuid import UUID
import uuid

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError

from app.config import SYSTEM_ACCOUNT_ID
from app.events.publisher import publish_event
from app.events.schemas import DepositCompletedPayload, DepositRejectedPayload
from app.exceptions import DepositNotFoundError
from app.models.account import Account
from app.models.deposit import Deposit
from app.models.ledger_entry import LedgerEntry
from app.schemas.deposit import DepositResponse


# Supported ISO 4217 currencies. Phase 3 only supports USD — adding a new
# currency is not just a matter of adding a string; FX policy, compliance,
# and reconciliation schedules all need to change. Keep the list tiny and
# explicit until multi-currency is scoped.
SUPPORTED_CURRENCIES = {"USD"}


# Exhaustive rejection reason strings. Keep this in sync with
# docs/product/phase-3-payments/SYSTEM-DESIGN.md Section 3.
REJECT_ACCOUNT_NOT_FOUND = "ACCOUNT_NOT_FOUND"
REJECT_ACCOUNT_NOT_ACTIVE = "ACCOUNT_NOT_ACTIVE"
REJECT_INVALID_AMOUNT = "INVALID_AMOUNT"
REJECT_UNSUPPORTED_CURRENCY = "UNSUPPORTED_CURRENCY"


async def simulate_deposit(
    db: AsyncSession,
    account_id: UUID,
    amount: Decimal,
    currency: str,
    source_type: str,
    external_reference: str,
) -> DepositResponse:
    """Process a (simulated) rail webhook for an inbound payment.

    Called by POST /v1/dev/simulate-deposit. Runs the entire flow inside a single
    DB transaction so there is no "partial" state — either both ledger legs +
    the deposit row + the outbox row commit together, or none of them do.

    Returns:
        DepositResponse. For completed deposits, `status='completed'` and
        `completed_at` is set. For rejected deposits, `status='rejected'` and
        `rejection_reason` is populated. For an idempotent duplicate, returns
        the original record (which may be completed OR rejected).

    Flow outline (details in SYSTEM-DESIGN.md Section 3):
        1. Try INSERT deposits (status='pending', external_reference=...)
           - On IntegrityError (UniqueViolation on external_reference): rollback,
             SELECT the existing row, return its current state as DepositResponse.
        2. Validate:
           - account exists?  (ACCOUNT_NOT_FOUND)
           - account.status == 'active'?  (ACCOUNT_NOT_ACTIVE)
           - amount > 0?  (INVALID_AMOUNT — also caught by Pydantic, defensive here)
           - currency in SUPPORTED_CURRENCIES?  (UNSUPPORTED_CURRENCY)
        3. On validation failure:
           - UPDATE deposits SET status='rejected', rejection_reason=..., updated_at=NOW()
           - publish_event('deposit.events', 'deposit.rejected', ...)
           - COMMIT. No ledger entry ever written.
        4. On validation pass:
           - SELECT account FOR UPDATE (lock the row — balance derivation must not
             race against concurrent credits on this account)
           - INSERT ledger_entry (debit leg): account=SYSTEM_ACCOUNT_ID,
             direction='debit', entry_type='deposit', reference_id=deposit.id
           - INSERT ledger_entry (credit leg): account=user_account,
             direction='credit', entry_type='deposit', reference_id=deposit.id
           - UPDATE deposits SET status='completed', completed_at=NOW(), updated_at=NOW()
           - publish_event('deposit.events', 'deposit.completed', ...)
           - COMMIT.

    Notes:
        - `actor_id` on the event envelope should be None — deposits are
          system-initiated from a webhook, not a user action.
        - Both ledger legs share the same `transaction_id`.
        - The credit leg's `idempotency_key` is not set (unlike seed) because
          the UNIQUE constraint on `deposits.external_reference` already
          guarantees single-write. Don't duplicate guards.
    """

    now = datetime.now(timezone.utc)

    # Step 1: INSERT pending. On UniqueViolation (duplicate external_reference),
    # rollback and return the existing record — the DB constraint IS the idempotency lock.
    try:
        deposit = Deposit(
            account_id=account_id,
            amount=amount,
            currency=currency,
            source_type=source_type,
            external_reference=external_reference,
            status="pending",
            created_at=now,
            updated_at=now,
        )
        db.add(deposit)
        await db.flush()
    except IntegrityError as e:
        await db.rollback()
        orig = getattr(e.orig, "__cause__", e.orig)
        is_duplicate_ref = (
            hasattr(orig, "constraint_name")
            and orig.constraint_name is not None
            and "external_reference" in orig.constraint_name
        )
        if is_duplicate_ref:
            existing = (await db.execute(
                select(Deposit).where(Deposit.external_reference == external_reference)
            )).scalar_one()
            return _deposit_to_response(existing)
        else:
            raise

    # Step 2: Validate — reject system account before touching the DB.
    rejection_reason = None
    if account_id == SYSTEM_ACCOUNT_ID:
        rejection_reason = REJECT_ACCOUNT_NOT_FOUND
    elif amount <= 0:
        rejection_reason = REJECT_INVALID_AMOUNT
    elif currency not in SUPPORTED_CURRENCIES:
        rejection_reason = REJECT_UNSUPPORTED_CURRENCY

    if rejection_reason is None:
        account_result = await db.execute(
            select(Account).where(Account.id == account_id).with_for_update()
        )
        account = account_result.scalar_one_or_none()
        if account is None:
            rejection_reason = REJECT_ACCOUNT_NOT_FOUND
        elif account.status != "active":
            rejection_reason = REJECT_ACCOUNT_NOT_ACTIVE

    # Step 3: Validation failed — mark rejected, publish, commit.
    if rejection_reason is not None:
        deposit.status = "rejected"
        deposit.rejection_reason = rejection_reason
        deposit.updated_at = now
        publish_event(
            db=db,
            topic="deposit.events",
            event_type="deposit.rejected",
            payload=DepositRejectedPayload(
                deposit_id=str(deposit.id),
                account_id=str(deposit.account_id),
                amount=str(deposit.amount),
                currency=deposit.currency,
                rejection_reason=rejection_reason,
                entry_type="deposit",
            ),
        )
        await db.commit()
        return _deposit_to_response(deposit)

    # Step 4: Validation passed — account already locked above, write both ledger legs.
    transaction_id = uuid.uuid4()
    db.add(LedgerEntry(
        id=uuid.uuid4(),
        transaction_id=transaction_id,
        account_id=deposit.account_id,
        direction="credit",
        amount=deposit.amount,
        currency=deposit.currency,
        entry_type="deposit",
        reference_id=deposit.id,
        created_at=now,
    ))
    db.add(LedgerEntry(
        id=uuid.uuid4(),
        transaction_id=transaction_id,
        account_id=SYSTEM_ACCOUNT_ID,
        direction="debit",
        amount=deposit.amount,
        currency=deposit.currency,
        entry_type="deposit",
        reference_id=deposit.id,
        created_at=now,
    ))
    deposit.status = "completed"
    deposit.completed_at = now
    deposit.updated_at = now
    publish_event(
        db=db,
        topic="deposit.events",
        event_type="deposit.completed",
        payload=DepositCompletedPayload(
            deposit_id=str(deposit.id),
            account_id=str(deposit.account_id),
            amount=str(deposit.amount),
            currency=deposit.currency,
            source_type=deposit.source_type,
            external_reference=deposit.external_reference,
            entry_type="deposit",
        ),
    )
    await db.commit()
    return _deposit_to_response(deposit)


async def get_deposit(
    db: AsyncSession,
    deposit_id: UUID,
    requesting_account_id: UUID,
) -> DepositResponse:
    """Load a deposit by ID, enforcing ownership.

    Called by GET /v1/deposits/{id}. Returns 404 (DepositNotFoundError) if the
    deposit doesn't exist OR the requester's account does not own it — we
    deliberately conflate "not found" and "not yours" so attackers can't probe
    for valid deposit IDs across accounts.
    """
    # 1. SELECT Deposit WHERE id = deposit_id
    deposit_result = await db.execute(select(Deposit).where(Deposit.id == deposit_id))
    deposit = deposit_result.scalar_one_or_none()
    # 2. If None OR deposit.account_id != requesting_account_id, raise DepositNotFoundError
    if deposit is None or deposit.account_id != requesting_account_id:
        raise DepositNotFoundError()
    # 3. Return _deposit_to_response(deposit)
    return _deposit_to_response(deposit)

def _deposit_to_response(d: Deposit) -> DepositResponse:
    """Shape a Deposit ORM row into the API response model."""
    return DepositResponse(
          deposit_id=str(d.id),
          account_id=str(d.account_id),
          amount=f"{d.amount:.4f}",
          currency=d.currency,
          status=d.status,
          source_type=d.source_type,
          external_reference=d.external_reference,
          rejection_reason=d.rejection_reason,
          created_at=d.created_at,
          completed_at=d.completed_at,
    )