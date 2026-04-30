import hashlib
import json
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from redis.asyncio import Redis

from app.models.account import Account
from app.models.ledger_entry import LedgerEntry
from app.models.transfer import Transfer
from app.services.account_service import get_balance
from app.schemas.transfer import TransferResponse
from app.exceptions import (
    AccountNotFoundError,
    InsufficientBalanceError,
    IdempotencyConflictError,
    IdempotencyKeyConsumedError,
    TransferNotFoundError,
)


async def transfer(
    db: AsyncSession,
    redis: Redis,
    from_account_id: UUID,
    to_account_id: UUID,
    amount: Decimal,
    idempotency_key: str,
) -> TransferResponse:
    # 1. Idempotency check — Redis fast path
    cached_raw = await redis.get(f"idempotency:{idempotency_key}")
    if cached_raw:
        cached = json.loads(cached_raw)
        request_hash = _hash_request(from_account_id, to_account_id, amount)
        if cached["request_hash"] != request_hash:
            raise IdempotencyConflictError()
        return TransferResponse(**json.loads(cached["response"]))

    # NOTE: The router performs account lookups on this same session before calling here,
    # so SQLAlchemy's autobegin is already active. We operate within that implicit
    # transaction and commit it explicitly — do NOT call db.begin() again.
    transfer_record = None

    try:
        # 2. Lock BOTH accounts in consistent UUID order to prevent bidirectional deadlock.
        #
        # Why two locks: ledger_entries has FK references to accounts. When we INSERT a
        # ledger entry, PostgreSQL acquires FOR KEY SHARE on the credit_account row to
        # enforce the FK. If Txn A holds FOR UPDATE on Alice and Txn B holds FOR UPDATE
        # on Bob, then Txn A's INSERT (credit=Bob) blocks on Txn B's FOR UPDATE on Bob,
        # and Txn B's INSERT (credit=Alice) blocks on Txn A's FOR UPDATE on Alice →
        # deadlock.
        #
        # Locking both in sorted UUID order guarantees all transactions acquire locks
        # in the same sequence, making circular waits impossible.
        lock_order = sorted([from_account_id, to_account_id], key=str)
        locked = {}
        for acc_id in lock_order:
            res = await db.execute(
                select(Account).where(Account.id == acc_id).with_for_update()
            )
            acc = res.scalar_one_or_none()
            if acc is None:
                raise AccountNotFoundError()
            locked[acc_id] = acc

        # 3. Derive balance from ledger (safe: sender row is locked)
        balance = await get_balance(db, from_account_id)
        if balance < amount:
            # Persist the failed attempt for audit trail and Phase 2 transfer.failed events.
            # The idempotency key is consumed — client must generate a new key to retry.
            now = datetime.now(timezone.utc)
            failed_record = Transfer(
                id=uuid.uuid4(),
                from_account_id=from_account_id,
                to_account_id=to_account_id,
                amount=amount,
                status="failed",
                failure_code="INSUFFICIENT_BALANCE",
                idempotency_key=idempotency_key,
                created_at=now,
                updated_at=now,
            )
            db.add(failed_record)
            await db.commit()  # persists the failed record and releases FOR UPDATE locks
            raise InsufficientBalanceError()

        # 4. Atomic double-entry: debit sender, credit receiver
        now = datetime.now(timezone.utc)
        entry = LedgerEntry(
            id=uuid.uuid4(),
            debit_account_id=from_account_id,
            credit_account_id=to_account_id,
            amount=amount,
            entry_type="transfer",
            reference_id=None,  # set after transfer_record id is known
            idempotency_key=idempotency_key,
            created_at=now,
        )
        transfer_record = Transfer(
            id=uuid.uuid4(),
            from_account_id=from_account_id,
            to_account_id=to_account_id,
            amount=amount,
            status="completed",
            idempotency_key=idempotency_key,
            created_at=now,
            updated_at=now,
        )
        db.add(entry)
        entry.reference_id = transfer_record.id
        db.add(transfer_record)
        await db.commit()  # COMMIT — both rows written or neither

    except IntegrityError as e:
        await db.rollback()
        # The DB unique constraint on idempotency_key fired — a Transfer with this key
        # already exists. Determine whether it was a success or failure.
        if "idempotency_key" in str(e.orig):
            result = await db.execute(
                select(Transfer).where(Transfer.idempotency_key == idempotency_key)
            )
            transfer_record = result.scalar_one()
            if transfer_record.status == "failed":
                # Key was consumed by a prior failed attempt.
                # Client must generate a new idempotency key to retry.
                raise IdempotencyKeyConsumedError()
            # Status is "completed" — Redis cache expired between commit and setex.
            # Return the committed transfer idempotently.
        else:
            raise

    response = _transfer_to_response(transfer_record)

    # 5. Cache successful response — only after confirmed commit
    #    Never cache 4xx (client may retry with fix) or 5xx (may not have committed)
    await redis.setex(
        f"idempotency:{idempotency_key}",
        86400,
        json.dumps({
            "request_hash": _hash_request(from_account_id, to_account_id, amount),
            "response": json.dumps(response.model_dump(), default=str),
        })
    )

    return response


async def get_transfer(db: AsyncSession, transfer_id: UUID, requesting_account_id: UUID) -> TransferResponse:
    result = await db.execute(select(Transfer).where(Transfer.id == transfer_id))
    record = result.scalar_one_or_none()
    if record is None:
        raise TransferNotFoundError()
    # Accessible by both sender and receiver; return 404 for unauthorized access (no info leak)
    if record.from_account_id != requesting_account_id and record.to_account_id != requesting_account_id:
        raise TransferNotFoundError()
    return _transfer_to_response(record)


def _transfer_to_response(t: Transfer) -> TransferResponse:
    return TransferResponse(
        transfer_id=str(t.id),
        from_account_id=str(t.from_account_id),
        to_account_id=str(t.to_account_id),
        amount=f"{t.amount:.8f}",
        status=t.status,
        failure_code=t.failure_code,
        created_at=t.created_at,
    )


def _hash_request(from_account_id: UUID, to_account_id: UUID, amount: Decimal) -> str:
    payload = f"{from_account_id}:{to_account_id}:{amount}"
    return hashlib.sha256(payload.encode()).hexdigest()
