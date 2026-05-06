import hashlib
import json
import logging
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from uuid import UUID

from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.events.publisher import publish_event
from app.events.schemas import TransferCompletedPayload, TransferFailedPayload
from app.exceptions import (
    AccountNotFoundError,
    IdempotencyConflictError,
    IdempotencyKeyConsumedError,
    InsufficientBalanceError,
    TransferNotFoundError,
)
from app.models.account import Account
from app.models.ledger_entry import LedgerEntry
from app.models.transfer import Transfer
from app.schemas.transfer import TransferResponse
from app.services.account_service import get_balance

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Week 7: Kafka producer REMOVED
# ---------------------------------------------------------------------------
# Week 6 had a module-level AIOKafkaProducer (start_producer / stop_producer)
# that published events inline after db.commit(). That was intentionally fragile —
# the dual-write problem meant events could be lost if Kafka was down.
#
# Week 7 replaces this with the outbox pattern: events are written to the outbox
# table in the SAME transaction as the domain data. The outbox relay (a separate
# worker) delivers them to Kafka asynchronously.
#
# The API process no longer connects to Kafka at all. Remove the kafka-init
# dependency from the api service in docker-compose.yml (already done).
# ---------------------------------------------------------------------------


async def transfer(
    db: AsyncSession,
    redis: Redis,
    from_account_id: UUID,
    to_account_id: UUID,
    amount: Decimal,
    idempotency_key: str,
    actor_user_id: UUID | None = None,
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

            #  Publish transfer.failed event via outbox (BEFORE commit)
            # Steps:
            # 1. Call publish_event() with:
            publish_event(
                db = db,
                topic = "transfer.events",
                event_type = "transfer.failed",
                payload = TransferFailedPayload(
                    transfer_id=str(failed_record.id),
                    from_account_id=str(from_account_id),
                    to_account_id=str(to_account_id),
                    amount=f"{amount:.8f}",
                    failure_code="INSUFFICIENT_BALANCE",
                    entry_type="transfer",
                    idempotency_key=idempotency_key,
                ),
                actor_id=actor_user_id,
            )

            # 2. The commit below will atomically persist BOTH the Transfer row
            #    AND the outbox row. If either fails, both are rolled back.
            #
            # Key insight: publish_event() calls db.add() — it does NOT commit.
            # The caller (you) commits. This is what makes it transactional.
            await db.commit()
            raise InsufficientBalanceError()

        # 4. Atomic double-entry: debit sender, credit receiver
        now = datetime.now(timezone.utc)
        entry = LedgerEntry(
            id=uuid.uuid4(),
            debit_account_id=from_account_id,
            credit_account_id=to_account_id,
            amount=amount,
            entry_type="transfer",
            reference_id=None,
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

        # Publish transfer.completed event via outbox (BEFORE commit)

        publish_event(
            db = db,
            topic = "transfer.events",
            event_type = "transfer.completed",
            payload = TransferCompletedPayload(
                transfer_id=str(transfer_record.id),
                from_account_id=str(from_account_id),
                to_account_id=str(to_account_id),
                amount=f"{amount:.8f}",
                entry_type="transfer",
                idempotency_key=idempotency_key,
            ),
            actor_id=actor_user_id,
        )

        await db.commit()

    except IntegrityError as e:
        await db.rollback()
        # Disambiguate: is this a duplicate idempotency key, or an unrelated constraint?
        # asyncpg exposes constraint_name on UniqueViolationError — use it instead of
        # fragile string matching on the error message.
        # Note: e.orig is the DBAPI wrapper; the raw asyncpg error (with constraint_name)
        # is on e.orig.__cause__.
        orig = getattr(e.orig, "__cause__", e.orig)
        is_idempotency_conflict = (
            hasattr(orig, "constraint_name")
            and orig.constraint_name is not None
            and "idempotency_key" in orig.constraint_name
        )
        if is_idempotency_conflict:
            result = await db.execute(
                select(Transfer).where(Transfer.idempotency_key == idempotency_key)
            )
            transfer_record = result.scalar_one_or_none()
            if transfer_record is None:
                raise
            # Validate request parameters match — same check as the Redis fast path.
            # Without this, a reused key with different params silently returns the
            # wrong transfer.
            existing_hash = _hash_request(
                transfer_record.from_account_id,
                transfer_record.to_account_id,
                transfer_record.amount,
            )
            if existing_hash != _hash_request(from_account_id, to_account_id, amount):
                raise IdempotencyConflictError()
            if transfer_record.status == "failed":
                raise IdempotencyKeyConsumedError()
        else:
            raise

    response = _transfer_to_response(transfer_record)

    # 5. Cache successful response — only after confirmed commit
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
    # Normalize to 8 decimal places to match DB precision (NUMERIC(20,8))
    payload = f"{from_account_id}:{to_account_id}:{amount:.8f}"
    return hashlib.sha256(payload.encode()).hexdigest()
