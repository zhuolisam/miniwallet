import uuid
from datetime import datetime, timezone
from decimal import Decimal
from uuid import UUID
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, text, and_, or_
from sqlalchemy.exc import IntegrityError

from app.config import SYSTEM_ACCOUNT_ID
from app.events.publisher import publish_event
from app.events.schemas import AccountOpenedPayload, SeedCompletedPayload
from app.models.account import Account
from app.models.ledger_entry import LedgerEntry
from app.schemas.account import TransactionItem, SeedResponse
from app.exceptions import (
    AccountAlreadyExistsError,
    AccountNotFoundError,
    IdempotencyConflictError,
)


async def open_account(db: AsyncSession, user_id: UUID) -> Account:
    now = datetime.now(timezone.utc)
    account = Account(
        id=uuid.uuid4(),
        user_id=user_id,
        status="active",
        created_at=now,
        updated_at=now,
    )
    db.add(account)

    # Publish account.opened event via outbox (US-2.4)

    publish_event(
        db = db,
        topic = "account.events",
        event_type = "account.opened",
        payload = AccountOpenedPayload(
            account_id=str(account.id),
            user_id=str(user_id),
            status=account.status,
        ),
        actor_id=user_id,  # the account owner is the actor
    )
    # Note: publish_event() calls db.add() — it does NOT commit.
    # The commit below will persist both the Account and the OutboxRow atomically.

    try:
        await db.commit()
    except IntegrityError:
        await db.rollback()
        raise AccountAlreadyExistsError()
    await db.refresh(account)
    return account


async def get_account_by_user(db: AsyncSession, user_id: UUID) -> Account | None:
    result = await db.execute(select(Account).where(Account.user_id == user_id))
    return result.scalar_one_or_none()


async def get_account_by_id(db: AsyncSession, account_id: UUID) -> Account | None:
    result = await db.execute(select(Account).where(Account.id == account_id))
    return result.scalar_one_or_none()


async def get_balance(db: AsyncSession, account_id: UUID) -> Decimal:
    result = await db.execute(
        text("""
            SELECT
                COALESCE(SUM(CASE WHEN credit_account_id = :id THEN amount ELSE 0 END), 0)
              - COALESCE(SUM(CASE WHEN debit_account_id  = :id THEN amount ELSE 0 END), 0)
            AS balance
            FROM ledger_entries
            WHERE credit_account_id = :id OR debit_account_id = :id
        """),
        {"id": str(account_id)}
    )
    row = result.scalar()
    return Decimal(str(row)) if row is not None else Decimal("0")


async def get_transactions(
    db: AsyncSession,
    account_id: UUID,
    page: int = 1,
    limit: int = 20,
    from_date: datetime | None = None,
    to_date: datetime | None = None,
    entry_type: str | None = None,
) -> tuple[list[TransactionItem], int]:
    conditions = [
        or_(
            LedgerEntry.credit_account_id == account_id,
            LedgerEntry.debit_account_id == account_id,
        )
    ]
    if from_date:
        conditions.append(LedgerEntry.created_at >= from_date)
    if to_date:
        conditions.append(LedgerEntry.created_at <= to_date)
    if entry_type:
        conditions.append(LedgerEntry.entry_type == entry_type)

    count_result = await db.execute(
        select(func.count()).select_from(LedgerEntry).where(and_(*conditions))
    )
    total = count_result.scalar_one()

    result = await db.execute(
        select(LedgerEntry)
        .where(and_(*conditions))
        .order_by(LedgerEntry.created_at.desc())
        .offset((page - 1) * limit)
        .limit(limit)
    )
    entries = result.scalars().all()

    items = []
    for e in entries:
        direction = "credit" if e.credit_account_id == account_id else "debit"
        items.append(TransactionItem(
            entry_id=str(e.id),
            direction=direction,
            amount=f"{e.amount:.8f}",
            entry_type=e.entry_type,
            reference_id=str(e.reference_id) if e.reference_id else None,
            created_at=e.created_at,
        ))

    return items, total


async def seed(db: AsyncSession, account_id: UUID, amount: Decimal, idempotency_key: str, actor_user_id: UUID) -> SeedResponse:
    account = await get_account_by_id(db, account_id)
    if account is None:
        raise AccountNotFoundError()

    now = datetime.now(timezone.utc)
    entry = LedgerEntry(
        id=uuid.uuid4(),
        debit_account_id=SYSTEM_ACCOUNT_ID,
        credit_account_id=account_id,
        amount=amount,
        entry_type="seed",
        reference_id=None,
        idempotency_key=idempotency_key,
        created_at=now,
    )
    db.add(entry)

    # Publish seed.completed event via outbox
    publish_event(
        db = db,
        topic = "account.events",
        event_type = "seed.completed",
        payload = SeedCompletedPayload(
            account_id=str(account_id),
            user_id=str(actor_user_id),
            amount=f"{amount:.8f}",
            entry_type="seed",
        ),
        actor_id=actor_user_id,
    )

    try:
        await db.commit()
    except IntegrityError:
        await db.rollback()
        # idempotent — fetch existing entry and verify request params match
        result = await db.execute(
            select(LedgerEntry).where(LedgerEntry.idempotency_key == idempotency_key)
        )
        entry = result.scalar_one()
        if entry.credit_account_id != account_id or entry.amount != amount:
            raise IdempotencyConflictError()

    new_balance = await get_balance(db, account_id)
    return SeedResponse(
        entry_id=str(entry.id),
        account_id=str(account_id),
        amount=f"{amount:.8f}",
        new_balance=f"{new_balance:.8f}",
    )
