import uuid
from datetime import datetime, timezone
from decimal import Decimal
from uuid import UUID
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, text
from sqlalchemy.exc import IntegrityError

from app.config import SYSTEM_ACCOUNT_ID
from app.events.publisher import publish_event
from app.events.schemas import AccountOpenedPayload, SeedCompletedPayload
from app.models.account import Account
from app.models.ledger_entry import LedgerEntry
from app.models.transaction_activity import TransactionActivity
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

    publish_event(
        db=db,
        topic="account.events",
        event_type="account.opened",
        payload=AccountOpenedPayload(
            account_id=str(account.id),
            user_id=str(user_id),
            status=account.status,
        ),
        actor_id=user_id,
    )

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
    """Derive balance from ledger legs. Simple single-index query per account."""
    result = await db.execute(
        text("""
            SELECT COALESCE(SUM(
                CASE WHEN direction = 'credit' THEN amount
                     WHEN direction = 'debit'  THEN -amount
                END
            ), 0) AS balance
            FROM ledger_entries
            WHERE account_id = :id
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
) -> tuple[list[TransactionItem], int, datetime | None]:
    """Read from transaction_activity (CQRS read model).

    Returns (items, total, as_of) where as_of is MAX(occurred_at) of the
    current page's rows — tells the client how fresh the data is.
    """

    base_q = select(TransactionActivity).where(
        TransactionActivity.account_id == account_id
    )

    if from_date:
        base_q = base_q.where(TransactionActivity.occurred_at >= from_date)
    if to_date:
        base_q = base_q.where(TransactionActivity.occurred_at <= to_date)
    if entry_type:
        base_q = base_q.where(TransactionActivity.entry_type == entry_type)

    count_q = select(func.count()).select_from(base_q.subquery())
    total = (await db.execute(count_q)).scalar_one()

    rows_q = base_q.order_by(TransactionActivity.occurred_at.desc()).offset((page - 1) * limit).limit(limit)
    rows = (await db.execute(rows_q)).scalars().all()

    as_of = max((r.occurred_at for r in rows), default=None)

    items = [
        TransactionItem(
            entry_id=str(row.id),
            direction=row.direction,
            amount=f"{row.amount:.4f}",
            currency=row.currency,
            entry_type=row.entry_type,
            reference_id=str(row.reference_id) if row.reference_id else None,
            created_at=row.occurred_at,
        )
        for row in rows
    ]

    return items, total, as_of


async def seed(db: AsyncSession, account_id: UUID, amount: Decimal, idempotency_key: str, actor_user_id: UUID) -> SeedResponse:
    account = await get_account_by_id(db, account_id)
    if account is None:
        raise AccountNotFoundError()

    now = datetime.now(timezone.utc)
    txn_id = uuid.uuid4()

    # Two legs: debit system account, credit user account
    debit_leg = LedgerEntry(
        id=uuid.uuid4(),
        transaction_id=txn_id,
        account_id=SYSTEM_ACCOUNT_ID,
        direction="debit",
        amount=amount,
        currency="USD",
        entry_type="seed",
        created_at=now,
    )
    credit_leg = LedgerEntry(
        id=uuid.uuid4(),
        transaction_id=txn_id,
        account_id=account_id,
        direction="credit",
        amount=amount,
        currency="USD",
        entry_type="seed",
        idempotency_key=idempotency_key,
        created_at=now,
    )
    db.add(debit_leg)
    db.add(credit_leg)

    publish_event(
        db=db,
        topic="account.events",
        event_type="seed.completed",
        payload=SeedCompletedPayload(
            account_id=str(account_id),
            user_id=str(actor_user_id),
            amount=f"{amount:.4f}",
            currency="USD",
            entry_type="seed",
        ),
        actor_id=actor_user_id,
    )

    try:
        await db.commit()
    except IntegrityError:
        await db.rollback()
        result = await db.execute(
            select(LedgerEntry).where(LedgerEntry.idempotency_key == idempotency_key)
        )
        existing = result.scalar_one_or_none()
        if existing is None:
            raise
        if existing.account_id != account_id or existing.amount != amount:
            raise IdempotencyConflictError()
        credit_leg = existing

    new_balance = await get_balance(db, account_id)
    return SeedResponse(
        entry_id=str(credit_leg.id),
        account_id=str(account_id),
        amount=f"{amount:.4f}",
        new_balance=f"{new_balance:.4f}",
    )
