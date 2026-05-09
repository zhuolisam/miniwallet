"""Backfill management command — generates outbox rows for all Phase 1 data.

Run ONCE after Phase 2 deploy, before switching /transactions to the CQRS
read model. This ensures audit_events and transaction_activity contain the
complete history from day one.

Generates synthetic events for:
- All accounts → account.opened
- All completed transfers → transfer.completed
- All failed transfers → transfer.failed
- All seed entries → seed.completed

All backfill events have actor_id=None — historical data has no actor
attribution. This is a documented limitation, not a bug.

IDEMPOTENT: Uses deterministic UUID5 event IDs derived from source entity IDs.
Running backfill multiple times produces the same event_ids, which hit the
consumer UNIQUE constraints (audit_events.event_id, transaction_activity
(event_id, account_id)) and are silently deduplicated.

Run with:
    docker compose run --rm api python -m management.backfill_events
"""

import asyncio
import logging
import uuid

from sqlalchemy import func, select

from app.models.outbox import OutboxRow
from app.models.account import Account
from app.models.transfer import Transfer
from app.models.ledger_entry import LedgerEntry
from app.events.publisher import publish_event
from app.events.schemas import (
    AccountOpenedPayload,
    TransferCompletedPayload,
    TransferFailedPayload,
    SeedCompletedPayload,
)

logger = logging.getLogger(__name__)

BACKFILL_NAMESPACE = uuid.UUID("b4cf1110-0000-0000-0000-000000000000")


def backfill_event_id(entity_type: str, entity_id: uuid.UUID) -> str:
    """Deterministic UUID5 — same entity always produces the same event_id."""
    return str(uuid.uuid5(BACKFILL_NAMESPACE, f"{entity_type}:{entity_id}"))


async def backfill(db_factory=None, force: bool = False):
    """Generate outbox rows for all Phase 1 data.

    Args:
        db_factory: Async session factory. Defaults to app.database.db_factory.
                    Tests pass their own factory bound to the test container.
        force: If True, skip the preflight guard (for re-running after cleanup).

    Raises:
        RuntimeError: If backfill has already run and force is False.
    """
    if db_factory is None:
        from app.database import db_factory as _default
        db_factory = _default

    if not force:
        async with db_factory() as db:
            count = (await db.execute(
                select(func.count()).where(OutboxRow.event_type == "account.opened")
            )).scalar_one()
        if count > 0:
            raise RuntimeError(
                f"Backfill appears to have already run ({count} account.opened outbox rows found). "
                "Pass force=True to override."
            )

    # --- Accounts ---
    async with db_factory() as db:
        async with db.begin():
            account_rows = (await db.execute(
                select(Account.id, Account.user_id, Account.status)
                .where(Account.user_id.is_not(None))
                .order_by(Account.created_at)
            )).all()

    for account_id, user_id, status in account_rows:
        async with db_factory() as db:
            async with db.begin():
                publish_event(
                    db, "account.events", "account.opened",
                    AccountOpenedPayload(
                        account_id=str(account_id),
                        user_id=str(user_id),
                        status=status,
                    ),
                    actor_id=None,
                    event_id=backfill_event_id("account.opened", account_id),
                )

    # --- Transfers (completed + failed) ---
    async with db_factory() as db:
        async with db.begin():
            transfer_rows = (await db.execute(
                select(Transfer.id, Transfer.from_account_id, Transfer.to_account_id,
                       Transfer.amount, Transfer.status, Transfer.failure_code,
                       Transfer.idempotency_key)
                .order_by(Transfer.created_at)
            )).all()

    for t_id, from_id, to_id, amount, status, failure_code, idem_key in transfer_rows:
        async with db_factory() as db:
            async with db.begin():
                if status == "completed":
                    publish_event(
                        db, "transfer.events", "transfer.completed",
                        TransferCompletedPayload(
                            transfer_id=str(t_id),
                            from_account_id=str(from_id),
                            to_account_id=str(to_id),
                            amount=f"{amount:.4f}",
                            currency="USD",
                            entry_type="transfer",
                            idempotency_key=idem_key,
                        ),
                        actor_id=None,
                        event_id=backfill_event_id("transfer.completed", t_id),
                    )
                elif status == "failed":
                    publish_event(
                        db, "transfer.events", "transfer.failed",
                        TransferFailedPayload(
                            transfer_id=str(t_id),
                            from_account_id=str(from_id),
                            to_account_id=str(to_id),
                            amount=f"{amount:.4f}",
                            currency="USD",
                            failure_code=failure_code or "UNKNOWN",
                            entry_type="transfer",
                            idempotency_key=idem_key,
                        ),
                        actor_id=None,
                        event_id=backfill_event_id("transfer.failed", t_id),
                    )

    # --- Seed entries ---
    # With the leg-based ledger, seeds are credit legs with entry_type='seed'
    # on the user's account (direction='credit').
    async with db_factory() as db:
        async with db.begin():
            seed_rows = (await db.execute(
                select(LedgerEntry.id, LedgerEntry.amount, LedgerEntry.account_id, Account.user_id)
                .join(Account, LedgerEntry.account_id == Account.id)
                .where(LedgerEntry.entry_type == "seed", LedgerEntry.direction == "credit")
                .order_by(LedgerEntry.created_at)
            )).all()

    for entry_id, amount, account_id, user_id in seed_rows:
        async with db_factory() as db:
            async with db.begin():
                publish_event(
                    db, "account.events", "seed.completed",
                    SeedCompletedPayload(
                        account_id=str(account_id),
                        user_id=str(user_id),
                        amount=f"{amount:.4f}",
                        currency="USD",
                        entry_type="seed",
                    ),
                    actor_id=None,
                    event_id=backfill_event_id("seed.completed", entry_id),
                )

    logger.info(
        "Backfill complete: %d accounts, %d transfers, %d seeds",
        len(account_rows), len(transfer_rows), len(seed_rows),
    )


if __name__ == "__main__":
    asyncio.run(backfill())
