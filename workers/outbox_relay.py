"""Outbox relay — delivers outbox rows to Kafka.

The relay is a background worker that:
1. Claims pending outbox rows (FOR UPDATE SKIP LOCKED — safe for concurrent relays)
2. Publishes each row to Kafka (send_and_wait — true at-least-once delivery)
3. Confirms the batch (marks rows published or returns failed ones to pending)

Two-phase pattern: the DB transaction in claim_batch is short — released before
any Kafka I/O. This avoids holding row locks during network calls.

Lifecycle:
- claim_batch():   pending → publishing  (short TX, locks released on commit)
- publish to Kafka: network I/O, no DB locks held
- confirm_batch():  publishing → published|pending|failed  (short TX)

Recovery: if the relay crashes between claim and confirm, rows stay 'publishing'.
recover_stuck_rows() resets them to 'pending' after 5 minutes.

Cleanup: published rows are deleted after 7 days to prevent unbounded table growth.

Run inside Docker:
    (handled by docker-compose.yml — service `outbox-relay`)

Run on host (for local testing):
    KAFKA_BOOTSTRAP_SERVERS=localhost:29092 uv run python -m workers.outbox_relay
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timedelta, timezone

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError
from sqlalchemy import delete, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import db_factory
from app.models.outbox import OutboxRow

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("outbox_relay")

BATCH_SIZE = 100
MIN_SLEEP = 1     # seconds — minimum polling interval
MAX_SLEEP = 30    # seconds — maximum backoff when idle or Kafka is down
MAX_OUTBOX_RETRIES = 10  # after this, row is marked 'failed' — needs manual intervention


async def claim_batch(session_factory) -> list[OutboxRow]:
    """Step 1: Claim pending rows atomically. Short transaction — released immediately.

    Uses FOR UPDATE SKIP LOCKED so two concurrent relay instances claim different
    rows — no duplicate publishes.

    Returns detached ORM objects. expire_on_commit=False on the session factory is
    required — without it, accessing row.topic or row.payload after the session
    closes triggers MissingGreenlet in async context.
    """
    # 1. Open a session from session_factory using `async with session_factory() as db:`
    async with session_factory() as db:
    # 2. Inside `async with db.begin():` (short transaction):
    # Key concept: FOR UPDATE SKIP LOCKED
    #   - FOR UPDATE: lock the selected rows so no other transaction can modify them
    #   - SKIP LOCKED: if a row is already locked by another relay instance, skip it
    #   - This makes it safe to run multiple relay processes for redundancy
        async with db.begin():
            q = (
                select(OutboxRow)
                .where(OutboxRow.status == "pending")
                .order_by(OutboxRow.created_at)
                .limit(BATCH_SIZE)
                .with_for_update(skip_locked=True)
            )
            result = await db.execute(q)
            rows = result.scalars().all()
            for row in rows:
                row.status = "publishing"
            return rows
    #    (The transaction commits on exit, releasing locks)
    # Reference: SYSTEM-DESIGN.md Section 6 — claim_batch



async def confirm_batch(session_factory, rows: list[OutboxRow]) -> None:
    """Step 3: Persist publish results. Short transaction.

    After the relay loop sets row.status to 'published', 'pending' (retry),
    or 'failed' on each row, this function merges those changes back to the DB.
    """
    # 1. Open a session from session_factory
    # 2. Inside a transaction, merge each row:
    #    `await db.merge(row)`
    #
    # Why merge? The rows are detached ORM objects (their original session is closed).
    # merge() reattaches them to a new session and persists the updated status.
    
    async with session_factory() as db:
        async with db.begin():
            for row in rows:
                await db.merge(row)
    # Reference: SYSTEM-DESIGN.md Section 6 — confirm_batch


async def recover_stuck_rows(session_factory) -> None:
    """Recovery: reset 'publishing' rows stuck > 5 min (process crash between claim and confirm).

    If the relay crashes after claiming rows (status='publishing') but before
    confirming them, those rows are stuck. This function resets them to 'pending'
    so they can be reclaimed.
    """
    # 1. Open a session from session_factory
    # 2. UPDATE outbox SET status = 'pending'
    #    WHERE status = 'publishing'
    #    AND created_at < NOW() - 5 minutes
    #
    # Why created_at and not a dedicated claimed_at column?
    #   We don't have a claimed_at column (keeping the schema simple). created_at
    #   is a proxy — rows claimed more than 5 min ago are almost certainly stuck.
    #   The relay processes batches in seconds, not minutes.
    async with session_factory() as db:
        async with db.begin():
            cutoff = datetime.now(timezone.utc) - timedelta(minutes=5)
            q = (
                update(OutboxRow)
                .where(
                    OutboxRow.status == "publishing",
                    OutboxRow.created_at < cutoff,
                )
                .values(status="pending")
            )
            result = await db.execute(q)
            logger.info("Recovered %d stuck outbox rows", result.rowcount)

    # Edge case: during a large backlog, old rows (created hours ago) could be
    # claimed and immediately reset by this function. Consumers are idempotent,
    # so duplicate publishes are harmless — just extra work.
    #
    # Reference: SYSTEM-DESIGN.md Section 6 — recover_stuck_rows


async def cleanup_published_rows(session_factory) -> None:
    """Periodic: delete old published and failed outbox rows.

    Without cleanup, the outbox table grows without bound. Published rows serve
    no purpose after Kafka's retention window (7 days). Failed rows are kept
    for 30 days so operators can investigate.
    """
    # 1. DELETE FROM outbox WHERE status = 'published' AND published_at < NOW() - 7 days
    # 2. Count and log 'failed' rows older than 30 days (warn operators before deletion)
    # 3. DELETE FROM outbox WHERE status = 'failed' AND created_at < NOW() - 30 days

    async with session_factory() as db:
        async with db.begin():
            published_cutoff = datetime.now(timezone.utc) - timedelta(days=7)
            result = await db.execute(
                delete(OutboxRow).where(
                    OutboxRow.status == "published",
                    OutboxRow.published_at < published_cutoff,
                )
            )
            logger.info("Deleted %d published outbox rows older than 7 days", result.rowcount)

            failed_cutoff = datetime.now(timezone.utc) - timedelta(days=30)
            count_result = await db.execute(
                select(func.count()).select_from(OutboxRow).where(
                    OutboxRow.status == "failed",
                    OutboxRow.created_at < failed_cutoff,
                )
            )
            failed_count = count_result.scalar_one()
            if failed_count > 0:
                logger.warning("Deleting %d failed outbox rows older than 30 days", failed_count)

            await db.execute(
                delete(OutboxRow).where(
                    OutboxRow.status == "failed",
                    OutboxRow.created_at < failed_cutoff,
                )
            )
    # Reference: SYSTEM-DESIGN.md Section 6 — cleanup_published_rows


async def relay_loop(session_factory, kafka_producer: AIOKafkaProducer) -> None:
    """Main relay loop — runs forever, polling the outbox and publishing to Kafka.

    The loop has three responsibilities:
    1. Periodic maintenance: recover stuck rows (every 5 min), cleanup old rows (daily)
    2. Claim a batch of pending rows
    3. Publish each row to Kafka, then confirm results

    Backoff logic:
    - No pending rows → exponential backoff (1s, 2s, 4s … 30s) to avoid CPU burn
    - All publishes failed → exponential backoff (Kafka may be down)
    - At least one publish succeeded → reset to MIN_SLEEP (Kafka is reachable)
    """

    # Skeleton:
    backoff = MIN_SLEEP
    last_recovery = 0     # monotonic time of last recover_stuck_rows call
    last_cleanup = 0      # monotonic time of last cleanup_published_rows call

    while True:
        now = time.monotonic()

        # Periodic maintenance
        if now - last_recovery > 300:      # every 5 minutes
            await recover_stuck_rows(session_factory)
            last_recovery = now
        if now - last_cleanup > 86400:     # every 24 hours
            await cleanup_published_rows(session_factory)
            last_cleanup = now

        #   Claim a batch
        claimed = await claim_batch(session_factory)
        if not claimed:
            backoff = min(backoff * 2, MAX_SLEEP)
            await asyncio.sleep(backoff)
            continue
        #   Publish each row to Kafka
        #   IMPORTANT: use send_and_wait(), not send()
        #   send() is fire-and-forget — the broker ack may never arrive.
        #   send_and_wait() blocks until the broker confirms the write.
        #   Without this, you could mark a row 'published' even though
        #   Kafka never received it — silently losing the event.
        for row in claimed:
            try:
                await kafka_producer.send_and_wait(
                    row.topic,
                    json.dumps(row.payload).encode(),
                )
                row.status = "published"
                row.published_at = datetime.now(timezone.utc)
            except KafkaError:
                row.retry_count += 1
                if row.retry_count >= MAX_OUTBOX_RETRIES:
                    row.status = "failed"
                    logger.error("Outbox row %s permanently failed after %d retries",
                                 row.id, MAX_OUTBOX_RETRIES)
                else:
                    row.status = "pending"  # return to pool for retry


        #   Confirm results
        await confirm_batch(session_factory, claimed)

        #   Backoff logic: back off only if NO rows were published
        published_count = sum(1 for r in claimed if r.status == "published")
        if published_count == 0 and claimed:
            backoff = min(backoff * 2, MAX_SLEEP)
        else:
            backoff = MIN_SLEEP
        await asyncio.sleep(backoff)
        
        # Reference: SYSTEM-DESIGN.md Section 6 — relay_loop

async def main():
    """Entrypoint: start the Kafka producer, then run the relay loop forever."""
    producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap_servers)
    await producer.start()
    logger.info("Outbox relay started. Polling for pending rows...")
    try:
        await relay_loop(db_factory, producer)
    finally:
        await producer.stop()
        logger.info("Outbox relay stopped.")


if __name__ == "__main__":
    asyncio.run(main())
