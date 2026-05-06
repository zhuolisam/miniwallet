---
title: FOR UPDATE SKIP LOCKED
tags: [engineering-concept, database, concurrency]
phase: 2
week: 7
updated: 2026-05-06
---

# FOR UPDATE SKIP LOCKED

PostgreSQL locking clause that enables safe concurrent polling from a work queue without coordination.

## The SQL

```sql
SELECT * FROM outbox 
WHERE status = 'pending' 
ORDER BY created_at 
LIMIT 100 
FOR UPDATE SKIP LOCKED
```

## What each part does

**`FOR UPDATE`** — locks selected rows in exclusive mode (write lock)
- No other transaction can UPDATE or DELETE these rows until commit/rollback
- Other transactions trying `SELECT ... FOR UPDATE` on same rows will **block and wait**
- Regular SELECTs can still read (they see pre-lock version via MVCC)

**`SKIP LOCKED`** — changes blocking behavior
- Instead of waiting for locked rows, **skip them immediately**
- Only return rows that are currently unlocked
- Makes the query non-blocking — never waits

## Use case: concurrent outbox relay workers

```
Time   Relay 1                          Relay 2
----   --------------------------------  --------------------------------
T0     SELECT ... FOR UPDATE SKIP LOCKED
       → locks rows 1-100
                                         SELECT ... FOR UPDATE SKIP LOCKED
                                         → rows 1-100 locked, SKIP them
                                         → locks rows 101-200

T1     publish rows 1-100 to Kafka      publish rows 101-200 to Kafka

T2     COMMIT (releases lock)           COMMIT (releases lock)
```

Each worker claims **different rows** automatically. No coordination needed (no Redis, no leader election).

## Without SKIP LOCKED

```
Relay 1: lock rows 1-100
Relay 2: try to lock same rows → BLOCKS, waits 30s for Relay 1
Relay 1: COMMIT
Relay 2: NOW gets rows 1-100 → DUPLICATE PUBLISH
```

## Where this is used

- [[outbox-relay]] — `workers/outbox_relay.py:claim_batch()`
- Production neobanks: Stripe, Shopify, GitHub use this for event delivery at scale
- Any high-throughput work queue that needs horizontal scaling without a single point of failure

## SQLAlchemy syntax

```python
q = (
    select(OutboxRow)
    .where(OutboxRow.status == "pending")
    .order_by(OutboxRow.created_at)
    .limit(BATCH_SIZE)
    .with_for_update(skip_locked=True)  # ← this
)
```

## Related

- [[postgresql-unique-constraint-indexing]] — Index locking behavior
- [[alembic-qna]] — Session-per-operation pattern (why short transactions matter)
