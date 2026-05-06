---
title: PostgreSQL UNIQUE Constraints and Implicit Indexes
tags: [engineering-concept, database]
phase: 1
week: 7
updated: 2026-05-04
---

# PostgreSQL UNIQUE Constraints and Implicit Indexes

## The Core Rule

**In PostgreSQL, every `UNIQUE` constraint automatically creates a B-tree index.** You never need to add a separate index on a column that already has a `UNIQUE` constraint — it would be a redundant duplicate.

This is not optional or configurable. The PostgreSQL docs state:

> PostgreSQL automatically creates a unique index when a unique constraint or primary key is defined for a table.

`PRIMARY KEY` = `UNIQUE` + `NOT NULL`, so PKs get the same implicit index.

## Why B-tree Specifically

PostgreSQL uses a **B+ tree** (a B-tree variant where all values live in leaf nodes, and leaves are linked for range scans). This gives:

| Operation | Complexity |
|-----------|-----------|
| Equality lookup (`WHERE key = ?`) | O(log n) — tree traversal |
| Range scan (`WHERE key BETWEEN a AND b`) | O(log n + k) — find start, walk leaves |
| Insert with uniqueness check | O(log n) — find position, check, insert |
| Min/Max | O(log n) — walk to leftmost/rightmost leaf |

For a table with 10M rows, a B+ tree of height ~4 means 4 page reads to find any row. A full table scan would read all ~10M rows sequentially.

### B-tree vs B+ tree

```
B-tree:   data stored in ALL nodes (internal + leaf)
B+ tree:  data stored ONLY in leaf nodes, internal nodes hold keys only
          leaf nodes are doubly-linked for fast range scans
```

PostgreSQL calls it "B-tree" in docs but the implementation is B+ tree (`src/backend/access/nbtree/`). This distinction matters because the linked leaf nodes are what make `ORDER BY idempotency_key` or range queries efficient without a separate sort step.

## How to Verify

```sql
-- Show all indexes on a table, including implicit ones
\d transfers

-- You'll see something like:
-- Indexes:
--     "transfers_pkey" PRIMARY KEY, btree (id)
--     "transfers_idempotency_key_key" UNIQUE CONSTRAINT, btree (idempotency_key)
```

The naming convention `{table}_{column}_key` is PostgreSQL's auto-generated name for unique constraint indexes.

## Minibank Application

In minibank, both `transfers.idempotency_key` and `ledger_entries.idempotency_key` are declared `UNIQUE NOT NULL`:

```python
# app/models/transfer.py
idempotency_key: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
```

This means the `IntegrityError` fallback queries in [[p2p-transfer-deep-dive|transfer_service]] and account_service — `SELECT ... WHERE idempotency_key = ?` — are B-tree index lookups, not table scans.

These queries only fire on the rare concurrent-duplicate path (two requests with the same key racing past the Redis cache). The hot path is the Redis idempotency check at the top of `transfer()`.

## When You DO Need an Explicit Index

- Columns used in `WHERE` / `JOIN` / `ORDER BY` that have **no unique constraint** (e.g., `status`, `from_account_id`)
- Composite lookups not covered by existing indexes (e.g., `WHERE account_id = ? AND created_at > ?`)
- Partial indexes for hot subsets (`CREATE INDEX ... WHERE status = 'pending'`)

## Common Mistake

Adding a redundant index on a unique column:

```sql
-- WRONG: this creates a duplicate of the implicit unique index
CREATE INDEX idx_transfers_idempotency ON transfers(idempotency_key);
ALTER TABLE transfers ADD CONSTRAINT uq_idem UNIQUE (idempotency_key);
-- Now you have TWO identical B-tree indexes. Writes pay double.
```
