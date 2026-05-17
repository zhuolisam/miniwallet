# Ledger Entry `idempotency_key` — Design Decision

## Context

`LedgerEntry.idempotency_key` is a nullable unique column that exists solely to support the **seed** (dev funding) operation.

Unlike transfers, deposits, and withdrawals — which each have a dedicated business entity with its own `idempotency_key` — seed has no corresponding model. The `LedgerEntry` itself is the only record produced, so the deduplication guard lives directly on the credit leg.

## Why not remove it?

Without this column, the seed endpoint has no deduplication mechanism. Double-seeding creates phantom balances that corrupt test assertions and mask bugs in balance derivation.

## Why not a `Seed` model?

Seed is a dev-only operation (test funding). Adding a dedicated table with idempotency, status tracking, and event publishing would be over-engineering for something that never ships to production.

## Why not Redis?

Redis-based idempotency (the transfer pattern) is non-durable. If Redis restarts between duplicate requests, the guard is lost. For transfers this is acceptable because the DB unique constraint on `transfers.idempotency_key` is the true safety net. Seed has no such backup entity — the ledger column *is* the safety net.

## Rules

- Only the **seed** operation sets `idempotency_key` on a `LedgerEntry`.
- Transfer, deposit, and withdrawal services must **never** set it — their idempotency lives on their respective business entities.
- The column remains `nullable` because the vast majority of ledger entries (all non-seed) will be NULL.

## Future

If seed ever becomes a production operation (e.g., partner-funded accounts), it should get its own entity and the `idempotency_key` on `LedgerEntry` should be removed at that point.
