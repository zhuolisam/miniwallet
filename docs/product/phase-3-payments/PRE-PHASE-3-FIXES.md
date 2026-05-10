# Pre-Phase 3 Fixes

Issues discovered in the existing Phase 1–2 codebase that must be addressed before Phase 3 implementation begins.

---

## 1. Add `reference_id` column to `ledger_entries`

**Problem:** Phase 3 requires linking ledger entries back to their source entity (deposit, withdrawal). The current `ledger_entries` table has no such column.

**Current state:**
- `ledger_entries` has: `id`, `transaction_id`, `account_id`, `direction`, `amount`, `currency`, `entry_type`, `idempotency_key`, `created_at`
- No column pointing to `deposits.id`, `withdrawals.id`, or `transfers.id`

**Fix:**
- New migration `0006_add_ledger_reference_id.py`:
  ```sql
  ALTER TABLE ledger_entries ADD COLUMN reference_id UUID;
  CREATE INDEX idx_ledger_entries_reference_id ON ledger_entries (reference_id) WHERE reference_id IS NOT NULL;
  ```
- Update `LedgerEntry` ORM model to include:
  ```python
  reference_id: Mapped[uuid.UUID | None] = mapped_column(PG_UUID(as_uuid=True), nullable=True)
  ```
- Existing Phase 1–2 entries remain `NULL`. Phase 3+ entries always populate it.

**Why blocking:** Withdrawal reversals reference the withdrawal via `reference_id`. Without it, recovery cannot query "which ledger entries belong to withdrawal X" — needed to check if compensation was already written.

---

## 2. Update transfer service to set `reference_id`

**Problem:** Once the column exists, `transfer_service.py` should populate it for consistency.

**Current state (`transfer_service.py:120-131`):**
```python
db.add(LedgerEntry(
    ...
    entry_type="transfer",
    created_at=now,
    # NO reference_id
))
```

**Fix:** Add `reference_id=transfer_record.id` to both legs. Same in `account_service.seed()` (can remain NULL for seed since there's no `seeds` table).

**Why recommended (not blocking):** Phase 3 entries will always populate `reference_id`. Leaving Phase 1 entries NULL is fine — the column is nullable. But doing this now means the activity consumer and future queries work uniformly.

---

## Summary

| # | Fix | Blocking? | Status |
|---|-----|-----------|--------|
| 1 | Add `reference_id` to `ledger_entries` (migration + model) | **Yes** | **Done** (0006 migration + ORM model) |
| 2 | `reference_id` in transfer/seed services | Recommended | **Done** (transfer_service populates it) |
