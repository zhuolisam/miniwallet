# Tech Debt — Phase 2

## TD-001: Ledger schema is single-entry disguised as double-entry

**Severity:** High — blocks clean multi-leg transactions (fees, FX, reversals)
**Introduced:** Phase 1
**Refactor by:** Phase 3 (payments introduce fees, making the current model untenable)
**Status:** RESOLVED — migrated to proper two-leg model with per-leg direction and amount

### Current state

Each `ledger_entries` row carries both `debit_account_id` and `credit_account_id`. This is one row per money movement.

### Problems

1. **Index-hostile queries.** Fetching all movements for an account requires `WHERE debit_account_id = :id OR credit_account_id = :id` — PostgreSQL cannot use a single index efficiently; it union-scans two indexes or falls back to seq scan on large tables.

2. **Multi-leg transactions don't fit.** A transfer + fee requires 3 legs (sender→receiver, sender→fee-account). The current model forces a second row with no structural link to the first — no `transaction_id` groups them.

3. **Direction is derived at query time.** The transactions endpoint computes `CASE WHEN credit_account_id = :id THEN 'credit' ELSE 'debit'` per row. With a leg-based model, direction is a stored column — one indexed lookup, no branching.

4. **No currency column.** Every amount must carry its currency. Retrofitting this onto historical data is painful and error-prone.

5. **`Numeric(20,8)` is crypto precision.** Fiat systems use `Numeric(19,4)` (ISO 4217 / SWIFT convention). Over-precision hides rounding bugs.

### Target state

```
transactions (groups legs)
  id, idempotency_key, entry_type, description, created_at, posted_at

ledger_entries (one row per leg)
  id, transaction_id (FK), account_id, direction (DEBIT|CREDIT),
  amount (positive), currency (ISO 4217), created_at
```

### Why it was deferred

Phase 1 prioritized teaching locking, idempotency, and derived-balance patterns. A single-row model is easier to reason about for simple A→B transfers with no fees. The tradeoff: schema migration on an append-only financial table is the most painful migration in banking — the longer we wait, the worse it gets.

### Migration strategy

1. Create new `transactions` + `ledger_entries_v2` tables with the leg-based schema.
2. Backfill: for each existing `ledger_entries` row, emit two legs (debit + credit) under a new `transaction_id`.
3. Dual-write during transition: new transfers write to both old and new tables.
4. Cut over `get_balance()` to read from `ledger_entries_v2`.
5. Validate: `SUM(all balances) = 0` invariant holds on new table.
6. Drop old table.

### Resolution

Fixed. The ledger now uses a proper leg-based model:

```sql
ledger_entries (one row per leg)
  id              UUID PRIMARY KEY,
  transaction_id  UUID NOT NULL,         -- groups legs of the same movement
  account_id      UUID NOT NULL,         -- which account this leg touches
  direction       VARCHAR(6) NOT NULL,   -- 'debit' | 'credit'
  amount          NUMERIC(19,4) NOT NULL,-- always positive
  currency        VARCHAR(3) NOT NULL,   -- ISO 4217
  entry_type      VARCHAR(30) NOT NULL,
  idempotency_key VARCHAR(255) UNIQUE,
  created_at      TIMESTAMPTZ NOT NULL
```

A transfer creates two legs (debit sender, credit receiver) sharing a `transaction_id`. Balance derivation is `SUM(CASE WHEN direction='credit' THEN amount ELSE -amount END) WHERE account_id = :id`. The global invariant `SUM(all credits) - SUM(all debits) = 0` holds structurally. Multi-leg transactions (fees, FX) now fit naturally by adding more legs under the same `transaction_id`.

---

## TD-002: Backfill generates non-deterministic event IDs

**Severity:** High — running backfill twice corrupts audit_events and transaction_activity with duplicates
**Introduced:** Phase 2 (Week 8)
**Refactor by:** Before first production deploy of Phase 2
**Status:** RESOLVED — deterministic UUID5 event IDs implemented

### Current state

`management/backfill_events.py` calls `publish_event()` which generates `event_id = uuid4()` — a fresh random UUID on every invocation. The consumer idempotency constraint (`UNIQUE(event_id)` on audit_events, `UNIQUE(event_id, account_id)` on transaction_activity) only deduplicates if the same `event_id` is replayed. A second backfill run produces entirely new `event_id`s, bypassing the constraint and inserting duplicate rows.

### Problems

1. **Accidental double-run corrupts data.** Deploy scripts, operator error, or a partial failure + retry all produce duplicate audit and activity rows. The "fix" requires manual DELETE + consumer offset reset — high-risk surgery on append-only tables.

2. **Preflight guard is fragile.** The guard counts `account.opened` outbox rows, which are deleted after 7 days by `cleanup_published_rows`. After that window, the guard allows a second run unconditionally.

3. **No way to detect corruption after the fact.** Since each duplicate has a unique `event_id`, there is no structural way to identify which rows are duplicates without joining back to the source entity (transfer/account) and counting.

### Target state

Backfill generates **deterministic** `event_id`s derived from the source entity:

```python
import uuid

BACKFILL_NAMESPACE = uuid.UUID("b4cf1110-0000-0000-0000-000000000000")

def backfill_event_id(entity_type: str, entity_id: uuid.UUID) -> str:
    """Deterministic UUID5 — same entity always produces the same event_id."""
    return str(uuid.uuid5(BACKFILL_NAMESPACE, f"{entity_type}:{entity_id}"))
```

With deterministic IDs, running backfill N times inserts rows on the first run and hits the UNIQUE constraint (no-op) on subsequent runs — naturally idempotent via the same mechanism live consumers use.

### Migration strategy

1. Replace `uuid4()` in backfill with `uuid5(BACKFILL_NAMESPACE, f"{entity_type}:{entity_id}")`.
2. Pass the deterministic `event_id` to `publish_event()` (requires adding an optional `event_id` parameter).
3. If backfill has already been run once with uuid4 IDs: no action needed — those IDs are already consumed. The fix prevents future corruption, not retroactive.

### Resolution

Fixed. `publish_event()` now accepts an optional `event_id` parameter. Backfill uses `uuid5(BACKFILL_NAMESPACE, f"{entity_type}:{entity_id}")` to generate deterministic IDs. Running backfill N times is safe — consumer UNIQUE constraints silently reject duplicate event_ids on subsequent runs. The preflight guard remains as a convenience warning, but `force=True` no longer carries data-corruption risk.

---

## TD-003: No currency column on monetary tables

**Severity:** Medium — blocks multi-currency support, violates regulatory GL requirements
**Introduced:** Phase 1
**Refactor by:** Phase 3 (payments introduce FX, making single-currency assumption untenable)
**Status:** RESOLVED — currency VARCHAR(3) added to all monetary tables and event payloads

### Current state

`ledger_entries.amount`, `transaction_activity.amount`, and all event payloads carry a numeric amount with no paired currency. The system implicitly assumes a single currency (USD) without encoding that assumption anywhere.

### Problems

1. **Regulatory non-compliance.** Financial regulators (APRA, FCA, MAS) require explicit currency in general ledger records. An auditor asking "100 of what?" has no structural answer — only an implicit convention.

2. **Event payloads are ambiguous.** `audit_events.payload` stores `"amount": "100.00000000"` with no currency. Once the audit log contains multi-currency transactions (Phase 3+), there is no way to distinguish historical single-currency records from new multi-currency ones without payload-version heuristics.

3. **Extending to multi-currency requires migrating every monetary table.** `ledger_entries`, `transfers`, `transaction_activity`, and all event schemas need a new column/field. The longer this is deferred, the more historical data lacks currency attribution.

4. **`Numeric(20,8)` compounds the problem.** ISO 4217 specifies 2–4 decimal places for fiat. Storing 8 decimal places without a currency creates ambiguity about precision semantics (is this fiat at over-precision, or crypto at correct precision?).

### Target state

Every monetary column has a paired currency:

```sql
-- ledger_entries
amount   NUMERIC(19,4)  NOT NULL,
currency VARCHAR(3)     NOT NULL,  -- ISO 4217 (e.g. "USD", "SGD", "GBP")

-- transaction_activity
amount   NUMERIC(19,4)  NOT NULL,
currency VARCHAR(3)     NOT NULL,
```

Event payloads include currency:

```json
{
  "amount": "100.0000",
  "currency": "USD"
}
```

### Migration strategy

1. Add `currency VARCHAR(3) NOT NULL DEFAULT 'USD'` to `ledger_entries`, `transfers`, and `transaction_activity`.
2. Backfill existing rows with `'USD'` (trivial — all historical data is single-currency).
3. Add `currency: str` field to all event payload schemas (`TransferCompletedPayload`, etc.).
4. Update `publish_event()` callers to pass currency from the account or transfer context.
5. Drop the DEFAULT after backfill — new rows must explicitly specify currency.

### Resolution

Fixed alongside TD-001. All monetary tables now carry `currency VARCHAR(3) NOT NULL DEFAULT 'USD'`:
- `ledger_entries.currency`
- `transfers.currency`
- `transaction_activity.currency`

All event payload schemas (`TransferCompletedPayload`, `TransferFailedPayload`, `SeedCompletedPayload`) include a required `currency: str` field. Numeric precision standardized to `NUMERIC(19,4)` (ISO 4217 / SWIFT convention for fiat). Multi-currency support can now be enabled by removing the DEFAULT and requiring explicit currency at the application layer.
