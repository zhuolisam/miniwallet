# Week 7 Implementation Tradeoffs

Design decisions made in the outbox relay implementation, documenting what was simplified for learning vs what production systems do.

---

## 1. `created_at` vs `claimed_at` for stuck row recovery

### What we do
```python
# recover_stuck_rows()
WHERE status = 'publishing' 
  AND created_at < NOW() - 5 minutes
```

Use `created_at` (when the business event was written to outbox) as a proxy for "when was this row claimed?"

### The problem: false positives during backlogs

```
Timeline for a single outbox row during a backlog:

T=0 hours:    Row created, status='pending', created_at=T0
              (Millions of pending rows ahead in queue)

T=3 hours:    Still pending, queue hasn't reached it yet

T=3h 0m 0s:   Relay Worker 1 claims it
              Status: pending → publishing

T=3h 0m 1s:   recover_stuck_rows() runs
              Checks: created_at < NOW() - 5 minutes?
              → created_at was 3 hours ago ✓
              → Resets status: publishing → pending (FALSE POSITIVE!)

T=3h 0m 2s:   Worker 1 finishes publishing to Kafka
              confirm_batch() finds row already reset
              → Duplicate publish will happen
```

A row created hours ago might only get claimed **now**, but `created_at < NOW() - 5 min` is still true. Recovery immediately resets it even though it was just claimed legitimately.

### Why this is acceptable

**Consumers are idempotent** — duplicate publishes cause wasted work but no data corruption:

```python
# audit_consumer.py
@consumer.on("account.created")
async def handle_account_created(event):
    # Idempotent: second write is a no-op
    await db.execute(
        insert(AuditEntry).values(...)
        .on_conflict_do_nothing()
    )
```

**Cost:**
- Extra Kafka publishes during backlogs
- Extra consumer processing (immediately rejected by conflict constraint)
- No financial impact, no user-visible impact

### What production does

Add a `claimed_at` column:

```sql
ALTER TABLE outbox ADD COLUMN claimed_at TIMESTAMPTZ;

-- In claim_batch():
UPDATE outbox 
SET status = 'publishing', claimed_at = NOW()
WHERE id IN (...)

-- In recover_stuck_rows():
WHERE status = 'publishing' 
  AND claimed_at < NOW() - 5 minutes  -- ✅ accurate
```

Now recovery only resets rows that were **claimed** > 5 minutes ago, regardless of when the business event was created.

**Used by:** Stripe, Shopify, all high-throughput event systems

### Decision rationale

- **Simpler schema** — one fewer column, easier for students to understand
- **Low false positive rate** — only matters during extreme backlogs (>hours of lag)
- **Negligible cost** — idempotent consumers already handle duplicates
- **Learning focus** — the core pattern (two-phase claim/confirm, FOR UPDATE SKIP LOCKED) is more important than optimizing edge cases

For a learning project processing <1000 events/sec, the tradeoff is correct. For production processing millions of events/sec, add `claimed_at`.

---

## Future tradeoffs (to be documented as we implement)

- Outbox retention (7 days) vs Kafka retention (default 7 days) alignment
- Single relay process vs multiple for redundancy
- Batch size (100) tuning based on message size and Kafka throughput
- Retry limit (10) before marking failed
