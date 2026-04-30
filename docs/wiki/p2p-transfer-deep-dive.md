# P2P Transfer — Deep Dive

---

## Is this close to how real digital banks implement P2P transfers?

**Yes, the core patterns are production-grade.** The design mirrors what companies like Stripe, Wise, and Revolut use:

| Pattern | This Code | Industry Standard |
|---|---|---|
| Double-entry ledger | `LedgerEntry` with debit/credit | Universal in fintech |
| Pessimistic locking | `with_for_update()` | Standard for balance checks |
| Idempotency | Redis + DB constraint | Stripe's exact model |
| Ordered lock acquisition | `sorted([...], key=str)` | Classic deadlock prevention |
| Immutable audit trail | Separate `Transfer` + `LedgerEntry` | Regulatory requirement |

**What's missing vs. a real bank:**
- No currency handling (USD vs EUR, FX rates)
- No fraud/AML checks before committing
- No async notification (webhooks, push notifications)
- No transfer limits or velocity checks
- No reversal/chargeback flow
- Status is always `"completed"` — real banks have `pending → processing → completed/failed`

---

## Why do we need an idempotency key?

**The problem it solves: duplicate execution on retry.**

Imagine this sequence without idempotency:

```
Client → POST /transfer (network timeout, client never gets response)
Client → POST /transfer (retry, assuming first failed)

Result: $100 debited TWICE from Alice
```

The client can't know if the first request committed or not. Mobile networks drop connections, load balancers time out, servers crash mid-commit. Without idempotency, a retry = a second transfer.

With an idempotency key, the second request returns the *same response* as the first — no second debit.

For how the client should generate and persist the idempotency key to avoid double entries, see [[idempotency-client-guide]].

---

**The `request_hash` ensures correctness of the Redis fast path.**

When Redis hits, you skip the DB entirely and return the cached response directly. Without the hash, you can't tell whether the cache hit corresponds to *this* request or a different one that reused the same key:

```
Request #1: key="abc", amount=100  → commits, cached in Redis
Request #2: key="abc", amount=999  → Redis HIT
```

Without `request_hash`, Request #2 gets a cache hit and returns the `amount=100` response to a client that submitted `amount=999` — silently wrong, no error raised.

With `request_hash`, the mismatch is detected immediately and `409 Conflict` is returned.

Note: this is not a safety concern — the DB unique constraint would reject the second insert regardless. The hash is purely about **not returning a mismatched cached response** when the Redis fast path is taken.

---

## What does `with_for_update()` do? — SQL at each step

### Step 1: Check Redis cache
```sql
-- No SQL, pure Redis GET
GET idempotency:{key}
```

### Step 2: Lock accounts (the critical section)
```sql
-- Lock account with lower UUID first
SELECT accounts.*
FROM accounts
WHERE accounts.id = '11111111-...'
FOR UPDATE;  -- BLOCKS other transactions from reading/modifying this row

-- Then lock account with higher UUID
SELECT accounts.*
FROM accounts
WHERE accounts.id = '99999999-...'
FOR UPDATE;
```

`FOR UPDATE` tells Postgres: "I'm about to modify this row — hold an exclusive lock until my transaction commits or rolls back." Any other transaction trying to `SELECT ... FOR UPDATE` on the same row will **block** until the lock is released.

**Without it, a race condition produces a negative balance:**

```
Txn A: reads Alice balance = $100
Txn B: reads Alice balance = $100  ← same snapshot!
Txn A: balance sufficient → commits debit
Txn B: balance sufficient → commits debit
Result: Alice debited twice, balance goes negative
```

**Why lock BOTH accounts in sorted UUID order?**

PostgreSQL acquires a `FOR KEY SHARE` lock on the credit account row when inserting a ledger entry (FK enforcement). If Txn A holds `FOR UPDATE` on Alice and waits for Bob, while Txn B holds `FOR UPDATE` on Bob and waits for Alice — that's a deadlock. Acquiring both locks in the same sorted order makes circular waits impossible.

### Step 3: Check balance
```sql
-- get_balance() derives balance from the ledger, not a stored column
SELECT COALESCE(SUM(amount), 0) FROM ledger_entries WHERE credit_account_id = 'alice-uuid'
MINUS
SELECT COALESCE(SUM(amount), 0) FROM ledger_entries WHERE debit_account_id = 'alice-uuid'
```

### Step 4: Insert ledger entry + transfer record
```sql
INSERT INTO ledger_entries
  (id, debit_account_id, credit_account_id, amount, entry_type,
   reference_id, idempotency_key, created_at)
VALUES
  ('uuid4', 'alice-uuid', 'bob-uuid', 100.00, 'transfer',
   NULL, 'client-key-xyz', '2026-04-26T...');

INSERT INTO transfers
  (id, from_account_id, to_account_id, amount, status,
   idempotency_key, created_at)
VALUES
  ('uuid4', 'alice-uuid', 'bob-uuid', 100.00, 'completed',
   'client-key-xyz', '2026-04-26T...');

-- reference_id patched in memory before flush:
UPDATE ledger_entries SET reference_id = 'transfer-uuid' WHERE id = 'entry-uuid';

COMMIT;  -- locks released here
```

### Step 5: Cache in Redis
```
SETEX idempotency:{key} 86400 {hash + response JSON}
```

---

## Why Redis? Why not just the DB unique constraint?

Redis is purely a **performance optimization** — the safety net is entirely the DB. If Redis disappeared tomorrow, correctness is fully preserved: the DB unique constraint on `idempotency_key` plus the atomic `COMMIT` guarantee exactly-once execution. The `IntegrityError` path exists precisely for this.

```
Request #1 (first ever)
  Redis GET → miss
  DB: lock → balance check → insert → COMMIT
  Redis SETEX → cached for 24h

Request #2 (retry within 24h)
  Redis GET → HIT → return cached response immediately
  No DB touched. Zero lock contention. ~1ms response.

Request #3 (Redis cache expired, e.g. after 24h+)
  Redis GET → miss
  DB: INSERT → UNIQUE VIOLATION on idempotency_key
  IntegrityError caught → SELECT transfer → return it
```

Redis buys you:
- Skip the DB round-trip + locking overhead on retries (~1ms vs ~50–100ms)
- No lock contention on hot accounts during retry storms

**Why store `request_hash` in Redis alongside the response?**

When Redis hits, you return the cached response without touching the DB. The hash lets you verify the incoming request matches what was originally cached — so you don't silently return a response for a different transfer (different amount, different accounts) that happened to share the same key. See the idempotency key section above.

---

## What could go wrong?

### 1. Gap between `COMMIT` and `SETEX`
```python
await db.commit()
# ← server crashes here
await redis.setex(...)  # never reached
```
The next retry hits a Redis miss → DB `IntegrityError` → fetched and returned correctly. The code handles this. ✓

### 2. Balance computed from full ledger history
`get_balance()` sums *all* ledger entries on every transfer. At 1M+ entries this becomes a full table scan. Real banks maintain a cached `balance` column updated atomically in the same transaction, or partition the ledger by time window.

### 3. No timeout on `FOR UPDATE`
A long-running transaction holding locks on both accounts blocks every other transfer involving those accounts. Should add `NOWAIT` or set `lock_timeout` at the session level with retry logic.

### 4. Status is always `"completed"`
For intra-bank synchronous transfers this is actually fine — Postgres's `COMMIT` is atomic, so if the server crashes before commit, the transaction is rolled back and no record exists. There is no partial state. A `"pending"` record would only be needed for cross-bank transfers (ACH, SWIFT) where the transfer leaves your system before you know if it succeeds, or for async processing flows.

### 5. No distributed lock on cache miss
If two requests with the *same* idempotency key arrive simultaneously and both get a Redis miss, both race to the DB. The unique constraint catches the loser via `IntegrityError`, but both execute the full locking + balance check flow first. Under high load on hot accounts this is wasteful and increases lock contention.

### 6. `reference_id` starts as `NULL`
The ledger entry is inserted with `reference_id=NULL` then patched to the transfer ID before commit. If the schema ever gains a `NOT NULL` constraint on that column, this will break. The pattern is intentional (circular FK dependency) but fragile.
