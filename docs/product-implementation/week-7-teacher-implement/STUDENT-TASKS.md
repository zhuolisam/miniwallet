# Week 7 Student Tasks — What You Implement

Six task groups with `# TODO: student` markers. This document explains each one, what concepts it teaches, and how to verify your work.

---

## Task 1: Wire `publish_event()` into Transfer Service (`app/services/transfer_service.py`)

**Difficulty:** Core exercise
**Concepts:** Outbox pattern, transactional event writes, replacing dual-write with atomic write
**Lines to write:** ~15 (two `publish_event()` calls)

### What to do

Two `# TODO: student` markers in `transfer_service.py`:

#### 1a. Failure path — `transfer.failed` event

After `db.add(failed_record)` and BEFORE `await db.commit()`:

```python
from app.events.publisher import publish_event

publish_event(db, "transfer.events", "transfer.failed", TransferFailedPayload(
    transfer_id=str(failed_record.id),
    from_account_id=str(from_account_id),
    to_account_id=str(to_account_id),
    amount=f"{amount:.8f}",
    failure_code="INSUFFICIENT_BALANCE",
), actor_id=actor_user_id)
```

#### 1b. Success path — `transfer.completed` event

After `db.add(transfer_record)` and BEFORE `await db.commit()`:

```python
publish_event(db, "transfer.events", "transfer.completed", TransferCompletedPayload(
    transfer_id=str(transfer_record.id),
    from_account_id=str(from_account_id),
    to_account_id=str(to_account_id),
    amount=f"{amount:.8f}",
    entry_type="transfer",
    idempotency_key=idempotency_key,
), actor_id=actor_user_id)
```

### Why it matters

**This is the fix for Week 6's dual-write problem.** In Week 6, the event was published AFTER `db.commit()` — outside the transaction boundary. If Kafka was down, the event was permanently lost.

Now `publish_event()` is called BEFORE commit. It does `db.add(OutboxRow(...))` — the outbox row is part of the same transaction as the Transfer row. The single `await db.commit()` atomically persists both. If the commit fails, both are rolled back. If it succeeds, the relay will deliver the event when Kafka is available.

**The API no longer talks to Kafka at all.** Only the relay does.

### Verify

```bash
uv run pytest tests/test_outbox_integration.py::test_transfer_creates_outbox_row -v
uv run pytest tests/test_outbox_integration.py::test_failed_transfer_creates_outbox_row -v
```

---

## Task 2: Wire `publish_event()` into Account Service (`app/services/account_service.py`)

**Difficulty:** Straightforward (same pattern as Task 1)
**Concepts:** Extending the outbox to non-transfer events
**Lines to write:** ~15 (two `publish_event()` calls)

### 2a. `open_account()` — `account.opened` event

After `db.add(account)` and BEFORE `await db.commit()`:

```python
from app.events.publisher import publish_event
from app.events.schemas import AccountOpenedPayload

publish_event(db, "account.events", "account.opened", AccountOpenedPayload(
    account_id=str(account.id),
    user_id=str(account.user_id),
    status=account.status,
), actor_id=user_id)
```

### 2b. `seed()` — `seed.completed` event

After `db.add(entry)` and BEFORE `await db.commit()`:

```python
from app.events.publisher import publish_event
from app.events.schemas import SeedCompletedPayload

publish_event(db, "account.events", "seed.completed", SeedCompletedPayload(
    account_id=str(account.id),
    user_id=str(account.user_id),
    amount=f"{amount:.8f}",
    entry_type="seed",
), actor_id=account.user_id)
```

### Why it matters

US-2.4 requires `account.opened` events for audit and notification consumers. The seed event is needed for the CQRS activity feed (Week 8) — without it, seed operations don't appear in transaction history.

### Verify

```bash
uv run pytest tests/test_outbox_integration.py::test_open_account_creates_outbox_row -v
uv run pytest tests/test_outbox_integration.py::test_seed_creates_outbox_row -v
```

---

## Task 3: Implement Outbox Relay Functions (`workers/outbox_relay.py`)

**Difficulty:** Core exercise — the most important task this week
**Concepts:** `FOR UPDATE SKIP LOCKED`, two-phase claim-publish-confirm, exponential backoff, crash recovery
**Lines to write:** ~80 across 5 functions

### 3a. `claim_batch()` — Claim pending rows

1. Open a session from `session_factory`
2. Inside a short transaction (`async with db.begin()`):
   - `SELECT` from OutboxRow WHERE status == "pending", ORDER BY created_at, LIMIT BATCH_SIZE
   - Use `.with_for_update(skip_locked=True)` — this is the key concurrency primitive
   - Set each row's status to `"publishing"`
3. Return the list of rows (detached ORM objects — session closes, but `expire_on_commit=False` keeps attributes accessible)

**Key concept: `FOR UPDATE SKIP LOCKED`**
- `FOR UPDATE` locks the selected rows so no other transaction can modify them
- `SKIP LOCKED` means if a row is already locked (by another relay instance), skip it instead of waiting
- This makes it safe to run 2+ relay processes for redundancy — they claim different batches

### 3b. `confirm_batch()` — Persist publish results

1. Open a new session
2. `await db.merge(row)` for each row — this reattaches the detached ORM objects and writes their updated status

### 3c. `recover_stuck_rows()` — Reset crashed relay's rows

1. `UPDATE outbox SET status = 'pending' WHERE status = 'publishing' AND created_at < NOW() - 5 minutes`
2. This handles the crash case: relay claimed rows but died before confirming them

### 3d. `cleanup_published_rows()` — Prevent unbounded table growth

1. `DELETE FROM outbox WHERE status = 'published' AND published_at < NOW() - 7 days`
2. Count and log `'failed'` rows older than 30 days (operator warning)
3. `DELETE FROM outbox WHERE status = 'failed' AND created_at < NOW() - 30 days`

### 3e. `relay_loop()` — The main loop

Follow the pseudocode in the TODO comment. The loop structure is:
1. Periodic maintenance (recovery every 5 min, cleanup every 24 hours)
2. Claim a batch
3. For each row: `send_and_wait()` → set status
4. Confirm the batch
5. Backoff logic: grow if idle or all failed, reset if any succeeded

**IMPORTANT: Use `send_and_wait()`, not `send()`**
- `send()` is fire-and-forget — the broker ack may never arrive
- `send_and_wait()` blocks until the broker confirms the write
- Without this, you'd mark a row "published" even though Kafka might not have received it

### Verify

```bash
uv run pytest tests/test_outbox_relay.py -v
# All 10 tests should pass once the relay functions are implemented.
```

---

## Task 4: Clean Up main.py Lifespan (`app/main.py`)

**Difficulty:** Warm-up
**Concepts:** Understanding what changed — the API no longer needs Kafka
**Lines to write:** 0 (just verify the existing code is clean)

### What to do

The teacher already removed `start_producer()` / `stop_producer()` from the lifespan. Your job: read the code, understand why, and remove the TODO comment once you're satisfied.

The lifespan is now just:
```python
@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
```

### Why it matters

In Week 6, the API connected directly to Kafka for inline publishing. In Week 7, the API only writes to the DB (outbox table). The relay connects to Kafka. The API's `kafka-init` dependency in docker-compose.yml is also removed — the API starts faster because it doesn't wait for Kafka.

---

## Task 5: Implement Outbox Integration Tests (`tests/test_outbox_integration.py`)

**Difficulty:** Straightforward (follows patterns in comments)
**Concepts:** Testing the write side of the outbox pattern, verifying atomicity
**Lines to write:** ~60 across 5 test functions

All tests follow the same pattern:
1. Make an API call via the HTTP client
2. Query the outbox table via `consumer_db_factory`
3. Assert the correct outbox row exists with the right event_type, topic, and payload

### Test list

| Test | What it verifies |
|------|-----------------|
| `test_transfer_creates_outbox_row` | Successful transfer → `transfer.completed` outbox row |
| `test_failed_transfer_creates_outbox_row` | Insufficient balance → `transfer.failed` outbox row |
| `test_open_account_creates_outbox_row` | Account creation → `account.opened` outbox row |
| `test_seed_creates_outbox_row` | Seed operation → `seed.completed` outbox row |
| `test_outbox_row_absent_when_transfer_rolled_back` | Failed lookup (404) → NO outbox row (atomicity) |

### Verify

```bash
uv run pytest tests/test_outbox_integration.py -v
# All 5 tests should pass once Tasks 1 and 2 are done.
```

---

## Task 6: Implement Outbox Relay Tests (`tests/test_outbox_relay.py`)

**Difficulty:** Moderate — requires understanding the relay lifecycle
**Concepts:** Testing DB state transitions, verifying concurrent-safe patterns
**Lines to write:** ~80 across 10 test functions

Each test:
1. Insert outbox rows using the `_insert_outbox_row()` helper
2. Call a relay function (claim_batch, confirm_batch, etc.)
3. Query the DB and assert the expected state

### Test list

| Test | What it verifies |
|------|-----------------|
| `test_claim_batch_returns_pending_rows` | Pending rows are claimed and marked 'publishing' |
| `test_claim_batch_skips_non_pending_rows` | Only 'pending' rows are claimed |
| `test_claim_batch_respects_batch_size` | At most BATCH_SIZE rows per call |
| `test_claim_batch_empty_outbox` | Empty outbox → empty list returned |
| `test_confirm_batch_persists_published_status` | Published status is persisted to DB |
| `test_confirm_batch_persists_retry` | Failed publish returns row to pending with incremented retry |
| `test_recover_stuck_rows_resets_old_publishing` | Stuck rows > 5 min reset to pending |
| `test_recover_stuck_rows_ignores_recent_publishing` | Recent publishing rows are NOT reset |
| `test_cleanup_deletes_old_published_rows` | Old published rows are deleted, recent ones survive |

### Verify

```bash
uv run pytest tests/test_outbox_relay.py -v
# All 10 tests should pass once Task 3 is done (but you can write the tests first — TDD!).
```

---

## Recommended Implementation Order

1. **Task 1** (transfer_service outbox) — the core pattern; understand publish_event()
2. **Task 2** (account_service outbox) — same pattern, different events
3. **Task 5** (integration tests) — verify Tasks 1+2 actually create outbox rows
4. **Task 3** (relay functions) — the most complex piece
5. **Task 6** (relay tests) — verify relay correctness
6. **Task 4** (main.py cleanup) — review and remove the TODO comment

---

## The Kill-Kafka Verification (after all tasks are done)

This is the proof that the outbox pattern works — the Week 6 failure mode is now gone:

```bash
# 1. Start everything
docker compose up --build -d

# 2. Register users, open accounts, seed money
#    (same as your Week 6 setup script)

# 3. Make a transfer — verify the outbox row is created
docker compose exec postgres psql -U minibank -c \
  "SELECT id, event_type, status FROM outbox ORDER BY created_at DESC LIMIT 5;"

# 4. Verify the audit consumer received it (via the relay)
docker compose exec postgres psql -U minibank -c \
  "SELECT event_id, event_type FROM audit_events ORDER BY occurred_at DESC LIMIT 5;"

# 5. Kill Kafka
docker compose stop kafka

# 6. Make another transfer (API still works — it writes to the outbox, not Kafka)
#    The transfer succeeds. The outbox row is created with status='pending'.
docker compose exec postgres psql -U minibank -c \
  "SELECT id, event_type, status FROM outbox ORDER BY created_at DESC LIMIT 5;"
# You should see a 'pending' row — the relay can't deliver because Kafka is down.

# 7. Restart Kafka
docker compose start kafka

# 8. Wait a few seconds for the relay to catch up, then check
docker compose exec postgres psql -U minibank -c \
  "SELECT id, event_type, status FROM outbox ORDER BY created_at DESC LIMIT 5;"
# The row should now be 'published'.

# 9. Verify the audit log — NO GAP this time!
docker compose exec postgres psql -U minibank -c \
  "SELECT t.id, ae.event_id IS NOT NULL as has_audit
   FROM transfers t
   LEFT JOIN audit_events ae ON ae.payload->'payload'->>'transfer_id' = t.id::text
   ORDER BY t.created_at;"
# Every transfer should show has_audit = true. The dual-write gap from Week 6 is fixed.
```
