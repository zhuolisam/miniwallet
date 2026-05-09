# Week 8 Student Tasks — What You Implement

Six files have `TODO:student` markers. This document explains each one, what concepts it teaches, and how to verify your work.

---

## Task 1: BaseConsumer `handle_message()` (`consumers/consumer_base.py`)

**Difficulty:** Core exercise
**Concepts:** At-least-once delivery, retry with headers, dead-letter routing
**Lines to write:** ~40

### What to do

Implement the `handle_message()` method in `BaseConsumer`. This is the retry/DLQ engine that all consumers inherit.

### Logic flow

```
1. Extract retry_count from message headers (b"x-retry-count")
2. Try: json.loads(message.value)
   └── JSONDecodeError → send to DLQ immediately, commit, return
3. Try: await self.process(event)
   └── Success → commit
   └── Exception:
       ├── retry_count >= 3 → send to DLQ, commit
       └── retry_count < 3  → re-publish with retry_count+1, commit
```

### Key details

- Headers are **bytes**: key is `b"x-retry-count"`, not `"x-retry-count"`
- Use `producer.send_and_wait()` (not `send()`) — at-least-once guarantee
- Always `await consumer.commit()` after DLQ or retry publish
- DLQ topic name: `f"{self.group_id}.dlq"`
- `dict(message.headers or [])` converts the header list to a lookup dict

### Verify

```bash
uv run pytest tests/test_dlq_routing.py -v
```

---

## Task 2: Activity Consumer `process()` (`workers/activity_consumer.py`)

**Difficulty:** Core exercise
**Concepts:** CQRS materialized view, dual-row pattern, Decimal handling, idempotency
**Lines to write:** ~35

### What to do

Implement `process()` in `ActivityConsumer`. This builds the read model that powers `/transactions`.

### The dual-row pattern

One `transfer.completed` event inserts **two** `TransactionActivity` rows:
- Debit row: `account_id=from_account_id, direction="debit"`
- Credit row: `account_id=to_account_id, direction="credit"`

This is how every neobank transaction feed works — each account sees their own view of the transfer.

### Key details

- `amount` arrives as a string (`"100.00000000"`) — convert with `Decimal(payload.amount)`
- `reference_id` is the `transfer_id` for transfer events, `None` for seeds
- `transfer.failed` → no row (failed transfers never moved money)
- `account.opened` → no row (not a financial transaction)
- Wrap in `try/except IntegrityError: pass` for idempotency

### Verify

```bash
uv run pytest tests/test_activity_consumer.py -v
```

---

## Task 3: Notification Consumer `process()` (`workers/notification_consumer.py`)

**Difficulty:** Easy
**Concepts:** Event-driven notifications, pattern matching
**Lines to write:** ~15

### What to do

Implement `process()` in `NotificationConsumer`. Parse the event and log the appropriate notification.

### Expected log output

| Event | Log |
|-------|-----|
| `transfer.completed` | 2 lines: sender "You sent $X" + receiver "You received $X" |
| `transfer.failed` | 1 line: "Transfer failed: {failure_code}" |
| `account.opened` | 1 line: "Your account is now active" |
| `seed.completed` | Nothing (no notification defined) |

### Key details

- Use `logger.info()` — tests verify via `caplog`
- Include the account_id/user_id in the log message (tests grep for it)
- No DB interaction — this consumer is stateless

### Verify

```bash
uv run pytest tests/test_notification_consumer.py -v
```

---

## Task 4: CQRS Query (`app/services/account_service.py`)

**Difficulty:** Moderate
**Concepts:** CQRS read model, eventual consistency, field renaming
**Lines to write:** ~30

### What to do

Implement `get_transactions()` to query `TransactionActivity` instead of `LedgerEntry`.

### What changes

| Aspect | Phase 1 (old) | Phase 2 (new) |
|--------|---------------|---------------|
| Table | `ledger_entries` | `transaction_activity` |
| Filter column | `credit_account_id` OR `debit_account_id` | `account_id` (single column) |
| Date column | `created_at` | `occurred_at` |
| Direction logic | Computed from credit/debit side | Stored in `direction` column |
| Return type | `(items, total)` | `(items, total, as_of)` |

### Key details

- `as_of = max(r.occurred_at for r in rows, default=None)` — scoped to current page
- Map `row.occurred_at` → `created_at` when constructing `TransactionItem` (field rename)
- The query is simpler than Phase 1: one `WHERE account_id = :id` instead of the OR condition

### Verify

```bash
uv run pytest tests/test_transactions_cqrs.py -v
```

---

## Task 5: Backfill Command (`management/backfill_events.py`)

**Difficulty:** Moderate
**Concepts:** Data migration, outbox reuse, idempotency guard
**Lines to write:** ~60

### What to do

Implement `backfill()` — generates synthetic outbox rows for all Phase 1 data.

### Three loops

1. **Accounts** → `account.opened` events
2. **Transfers** → `transfer.completed` or `transfer.failed` events
3. **Seeds** → `seed.completed` events

### Pattern for each

```python
async with db_factory() as db:
    async with db.begin():
        publish_event(db, topic, event_type, PayloadModel(...), actor_id=None)
```

### Key details

- `actor_id` is ALWAYS `None` for backfill (historical data has no actor)
- Transfer's `failure_code` may be `None` — use `"UNKNOWN"` fallback
- The preflight guard checks for existing `account.opened` outbox rows
- Read all IDs first (one query), then write outbox rows one-per-session

### Verify

```bash
uv run pytest tests/test_backfill.py -v
```

---

## Task 6: Tests (5 files)

**Difficulty:** Straightforward (patterns are documented)
**Lines to write:** ~120 across all test files

Each test has setup code (teacher-provided) and assertion code (you implement). The pattern is always:
1. Create test data (accounts, events)
2. Call the function under test
3. Query the DB or inspect the response
4. Assert expected state

### Test files

| File | Focus | Key assertion |
|------|-------|---------------|
| `test_activity_consumer.py` | Activity rows created correctly | Row count, direction, amount |
| `test_notification_consumer.py` | Correct log output | `caplog.records` contains expected strings |
| `test_transactions_cqrs.py` | API reads from read model | Response shape, as_of, filters |
| `test_backfill.py` | Outbox rows generated | Event types present, actor_id=None |
| `test_dlq_routing.py` | BaseConsumer retry/DLQ | Mock producer assertions |

### Verify all

```bash
uv run pytest tests/test_activity_consumer.py tests/test_notification_consumer.py tests/test_transactions_cqrs.py tests/test_backfill.py tests/test_dlq_routing.py -v
```

---

## Recommended Implementation Order

1. **Task 3** (notification consumer) — simplest, no DB, builds confidence
2. **Task 2** (activity consumer) — core CQRS concept, tests are straightforward
3. **Task 4** (CQRS query) — natural follow-on from activity consumer
4. **Task 1** (handle_message) — the hardest piece; DLQ routing requires careful header handling
5. **Task 5** (backfill) — combines everything: outbox, typed payloads, data queries
6. **Task 6** (tests) — fill in assertions as you go, or do them all at the end

---

## The Integration Test (after all tasks are done)

This is the end-to-end verification that everything connects:

```bash
# 1. Start everything
docker compose up --build -d

# 2. Register users, open accounts, seed money
# (via curl or the dev seed endpoint)

# 3. Run backfill to populate historical data
docker compose run --rm api python -m management.backfill_events

# 4. Wait a few seconds for consumers to process

# 5. Check audit_events has entries for ALL events
docker compose exec postgres psql -U minibank -c \
  "SELECT event_type, COUNT(*) FROM audit_events GROUP BY event_type;"

# 6. Check transaction_activity has the read model
docker compose exec postgres psql -U minibank -c \
  "SELECT account_id, direction, entry_type, amount FROM transaction_activity LIMIT 10;"

# 7. Verify GET /transactions reads from the read model
curl -s http://localhost:8000/v1/accounts/me/transactions \
  -H "Authorization: Bearer <token>" | jq .

# 8. Check notification-consumer logs
docker compose logs notification-consumer | grep "NOTIFY"

# 9. Make a new transfer and watch all 3 consumers process it
curl -X POST http://localhost:8000/v1/transfers \
  -H "Authorization: Bearer <token>" \
  -H "Idempotency-Key: test-integration" \
  -H "Content-Type: application/json" \
  -d '{"to_email": "bob@example.com", "amount": "25.00"}'

# Watch logs:
docker compose logs --tail=5 audit-consumer activity-consumer notification-consumer
```
