# Week 6 Student Tasks — What You Implement

Four files have `# TODO: student` markers. This document explains each one, what concepts it teaches, and how to verify your work.

---

## Task 1: Kafka Producer Lifecycle (`app/main.py`)

**Difficulty:** Warm-up
**Concepts:** FastAPI lifespan, async resource management
**Lines to write:** ~2

### What to do

In the `lifespan()` async context manager:
1. Call `await start_producer()` **before** `yield`
2. Call `await stop_producer()` **after** `yield`

### Why it matters

The AIOKafkaProducer must be started before any request can publish an event, and stopped cleanly on shutdown to flush in-flight sends. The lifespan context manager is FastAPI's mechanism for this — it runs once at startup, yields while the app serves requests, then runs cleanup on shutdown.

### Verify

```bash
docker compose up --build api
# Look for no crash on startup. If start_producer() fails to connect to Kafka,
# the API will still start — the producer just won't be available.
```

---

## Task 2: Producer Init & Event Publishing (`app/services/transfer_service.py`)

**Difficulty:** Core exercise
**Concepts:** AIOKafkaProducer, event envelope design, the dual-write problem
**Lines to write:** ~40

### 2a. `start_producer()`

1. Create an `AIOKafkaProducer` with `bootstrap_servers=settings.kafka_bootstrap_servers`
2. Use `value_serializer=lambda v: json.dumps(v).encode("utf-8")` so you can pass dicts directly
3. Assign to the global `kafka_producer`
4. Call `await kafka_producer.start()`

### 2b. `stop_producer()`

1. If `kafka_producer is not None`, call `await kafka_producer.stop()`
2. Set `kafka_producer = None`

### 2c. Publish `transfer.completed` event (after the success commit)

After `await db.commit()` on the success path, build and send the event:

```python
event = {
    "event_id":    str(uuid.uuid4()),       # fresh UUID, NOT the transfer ID
    "event_type":  "transfer.completed",
    "occurred_at": now.isoformat(),
    "version":     "1",
    "actor_id":    str(actor_user_id) if actor_user_id else None,
    "payload": {
        "transfer_id":     str(transfer_record.id),
        "from_account_id": str(from_account_id),
        "to_account_id":   str(to_account_id),
        "amount":          f"{amount:.8f}",
        "entry_type":      "transfer",
        "idempotency_key": idempotency_key,
    },
}
if kafka_producer is not None:
    await kafka_producer.send_and_wait("transfer.events", value=event)
```

### 2d. Publish `transfer.failed` event (after the failure commit)

Same pattern but with `event_type: "transfer.failed"` and `failure_code` in the payload instead of `entry_type`/`idempotency_key`.

### Key insight: The dual-write problem

The publish happens **after** `db.commit()` but is **not part of the same transaction**. If the process crashes (or Kafka is down) between the commit and the send, the DB has the transfer but Kafka never received the event. The audit log will have a gap. This is **intentional** — you will fix it in Week 7 with the outbox pattern.

### Verify

```bash
docker compose up --build
# Make a transfer via curl:
curl -X POST http://localhost:8000/v1/transfers \
  -H "Authorization: Bearer <token>" \
  -H "Idempotency-Key: test-1" \
  -H "Content-Type: application/json" \
  -d '{"to_email": "bob@example.com", "amount": "50.00"}'

# Check the audit consumer log:
docker compose logs audit-consumer
# You should see "Received transfer.completed ..." if the consumer is also implemented.
```

---

## Task 3: Audit Consumer (`workers/audit_consumer.py`)

**Difficulty:** Core exercise
**Concepts:** AIOKafkaConsumer, manual offset commit, at-least-once delivery, idempotent writes
**Lines to write:** ~35

### 3a. `process(event)` — Persist one event to audit_events

1. Parse fields from the event envelope:
   - `event_id = uuid.UUID(event["event_id"])`
   - `event_type = event["event_type"]`
   - `actor_id = uuid.UUID(event["actor_id"]) if event.get("actor_id") else None`
   - `occurred_at = datetime.fromisoformat(event["occurred_at"])`
   - `resource_type = _RESOURCE_TYPE.get(event_type)`
   - `resource_id`: extract from payload — `transfer_id` for transfer events, `account_id` for account events

2. Open a DB session and INSERT:
   ```python
   async with db_factory() as db:
       async with db.begin():
           db.add(AuditEvent(...))
   ```

3. **Critical:** Wrap in `try/except IntegrityError` — on duplicate `event_id`, log a warning and return. Do NOT re-raise. This makes the consumer idempotent.

### 3b. `run()` — The consumer loop

1. Create `AIOKafkaConsumer(TOPIC, ...)` with `enable_auto_commit=False`
2. `await consumer.start()`
3. `try/finally` — in finally: `await consumer.stop()`
4. Inside try: `async for msg in consumer:` → `json.loads(msg.value)` → `await process(event)` → `await consumer.commit()`

### Why `enable_auto_commit=False`

With auto-commit, Kafka periodically advances the offset regardless of whether processing succeeded. If the consumer crashes after auto-commit but before the DB INSERT, the event is permanently lost — Kafka won't redeliver it. Manual commit after the DB write guarantees at-least-once delivery.

### Verify

```bash
docker compose up --build
# Make a transfer, then check audit_events:
docker compose exec postgres psql -U minibank -c "SELECT event_id, event_type, resource_type FROM audit_events;"
# Should show one row per transfer event.
```

---

## Task 4: Tests (`tests/test_audit_consumer.py`)

**Difficulty:** Straightforward (follows patterns in comments)
**Concepts:** Testing consumers by calling `process()` directly, verifying DB state
**Lines to write:** ~50 across 6 test functions

All tests follow the same pattern:

1. Build an event with `make_event(event_type, payload)`
2. Call `await process(event)` — this is the function from `workers/audit_consumer.py`, not a Kafka consumer
3. Open a session via `consumer_db_factory` and query `audit_events`
4. Assert the row has the expected field values

### Test list

| Test | What it verifies |
|------|-----------------|
| `test_audit_persists_transfer_completed` | `transfer.completed` → row with `resource_type="transfer"`, correct payload |
| `test_audit_persists_transfer_failed` | `transfer.failed` → row with `resource_type="transfer"` |
| `test_audit_persists_account_opened` | `account.opened` → row with `resource_type="account"`, correct `resource_id` |
| `test_audit_persists_seed_completed` | `seed.completed` → row with `resource_type="account"` |
| `test_audit_idempotent_on_duplicate_event_id` | Same event twice → exactly one row, no exception raised |
| `test_audit_stores_correct_resource_type_for_all_event_types` | All 4 event types → correct `resource_type` mapping |

### Verify

```bash
uv run pytest tests/test_audit_consumer.py -v
# All 6 tests should pass once process() is implemented.
```

---

## Recommended Implementation Order

1. **Task 1** (main.py lifespan) — 2 lines, gets you comfortable
2. **Task 3a** (process function) — this is the most important piece; tests depend on it
3. **Task 4** (tests) — run tests to validate your process() implementation
4. **Task 2** (producer + publish) — wire up the Kafka side
5. **Task 3b** (consumer loop) — wire up the full consumer loop
6. **Docker smoke test** — `docker compose up --build`, make a transfer, check audit_events

---

## The Failure Experiment (after all tasks are done)

This is the whole point of Week 6 — experiencing the dual-write problem firsthand:

```bash
# 1. Start everything
docker compose up --build -d

# 2. Register users, open accounts, seed money (via curl or a script)

# 3. Make a transfer — verify it appears in audit_events
docker compose exec postgres psql -U minibank -c "SELECT count(*) FROM audit_events;"

# 4. Kill Kafka
docker compose stop kafka

# 5. Make another transfer (API still works — it just can't publish)
# The transfer will succeed in the DB but the Kafka send will fail/timeout.

# 6. Restart Kafka
docker compose start kafka

# 7. Check for the audit gap
docker compose exec postgres psql -U minibank -c \
  "SELECT t.id, ae.event_id IS NOT NULL as has_audit
   FROM transfers t
   LEFT JOIN audit_events ae ON ae.payload->>'transfer_id' = t.id::text
   ORDER BY t.created_at;"
# The transfer from step 5 will show has_audit = false.
# This is the problem Week 7's outbox pattern solves.
```
