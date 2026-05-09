# Week 8 File Map

Quick reference for every file created or modified, who owns it, and its status.

## Legend

- **FULL** — complete code, no TODOs (teacher-provided)
- **SKELETON** — function signatures + docstrings + `TODO:student` markers

---

## New Files

| File | Owner | Status | Purpose |
|------|-------|--------|---------|
| `app/models/transaction_activity.py` | teacher | FULL | TransactionActivity ORM model (CQRS read model) |
| `alembic/versions/0005_add_transaction_activity.py` | teacher | FULL | Migration for transaction_activity table |
| `consumers/__init__.py` | teacher | FULL | Package marker (empty) |
| `consumers/consumer_base.py` | student | SKELETON | BaseConsumer: `handle_message()` to implement |
| `workers/activity_consumer.py` | student | SKELETON | ActivityConsumer: `process()` to implement |
| `workers/notification_consumer.py` | student | SKELETON | NotificationConsumer: `process()` to implement |
| `management/backfill_events.py` | student | SKELETON | `backfill()` function to implement |
| `tests/test_activity_consumer.py` | student | SKELETON | 5 test assertions to implement |
| `tests/test_notification_consumer.py` | student | SKELETON | 4 test assertions to implement |
| `tests/test_transactions_cqrs.py` | student | SKELETON | 6 test assertions to implement |
| `tests/test_backfill.py` | student | SKELETON | 4 test assertions to implement |
| `tests/test_dlq_routing.py` | student | SKELETON | 3 test assertions to implement |

## Modified Files

| File | Owner | Change |
|------|-------|--------|
| `app/schemas/common.py` | teacher | Added `as_of: datetime | None = None` to PaginationMeta |
| `app/services/account_service.py` | student | SKELETON: `get_transactions()` migrated to CQRS read model |
| `app/routers/accounts.py` | teacher | Updated to unpack 3-tuple and include `as_of` in response |
| `alembic/env.py` | teacher | Added `transaction_activity` to model imports |
| `docker-compose.yml` | teacher | Added `activity-consumer` and `notification-consumer` services |
| `kafka/create-topics.sh` | teacher | Added 3 DLQ topics with infinite retention |
| `Dockerfile` | teacher | Added `COPY consumers/ consumers/` |
| `tests/conftest.py` | teacher | Added TransactionActivity import + TRUNCATE + transfer_factory + seed_factory |
| `workers/audit_consumer.py` | teacher | Upgraded from Week 6 minimal to BaseConsumer subclass (process() preserved) |

## Unchanged Files (for reference)

These files are relevant context but not modified in Week 8:

| File | Relevance |
|------|-----------|
| `app/events/schemas.py` | Payload models used by all consumers and backfill |
| `app/events/publisher.py` | `publish_event()` used by backfill |
| `app/models/outbox.py` | OutboxRow model — backfill writes to this |
| `app/models/audit_event.py` | AuditEvent model — audit consumer writes here |
| `workers/outbox_relay.py` | Relay delivers backfill outbox rows to Kafka |
| `app/models/transfer.py` | Transfer model — backfill reads from this |
| `app/models/ledger_entry.py` | LedgerEntry model — backfill reads seed entries |
