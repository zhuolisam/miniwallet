# Week 7 File Map

Quick reference for every file created or modified, who owns it, and its status.

## Legend

- **FULL** — complete code, no TODOs (teacher-provided)
- **SKELETON** — function signatures + docstrings + `# TODO: student` markers

---

## New Files

| File | Owner | Status | Purpose |
|------|-------|--------|---------|
| `app/models/outbox.py` | teacher | FULL | OutboxRow ORM model with partial indexes |
| `app/events/publisher.py` | teacher | FULL | `publish_event()` — only way to write to outbox |
| `alembic/versions/0004_add_outbox.py` | teacher | FULL | Migration: outbox table + partial indexes |
| `workers/outbox_relay.py` | student | SKELETON | 5 functions to implement: claim, confirm, recover, cleanup, relay_loop |
| `management/__init__.py` | teacher | FULL | Package marker (empty, prepares for Week 8 backfill) |
| `tests/test_outbox_relay.py` | student | SKELETON | 10 test functions for relay logic |
| `tests/test_outbox_integration.py` | student | SKELETON | 5 test functions for outbox writes via API |

## Modified Files

| File                               | Owner   | Change                                                                        |
| ---------------------------------- | ------- | ----------------------------------------------------------------------------- |
| `app/services/transfer_service.py` | student | SKELETON: removed Kafka producer, added TODO markers for `publish_event()`    |
| `app/services/account_service.py`  | student | SKELETON: added TODO markers for `account.opened` and `seed.completed` events |
| `app/main.py`                      | student | SKELETON: removed producer lifecycle from lifespan, added TODO comment        |
| `alembic/env.py`                   | teacher | Added `outbox` to model imports                                               |
| `docker-compose.yml`               | teacher | Added `outbox-relay` service, removed `kafka-init` from `api` depends         |
| `Dockerfile`                       | teacher | Added `COPY management/ management/`                                          |
| `tests/conftest.py`                | teacher | Added `outbox` to TRUNCATE statements, imported `OutboxRow` model             |

## Unchanged Files (for reference)

These files are relevant context but not modified in Week 7:

| File | Relevance |
|------|-----------|
| `app/events/schemas.py` | Event envelope + payload models — `publish_event()` uses these |
| `app/models/audit_event.py` | AuditEvent model — consumers insert here (unchanged) |
| `workers/audit_consumer.py` | Week 6 consumer — still works, will be refactored in Week 9 |
| `kafka/create-topics.sh` | Topic creation — unchanged (DLQ topics added in Week 9) |
| `app/routers/transfers.py` | Transfer router — unchanged (already passes `actor_user_id`) |
