# Week 6 File Map

Quick reference for every file created or modified, who owns it, and its status.

## Legend

- **FULL** — complete code, no TODOs (teacher-provided)
- **SKELETON** — function signatures + docstrings + `# TODO: student` markers

---

## New Files

| File | Owner | Status | Purpose |
|------|-------|--------|---------|
| `Dockerfile` | teacher | FULL | Shared image for api + all workers |
| `.env` | teacher | FULL | Docker-internal env vars (not committed) |
| `kafka/create-topics.sh` | teacher | FULL | Creates `transfer.events` + `account.events` |
| `app/models/audit_event.py` | teacher | FULL | AuditEvent ORM model |
| `alembic/versions/0003_add_audit_events.py` | teacher | FULL | Migration for audit_events table |
| `workers/__init__.py` | teacher | FULL | Package marker (empty) |
| `workers/audit_consumer.py` | student | SKELETON | `process()` and `run()` to implement |
| `tests/test_audit_consumer.py` | student | SKELETON | 6 test functions to implement |

## Modified Files

| File | Owner | Change |
|------|-------|--------|
| `app/config.py` | teacher | Added `kafka_bootstrap_servers` field |
| `app/database.py` | teacher | Added `db_factory = AsyncSessionLocal` alias |
| `app/main.py` | student | SKELETON: `lifespan()` needs `start_producer`/`stop_producer` calls |
| `app/services/transfer_service.py` | student | SKELETON: `start_producer()`, `stop_producer()`, event publish after each commit |
| `app/routers/transfers.py` | teacher | Added `actor_user_id=current_user.id` kwarg |
| `alembic/env.py` | teacher | Added `audit_event` to model imports |
| `docker-compose.yml` | teacher | Added zookeeper, kafka, kafka-init, api, audit-consumer |
| `tests/conftest.py` | teacher | Added Phase 2 fixtures + AuditEvent import + TRUNCATE update |

## Unchanged Files (for reference)

These Phase 1 files are relevant context but not modified:

| File | Relevance |
|------|-----------|
| `app/models/transfer.py` | Transfer model — events reference `transfer_record.id` |
| `app/schemas/transfer.py` | TransferResponse — unchanged, returned by the API |
| `app/exceptions.py` | InsufficientBalanceError — raised after publishing `transfer.failed` |
| `app/dependencies.py` | `get_current_user` — provides `current_user.id` for `actor_user_id` |
