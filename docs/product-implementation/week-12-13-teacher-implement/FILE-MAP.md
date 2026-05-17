# Week 12–13 File Map

Quick reference for every file created or modified, who owns it, and its status.

## Legend

- **FULL** — complete code, no TODOs (teacher-provided)
- **SKELETON** — function signatures + docstrings + `# TODO: student` markers

---

## New Files

| File | Owner | Status | Purpose |
|------|-------|--------|---------|
| `app/circuit_breaker.py` | teacher | FULL | Three-state circuit breaker class |
| `app/routers/health.py` | teacher | FULL | GET /v1/health endpoint |
| `app/routers/scheduled_payments.py` | teacher | FULL | CRUD router for scheduled payments |
| `app/models/scheduled_payment.py` | teacher | FULL | ScheduledPayment ORM model |
| `app/models/scheduled_payment_execution.py` | teacher | FULL | Execution log ORM model |
| `app/schemas/scheduled_payment.py` | teacher | FULL | Request/response Pydantic schemas |
| `alembic/versions/0009_add_scheduled_payments.py` | teacher | FULL | Migration for scheduled_payments + executions |
| `app/services/scheduled_payment_service.py` | student | SKELETON | CRUD service (create, list, cancel) |
| `workers/saga_recovery.py` | student | SKELETON | Two-phase claim + resolve stuck withdrawals |
| `workers/payment_scheduler.py` | student | SKELETON | Poll + execute due scheduled payments |
| `tests/test_circuit_breaker.py` | student | SKELETON | Circuit breaker state machine tests |
| `tests/test_saga_recovery.py` | student | SKELETON | Recovery scenario tests |
| `tests/test_scheduled_payments.py` | student | SKELETON | CRUD + scheduler tests |
| `tests/test_withdrawal_circuit_breaker.py` | student | SKELETON | Pre-flight integration tests |
| `tests/test_health.py` | student | SKELETON | Health endpoint tests |

## Modified Files

| File | Owner | Change |
|------|-------|--------|
| `app/main.py` | teacher | Rewrote lifespan: adds circuit_breaker, recovery loop, scheduler loop |
| `app/dependencies.py` | teacher | Added `get_circuit_breaker()` Depends bridge + imported CircuitBreaker |
| `app/routers/withdrawals.py` | teacher | Added circuit breaker pre-flight + passes CB to service |
| `app/services/withdrawal_service.py` | teacher | Added `circuit_breaker` param, routes rail call through CB |
| `app/events/schemas.py` | teacher | Added PaymentExecutedPayload, PaymentSkippedPayload + dispatch entries |
| `app/exceptions.py` | teacher | Added ScheduledPaymentNotFoundError, InvalidStartTimeError, CannotPaySelfError |
| `pyproject.toml` | teacher | Added `python-dateutil>=2.9.0` dependency |
| `tests/conftest.py` | teacher | Imported new models, updated TRUNCATE statements |

## Unchanged Files (for reference)

These existing files are relevant context but not modified:

| File | Relevance |
|------|-----------|
| `app/services/transfer_service.py` | Reused by scheduler — `transfer()` is the execution engine |
| `app/models/withdrawal.py` | Recovery worker queries and updates these rows |
| `app/models/ledger_entry.py` | Recovery writes reversal entries here |
| `rail/simulator.py` | Recovery calls `send_withdrawal()` and `query_status()` |
| `app/events/publisher.py` | Recovery and scheduler both call `publish_event()` |
| `app/config.py` | SYSTEM_ACCOUNT_ID used in compensation ledger entries |
| `workers/outbox_relay.py` | Delivers outbox rows created by recovery/scheduler — no changes needed |
