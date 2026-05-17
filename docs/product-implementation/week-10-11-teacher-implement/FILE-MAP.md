# Week 10–11 File Map

Quick reference for every file created or modified, who owns it, and its status.

## Legend

- **FULL** — complete code, no TODOs (teacher-provided)
- **SKELETON** — function signatures + docstrings + `# TODO: student` markers

---

## New Files

| File | Owner | Status | Purpose |
|------|-------|--------|---------|
| `alembic/versions/0007_add_deposits.py` | teacher | FULL | Migration for `deposits` table |
| `alembic/versions/0008_add_withdrawals.py` | teacher | FULL | Migration for `withdrawals` table + recovery index |
| `app/models/deposit.py` | teacher | FULL | Deposit ORM model |
| `app/models/withdrawal.py` | teacher | FULL | Withdrawal ORM model + `before_flush` `updated_at` listener |
| `app/schemas/deposit.py` | teacher | FULL | `SimulateDepositRequest`, `DepositResponse` |
| `app/schemas/withdrawal.py` | teacher | FULL | `WithdrawalRequest`, `WithdrawalResponse` |
| `rail/__init__.py` | teacher | FULL | Package marker (empty) |
| `rail/simulator.py` | teacher | FULL | `BankRailSimulator`, `RailError`, `RailResult`, `RailStatus` |
| `app/services/deposit_service.py` | student | SKELETON | `simulate_deposit`, `get_deposit`, `_deposit_to_response` |
| `app/services/withdrawal_service.py` | student | SKELETON | `create_withdrawal`, `_complete`, `_compensate`, `get_withdrawal`, `_withdrawal_to_response` |
| `app/routers/deposits.py` | student | SKELETON | `GET /v1/deposits/{id}` |
| `app/routers/withdrawals.py` | student | SKELETON | `POST /v1/withdrawals`, `GET /v1/withdrawals/{id}` |
| `tests/test_deposits.py` | student | SKELETON | 7 test functions to implement |
| `tests/test_withdrawals.py` | student | SKELETON | 7 test functions to implement |

---

## Modified Files

| File | Owner | Change |
|------|-------|--------|
| `alembic/env.py` | teacher | Added `deposit`, `withdrawal` to model imports |
| `app/events/schemas.py` | teacher | Added 5 payloads (`DepositCompleted/Rejected`, `WithdrawalInitiated/Completed/Failed`) + `PAYLOAD_MODELS` entries |
| `app/exceptions.py` | teacher | Added `DepositNotFoundError`, `WithdrawalNotFoundError`, `BankRailUnavailableError` |
| `app/dependencies.py` | teacher | Added `get_rail()` Depends bridge |
| `app/main.py` | teacher | Instantiates `BankRailSimulator` on `app.state.rail` in lifespan; registers deposit + withdrawal routers |
| `app/routers/dev.py` | student | SKELETON: `POST /v1/dev/simulate-deposit` added (dev-only, no JWT) |
| `kafka/create-topics.sh` | teacher | Creates `deposit.events` + `withdrawal.events` topics |
| `workers/activity_consumer.py` | student | SKELETON: added dispatch branches for Deposit/Withdrawal payloads (3 new + 1 intentional no-op) |
| `tests/conftest.py` | teacher | Imports `Deposit`, `Withdrawal` models; TRUNCATEs new tables |

---

## Unchanged Files (for reference)

These files are relevant context but not modified in Weeks 10–11:

| File | Relevance |
|------|-----------|
| `app/config.py` | `SYSTEM_ACCOUNT_ID` is the counter-party for deposit credits and withdrawal debits |
| `app/events/publisher.py` | `publish_event()` writes Phase 3 events to the outbox (no changes needed) |
| `app/models/ledger_entry.py` | `reference_id` (added in migration 0006) links ledger rows to deposit/withdrawal source rows |
| `app/services/account_service.py` | `get_balance()` is called by the withdrawal service; `seed()` is the reference pattern for IntegrityError-based idempotency |
| `app/services/transfer_service.py` | Reference pattern for `SELECT account FOR UPDATE` + two-leg ledger write + Redis/DB idempotency combo |
| `workers/outbox_relay.py` | Delivers Phase 3 events to Kafka unchanged — it reads `topic` from each outbox row |
| `docker-compose.yml` | No changes — `api`, `outbox-relay`, and `activity-consumer` already run |

---

## Out-of-Scope Files (NOT created in Weeks 10–11)

These belong to later weeks — the scaffolding leaves room for them without forcing them to exist now:

| File | Reason to defer | Lands in |
|------|-----------------|----------|
| `app/circuit_breaker.py` | Useful only once recovery exists (Week 12) | Week 12 |
| `app/routers/health.py` | Exposes breaker state | Week 12 |
| `workers/saga_recovery.py` | Operates on withdrawal rows; recovery index already exists | Week 12 |
| `app/models/scheduled_payment*.py` | No new banking primitive | Week 13 |
| `app/services/scheduled_payment_service.py` | Reuses `transfer()` | Week 13 |
| `app/routers/scheduled_payments.py` | Orchestration only | Week 13 |
| `workers/payment_scheduler.py` | Orchestration only | Week 13 |
| `alembic/versions/0009_add_scheduled_payments.py` | Week 13 migration | Week 13 |
