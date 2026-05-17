# Week 10–11 Scaffold — What the Teacher Built

Everything listed here is **complete, working code** — no TODOs. It is the foundation your Phase 3 implementation builds on.

Weeks 10–11 cover user stories **US-3.1 (deposit)** and **US-3.2 (withdrawal saga)**. US-3.3 (saga recovery), US-3.4 (circuit breaker), and US-3.5 (scheduled payments) are **out of scope** for these two weeks — the recovery/circuit-breaker scaffolding is deliberately *not* wired up here. You will implement those in Week 12.

---

## 1. Database Migrations

### `alembic/versions/0007_add_deposits.py` — Deposits table

| Column | Type | Constraint | Purpose |
|--------|------|------------|---------|
| `id` | UUID | PK | Deposit identity |
| `account_id` | UUID | FK → accounts | Which user gets credited |
| `amount` | NUMERIC(19,4) | CHECK > 0 | Money in |
| `currency` | VARCHAR(3) | NOT NULL, default 'USD' | ISO 4217 |
| `status` | VARCHAR(20) | NOT NULL, default 'pending' | `pending → completed | rejected` |
| `source_type` | VARCHAR(30) | NOT NULL | `bank_transfer | card_topup | direct_debit` |
| `external_reference` | VARCHAR(255) | **UNIQUE NOT NULL** | Rail's txn ID — natural idempotency key |
| `rejection_reason` | VARCHAR(100) | nullable | Exhaustive set, see `deposit_service.py` |
| `created_at` / `updated_at` | TIMESTAMPTZ | NOT NULL | Audit |
| `completed_at` | TIMESTAMPTZ | nullable | Set ONLY on transition to completed |

Partial index `idx_deposits_account_id` supports per-user deposit listing later.

### `alembic/versions/0008_add_withdrawals.py` — Withdrawals table

| Column | Type | Constraint | Purpose |
|--------|------|------------|---------|
| `id` | UUID | PK | Withdrawal identity |
| `account_id` | UUID | FK → accounts | Sender |
| `amount` | NUMERIC(19,4) | CHECK > 0 | Money out |
| `currency` | VARCHAR(3) | NOT NULL, default 'USD' | ISO 4217 |
| `status` | VARCHAR(20) | NOT NULL, default 'pending' | `pending → submitted → completed | failed` |
| `failure_code` | VARCHAR(50) | nullable | `INVALID_ACCOUNT | BENEFICIARY_CLOSED | TIMEOUT | NETWORK_ERROR | CIRCUIT_OPEN` |
| `destination_type` | VARCHAR(30) | NOT NULL | `bank_transfer | card_withdrawal` |
| `destination_details` | JSONB | NOT NULL | Per-rail fields (sort_code, iban, …) — we do NOT schema-validate |
| `external_reference` | VARCHAR(255) | nullable | Rail's txn ID, NULL until rail accepts |
| `idempotency_key` | VARCHAR(255) | **UNIQUE NOT NULL** | Client-supplied header value |
| `created_at` / `submitted_at` / `completed_at` / `updated_at` | TIMESTAMPTZ | | Status timestamps |

Two indexes shipped with the table:

- `idx_withdrawals_recovery` — partial index on `updated_at WHERE status IN ('pending', 'submitted')`. **Used by the Week 12 saga recovery job.** Created now so Week 12 needs no schema change.
- `idx_withdrawals_account_id` — for future `GET /v1/withdrawals` list.

### `alembic/env.py` — Model imports

Added `deposit` and `withdrawal` to the import line so Alembic's autogenerate sees the new models and `Base.metadata.create_all` picks them up in tests.

---

## 2. Data Layer

### `app/models/deposit.py` — Deposit ORM

Plain SQLAlchemy model — no listeners, no hooks. The deposit flow is a single atomic transaction; there is no "stuck" state to reconcile.

### `app/models/withdrawal.py` — Withdrawal ORM + `before_flush` listener

Two critical details beyond the column list:

1. **`destination_details` is `JSONB`**, not a typed column set. Different rails need different fields (UK Faster Payments: sort_code + account_number; SEPA: IBAN; US ACH: routing + account). We are not the authority on validity — the rail is.

2. **Session-level `before_flush` listener auto-sets `updated_at`** on every modified `Withdrawal` instance. This is defensive: the Week 12 saga-recovery sweeper uses `WHERE updated_at < cutoff` to find stuck rows. If a new code path mutates a `Withdrawal` but forgets to refresh `updated_at`, the row becomes invisible to recovery. The listener makes correctness robust to human error.

### `app/events/schemas.py` — Five new payload models + dispatch entries

```python
DepositCompletedPayload
DepositRejectedPayload
WithdrawalInitiatedPayload
WithdrawalCompletedPayload
WithdrawalFailedPayload
```

All registered in `PAYLOAD_MODELS` — the activity consumer and any future consumer can parse them via `parse_event()` without special casing.

### `app/schemas/deposit.py` — `SimulateDepositRequest` + `DepositResponse`

Request-side validation:

- `amount > 0` (Pydantic field_validator).
- `source_type ∈ {bank_transfer, card_topup, direct_debit}` (explicit allowlist — a real webhook with an unknown type is a compliance red flag, not something to default to "other").

### `app/schemas/withdrawal.py` — `WithdrawalRequest` + `WithdrawalResponse`

Same shape: `amount > 0` + `destination_type` allowlist. `destination_details` is an opaque `dict` by design.

### `app/exceptions.py` — Three new domain errors

- `DepositNotFoundError` (404) — ownership-enforced lookup miss.
- `WithdrawalNotFoundError` (404) — ownership-enforced lookup miss.
- `BankRailUnavailableError` (503) — reserved for the Week 12 circuit breaker pre-flight. Defined now so the response contract is stable.

---

## 3. Bank Rail Simulator (`rail/simulator.py`)

This is treated like a **vendor SDK** — you (student) do not modify it, you call it.

```
BankRailSimulator
    .send_withdrawal(withdrawal_id, amount, destination) -> RailResult | raises RailError
    .query_status(external_reference)                     -> RailStatus   (used in Week 12)
    .force_outcome(withdrawal_id, outcome)                # test hook
```

Two knobs for teaching:

1. **`RAIL_FAILURE_RATE` env var** (float 0.0–1.0) — probabilistic random failures. Default `0.0` for deterministic tests.
2. **`force_outcome(id, "success" | "fail:CODE")`** — pop-once queue keyed by withdrawal_id. Use this in tests to deterministically drive the compensation path.

`RailError.code` values match the `withdrawals.failure_code` enum: `INVALID_ACCOUNT | BENEFICIARY_CLOSED | TIMEOUT | NETWORK_ERROR`. Keep these in sync if you extend.

---

## 4. Router Wiring

### `app/main.py` — Lifespan + router registration

```python
# lifespan now instantiates the rail simulator as a process-wide singleton
app.state.rail = BankRailSimulator()

# New routers registered
app.include_router(deposits.router,    prefix="/v1/deposits",    tags=["deposits"])
app.include_router(withdrawals.router, prefix="/v1/withdrawals", tags=["withdrawals"])
```

Note: the circuit breaker, saga-recovery background loop, and scheduler loop are **not** added to the lifespan yet. Week 12 extends `lifespan()` with `recover_stuck_withdrawals`, `scheduler_loop`, and the circuit breaker singleton.

### `app/dependencies.py` — `get_rail` Depends()

```python
def get_rail(request: Request) -> BankRailSimulator:
    return request.app.state.rail
```

Bridges `app.state` to FastAPI's `Depends()` system. Tests override via `app.dependency_overrides[get_rail]` to inject a pre-configured simulator.

### `app/routers/dev.py` — `POST /v1/dev/simulate-deposit` added

Guarded by `APP_ENV == "development"` (same pattern as `/v1/dev/seed`). **No JWT** — in production, the equivalent endpoint would authenticate via an HMAC signature from the bank partner, not a user JWT. Our simulator is dev-only, so dropping the auth entirely matches the shape of a webhook handler.

### `app/routers/deposits.py` — `GET /v1/deposits/{id}` (SKELETON)

JWT-protected. Ownership enforced: 404 on either "doesn't exist" or "not yours".

### `app/routers/withdrawals.py` — `POST` + `GET /{id}` (SKELETON)

`POST` requires `Idempotency-Key` header (same pattern as `/v1/transfers`). `GET` is ownership-enforced.

---

## 5. Kafka Topics

### `kafka/create-topics.sh`

Added two topics, partitions=1 replication-factor=1 (same as existing):

- `deposit.events` — carries `deposit.completed` + `deposit.rejected`
- `withdrawal.events` — carries `withdrawal.initiated | completed | failed`

### `workers/activity_consumer.py`

- Subscribes to the two new topics alongside `transfer.events` and `account.events`.
- Imports the four new payload types and has skeleton dispatch branches (labeled `TODO:student`) for each.
- `WithdrawalCompletedPayload` branch is deliberately a **no-op** — the debit row is already created on `withdrawal.initiated`, and adding another row on completion would double-count. The branch exists explicitly so a future developer doesn't re-add the row by mistake.

---

## 6. Test Infrastructure (`tests/conftest.py` updates)

- Import `Deposit` and `Withdrawal` models so `Base.metadata.create_all` builds their tables.
- Extend the TRUNCATE lists in both `db_session` and `consumer_db_factory` to include `deposits` and `withdrawals`.

No new fixtures needed — existing `alice_account`, `alice_headers`, `seeded_alice_account`, `bob_*`, and `client` cover all Phase 3 API tests.

---

## 7. What's Deliberately NOT Scaffolded

These belong to later weeks — do not let the PRD tempt you into building them now:

| Out-of-scope component | Reason | Where it lands |
|---|---|---|
| `app/circuit_breaker.py` | Useful only once withdrawals exist (Week 11 ✓) AND recovery needs it (Week 12). | Week 12 |
| `workers/saga_recovery.py` | Operates on `withdrawals` rows. The recovery index (`idx_withdrawals_recovery`) and `before_flush` updated_at listener are already in place. | Week 12 |
| `app/routers/health.py` | Exposes circuit breaker state — needs the breaker first. | Week 12 |
| `app/dependencies.py::get_circuit_breaker` | Same. | Week 12 |
| Scheduled payments (models, service, router, worker) | No new banking primitive — reuses `transfer()`. | Week 13 |
| `BankRailUnavailableError` in the happy-path endpoint | Raised by the Week 12 pre-flight check. Class is defined now so the response contract is stable. | Week 12 |

The Week 11 saga calls `rail.send_withdrawal` **directly** — no circuit breaker wrapper. The compensation path still works without one; the circuit breaker is a UX/operational optimization, not a correctness requirement.

---

## 8. Summary of Files

See `FILE-MAP.md` for the full per-file ownership and status table.
