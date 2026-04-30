# MiniBank — Digital Banking Backend Study Plan

## Context

This is a study plan for a **digital banking system** inspired by neobanks like Ryt Bank (SG) and GX Bank (MY). The goal is to learn backend engineering patterns used in fintech — event-driven architecture, system design patterns, observability, and API design — by building a realistic mini banking backend over ~19 weeks at 3–5 hrs/week.

## What You'll Build

**MiniBank** — a simplified digital banking backend:
- Register and authenticate
- Open a single account per user
- P2P transfers between users (like PayNow/DuitNow)
- Deposit/withdraw (simulated bank rails)
- Scheduled recurring payments
- Transaction history with filtering
- Event-driven notifications and audit trail

**Not in scope:** frontend, real bank rails, KYC provider, card issuing, lending, investments.

## Tech Stack

| Component | Choice | Why |
|-----------|--------|-----|
| Language | Python 3.12+ (FastAPI) | Leverage existing skills; focus on domain |
| Database | PostgreSQL | ACID transactions for financial data |
| Event bus | Apache Kafka (via `confluent-kafka`) | Core learning goal — event-driven architecture |
| Cache | Redis | Idempotency cache (Phase 1), rate limiting (Phase 6) |
| API docs | OpenAPI 3.1 (contract-first) | Learning goal — schema-first API design |
| Observability | OpenTelemetry + Prometheus + Grafana | Structured logging, tracing, metrics |
| Containers | Docker Compose | Run all infra locally |
| Testing | pytest + testcontainers | Integration tests with real DB/Kafka |
| Internal comms | gRPC (Phase 5) | Service-to-service communication |

---

## Phase 1: Foundation — Accounts & Double-Entry Ledger (Week 1–5)

**~3–4 hrs/week × 5 weeks**

### Week 1 — Project setup & API contract
- **Contract-first API design**: Write OpenAPI 3.1 spec for all Phase 1 endpoints before any code
- Set up project: `uv init`, FastAPI, SQLAlchemy 2.0 (async), Alembic, Docker Compose (Postgres + Redis)
- Design PostgreSQL schema:
  - `users` (id, email, hashed_password, created_at)
  - `accounts` (id, user_id, status, created_at) — one per user, `UNIQUE(user_id)`; seed one **system account** at deploy time
  - `ledger_entries` (id, debit_account_id, credit_account_id, amount, entry_type, reference_id, idempotency_key, created_at)
  - `transfers` (id, from_account_id, to_account_id, amount, status, idempotency_key, created_at)
- **Why a system account?** Double-entry requires every credit to have a matching debit. When seeding funds, the system account is debited and the user account is credited — the ledger always sums to zero.
- **Alembic migration strategy**: establish a convention now — one migration file per schema change, named sequentially (`0001_initial_schema.py`, `0002_add_outbox.py`, etc.). Each phase will add migrations. Never edit a committed migration; always add a new one.
- **API design patterns**: Resource naming, consistent error responses (`{data}` / `{error: {code, message}}`), pagination schema

### Week 2 — Auth & balance primitives
- `POST /v1/auth/register` + `POST /v1/auth/login` — JWT access token (15 min) + opaque refresh token (7 days, stored in Redis)
- `POST /v1/auth/refresh` — token rotation; old refresh token invalidated immediately
- `GET /v1/users/me` — current user profile
- `get_balance(account_id)` — derived from `SUM` of ledger entries, `Decimal` everywhere (never floats)
- `POST /v1/dev/seed` — debit system account, credit user account; `entry_type = seed`

### Week 3 — Accounts API
- `POST /v1/accounts` — open the user's single account
- `GET /v1/accounts/me` — view account + balance
- `GET /v1/accounts/me/balance` — balance only
- Verify end-to-end: register → open account → seed → balance correct

### Week 4 — P2P transfer & idempotency
- `POST /v1/transfers` — transfer funds to another user by email or account ID:
  - **What `SELECT ... FOR UPDATE` locks**: balance is derived from `SUM(ledger_entries)`, not stored in `accounts`. You lock the **`accounts` row** to serialize access. The sequence is: `BEGIN` → `SELECT id FROM accounts WHERE id = $sender FOR UPDATE` (acquire row lock) → `SELECT SUM(...) FROM ledger_entries` (safe balance check, no interleaving possible) → `INSERT INTO ledger_entries` → `INSERT INTO transfers` → `COMMIT`
  - Reject if balance < amount (`INSUFFICIENT_BALANCE`)
  - `Idempotency-Key` header required — check Redis first (fast path, return cached response), DB unique constraint as safety net; cache successful `2xx` response for 24h; do **not** cache `4xx` errors (client may retry with a fix)
- `GET /v1/transfers/{id}` — transfer status

### Week 5 — Hardening & correctness proof
- `GET /v1/accounts/me/transactions` — paginated transaction list (offset pagination for now)
- Request validation with Pydantic v2 strict mode
- **Concurrency test**: 10 parallel transfers from the same account — assert no overdraft and no money created or destroyed. This is the proof that your `SELECT ... FOR UPDATE` is correct.
- Integration test: register two users → seed → transfer → verify both balances + ledger sums to zero

### Skills practiced
- Double-entry accounting — system account, ledger always sums to zero
- DB transactions, row-level locking (`SELECT ... FOR UPDATE` on the `accounts` row)
- Idempotency (Redis fast path + DB unique constraint safety net)
- Contract-first OpenAPI design, REST best practices
- Decimal precision for money

### Done when
- `transfer()` is atomic: concurrent test proves no overdraft and no money created/destroyed
- Ledger sums to zero across all accounts (including system account)
- OpenAPI spec matches implementation
- Idempotency: sending the same transfer twice only debits once

---

## Phase 2: Event-Driven Architecture (Week 6–9)

**~3–4 hrs/week × 4 weeks**

> **Teaching order**: In Week 6 you publish events directly to Kafka — no outbox. In Week 7 you experience why that's fragile and build the outbox to fix it. This sequence is intentional: you understand the problem before the solution, and the outbox pattern sticks.

### Week 6 — Kafka setup & direct publish (fragile, intentional)
- Add Kafka + Zookeeper to Docker Compose
- Define event schemas (common JSON envelope: `event_id`, `event_type`, `occurred_at`, `version`, `payload`):
  - `account.opened`, `transfer.completed`, `transfer.failed`
- **Direct publish**: after a successful transfer, publish `transfer.completed` to Kafka inline in the transfer function
- Build the audit log consumer against this — it works, it's fast, it feels clean
- **Then break it**: kill Kafka, make a transfer, restart Kafka → the event is gone. The audit log has a gap.
- This is the exact failure mode the outbox exists to prevent. You've now experienced it firsthand.

### Week 7 — Outbox pattern (fix the fragility)
- **Outbox table** (new Alembic migration): `outbox` (id, topic, event_type, payload, status, retry_count, created_at, published_at)
- Replace direct publish: in the same DB transaction as the transfer write, INSERT an outbox row (`BEGIN` → INSERT ledger entries + INSERT outbox row → `COMMIT`)
- **Outbox relay process**: `SELECT id, payload FROM outbox WHERE status = 'pending' ORDER BY created_at FOR UPDATE SKIP LOCKED` → publish to Kafka → mark `status = published`. `FOR UPDATE SKIP LOCKED` ensures that if you ever run two relay processes, they claim different rows — no duplicate publishes.
- Repeat the kill-Kafka test: transfer succeeds, Kafka goes down, comes back up → relay delivers the event. The audit log gap is gone.

### Week 8 — Event consumers
- **Consumer: Audit log** — append-only `audit_events` table, populated purely from Kafka events; never written directly from API
  - **Idempotent consumer**: DB unique constraint on `event_id` — replaying the same event is a no-op
- **Consumer: Notification service** — log notifications to stdout (simulate email/push); consumer group pattern
- **Consumer: Activity view builder** (CQRS read model)
  - `transaction_activity` table built from transfer events
  - **Write side**: ledger entries (source of truth) — **Read side**: `transaction_activity` (fast reads, eventual consistency)
  - `GET /v1/accounts/me/activity` — reads from read model; document the lag clearly in the API response (`as_of` timestamp)
  - **Consumer offset management**: understand the difference between starting from `latest` (only new events) vs `earliest` (replay all events to rebuild the read model from scratch)

### Week 9 — Reliability
- **Dead letter topic (DLT)**: failed events go to `*.dlq` after N retries — never silently dropped
- Test: consumer throws on a malformed event → verify it lands in DLT, not lost
- Consumer lag is observable (logged + Prometheus metric in Phase 4)

### Skills practiced
- Kafka producer/consumer, consumer groups, offsets
- Direct publish → outbox: experience the failure first, then fix it
- Outbox pattern (`FOR UPDATE SKIP LOCKED` for safe concurrent relay)
- CQRS (read/write separation), eventual consistency, consumer offset management
- Idempotent consumers, dead letter topics

### Done when
- Kill-Kafka test passes: no event lost after Kafka recovers
- Audit log populated purely from events — API never touches it directly
- `GET /v1/accounts/me/activity` reads from CQRS read model with correct `as_of` timestamp
- You can whiteboard the full event flow (transfer → outbox → relay → Kafka → consumers) in 5 minutes

---

## Phase 3: Advanced Payments (Week 10–13)

**~3–4 hrs/week × 4 weeks**

> Phase 1 handles P2P transfers as a synchronous single-DB transaction. Phase 3 introduces patterns needed when money crosses a system boundary (external bank rails) and when payments run automatically on a schedule.

### Week 10 — Deposits (inbound bank rail)
- **Architecture reality**: in a real neobank, deposits are **push events** — the bank rail sends a webhook to you; the user does not call your API to deposit. The user transfers money to MiniBank's virtual account via their own bank, and the rail sends: `POST /webhooks/bank-rail { event: "credit_received", amount: 100, ref: "..." }`.
- **Simulation**: use `POST /v1/dev/simulate-deposit` (dev-only) to fake an incoming webhook. This keeps the architecture honest — deposits are initiated externally, not by users.
- `GET /v1/deposits/{id}` — deposit status
- **State machine**: `pending → processing → completed / failed`
- **Deposit idempotency**: the rail may send the same webhook twice. Use the rail's `external_ref` as an idempotency key — a DB unique constraint prevents double-credit. This is a correctness concern, not an operational one; it belongs here, not in Phase 6.
- Events via outbox: `deposit.received`, `deposit.completed`

### Week 11 — Withdrawals + Saga (orchestration)
- `POST /v1/withdrawals` — initiate outgoing transfer to external bank
- `GET /v1/withdrawals/{id}` — withdrawal status
- **Saga — orchestration style** (one function owns the full flow):
  1. `BEGIN` → debit user account → INSERT withdrawal row (`saga_status = debited`) → `COMMIT`
  2. Call bank rail simulator — may fail
  3. Success: UPDATE withdrawal (`saga_status = completed`) → INSERT outbox row → publish `withdrawal.completed`
  4. Failure: `BEGIN` → credit user back → UPDATE withdrawal (`saga_status = compensated`) → `COMMIT` → publish `withdrawal.compensated`
- **Saga recovery** — the hard part that's usually skipped: if the process crashes between step 1 (DB commit) and step 2 (rail call), the withdrawal is stuck at `saga_status = debited` with no rail call ever made. On startup (and on a schedule), run a recovery job: find withdrawals in `debited` status older than N minutes → resume (retry rail call) or compensate. Without this, a single crash leaks money indefinitely.
- **Why orchestration, not choreography?** The full saga state is in one row (`saga_status`). Auditors and on-call engineers can query a single table to understand exactly what happened to any withdrawal. Choreography (event chains across topics) is used for side effects — notifications, audit log — not money movement.

### Week 12 — Circuit breaker
- **Circuit breaker** protecting the bank rail call. Implement it as a small custom class (~50 lines) — using a library hides the mechanics; you want to understand the state machine:
  - `CLOSED` → normal operation, failures counted
  - `OPEN` → tripped, all calls fast-fail immediately with `BANK_RAIL_UNAVAILABLE` (no waiting for timeout)
  - `HALF_OPEN` → one probe call allowed after cooldown; success → `CLOSED`, failure → `OPEN`
- **State storage**: in-memory for this project (resets on restart — acceptable for single process). In production, state lives in Redis so all instances share it.
- **Trip condition**: N consecutive failures (e.g., 3) within a time window
- **Cooldown**: 30 seconds before attempting `HALF_OPEN`
- Circuit state exposed in `GET /v1/health` so you can observe it during tests

### Week 13 — Scheduled payments + testing
- `POST /v1/scheduled-payments` — recurring P2P payment (daily/weekly/monthly)
- Schema: `scheduled_payments` (id, from_account_id, to_account_id, amount, frequency, next_run_at, status)
- **Scheduler worker**: polls `SELECT ... FROM scheduled_payments WHERE next_run_at <= NOW() AND status = 'active' FOR UPDATE SKIP LOCKED` — the `FOR UPDATE SKIP LOCKED` prevents two scheduler instances from double-executing the same payment
- On execution: reuse `transfer()` from Phase 1 → publish `payment.executed` via outbox; advance `next_run_at`
- Insufficient balance: mark `skipped`, publish `payment.skipped`, advance `next_run_at` — no complex retry
- `GET /v1/scheduled-payments`, `DELETE /v1/scheduled-payments/{id}`
- **Testing week**:
  - Integration test: deposit → withdraw → verify saga compensation on rail failure
  - Chaos test: randomly fail withdrawal rail step mid-saga → verify recovery job cleans up stuck `debited` withdrawals
  - Test: circuit breaker trips after 3 failures, fast-fails, recovers after 30s
  - Test: two scheduler goroutines simultaneously → only one execution per payment

### Skills practiced
- Saga pattern — orchestration style, explicit state, recovery job for crash scenarios
- `FOR UPDATE SKIP LOCKED` — safe concurrent workers (relay, scheduler)
- State machines for payment lifecycle
- Circuit breaker — custom implementation, three-state machine
- Deposit idempotency via external reference deduplication
- Scheduled job execution

### Done when
- Withdrawal saga compensates correctly when rail fails — no money lost
- Recovery job cleans up withdrawals stuck at `debited` after simulated crash
- Scheduled payments execute exactly once per cycle under concurrent schedulers
- Circuit breaker fast-fails and auto-recovers

---

## Phase 4: Observability & Reconciliation (Week 14–16)

**~3–4 hrs/week × 3 weeks**

### Week 14 — Structured logging & tracing
- Replace print/basic logging with **structlog** (structured JSON logs)
- Add **OpenTelemetry** instrumentation:
  - Auto-instrument FastAPI, SQLAlchemy, Kafka producer/consumer
  - Trace a full request: API → DB → outbox relay → Kafka → consumer
- Export traces to Jaeger (Docker Compose)
- Correlation IDs: every request gets a trace ID, propagated through outbox events into consumers — trace spans the full async chain

### Week 15 — Metrics & dashboards
- **Prometheus metrics** via `prometheus-fastapi-instrumentator`:
  - Request latency (p50, p95, p99), error rates, throughput
  - Custom business metrics: transfers/min, active accounts, Kafka consumer lag, DLT size, outbox backlog size
- **Grafana dashboards** (Docker Compose):
  - API health dashboard
  - Event pipeline dashboard (publish rate, consumer lag, DLT size, outbox backlog)
- **Health check**: `GET /v1/health` — DB, Kafka, Redis connectivity + circuit breaker state + last reconciliation result

### Week 16 — Reconciliation
- **Reconciliation job**:
  - The invariant: `SUM(all user account balances) + SUM(system account balance) = 0` — the ledger always nets to zero
  - Compare derived balance per account vs ledger entry sums; flag any account where they disagree
  - Output a report; alert on any mismatch
  - Publish `reconciliation.completed` or `reconciliation.alert` events via outbox
- **Alerting**: log alert when recon fails or error rate spikes (simulate PagerDuty-style)

### Skills practiced
- Structured logging, distributed tracing (OpenTelemetry)
- Prometheus metrics, Grafana dashboards
- Reconciliation — verifying the double-entry invariant at scale

### Done when
- Can trace a single transfer end-to-end in Jaeger: HTTP → DB → relay → Kafka → consumer (async span)
- Grafana shows real-time API and event pipeline metrics
- Reconciliation catches intentionally corrupted balances (manually corrupt a ledger row, run recon, see alert)

---

## Phase 5: gRPC & Service Extraction (Week 17)

**~3–4 hrs/week × 1 week**

- Extract the **notification service** as a separate gRPC service:
  - Define `.proto` file using `buf` CLI (modern standard for proto codegen)
  - Implement gRPC server (Python `grpcio`)
  - API calls notification service via gRPC for time-sensitive notifications; async Kafka consumer remains for low-priority bulk notifications
- **gRPC interceptors** (server-side, applied in order):
  1. Auth interceptor — verify `x-api-key` gRPC metadata header; reject unauthenticated calls
  2. Logging interceptor — log method, caller identity, duration (structlog)
  3. Tracing interceptor — extract/create OTel span from gRPC metadata; continue the trace from the API call
- **gRPC health check** using the standard gRPC Health Checking Protocol (not a custom proto)
- Compare REST vs gRPC: latency, schema enforcement, streaming capability — write down conclusions
- Final documentation:
  - README with architecture decisions and how to run
  - Mermaid system diagram
  - `docker compose up` starts everything with no manual steps

---

## Phase 6: API Hardening (Week 18–19)

**~3–4 hrs/week × 2 weeks**

> The correctness layer (idempotency, locking, concurrency, saga recovery) was built in Phases 1–3. This phase adds the operational layer: controls that protect the API from abuse and make it production-grade.

### Week 18 — Rate limiting & pagination
- **Rate limiting middleware** (Redis token bucket): 100 req/min per user, configurable per endpoint
- **Cursor-based pagination**: replace offset pagination in `GET /v1/accounts/me/transactions` and `GET /v1/accounts/me/activity`
  - Why cursor: offset pagination returns inconsistent results when rows are inserted between pages (page 2 may repeat or skip rows); a cursor anchored to `(created_at, id)` is stable
- **Request/response logging**: sanitize PII from logs (mask email, truncate account IDs) — separate concern from OTel tracing

### Week 19 — API gateway patterns
- **Request throttling**: per-user per-endpoint limits stricter than global rate limit (e.g., max 5 `POST /v1/transfers` per minute per user)
- **API key management**: service-to-service authentication for internal/admin endpoints
- **API versioning**: verify `/v1/` prefix pattern is enforced; document the upgrade path to `/v2/`

### Skills practiced
- Rate limiting (token bucket algorithm)
- Cursor-based pagination (stable under concurrent inserts)
- API gateway patterns (throttling, API key auth, PII log sanitization)

### Done when
- Rate limiting blocks a user exceeding 100 req/min
- Cursor pagination returns stable, consistent pages under concurrent inserts
- Internal endpoints reject requests without a valid API key

---

## Verification Checklist (After Every Phase)

1. **Can money be lost or created from thin air?** → If yes, you have a bug
2. **Does the ledger sum to zero across all accounts?** → Double-entry invariant
3. **What happens if the process crashes mid-operation?** → Atomicity / saga recovery job
4. **What happens if the same request is sent twice?** → Idempotency
5. **What happens if a worker runs as two concurrent processes?** → `FOR UPDATE SKIP LOCKED`
6. **Can I explain this in a 5-minute whiteboard session?** → Understanding
7. **Can I trace a failure through the system?** → Observability

---

## Skills × Features Matrix

Shows which **features you're building** (columns) while learning each **skill** (rows).

| Skill | Accounts & Ledger | P2P Transfers | Deposits | Withdrawals | Scheduled Payments | Notifications | Audit Trail | Reconciliation | Health & Monitoring |
|-------|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|
| **Double-entry ledger** | System account + balance derivation | Debit/credit (single DB tx) | Credit user, debit system | Debit user; credit back on failure | Debit/credit on schedule | — | — | Verify total nets to zero | — |
| **REST API design** | `POST /accounts`, `GET /me`, error envelope | `POST /transfers`, `GET /transfers/{id}` | `POST /dev/simulate-deposit`, status | `POST /withdrawals`, status | `POST /scheduled-payments`, CRUD | — | — | — | `GET /health` |
| **OpenAPI contract-first** | Write all Phase 1 specs before code | Included in Phase 1 | Spec before code | Spec before code | Spec before code | — | — | — | — |
| **Idempotency** | — | `Idempotency-Key` — Redis + DB unique constraint | `external_ref` unique constraint (double-credit prevention) | Prevent double-debit | Prevent double-execution | — | — | — | — |
| **DB transactions & locking** | — | Lock `accounts` row → derive balance → insert ledger | — | Lock `accounts` row in saga step 1 | `FOR UPDATE SKIP LOCKED` on scheduler worker | — | — | — | — |
| **Concurrency** | — | 10 parallel transfers — prove no overdraft | — | — | Two schedulers — prove exactly-once execution | — | — | — | — |
| **Event-driven (Kafka)** | `account.opened` | `transfer.completed/failed` | `deposit.received/completed` | `withdrawal.completed/compensated` | `payment.executed/skipped` | Consume all events → log notifications | Consume all events → append-only log | `reconciliation.alert` | Consumer lag metrics |
| **Outbox pattern** | — | Write ledger + outbox in same TX; `FOR UPDATE SKIP LOCKED` in relay | Write deposit + outbox in same TX | Write withdrawal + outbox in same TX | Write payment + outbox in same TX | — | — | — | Outbox backlog metric |
| **CQRS** | — | Materialized `transaction_activity` from events | — | — | — | — | Read model from event stream | — | — |
| **Saga (orchestration)** | — | — | — | Debit → call rail → compensate if fail; recovery job for crash scenarios | — | — | — | — | — |
| **Circuit breaker** | — | — | — | Trip on repeated rail failures; `CLOSED/OPEN/HALF_OPEN` state machine | — | — | — | — | Circuit state in health |
| **State machine** | Account (active/frozen/closed) | Transfer (pending→completed/failed) | Deposit lifecycle | Withdrawal + `saga_status` | Payment (active/paused/cancelled/skipped) | — | — | — | — |
| **Observability** | — | Trace: API → DB → relay → Kafka → consumer | — | Trace saga steps + recovery | — | — | — | Alert on invariant mismatch | Prometheus, Grafana, Jaeger |
| **gRPC** | — | — | — | — | — | Extract as gRPC service | — | — | gRPC health check + interceptors |
| **Reconciliation** | Total ledger nets to zero | — | — | Verify no funds lost in saga | — | — | — | Build & run recon job | Recon result in health endpoint |
| **Rate limiting** | — | Throttle transfer endpoint | — | — | — | — | — | — | — |
| **Cursor pagination** | Transaction list (stable pages) | — | — | — | — | — | — | — | — |
| **API gateway patterns** | — | — | — | — | — | — | PII sanitized in logs | — | API key auth for internal services |

### Reading the matrix

- **Read a row** to see: "When I'm learning skill X, which features will I touch?"
- **Read a column** to see: "When I'm building feature Y, which skills am I practicing?"
- Each cell describes the **specific thing you'll implement** at that intersection

### Skills Summary (by phase)

| Skill Area | Phase | Key Feature Vehicle |
|-----------|-------|-------------------|
| Double-entry ledger + system account | Phase 1 | Account opening, seed, balance derivation |
| REST API design | Phase 1 | All CRUD endpoints, error handling |
| OpenAPI / contract-first | Phase 1 | Write spec before any endpoint code |
| DB transactions & locking | Phase 1 | Lock `accounts` row → `SELECT ... FOR UPDATE` |
| Idempotency | Phase 1, 3 | Transfer deduplication (Phase 1); deposit `external_ref` (Phase 3) |
| Concurrency | Phase 1, 3 | Parallel transfer test (Phase 1); parallel scheduler test (Phase 3) |
| Event-driven (Kafka) | Phase 2 | Transfer events → audit log & notifications |
| Outbox pattern | Phase 2 | Direct publish → experience failure → retrofit outbox |
| `FOR UPDATE SKIP LOCKED` | Phase 2, 3 | Outbox relay (Phase 2); scheduler + relay (Phase 3) |
| CQRS | Phase 2 | Transaction activity read model |
| Saga (orchestration) | Phase 3 | Withdrawal with rail compensation + crash recovery |
| Circuit breaker | Phase 3 | Custom 3-state machine protecting bank rail |
| State machine | Phase 3 | Withdrawal `saga_status`, payment lifecycle |
| Observability | Phase 4 | Tracing, metrics, dashboards across all features |
| Reconciliation | Phase 4 | Verify double-entry invariant at scale |
| gRPC | Phase 5 | Notification service extraction |
| Rate limiting | Phase 6 | Per-user token bucket |
| Cursor pagination | Phase 6 | Stable transaction list under concurrent inserts |
| API gateway patterns | Phase 6 | Throttling, API key auth, PII handling |
