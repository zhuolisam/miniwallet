# PRD — Phase 2: Event-Driven Architecture

**Phase:** 2 of 6
**Scope:** Kafka · Domain events · Outbox pattern · CQRS · Audit log · Notifications · Dead-letter topics
**Weeks:** 6–9 · ~3–4 hrs/week
**Status:** `not started`

---

## Problem Statement

Phase 1 is synchronous — every operation is a direct DB write with no observable side effects beyond the immediate response. As the system grows, other services need to react to state changes: audit logging, notifications, analytics, read models. Coupling these directly to the transfer function creates a maintenance problem and a reliability risk. Phase 2 decouples state changes from their downstream effects using Kafka as an event bus — and introduces the outbox pattern to ensure events are never lost.

---

## Goals

1. Every significant state change produces a domain event published to Kafka
2. An audit log is populated **exclusively** from Kafka events — never written directly from the API
3. A notification service logs simulated notifications by consuming events
4. A CQRS read model (`transaction_activity`) is built from events, **replacing** the ledger-backed read path for `GET /v1/accounts/me/transactions`
5. The outbox pattern ensures no event is lost even if Kafka is temporarily unavailable
6. Failed events are routed to a per-consumer dead-letter topic after 3 retries — never silently dropped
7. Historical Phase 1 data is backfilled into the event pipeline so read models and audit trail are complete

---

## Out of Scope

- Real notification delivery (email, push) — Phase 2 only logs to stdout
- Schema Registry (Avro) — JSON schemas sufficient for learning
- Multi-partition topics — single partition per topic; ordering discussed as a conscious trade-off
- Cursor-based pagination on `/transactions` (Phase 6)

---

## User Stories

### Week 6 — Direct publish (intentionally fragile)

**US-2.1 — Direct Kafka publish**
> As a system, when a transfer completes, a `transfer.completed` event is published directly to Kafka inline in the transfer function. When a transfer fails (insufficient balance), a `transfer.failed` event is published inline.
>
> *Then*: kill Kafka, make a transfer, restart Kafka — the event is gone. This failure is experienced intentionally before introducing the fix.

**US-2.2 — Audit consumer (against direct publish)**
> As a developer, I build a **minimal** audit log consumer against direct-publish events — just `json.loads` + INSERT, no retry, no DLQ. It works in the happy path. Then I observe the audit gap caused by US-2.1's failure mode — a transfer exists in the DB but has no corresponding audit log entry.
>
> *Note: Week 6 focuses on the Kafka consumer API and the dual-write problem. Retry logic and DLQ routing (BaseConsumer) are added in Week 9.*

### Week 7 — Outbox pattern (fixes Week 6)

**US-2.3 — Outbox pattern**
> As a system, when a transfer completes or fails, an outbox row is written in the **same DB transaction** as the transfer record. A separate relay process publishes outbox rows to Kafka and marks them delivered. Killing Kafka no longer loses events.

**US-2.4 — Account opened event**
> As a system, when a new account is opened, an `account.opened` outbox row is written in the same transaction as the account creation. The relay publishes it to `account.events`.

### Week 8 — Event consumers

**US-2.5 — Audit log from events**
> As a developer, I can query the `audit_events` table and see every state change with its `actor_id`, timestamp, and resource — populated exclusively from Kafka events. The API never touches `audit_events` directly.

**US-2.6 — Migrate `/transactions` to CQRS read model**
> As a user, `GET /v1/accounts/me/transactions` now reads from `transaction_activity` — a materialized view built by a Kafka consumer — instead of querying the ledger directly. The response shape is unchanged (`created_at` field preserved for backward compatibility). Seed entries (`entry_type=seed`) appear in the feed via `seed.completed` events — the activity consumer handles both `transfer.completed` and `seed.completed`. All Phase 1 query parameters (`page`, `limit`, `from_date`, `to_date`, `entry_type`) are preserved; date filters now apply to `occurred_at` in `transaction_activity`. A new `as_of` field in the response meta shows the maximum event timestamp in the current result set, making eventual consistency visible. The read model may lag behind real-time by seconds.
>
> *Why the same endpoint:* A real neobank has one transaction history screen. Users don't know or care whether the data comes from a ledger or a read model. The CQRS migration is an internal architecture change, not a product change.

**US-2.7 — Notification consumer**
> As a system, a notification consumer logs simulated notifications to stdout when events are received:
> - `transfer.completed` → "You sent $X to {to_account}" / "You received $X from {from_account}"
> - `transfer.failed` → "Transfer of $X failed: {failure_code}"
> - `account.opened` → "Welcome! Your account is now active."

**US-2.8 — Historical data backfill**
> As a system operator, before migrating `/transactions` to the CQRS read model, I run a management command that generates synthetic events for all existing Phase 1 data — transfers (`transfer.completed` and `transfer.failed`), seed operations (`seed.completed`), and account opens (`account.opened`) — publishing them to the outbox. The relay delivers them to consumers, ensuring the read model and audit log are complete from day one. Historical events have `actor_id: null` — this is a known and documented limitation, not a bug.

### Week 9 — Reliability

**US-2.9 — Dead-letter topic**
> As a system operator, events that fail to process after 3 retries are moved to the consumer's dedicated DLQ topic for manual inspection — never silently discarded.

**US-2.10 — Consumer lag observability**
> As a system operator, consumer lag is logged periodically (every N messages) and exposed via a counter so I can detect when a consumer is falling behind.
>
> *Note: BaseConsumer processes one message at a time (`async for message in consumer`), not batches. Lag is therefore logged per-message or on a configurable interval — not "per batch." Batched consumption via `getmany()` is a Phase 4 optimization.*

---

## Event Contracts

All events share a common envelope:

```json
{
  "event_id": "uuid (generated fresh at publish time, not entity ID)",
  "event_type": "transfer.completed",
  "occurred_at": "2024-01-15T10:30:00Z",
  "version": "1",
  "actor_id": "uuid (user who initiated the action)",
  "payload": { ... }
}
```

### transfer.completed

```json
{
  "transfer_id": "uuid",
  "from_account_id": "uuid",
  "to_account_id": "uuid",
  "amount": "100.00000000",
  "entry_type": "transfer",
  "idempotency_key": "client-key-123"
}
```

### transfer.failed

```json
{
  "transfer_id": "uuid",
  "from_account_id": "uuid",
  "to_account_id": "uuid",
  "amount": "100.00000000",
  "entry_type": "transfer",
  "idempotency_key": "client-key-123",
  "failure_code": "INSUFFICIENT_BALANCE"
}
```

### account.opened

```json
{
  "account_id": "uuid",
  "user_id": "uuid",
  "status": "active"
}
```

### seed.completed

```json
{
  "account_id": "uuid",
  "user_id": "uuid",
  "amount": "1000.00000000",
  "entry_type": "seed"
}
```

---

## Acceptance Criteria

### Correctness
- Kill Kafka mid-transfer → restart → event eventually delivered (outbox guarantee)
- Audit log contains an entry for every transfer (completed and failed) and every account open
- `GET /v1/accounts/me/transactions` reads from `transaction_activity`, not the ledger — verified by querying with ledger data intentionally removed
- Transaction history includes both Phase 1 historical data and Phase 2 new data (backfill verified)

### Reliability
- A malformed event lands in the consumer's DLQ after 3 retries — consumer does not crash or stall
- Two concurrent relay instances do not duplicate-publish the same event (`FOR UPDATE SKIP LOCKED`)
- Relay uses exponential backoff when Kafka is unavailable — does not spin-loop

### Observability
- Consumer lag is logged per message or on a periodic interval (BaseConsumer processes one message at a time, not batches)
- The `as_of` field in the `/transactions` response is `MAX(occurred_at)` from the result set — null when no results

### Idempotency
- Replaying the same event into any consumer produces no duplicate rows (unique constraint on `event_id` + `account_id` where applicable)

---

## Week-by-Week Summary

| Week | Focus | Key Deliverable |
|------|-------|-----------------|
| 6 | Direct publish + break it | Experience the failure mode firsthand |
| 7 | Outbox pattern | Kill-Kafka test passes; events are never lost |
| 8 | Consumers + CQRS + backfill | Audit log, `/transactions` migrated to read model, notifications all populated |
| 9 | Reliability + DLQ | Retry with backoff, dead-letter routing, lag observability |
