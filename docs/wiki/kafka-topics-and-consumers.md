---
title: Kafka Topics and Consumer Groups (Phase 2)
tags: [phase-2, kafka, event-driven-architecture, cqrs, consumer-groups, system-design]
updated: 2026-04-29
---

# Kafka Topics and Consumer Groups

Phase 2 introduces Kafka as the event bus for MiniBank. This page maps out what topics exist, what events flow through them, and what each consumer does with those events.

---

## Topics

Two business topics carry all domain events. Three DLQ topics hold failed messages.

```
transfer.events                      ← all transfer outcomes
account.events                       ← all account lifecycle events

minibank.audit-consumer.dlq
minibank.notification-consumer.dlq
minibank.activity-consumer.dlq
```

See [[dead-letter-queue]] for why DLQs exist and why they are per-consumer.

---

## Events per Topic

### `transfer.events`

| Event | When produced |
|-------|--------------|
| `transfer.completed` | Sender debited, receiver credited successfully |
| `transfer.failed` | Transfer rejected — e.g. insufficient balance |

### `account.events`

| Event | When produced |
|-------|--------------|
| `account.opened` | User opened their first account |

### Event envelope

Every event, regardless of type, shares the same wrapper:

```json
{
  "event_id": "uuid — fresh UUID generated at publish time, NOT the entity's ID",
  "event_type": "transfer.completed",
  "occurred_at": "2024-01-15T10:30:00Z",
  "version": "1",
  "actor_id": "uuid — the user who triggered the action",
  "payload": { ... }
}
```

`event_id` is generated fresh in `publish_event()` and lives in the outbox payload forever. It is not the transfer's ID or the outbox row's PK — it is a stable identity for this specific event, used by consumers for idempotency.

---

## Consumer Groups

Kafka delivers every event to every consumer group. Within a group, each message goes to only one instance. Because all three consumers are in separate groups, each gets its own independent copy of every event.

| Consumer | Group ID | Subscribes to |
|----------|----------|---------------|
| Audit | `minibank.audit-consumer` | `transfer.events`, `account.events` |
| Notification | `minibank.notification-consumer` | `transfer.events`, `account.events` |
| Activity | `minibank.activity-consumer` | `transfer.events` |

Group IDs are prefixed with `minibank.` to avoid collision with other applications on the same Kafka cluster. Accidentally reusing a group ID across two different consumers would cause Kafka to distribute messages between them — each would receive only some events, silently dropping the rest.

---

## What Each Consumer Does

### `minibank.audit-consumer` — Compliance record

Writes every event it receives to `audit_events` — no filtering. This is the immutable audit trail required by banking regulators (MAS TRM, BNM RMiT). It stores who did what (`actor_id`), to which resource, and when.

**Mental model:** A court reporter. Records everything verbatim. Never skips an event.

Idempotent via `UNIQUE(event_id)` on `audit_events` — replaying an event is a no-op.

### `minibank.notification-consumer` — Customer comms

Translates events into human-readable messages. In production this would call a push notification provider (Firebase, APNs) or SMS gateway. In Phase 2 it logs to stdout.

- `transfer.completed` → "You sent $X" (sender) + "You received $X" (receiver)
- `transfer.failed` → "Transfer failed: INSUFFICIENT_BALANCE" (sender)
- `account.opened` → "Your account is now active" (user)

**Mental model:** Customer service dispatch. Stateless — no DB writes.

### `minibank.activity-consumer` — CQRS read model

Builds `transaction_activity` — the table that backs `GET /v1/accounts/me/transactions` in Phase 2. For each `transfer.completed` event it inserts **two rows**: a debit row for the sender and a credit row for the receiver. Ignores `transfer.failed` — failed transfers don't appear in transaction history.

**Mental model:** A materialized view builder. The ledger (write side) is the source of truth for money movement. `transaction_activity` (read side) is a pre-shaped copy optimised for the transaction history query. Phase 2 re-points the `/transactions` endpoint from the ledger to this table — same API, different data source.

Idempotent via `UNIQUE(event_id, account_id)` — note it is NOT `UNIQUE(event_id)` alone, because one event produces two rows (one per account).

---

## Full Data Flow

```
TransferService ──┐
                  ├──► outbox ──► Relay ──► transfer.events ──┬──► audit-consumer     → audit_events
AccountService ──┘                      └──► account.events  ├──► notification-consumer → stdout
                                                              └──► activity-consumer  → transaction_activity
```

Each consumer is independent. If `activity-consumer` is down, `audit-consumer` and `notification-consumer` keep running unaffected. The broken messages sit in Kafka until the consumer recovers — no data is lost.

---

## Related

- [[dead-letter-queue]] — What happens when a consumer fails to process an event after 3 retries
- [[p2p-transfer-deep-dive]] — How a transfer is written to the DB before any events are produced
- [[eda-saga-and-monolith]] — When event-driven architecture is the right pattern
