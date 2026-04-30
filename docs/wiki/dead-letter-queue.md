---
title: Dead Letter Queue (DLQ)
tags: [phase-2, kafka, event-driven-architecture, reliability, system-design, engineering-concept]
updated: 2026-04-29
---

# Dead Letter Queue

A Dead Letter Queue (DLQ) is a holding pen for messages that a consumer repeatedly failed to process. Instead of dropping the message or letting the consumer get stuck forever, the broken message is parked somewhere safe so the consumer can keep moving and a human can fix the problem later.

---

## The Problem

Imagine the audit consumer receives a corrupted event:

```json
{
  "event_id": "abc-123",
  "event_type": "transfer.completed",
  "payload": null   ← corrupted
}
```

Three options:

| Option | What happens | Why it's wrong |
|--------|-------------|----------------|
| **Crash the consumer** | Process dies, Docker restarts it, reads the same message, dies again — infinite loop | Everything behind this message is blocked forever |
| **Skip silently** | Log a warning, move on, message gone | The audit record for transfer abc-123 disappears permanently. Regulatory fine. |
| **DLQ** | After 3 retries, publish to `minibank.audit-consumer.dlq`, commit offset, move forward | Consumer makes progress. Broken message is preserved. Human fixes it later. |

Option C is the only acceptable answer in banking.

---

## How It Works in MiniBank

The [[kafka-topics-and-consumers#minibank.audit-consumer — Compliance record|BaseConsumer]] tracks retry count via a Kafka message header `x-retry-count`:

1. Consumer receives message, tries to process it → fails
2. Increments `x-retry-count`, re-publishes to the **same topic**, commits original offset
3. Consumer receives the retry message, tries again → fails again
4. After 3 attempts: publishes to `{group-id}.dlq`, commits offset, logs an error

The consumer never blocks. The broken message is never lost.

```
transfer.events
  └──► audit-consumer (attempt 1: fail)
  └──► audit-consumer (attempt 2: fail, x-retry-count: 1)
  └──► audit-consumer (attempt 3: fail, x-retry-count: 2)
  └──► minibank.audit-consumer.dlq  ← parked here
  └──► audit-consumer continues with next message ✓
```

---

## Why Per-Consumer DLQs, Not Per-Topic

MiniBank has three DLQ topics — one per consumer group:

```
minibank.audit-consumer.dlq
minibank.notification-consumer.dlq
minibank.activity-consumer.dlq
```

Not one shared `transfer.events.dlq`.

**Why:** The failure is consumer-specific. The same `transfer.completed` event might process fine in the audit consumer but crash the activity consumer due to a DB constraint violation. With per-consumer DLQs, the queue name immediately tells you which pipeline failed and where to start debugging. With a shared DLQ, you have a pile of failed messages with no indication of which consumer rejected them.

---

## DLQ Retention: Infinite

DLQ topics have **infinite retention** (`retention_ms: -1`). Regular business topics expire after 7 days — old events are irrelevant once consumers have processed them. DLQ messages must never expire because:

1. You don't know when you'll get to fixing the root cause
2. Losing a DLQ message means the compliance record has a permanent gap
3. The message may be needed months later for an audit or incident investigation

---

## What Happens After a Message Lands in the DLQ

In a production neobank:

1. Alert fires — PagerDuty or Slack: "minibank.activity-consumer.dlq has new messages"
2. Engineer inspects the message to understand the failure
3. Root cause fixed — usually a bug in the consumer or a malformed event at the source
4. Message is replayed back into the original topic using a Kafka consumer tool
5. Consumer processes it successfully this time

The message waited. Nothing was lost.

---

## The Banking Stakes

Without a DLQ, a single malformed event causes one of two outcomes:
- The consumer stalls and all subsequent events pile up unprocessed (audit lag grows to days)
- The event is silently dropped and the audit trail has a gap

Either outcome can trigger a regulatory finding. MAS TRM (Singapore) and BNM RMiT (Malaysia) require demonstrable audit completeness. "We dropped the event" is not an acceptable answer. "It's in the DLQ and we're remediating" is.

---

## Related

- [[kafka-topics-and-consumers]] — Full map of topics, events, and consumer groups in Phase 2
- [[eda-saga-and-monolith]] — Broader event-driven architecture context
