
  1. Outbox Pattern

  The insight that you cannot publish to Kafka inside a DB transaction, and you cannot publish after the commit (Kafka could be down). The outbox row written in the same transaction as
  the business write is the only correct solution. Junior engineers publish directly and discover the gap during an incident.

  2. FOR UPDATE SKIP LOCKED

  Used in two places — the outbox relay and the scheduled payment scheduler. Without it, two instances of the relay publish the same outbox row twice. Two scheduler instances execute the
   same payment twice. This is the concurrency primitive that makes both safe to scale horizontally.

  3. Idempotent Consumers

  At-least-once delivery means the same Kafka message can arrive twice — on restart, on rebalance, on replay. The event_id UNIQUE constraint in audit_events and transaction_activity
  makes the second arrival a silent no-op instead of a duplicate record. Junior engineers assume each message arrives exactly once.

  4. Saga Recovery Job

  The crash window between TX1 (debit, saga_status=debited) and TX2 (complete/compensate) is where money gets stuck permanently if no one is watching. The recovery job finds withdrawals
  stuck at debited for more than 5 minutes and compensates them. Most implementations skip this entirely and discover stuck money in production.

  5. Saga Orchestration vs Choreography

  Choosing orchestration for money movement (one function, explicit saga_status column) over choreography (events between services). The practical consequence: finding a stuck withdrawal
   is one SQL query vs reconstructing an event sequence across Kafka topics at 2am during an incident.

  6. Circuit Breaker State Machine

  Three states — CLOSED, OPEN, HALF_OPEN. The non-obvious part is HALF_OPEN: after the cooldown, you let exactly one probe call through. If it succeeds you close the circuit; if it fails
   you reopen. Without HALF_OPEN, the circuit either never recovers automatically or hammers a recovering rail with full traffic immediately.

  7. CQRS Read Model

  transaction_activity is built entirely from Kafka events, never written by the API directly. The consequence is eventual consistency — the read model can lag by seconds. Exposing as_of
   timestamp in the response makes that explicit to API consumers rather than hiding it. Junior engineers build read models that are secretly inconsistent with no indication to the
  caller.

  8. Dead Letter Topic

  After 3 failed processing attempts, the event goes to transfer.events.dlq instead of being dropped or blocking the consumer. Without it, one bad message stops the consumer from
  processing anything behind it (the consumer keeps retrying forever). The DLQ preserves the event for inspection and replay without blocking healthy messages.

  ---
  These are the eight things that separate a working prototype from something you'd trust with real money.