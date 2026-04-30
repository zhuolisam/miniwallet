# EDA, Saga, and Where Monoliths Fit

---

## Why Does EDA Exist? What Problem Does It Solve?

Start with the problem. Imagine a transfer completes. Now you need to:

1. Write to the audit log
2. Send a notification
3. Update the activity feed (read model)

**Without EDA**, your `transfer()` function calls all three directly:

```python
async def transfer(...):
    # ... debit, credit ...
    await audit_log.record(transfer)        # what if this is slow?
    await notification.send(transfer)       # what if this fails?
    await activity_feed.update(transfer)    # what if this throws?
```

Now `transfer()` fails if the notification service is down. It's slow if the activity feed is slow. It's tightly coupled to three unrelated concerns. Every new "thing that should happen after a transfer" requires editing `transfer()`.

**With EDA**, `transfer()` publishes one event. Everything else subscribes:

```python
async def transfer(...):
    # ... debit, credit ...
    await outbox.publish("transfer.completed", payload)
    # done — transfer() has no idea who's listening
```

EDA is solving **coupling and blast radius**. The transfer function's correctness is no longer hostage to the audit service's availability. New consumers (analytics, fraud scoring) can be added without touching `transfer()`.

The second thing EDA solves is **temporal decoupling** — the producer and consumer don't need to be running at the same time. If the notification service is down for 5 minutes, the events queue up in Kafka and get processed when it recovers. In the direct-call model, those 5 minutes of notifications are gone.

---

## Do You Need the Saga Pattern in a Modular Monolith?

Short answer: **yes, but the reason is different from microservices**.

Here's the common explanation you'll read online: "Sagas are for distributed transactions — when you can't do a 2-phase commit across microservice databases." That's true but incomplete. It makes people think: *my monolith has one database, so I can just use a DB transaction. Sagas don't apply to me.*

The mistake is assuming "one DB = one transaction." The withdrawal operation **cannot** be a single DB transaction because it crosses a system boundary — the bank rail. You do this:

1. Debit the account (DB write)
2. Call the bank rail (external HTTP call)
3. Record completion (DB write)

Step 2 is outside the database. You cannot wrap steps 1, 2, 3 in a single `BEGIN...COMMIT`. If the rail call fails after the debit, you have a half-finished operation with real money consequences.

That is exactly what the Saga pattern solves — **multi-step operations where some steps are not transactional**. The distinction isn't monolith vs microservice. It's: *does this operation cross a boundary that a DB transaction cannot span?*

MiniBank hits this with:
- Withdrawal (DB debit + external rail call)
- Scheduled payment (DB debit + transfer, where a crash between steps leaves money in limbo)

For pure in-DB operations like P2P transfer (debit sender, credit receiver — both in the same DB), you don't need a saga. A single DB transaction is correct, simpler, and faster.

**Rule of thumb:** Use a saga when "one transaction" is not possible because at least one step is external or non-atomic. Use a DB transaction when everything can commit together.

---

## Choreography vs Orchestration

Both are "saga patterns" but they're architecturally very different.

**Choreography** (event-driven): each service listens for events and reacts. No central coordinator. Used in microservices because no single service should own the full flow.

```
TransferService publishes transfer.debited →
RailService consumes it, calls rail, publishes transfer.completed or transfer.failed →
CompensationService consumes transfer.failed, publishes refund.initiated →
...
```

To understand what happened in a failed withdrawal, you reconstruct the sequence from 4 Kafka topics. At 2am, during an incident. This is hard.

**Orchestration** (what MiniBank uses for money movement): one function owns the full saga. The state is explicit in a `saga_status` column.

```python
async def withdraw(db, rail, withdrawal_id):
    # Step 1: debit
    # Step 2: call rail
    # Step 3: complete or compensate
    # All controlled here — saga_status column is always the current truth
```

You can query `SELECT * FROM withdrawals WHERE saga_status = 'debited'` and immediately see stuck operations. Choreography doesn't give you that — you'd have to correlate events across topics.

**MiniBank uses both:**

| Pattern | Used for | Why |
|---------|----------|-----|
| Orchestration | Withdrawal saga, scheduled payment execution | Explicit state, auditable, debuggable at 2am |
| Choreography | Notifications, audit log, activity feed | Loose coupling, easy to add new consumers without touching the producer |

---

## Monolith vs Microservice: When Does the Distinction Matter?

For EDA and sagas, the relevant distinction is not monolith vs microservice — it's **"does this operation cross a boundary that a DB transaction cannot span?"**

A microservice architecture forces that boundary everywhere (every service has its own DB, so every cross-service operation needs a saga). A modular monolith only hits it at external integrations — bank rail, third-party APIs. The same patterns apply; the surface area is smaller.

Starting with a modular monolith is the right call for a learning project:
- All the interesting patterns (saga, circuit breaker, outbox, EDA) still apply
- You don't carry the operational overhead of 10 separate deployments
- The transition to microservices, if ever needed, is an extraction of already-isolated modules — not a rewrite

The common mistake is thinking: "I'll learn these patterns properly once I'm doing microservices." You won't. You'll be too busy managing infrastructure. A modular monolith gives you the full pattern library with the complexity you can actually control.
