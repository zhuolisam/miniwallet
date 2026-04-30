# Circuit Breaker

A stability pattern that **stops your system from hammering a failing external dependency**. The name comes from an electrical circuit breaker — when there's a fault, the breaker trips and cuts the connection before the damage spreads.

In software, the "fault" is a downstream service that keeps failing. Without a circuit breaker, every request to your API waits for the full timeout of the failing service before getting an error. 100 concurrent requests × 30 second timeout = your API is effectively down.

---

## The Three States

```
CLOSED ──(3 consecutive failures)──▶ OPEN
  ▲                                    │
  │                                    │ (30s cooldown)
  │                                    ▼
CLOSED ◀──(probe succeeds)────── HALF_OPEN
                                       │
                         (probe fails)─┘
                                       ▼
                                     OPEN
```

**CLOSED** — normal operation. All calls pass through.

**OPEN** — the rail is known to be down. Calls fail immediately with `BANK_RAIL_UNAVAILABLE` without even attempting the network call. Fast failure, no waiting.

**HALF_OPEN** — cooldown elapsed, recovery is possible. One probe call is let through. If it succeeds, circuit closes and normal operation resumes. If it fails, circuit reopens and the 30 second cooldown resets.

`HALF_OPEN` is the subtle state most people miss. Without it you have two bad options: never auto-recover, or flood a recovering service with full traffic the moment cooldown ends.

---

## Which Feature Uses It

Only **withdrawal**. Specifically, wrapping the bank rail call:

```python
# withdrawal_service.py
async def withdraw(...):
    debit_account()                           # TX 1 — no circuit breaker, pure DB

    try:
        result = await circuit_breaker.call(  # ← wraps the rail call
            rail.send_withdrawal,
            withdrawal.id, amount, destination_ref
        )
        complete(saga_status="completed")     # TX 2a

    except CircuitOpenError:
        # circuit is open — fail fast, don't even try the rail
        compensate(saga_status="compensated") # TX 2b — refund immediately
    except RailError:
        # circuit closed but rail rejected — record failure, maybe trip circuit
        compensate(saga_status="compensated") # TX 2b
```

When `CircuitOpenError` is raised, the saga immediately compensates — refunds the debit — instead of waiting for a timeout that won't succeed anyway.

---

## Why Only Withdrawal

| Feature | Needs circuit breaker? | Reason |
|---------|----------------------|--------|
| Deposit | No | Inbound — the rail calls you, you don't call the rail |
| P2P transfer | No | Purely internal DB operations, no external call |
| Withdrawal | Yes | Reaches out to the bank rail synchronously |

The circuit breaker only applies where your system makes an outbound call to an external dependency. Withdrawal is the only such place in MiniBank.

---

## What It Looks Like in Production

Imagine the bank rail goes down at 2pm.

**Without a circuit breaker:**
- Every withdrawal attempt waits 30 seconds for the timeout, then fails
- API thread pool fills up with waiting requests
- Other endpoints start slowing down because threads are exhausted
- The system looks unhealthy even though only one downstream dependency is down

**With a circuit breaker:**
- First 3 withdrawals fail after timeout, tripping the circuit
- Every subsequent withdrawal fails in microseconds with `BANK_RAIL_UNAVAILABLE`
- Rest of the API is completely unaffected
- After 30 seconds, one probe goes through — if the rail recovered, circuit closes automatically

---

## Relationship to the Withdrawal Saga

The circuit breaker and the saga work together. The saga handles "what do we do when the rail fails." The circuit breaker handles "should we even try the rail."

```
saga_status=debited
       │
       ▼
circuit_breaker.call(rail.send_withdrawal)
       │
  ┌────┴─────────────────────┐
  ▼                          ▼
RailError               CircuitOpenError
(tried, failed)         (didn't try — known down)
       │                          │
       └──────────┬───────────────┘
                  ▼
          compensate(saga_status=compensated)
          — balance restored either way
```

From the saga's perspective, `RailError` and `CircuitOpenError` are handled identically — both result in compensation. The circuit breaker is an optimisation on top: it avoids the timeout cost when the rail is known to be down.

---

## Circuit State in Health Check

The circuit breaker state is exposed in `GET /v1/health`:

```json
{
  "data": {
    "circuit_breaker": {
      "state": "OPEN",
      "failure_count": 3
    }
  }
}
```

This lets an operator know at a glance whether the bank rail is reachable without having to attempt a withdrawal or dig through logs.
