# Idempotency — Client Implementation Guide

See also: [[p2p-transfer-deep-dive]] for how the server handles idempotency keys.

---

## The server's contract

The server executes a transfer **exactly once per idempotency key**. That's it. It has no way to detect client mistakes — if the client sends two different keys, the server correctly executes two separate transfers.

The server enforces one rule: `Idempotency-Key` must be supplied by the client. If missing, it returns `400 MISSING_IDEMPOTENCY_KEY`. The server never generates the key on the client's behalf — a server-generated key would be fresh on every request, making retries unrecognizable as duplicates and defeating the purpose entirely.

---

## How the client should implement it

The key must be generated **once when the user initiates the action**, tied to that specific intent, and reused on every retry attempt.

### Key generation — tie it to user intent

```python
# Derive deterministically from the transfer parameters + timestamp of original intent
key = sha256(f"{user_id}:{to_account_id}:{amount}:{initiated_at}")
```

`initiated_at` is the timestamp of the original user action, **not** the current time. This means retries always produce the same key even across app restarts.

### Full retry flow

```
User taps "Send $100 to Bob"
→ initiated_at = now()
→ key = sha256(user_id + to_account + amount + initiated_at)
→ persist key + initiated_at to local storage
→ POST /transfer { ..., Idempotency-Key: key }

     ↓ network timeout — no response received

User taps "Retry" (or auto-retry triggers)
→ load key from local storage        ← same key as original attempt
→ POST /transfer { ..., Idempotency-Key: key }
→ server recognises key → returns cached response, no second debit ✓

Transfer confirmed
→ discard key from local storage
→ next transfer to Bob starts fresh
```

### Why the confirmation screen matters

The "Are you sure?" screen is the moment the key should be generated and stored — before any network call is made. This guarantees the same key is available for every subsequent retry of that specific user action.

---

## What the server cannot protect against

| Client mistake | Result |
|---|---|
| Generate a new key on each retry | Server executes multiple transfers |
| Forget to persist the key (lost on app restart) | New key on retry → duplicate transfer |
| Reuse a key for a genuinely different transfer | Server returns `409 Conflict` |

All three are client-side bugs. The server's idempotency mechanism only helps when the client holds and reuses the correct key.
