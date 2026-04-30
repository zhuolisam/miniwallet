# Tech Debt — Phase 1: Foundation

Items identified during Phase 1 → Phase 2 design review that were not fixed. Each entry records what the problem is, why it was deferred, and when it needs to be resolved.

---

## TD-01 — Services own the commit: no clean hook for cross-cutting writes

**Where:** `app/services/account_service.py:open_account()`, `account_service.py:seed()`

**Problem:** Both functions call `await db.commit()` internally. Phase 2 needs to write an outbox row in the **same DB transaction** as the domain write (account creation, seed). Because the commit is inside the function, Phase 2 must modify each service to call `publish_event(db, ...)` before its commit — coupling event publishing directly into the service body.

This works but is not architecturally clean. A unit-of-work pattern (caller controls when to commit) would let Phase 2 inject the outbox write without touching the service internals.

**Why deferred:** The pragmatic fix (add `publish_event(db, ...)` before each commit) is sufficient for this project's scope. A full unit-of-work refactor adds complexity that isn't justified by a single-service monolith.

**Resolution:** Phase 2 — when adding outbox writes to `open_account()` and `seed()`, follow the established pattern: call `publish_event(db, topic, event_type, payload)` immediately before `await db.commit()`. The `publish_event` helper does a single INSERT into the outbox table within the current transaction.

---

## TD-02 — Router holds the DB transaction open during recipient resolution

**Where:** `app/routers/transfers.py:38–66`

**Problem:** The transfer router resolves the recipient (by email or account ID) on the same SQLAlchemy session that the transfer service later uses for `SELECT ... FOR UPDATE`. This means the DB transaction is open — and eventually the `FOR UPDATE` locks are held — from the first recipient lookup query through to the final `COMMIT`. Under high concurrency, this increases lock contention.

The lookup queries themselves are read-only and do not benefit from being inside the same transaction as the money movement. Isolating them in a short-lived read transaction would reduce the window during which the `FOR UPDATE` locks are held.

**Why deferred:** This is a performance concern, not a correctness bug. At the scale of this project (dev/learning environment), it has no measurable impact. Addressing it requires introducing a second session for the read phase, which adds complexity to the request handling layer.

**Resolution:** Phase 6 (API hardening) — when optimizing request handling and adding rate limiting middleware, revisit the session lifecycle in the transfers router. Consider using a separate read session for recipient resolution and opening the write session only at the point of calling `transfer_service.transfer()`.

---

## TD-03 — `account.status` is always "active" — no frozen/closed lifecycle

**Where:** `app/models/account.py`, `app/services/account_service.py`

**Problem:** The `Account.status` column exists (`VARCHAR(20)`) but only ever holds `"active"`. The study plan mentions `active / frozen / closed` as a state machine for accounts. There is no way to freeze a user's account (e.g., for compliance), close it, or reject transfers to/from non-active accounts. The transfer service does not check `account.status` before executing a transfer.

**Why deferred:** Frozen/closed account states are an operational and compliance feature. Phase 1 scope is correctness of the double-entry ledger. Adding status enforcement before the event pipeline exists would mean the state changes have no observable side effects (no events fired), making the implementation half-baked.

**Resolution:** Phase 3 or Phase 4 — after the event pipeline is in place, add:
- `PATCH /v1/admin/accounts/{id}/status` (admin endpoint, API key protected)
- Status check in `transfer_service.transfer()`: reject if either account is not `"active"`
- Events: `account.frozen`, `account.closed` via outbox

---

## TD-04 — `LedgerEntry` has no `updated_at`

**Where:** `app/models/ledger_entry.py`

**Problem:** Ledger entries are immutable by design (you never update a ledger entry — you reverse it with a new entry). However, the absence of `updated_at` means there is no standard column for detecting row modifications if an audit tool or replication system checks for it.

**Why deferred:** Ledger entries are genuinely append-only. Adding `updated_at` to an immutable table is arguably wrong — it implies mutability. The correct fix is to enforce immutability at the DB level (a trigger that prevents UPDATEs on `ledger_entries`), not to add a timestamp column.

**Resolution:** Phase 4 (Reconciliation) — when building the reconciliation job, add a DB trigger that raises an error on any UPDATE or DELETE to `ledger_entries`. This is the correct enforcement mechanism. Do not add `updated_at`.

---

## TD-05 — Offset pagination in `GET /v1/accounts/me/transactions`

**Where:** `app/services/account_service.py:get_transactions()`, `app/routers/accounts.py`

**Problem:** The transactions endpoint uses offset-based pagination (`OFFSET (page-1) * limit`). Under concurrent inserts (which happen constantly in a bank), offset pagination returns inconsistent results: new transactions inserted between page fetches can cause rows to appear on multiple pages or be skipped entirely.

**Why deferred:** This is explicitly scheduled for Phase 6. The study plan calls out cursor-based pagination as a Phase 6 learning goal, anchored on `(created_at, id)` for stable ordering under concurrent inserts.

**Resolution:** Phase 6 — replace offset pagination with cursor-based pagination in both `GET /v1/accounts/me/transactions` and the Phase 2 `GET /v1/accounts/me/activity` endpoint.

---

## TD-06 — `actor_id` not captured in Phase 1 service signatures

**Where:** `app/services/transfer_service.py`, `app/services/account_service.py`

**Problem:** The Phase 2 audit log requires `actor_id` (the user who initiated the action) in every event payload. Phase 1 service functions accept `account_id` but not `user_id`. The router has `current_user.id` but does not pass it into the service layer.

**Why deferred:** The cleanest fix is a Phase 2 design decision, not a Phase 1 code change: the `publish_event(db, topic, event_type, payload, actor_id)` helper is called from the router (which has `current_user.id`), not from inside the service. This avoids adding a `user_id` parameter to every service function and keeps the service layer focused on domain logic.

**Resolution:** Phase 2 — when building `app/events/publisher.py`, the `publish_event()` call site is the router (or a thin wrapper), where `current_user.id` is available. Pass `actor_id=current_user.id` into the helper at the call site. No service signature changes required.
