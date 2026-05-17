---
title: "SQLAlchemy merge() vs add() — When Objects Are Already Persistent"
tags: [engineering-concept, sqlalchemy, orm]
phase: 3
week: 11
updated: 2026-05-17
---

# SQLAlchemy `merge()` vs `add()` — When Objects Are Already Persistent

## The Misconception

A common mistake is calling `db.merge(obj)` to "make sure" modifications get committed. This usually comes from confusion about [[sqlalchemy-session-states|object states]] after a commit or when passing objects between functions.

## Object States in SQLAlchemy

An ORM instance is always in one of four states:

| State | Meaning | Tracked by session? |
|-------|---------|---------------------|
| **Transient** | Created with `MyModel(...)`, not yet added | No |
| **Pending** | After `db.add(obj)`, before flush | Yes |
| **Persistent** | Flushed/committed — has a DB identity | Yes |
| **Detached** | Explicitly expelled (`db.expunge()`) or session closed | No |

The key insight: **`commit()` does NOT detach objects.** It expires their attribute values (so the next access triggers a lazy load), but the object remains **persistent** — still tracked in the session's identity map.

## What `merge()` Actually Does

```python
merged = await db.merge(obj)
```

1. Looks up the object's primary key in the identity map
2. If found: copies `obj`'s attribute values onto the existing tracked instance
3. If NOT found: issues a `SELECT` by PK to check if the row exists in the DB
4. Returns the tracked instance (which may be a *different* Python object than `obj`)

This is designed for **reattaching detached objects** — e.g., an object deserialized from a cache, received from another session, or explicitly expunged.

## When `merge()` Is Wasteful

If the object is already persistent (the common case when passing it between functions within the same request):

```python
async def _complete(db: AsyncSession, withdrawal: Withdrawal, ref: str):
    async with db.begin():
        withdrawal.status = 'completed'          # triggers dirty tracking
        withdrawal.external_reference = ref
        withdrawal.completed_at = datetime.now(timezone.utc)
        await db.merge(withdrawal)               # ← WASTEFUL: issues unnecessary SELECT
        # ...
    # commit flushes the dirty attributes automatically
```

Here `withdrawal` was added to this session earlier in the request. It's persistent. Setting attributes marks it dirty. The `async with db.begin()` block flushes dirty objects on exit. `merge()` adds a round-trip SELECT that returns the same object.

**Just remove it:**

```python
async def _complete(db: AsyncSession, withdrawal: Withdrawal, ref: str):
    async with db.begin():
        withdrawal.status = 'completed'
        withdrawal.external_reference = ref
        withdrawal.completed_at = datetime.now(timezone.utc)
        # SQLAlchemy auto-flushes dirty persistent objects on commit
        # ...
```

## When `merge()` IS Needed

- Object came from a **different session** (e.g., background task received a serialized row)
- Object was **explicitly expunged** via `db.expunge(obj)`
- Object was **deserialized** from JSON/cache (has a PK but no session binding)
- Saga recovery: a long-lived worker loads an object, closes the session, then later needs to update it in a new session

## The Rule

> If you called `db.add(obj)` or loaded the object via a query **in the same session**, you never need `merge()`. Just mutate the attributes — SQLAlchemy's unit-of-work pattern handles the rest.

## Minibank Example — Withdrawal Saga

The `withdrawal` object flows through:

```
create_withdrawal()
  → db.add(withdrawal)         # now persistent
  → await db.commit()          # attributes expired, still persistent
  → async with db.begin():     # submitted status flip, still same session
  → rail call (no DB)
  → _complete(db, withdrawal)  # same db, same session, still persistent
      → set attributes         # dirty
      → async with db.begin()  # auto-flushes dirty objects on exit
```

At no point does `withdrawal` leave the session. `merge()` was unnecessary throughout.
