---
title: DB Migration Production Q&A
tags: [engineering-concept, database, migrations, deployment]
phase: 1
week: 6
updated: 2026-05-03
---

# DB Migration Production Q&A

How database migrations are handled in real-world deployments — patterns, rollback strategies, and the discipline that separates fintech engineers from general backend engineers.

---

## Does migration run on every deployment?

Yes — but *how* it runs varies by maturity of the stack.

---

## Pattern 1: Entrypoint Script (early stage / single instance)

```sh
#!/bin/sh
set -e
alembic upgrade head
exec uvicorn app.main:app --host 0.0.0.0 --port 8000
```

Simple and works fine for single-instance deployments. The `set -e` ensures the container crashes if migration fails, rather than starting the app against a broken schema.

**Problem at scale**: if you run 5 replicas, all 5 race to call `alembic upgrade head` on startup. Alembic uses an advisory lock on the `alembic_version` table so only one wins — but it adds startup latency and unnecessary work.

**What minibank uses** — appropriate for a learning project.

---

## Pattern 2: Init Container (Kubernetes — most common at neobanks)

```yaml
initContainers:
  - name: migrate
    image: myapp:v2.1.0
    command: ["alembic", "upgrade", "head"]
```

The init container must succeed before any app container starts. Kubernetes enforces this sequencing. Used by Monzo, Revolut, and most k8s-native fintechs.

- Migration runs exactly once per deploy, not once per replica
- Container crash on failure surfaces immediately in the deploy pipeline
- Same image as the app — migrations and app code are always in sync

---

## Pattern 3: CI/CD Pipeline Step (most control)

```
deploy pipeline:
  1. run migrations  ← discrete step, fails loudly
  2. health check new schema
  3. deploy new app version
  4. shift traffic
```

Migration is a named pipeline stage with its own logs, alerts, and approval gates. Common at larger banks where a DBA or platform team owns the migration step. Gives you a human checkpoint before new app code ever sees the new schema.

---

## How do you roll back a migration?

Alembic provides `downgrade`:

```bash
# Roll back one revision
alembic downgrade -1

# Roll back to a specific revision
alembic downgrade 0001_initial_schema
```

**But in practice, teams avoid `downgrade`** for two reasons:

1. **Data loss** — `downgrade` typically drops columns or tables. Any data written by the new app version is destroyed.
2. **Linear history** — most teams write a *new forward migration* that undoes the bad change. This keeps the migration history auditable and append-only, which matters for compliance.

**The real rollback strategy**: roll back the *application image* (previous Docker tag), keep the schema as-is. This only works if you practiced backwards-compatible migrations (see below).

---

## The key discipline: backwards-compatible migrations

This is what separates fintech engineers from general backend engineers.

**The rule**: the old app version must be able to run against the new schema.

```
old app + old schema
    → old app + new schema   ← migration runs here
    → new app + new schema   ← traffic shifts here
```

If the old app can tolerate the new schema, you can roll back the app without touching the database.

### The expand/contract pattern

Never make a breaking schema change in a single deploy. Split it across two:

| Step | Deploy | What happens |
|------|--------|-------------|
| Expand | Deploy N | Add new column (nullable, with default) |
| Dual-write | Deploy N | App writes to both old and new column |
| Backfill | Migration | Populate new column for existing rows |
| Contract | Deploy N+1 | Drop old column once all reads use new one |

### What to never do in a single migration

| Dangerous | Why | Safe alternative |
|-----------|-----|-----------------|
| Rename a column | Old app reads the old name, breaks immediately | Add new column → dual-write → backfill → drop old |
| Add NOT NULL column without default | Old app doesn't set it, INSERT fails | Add as nullable first, backfill, add constraint later |
| Drop a column the old app reads | Old app crashes on SELECT | Deploy app change first, then drop column |

Banks call this discipline **zero-downtime migrations**. It is standard practice at Monzo, Wise, and Revolut — blue/green deployments and rolling restarts only work if the schema is backwards-compatible.

---

## Summary

| Concern | Answer |
|---------|--------|
| Does migration run every deploy? | Yes — via init container, pipeline step, or entrypoint script |
| How to roll back? | Roll back the app image; write a forward migration to undo schema changes |
| Can you use `alembic downgrade`? | Technically yes, but rarely done in production due to data loss risk |
| What prevents deploy failures? | Backwards-compatible migrations (expand/contract pattern) |

---

## See also

- [[alembic-qna]] — How Alembic, models, `env.py`, and `get_db()` wire together
