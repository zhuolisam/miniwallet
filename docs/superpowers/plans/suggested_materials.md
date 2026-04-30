# Suggested Study Materials — MiniBank

Curated reading list to accompany the MiniBank study plan. Ordered by when to read, not by importance.

---

## Core Books

### 1. Designing Data-Intensive Applications (DDIA)
**Author:** Martin Kleppmann (O'Reilly)
**When to read:** Alongside the build — specific chapters map to specific phases (see below)
**Why:** The most important backend engineering book written in the last decade. Not a "how to use tool X" book — it explains *why* distributed systems fail in specific ways and how to reason about them. Dense. Every chapter is worth rereading after you've built the thing it describes.

**Chapter reading order by phase:**

| Phase | DDIA Chapters | What you'll understand after |
|-------|--------------|------------------------------|
| Phase 1 | Ch. 7 — Transactions | Why `SELECT FOR UPDATE` works; what isolation levels mean; why `SERIALIZABLE` exists |
| Phase 2 | Ch. 5 — Replication, Ch. 11 — Stream Processing | Why the read model lags; what "at-least-once delivery" means in practice |
| Phase 3 | Ch. 9 — Consistency and Consensus | Why you can't do distributed transactions without sagas; two-phase commit and why banks avoid it |
| Phase 4 | Ch. 1 — Reliable, Scalable, Maintainable Systems | The vocabulary for talking about observability and SLOs |

---

### 2. Release It! — Design and Deploy Production-Ready Software
**Author:** Michael Nygard (Pragmatic Programmers)
**When to read:** Before starting Phase 3 (specifically before implementing the circuit breaker)
**Why:** Short, practical, full of real production failure stories. Nygard invented the circuit breaker pattern. Reading the circuit breaker, bulkhead, and timeout chapters before you implement Phase 3 will change how you think about the 50 lines of code you're writing. The cascade failure stories are unforgettable — you'll think about them every time you add a network call.

**Chapters to prioritize:**
- Part II: Stability Patterns — Circuit Breaker, Timeouts, Bulkheads, Fail Fast
- Part I: Chapter 1-3 — the production incident stories that motivate every pattern

---

### 3. Building Event-Driven Microservices
**Author:** Adam Bellemare (O'Reilly)
**When to read:** After finishing Phase 3, before starting Phase 4
**Why:** A practitioner's guide to EDA at scale. Heavy on Kafka topology, consumer groups, schema evolution with Avro, and event streams as the system of record. The mental models are solid. The examples assume 20 microservices, not a modular monolith — but by Phase 3 you'll have built the core patterns and the book reads like a post-mortem on decisions you already made. You'll know which parts apply to you and which are overkill.

**What to skim:** Avro/Schema Registry chapters — JSON is sufficient for this project. Focus on the event design and consumer group chapters.


### Other materials I read
https://medium.com/geekculture/design-patterns-for-microservices-circuit-breaker-pattern-276249ffab33

https://oneuptime.com/blog/post/2026-01-23-saga-pattern-python/view
