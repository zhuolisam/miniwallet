# Wiki Index

Study notes for the [minibank](../superpowers/plans/minibank-study-plan.md) project.

---

## Database & Migrations

- [[alembic-qna]] — How Alembic, models, `env.py`, and `get_db()` wire together; when migrations are generated vs hand-written
- [[db-migration-production-qna]] — How migrations run in production: init containers, CI/CD pipeline steps, rollback strategy, and the expand/contract pattern

## Transfers & Payments

- [[p2p-transfer-deep-dive]] — How P2P transfers work end-to-end: double-entry ledger, pessimistic locking, deadlock prevention, idempotency, Redis caching, and known limitations
- [[idempotency-client-guide]] — Why idempotency key ownership belongs to the client; how to generate, persist, and reuse keys to prevent double entries

## Security & Auth

- [[bcrypt-password-hashing]] — How bcrypt works: random salts, `str.encode()` / `bytes.decode()`, cost factor, and usage in auth_service.py

## Event-Driven Architecture

- [[kafka-infrastructure]] — How Kafka is set up in Docker: containers, dual listeners, topic creation, producer/consumer wiring, and the configuration flow from `.env` to application code
- [[kafka-topics-and-consumers]] — Phase 2 topic map: what events flow through each topic, what each consumer does, and why consumer group IDs matter
- [[dead-letter-queue]] — What a DLQ is, why it exists, per-consumer vs per-topic naming, and why banking requires infinite retention

## Resilience Patterns

- [[circuit-breaker]] — Circuit breaker states, which features use it, and its relationship to the withdrawal saga
- [[eda-saga-and-monolith]] — When EDA and the Saga pattern are needed; choreography vs orchestration; monolith vs microservice tradeoffs
