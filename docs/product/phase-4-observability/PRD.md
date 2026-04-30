# PRD — Phase 4: Observability & Reconciliation

**Phase:** 4 of 6
**Scope:** Structured logging · Distributed tracing · Metrics · Dashboards · Health check · Reconciliation
**Weeks:** 14–16 · ~3–4 hrs/week
**Status:** `not started`

> **Note:** Expand to full detail before starting Phase 4 implementation.

---

## Problem Statement

By Phase 3, the system has an API, two background workers, a circuit breaker, and a Kafka pipeline. When something breaks in production, the question is: *what happened, where, and why?* Today the answer is "grep the logs and hope." Phase 4 makes the system's internal state legible — every request traceable end-to-end, every service measurable, and any ledger discrepancy automatically detectable.

---

## Goals

1. Every request produces structured JSON logs with a correlation ID that follows it across services and Kafka consumers
2. A single request can be traced from API → DB → Kafka → consumer in Jaeger
3. Prometheus scrapes metrics; Grafana shows API health and Kafka pipeline dashboards
4. A `GET /v1/health` endpoint reports DB/Kafka/Redis connectivity, circuit breaker state, and last reconciliation result
5. A reconciliation job detects when the double-entry ledger diverges from zero

---

## Out of Scope

- Real alerting integrations (PagerDuty, OpsGenie)
- Log aggregation pipeline (ELK, Loki) — stdout JSON is sufficient for learning
- SLA/SLO definitions
- Rate limiting, API key auth, PII redaction — these move to Phase 6

---

## User Stories

**US-4.1 — Distributed tracing**
> As a developer debugging a production issue, I can search Jaeger by request ID and see the full trace of a failed withdrawal including which DB query was slow and which Kafka message was published.

Acceptance criteria:
- `traceparent` header propagated across HTTP, DB spans, and Kafka message headers
- Trace visible in Jaeger: HTTP handler → SQLAlchemy → Kafka producer → consumer
- Manual spans for: balance calculation, idempotency check, circuit breaker state change

**US-4.2 — Metrics & dashboards**
> As a developer, I can open Grafana and see current request throughput, p95 latency, error rate, and Kafka consumer lag — all in one dashboard.

Acceptance criteria:
- `http_requests_total`, `http_request_duration_seconds` (histogram), `transfers_total` counters
- `kafka_consumer_lag` gauge per topic/consumer group
- `reconciliation_discrepancy` gauge (0 = clean)
- Two Grafana dashboards: API health, event pipeline

**US-4.3 — Health check**
> As an operator, `GET /v1/health` tells me whether the system is operational in a single call.

Acceptance criteria:
- Checks: DB connectivity, Kafka connectivity, Redis connectivity
- Includes circuit breaker state (from Phase 3)
- Includes last reconciliation result (status + timestamp + difference)
- Returns `200` when healthy, `503` when any check fails

**US-4.4 — Reconciliation job**
> As a system, a scheduled job verifies that the sum of all ledger credits minus debits equals zero — the double-entry invariant.

Acceptance criteria:
- Job runs on a schedule (every 5 minutes) and on startup
- Discrepancy → structured log alert + `reconciliation_discrepancy` Prometheus gauge increment + row in `reconciliation_runs`
- Last run result visible in `GET /v1/health`
- Test: intentionally corrupt a ledger entry → reconciliation detects and logs it

---

## Acceptance Criteria (Phase)

- Trace a transfer: one request ID visible in API logs, DB query spans, and Kafka producer span in Jaeger
- Grafana dashboard loads with real-time metrics from a running transfer
- `GET /v1/health` returns healthy when all services up; returns `503` when DB is down
- Intentionally corrupt a ledger entry → reconciliation job detects and logs the discrepancy
