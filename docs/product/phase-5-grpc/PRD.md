# PRD — Phase 5: gRPC & Service Extraction

**Phase:** 5 of 6
**Scope:** gRPC notification service · Protobuf · buf CLI · Service-to-service auth · Full dockerization
**Week:** 17 · ~4–5 hrs total
**Status:** `not started`

> **Note:** Expand to full detail before starting Phase 5 implementation.

---

## Problem Statement

Phases 1–4 use two communication patterns: synchronous HTTP (client → API) and async Kafka (API → consumers). Phase 5 introduces a third: synchronous service-to-service gRPC. The notification service is extracted as a standalone process. The API calls it directly via gRPC for time-sensitive notifications — separate from the existing Kafka-based async notification consumer.

The goal is to experience the difference between REST and gRPC firsthand: schema enforcement via Protobuf, the `buf` codegen workflow, interceptors for cross-cutting concerns, and when synchronous service calls are the right tool vs Kafka.

---

## Goals

1. The notification service runs as a standalone gRPC process, separate from the API
2. A `.proto` file defines the notification service contract (schema-first, like OpenAPI for REST)
3. gRPC interceptors handle auth (API key), logging, and trace propagation
4. The gRPC call is visible as a child span in Jaeger (cross-service trace continuity)
5. The full system starts with `docker compose up` — no manual steps
6. A README documents architecture decisions and how to run the project

---

## Out of Scope

- gRPC streaming (unary calls only)
- Service mesh (Envoy, Istio)
- Production TLS for gRPC (plaintext on Docker internal network is fine)
- Replacing the Kafka notification consumer — both coexist to demonstrate the tradeoff

---

## User Stories

**US-5.1 — gRPC notification service**
> As a system, when a transfer completes, the API calls the notification gRPC service synchronously to deliver a time-sensitive notification.

Acceptance criteria:
- `NotificationService.SendNotification` RPC defined in `.proto`
- API calls it inline after a successful transfer (before returning the response)
- Notification service logs the delivery to stdout (simulated — no real push/email)
- gRPC call visible as a child span in Jaeger

**US-5.2 — Interceptors**
> As a system, all gRPC calls are authenticated, logged, and traced via interceptors on the server side.

Acceptance criteria:
- Auth interceptor: rejects calls without a valid `x-api-key` metadata header (returns `UNAUTHENTICATED`)
- Logging interceptor: logs method name, caller service, and duration (structlog)
- Tracing interceptor: extracts `traceparent` from gRPC metadata, creates child span

**US-5.3 — buf CLI codegen**
> As a developer, I can regenerate the Python gRPC stubs from the `.proto` file with a single command (`buf generate`).

Acceptance criteria:
- `buf.yaml` and `buf.gen.yaml` checked into the repo
- `make proto` (or equivalent) regenerates stubs
- Generated stubs not hand-edited — regeneration is always safe

**US-5.4 — Full docker compose**
> As a developer, `docker compose up` starts the full system with no manual steps.

Acceptance criteria:
- All services start cleanly: API, notification service, PostgreSQL, Redis, Kafka, Jaeger, Prometheus, Grafana
- Health checks prevent dependent services from starting before their dependencies are ready
- A `README.md` explains architecture decisions for each phase and how to run the project

---

## Acceptance Criteria (Phase)

- `docker compose up` starts all services without error
- Transfer triggers a gRPC call; gRPC span visible in Jaeger as child of the HTTP transfer span
- Auth interceptor rejects calls with missing/wrong API key
- `buf generate` regenerates stubs cleanly
- README covers the architecture, design decisions, and `docker compose up` instructions
