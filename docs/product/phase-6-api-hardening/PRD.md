# PRD — Phase 6: API Hardening

**Phase:** 6 of 6
**Scope:** Rate limiting · Cursor pagination · API key auth · PII redaction · Throttling
**Weeks:** 18–19 · ~3–4 hrs/week
**Status:** `not started`

> **Note:** Expand to full detail before starting Phase 6 implementation.

---

## Problem Statement

Phases 1–5 build a correct, observable system. Phase 6 makes it production-safe for public exposure. Three problems need solving: an unauthenticated caller can overwhelm the API with requests; offset-based pagination returns inconsistent results under concurrent inserts; and sensitive fields (passwords, tokens) occasionally leak into logs. Phase 6 addresses these at the infrastructure layer — no changes to business logic.

---

## Goals

1. Per-user rate limiting prevents any single caller from degrading the service for others
2. Cursor-based pagination returns stable results even when new records are inserted mid-page
3. Internal endpoints (dev/seed, health/metrics) require an API key — not a user JWT
4. Log lines never contain plaintext passwords, tokens, or full account numbers
5. Per-endpoint throttling caps expensive operations (transfer endpoint) independently of general rate limits

---

## Out of Scope

- WAF / DDoS protection (network layer)
- OAuth2 / OIDC for third-party API access
- Card number tokenization (no cards in this project)

---

## User Stories

**US-6.1 — Rate limiting**
> As an operator, no single user can make more than N requests per minute to the API. Requests beyond the limit receive a `429 Too Many Requests` with a `Retry-After` header.

Acceptance criteria:
- Redis token bucket per `user_id` (authenticated) or `IP` (unauthenticated)
- Default limit: 60 req/min for authenticated users, 10 req/min for unauthenticated
- Response headers: `X-RateLimit-Limit`, `X-RateLimit-Remaining`, `X-RateLimit-Reset`
- `429` response body follows the standard error envelope: `{ "error": { "code": "RATE_LIMITED", "message": "..." } }`
- Limit enforcement is atomic (no race condition between check and decrement)

**US-6.2 — Per-endpoint throttling**
> As an operator, the transfer endpoint has a tighter per-user limit (10 transfers/min) to prevent abuse, independent of the general rate limit.

Acceptance criteria:
- Separate Redis key per `user_id:endpoint`
- Transfer limit: 10/min; deposit simulate: 20/min
- Same `429` response and headers as general rate limiting

**US-6.3 — Cursor-based pagination**
> As a user, `GET /v1/accounts/me/transactions` returns stable, non-duplicating pages even when new transactions are inserted between page fetches.

Acceptance criteria:
- Cursor is an opaque base64-encoded `(created_at, id)` tuple — not a page number
- Query: `WHERE (created_at, id) < (cursor_created_at, cursor_id) ORDER BY created_at DESC, id DESC`
- Response includes `next_cursor` (null if last page)
- Old `page`/`limit` query params removed from the transactions endpoint
- Applied to both `GET /v1/accounts/me/transactions` and `GET /v1/accounts/me/activity`

**US-6.4 — API key auth for internal endpoints**
> As an operator, the dev seed endpoint and internal service endpoints require a static API key in the `X-API-Key` header — not a user JWT.

Acceptance criteria:
- Endpoints: `POST /v1/dev/simulate-deposit`, `GET /v1/health`, `GET /metrics`
- Missing or wrong API key → `401 UNAUTHORIZED`
- API key stored in environment variable (`INTERNAL_API_KEY`), not in DB
- Middleware checks key before routing — no business logic change needed

**US-6.5 — PII redaction in logs**
> As an operator, no log line ever contains a plaintext password, access token, refresh token, or full account number — even on errors.

Acceptance criteria:
- structlog processor strips/masks: `password`, `hashed_password`, `access_token`, `refresh_token`, `authorization` header
- Request body logging redacts sensitive fields before writing
- Test: make a login request with a password → password does not appear in any log line

---

## Acceptance Criteria (Phase)

- Make 61 requests/min from one user → 61st returns 429 with correct headers
- Insert a transfer while paginating → no duplicate or missing entries across pages
- Call `POST /v1/dev/simulate-deposit` without `X-API-Key` → 401
- Login request → grep logs → password field absent
- Transfer endpoint: 11th transfer in one minute → 429 (while general limit not reached)
