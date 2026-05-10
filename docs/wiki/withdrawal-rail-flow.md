---
title: Production Withdrawal Flow — Our Bank → Rail → Recipient Bank
tags: [system-design, product-feature, engineering-concept, payment-rails]
phase: 3
week: 11
updated: 2026-05-10
---

# Production Withdrawal Flow

How a withdrawal actually works in production when our user sends money to a user at another bank. This expands the [[p2p-transfer-deep-dive]] mental model — instead of money moving inside our ledger only, it crosses a bank boundary via a shared rail.

---

## Scenario

Alice (our user) wants to send £50 to Bob at HSBC.

The key insight: **we never talk to HSBC directly**. We talk to a rail (like UK Faster Payments), and the rail talks to HSBC. Banks don't peer — they all hub-and-spoke through the clearing system.

---

## End-to-end sequence

```
┌──────────┐        ┌──────────┐         ┌─────────────┐        ┌──────────┐
│  Alice   │        │ Our Bank │         │  Rail       │        │  HSBC    │
│ (client) │        │(Minibank)│         │ (Faster     │        │ (Bob's   │
│          │        │          │         │  Payments)  │        │   bank)  │
└─────┬────┘        └────┬─────┘         └──────┬──────┘        └─────┬────┘
      │                  │                      │                     │
   1. │──POST /withdraw──▶                      │                     │
      │  £50, sort=HSBC, │                      │                     │
      │  acct=Bob        │                      │                     │
      │                  │                      │                     │
   2. │                  │──debit Alice £50────│                     │
      │                  │  (ledger write)     │                     │
      │                  │                      │                     │
   3. │                  │──POST /payments──────▶                     │
      │                  │  "send £50 to Bob"  │                     │
      │                  │                      │                     │
   4. │                  │◀──ref=RAIL-XYZ───────│                     │
      │                  │  "accepted, queued" │                     │
      │                  │                      │                     │
   5. │◀─201 Created─────│                      │                     │
      │  status=submitted│                      │                     │
      │                  │                      │                     │
      │                  │                      │──settle────────────▶│
   6. │                  │                      │  "£50 for          │
      │                  │                      │   Bob, from Alice" │
      │                  │                      │                     │
      │                  │                      │                  7. │ credit Bob £50
      │                  │                      │                     │ (HSBC's internal
      │                  │                      │                     │  ledger write)
      │                  │                      │                     │
   8. │                  │◀───webhook POST──────│                     │
      │                  │  "RAIL-XYZ          │                     │
      │                  │   completed"         │                     │
      │                  │                      │                     │
   9. │                  │──update withdrawal──│                     │
      │                  │  status=completed   │                     │
      │                  │  (ledger unchanged) │                     │
      ▼                  ▼                      ▼                     ▼
```

## What each actor does

| Step | Actor | What happens |
|------|-------|--------------|
| 1 | Alice | Clicks "Send £50" in our app → hits our API |
| 2 | Our Bank | Debit Alice's ledger **immediately** (reserve funds) — see "Why debit first" below |
| 3 | Our Bank | Call the rail's API: "please send £50 to HSBC, sort 40-00-01, acct 12345" |
| 4 | Rail | Accepts the instruction, gives us a reference (`RAIL-XYZ`), status = queued |
| 5 | Our Bank | Respond to Alice: "submitted, check back" |
| 6 | Rail | Actually moves the money on the interbank network (FPS in UK, ACH in US, SEPA in EU, DuitNow in Malaysia) |
| 7 | HSBC | Rail tells HSBC "£50 arrived for Bob" — HSBC credits Bob's account (**this is HSBC's deposit flow**) |
| 8 | Rail | Rail calls **our webhook**: "RAIL-XYZ completed successfully" |
| 9 | Our Bank | Update `withdrawals.status = 'completed'`. **No ledger change** — Alice was already debited in step 2 |

---

## The rail is a hub, not a pipe

Banks don't have N×N API integrations. They all connect to a shared clearing system:

- **UK:** Faster Payments Service (FPS), run by Pay.UK
- **US:** ACH (slow, 1-3 days), Fedwire (same-day), RTP (instant, newer)
- **EU:** SEPA, SEPA Instant
- **Malaysia:** DuitNow (instant P2P), IBG (batch), RENTAS (wholesale) — all under [PayNet](https://paynet.com.my)

When HSBC wants to send money to Barclays, they don't call Barclays' API. They submit to FPS, FPS routes to Barclays. This is called the **clearing and settlement layer**.

Think of the rail as a router: we push packets into it with a destination address (sort code + account number), the router figures out which bank that address belongs to and delivers it. Neither bank needs to know the other exists at the API level.

---

## Why two webhooks, two references, two IDs

### Two webhooks

**Deposits use a rail → us webhook** (inbound notification — [[p2p-transfer-deep-dive]] doesn't have this because P2P stays inside our ledger). The rail discovers money arrived and pushes that event to us.

**Withdrawals use both directions:**
- Us → rail: "please send this payment" (request/response)
- Rail → us (webhook): "the payment you asked about is now completed/failed"

The second one is why real rails are **asynchronous** — `rail.send_withdrawal()` returns `submitted` only. Final status arrives via webhook seconds to days later (depending on scheme).

### Two references

| ID | Owned by | Purpose |
|----|----------|---------|
| `withdrawal.id` (our UUID) | Us | Internal record. What Alice sees ("your withdrawal W-12345"). Used in logs, reconciliation, `ledger_entries.reference_id` |
| `external_reference` (`RAIL-XYZ`) | Rail | Rail's internal ID. Used when we call rail's status API or when rail sends us webhooks |

Same pattern as Stripe: you have your `order_id`, Stripe has `ch_XXX`, both stored on the same row. When Stripe webhooks back, they send `ch_XXX`, you look up by `external_reference`.

### When the rail generates `external_reference`

- **Deposit flow:** rail generates it BEFORE contacting us (it's already on the webhook payload when they call)
- **Withdrawal flow:** rail generates it AFTER we submit (returned as the response to step 3)

---

## Why debit Alice first (step 2 before step 3)

Between "Alice clicks submit" and "rail confirms" (could be seconds to days depending on scheme), Alice could initiate transfers, other withdrawals, or receive scheduled payments. If we don't debit immediately, her available balance is a lie — she can overdraw.

Every neobank debits on submission and compensates on failure. This is not a design choice; it's the only correct approach. It's also why we need the [[circuit-breaker]] and saga pattern — they handle the "what if the rail fails after we've already taken the money" problem.

---

## Failure modes

### What if Bob's account is inactive at HSBC?

HSBC detects it — we can't. We have no visibility into HSBC's account status. Flow:

```
Step 3: we submit to rail
Step 4: rail accepts, gives us RAIL-XYZ
Step 6: rail → HSBC
Step 7: HSBC checks Bob's account → CLOSED
        HSBC → rail: "REJECT, beneficiary closed"
Step 8: rail → us (webhook): "RAIL-XYZ failed, reason=BENEFICIARY_CLOSED"
Step 9: we write compensating ledger entry restoring Alice's £50
        withdrawal.status=failed, failure_code=BENEFICIARY_CLOSED
```

The rail is the messenger for failures, same as for success.

### Failure code catalog (from our design)

| `failure_code` | What happened at the recipient bank |
|---------------|-------------------------------------|
| `INVALID_ACCOUNT` | Sort code + account number combo doesn't exist at any bank |
| `BENEFICIARY_CLOSED` | Account existed once but is closed |
| `TIMEOUT` | Recipient bank didn't respond within scheme SLA |
| `NETWORK_ERROR` | Rail itself had an issue (not the recipient bank) |
| `CIRCUIT_OPEN` | Rare — our circuit breaker tripped during the saga |

Each maps to compensation via `withdrawal_reversal` ledger entries. See [[eda-saga-and-monolith]] for how the saga pattern handles this, and [[circuit-breaker]] for why we pre-check the rail's health before debiting.

---

## Related modern quirks: proxy payments

Real rails increasingly support proxy-based addressing — you don't need to know the recipient's account number:

- **UK (FPS + Confirmation of Payee):** mandated by the FCA since 2020. Every new-beneficiary push payment must be name-checked. That's why UK banking apps show "we found Bob Smith — is that who you meant?" before confirming.
- **Malaysia (DuitNow):** supports phone number, NRIC, or business registration as proxy. Resolved via DuitNow's National Addressing Database.
- **Singapore (PayNow):** same pattern, NRIC or phone.
- **India (UPI):** VPA (virtual payment address) like `alice@hdfc`.

Our simulator doesn't model this, but in a real Malaysian neobank build you'd have a pre-validation step before step 2 (before debiting) to resolve the proxy and get a name-check result.

---

## Why our simulator is synchronous

Our simulator's `send_withdrawal()` returns success/failure synchronously. Production rails are asynchronous (steps 4 and 8 are separate events).

What would change with a real rail:

| Simulated (Phase 3) | Production (real rail) |
|---------------------|------------------------|
| `send_withdrawal()` returns success synchronously | `send_withdrawal()` returns `submitted` only |
| TX 3a runs immediately after rail call | TX 3a runs when webhook arrives |
| Saga recovery is the primary resolution path | Saga recovery is the backstop for missed webhooks |
| No webhook endpoint needed | `POST /webhooks/rail/status` + HMAC verification required |

A real implementation would be a straightforward extension: the webhook handler calls the same `_complete()` / `_compensate()` functions the saga recovery job already uses.

---

## The symmetry insight

Alice's withdrawal from our bank IS Bob's deposit at HSBC — same money event, viewed from two sides:

- **Our ledger:** Alice debit, system-account credit (money left our bank)
- **HSBC's ledger:** system-account debit, Bob credit (money entered their bank)
- **Rail's role:** it's the referee — owes HSBC £50, is owed £50 by us

At end of day, the rail **settles net positions** between banks. This is why "instant" payments take seconds at the user level but "same-day settlement" is end-of-day batch at the bank level. The user-visible speed and the interbank accounting speed are different things.

See [[transfer-vs-ledger-separation]] for how this dual-view (business event vs accounting record) shapes our data model.

---

## Related notes

- [[p2p-transfer-deep-dive]] — Internal transfers (both accounts in our bank, no rail involved)
- [[eda-saga-and-monolith]] — The saga pattern that handles cross-boundary failures
- [[circuit-breaker]] — Pre-flight check so we don't debit users when the rail is down
- [[idempotency-client-guide]] — Why `Idempotency-Key` header is required on `POST /v1/withdrawals`
- [[transfer-vs-ledger-separation]] — Business event vs accounting record (same pattern applies to withdrawal)
