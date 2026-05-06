---
title: Transfer vs Ledger Entry — Why Both Exist
tags: [system-design, accounting, data-model]
phase: 1
week: 7
updated: 2026-05-05
---

# Transfer vs Ledger Entry — Why Both Exist

Real neobanks separate the **business event** (Transfer) from the **accounting record** (LedgerEntry). They serve different audiences and have fundamentally different lifecycles.

---

## Transfer = The Business Event

**Audience:** Customer, product, support, API consumers.

**Purpose:** Records *what the user asked to do* and *what happened*.

- Has mutable `status` (completed, failed, pending)
- Has `failure_code` for UX ("why did my payment fail?")
- Has `idempotency_key` for API safety
- Powers transaction history, notifications, dispute resolution
- In production: also carries description, category, metadata, compliance flags, FX rate

## LedgerEntry = The Accounting Record

**Audience:** Finance, compliance, auditors, regulators.

**Purpose:** Records *how money moved* in [[double-entry-accounting]] form.

- **Immutable** — you never update or delete a ledger row. Corrections are posted as reversals.
- Balance is *derived* from `SUM(credits) - SUM(debits)`, never stored as a mutable field on the account
- Forms the audit trail that satisfies regulators (MAS, FCA, APRA depending on jurisdiction)
- In production: this is what reconciliation runs against nightly

---

## Why Not One Table?

| Property | Transfer | LedgerEntry |
|----------|----------|-------------|
| Mutable? | Yes (status changes) | Never |
| Deletable? | Soft-delete possible | Never (post reversal instead) |
| Query pattern | By user, by date range | By account, aggregate sums |
| Source of truth for | "What happened" | "Where is the money" |

Additionally, a single transfer can produce **multiple** ledger entries:

```
Transfer: "Sam sends USD 100 to Alice (cross-currency)"
  -> LedgerEntry 1: debit Sam USD $100,       credit FX Pool USD $100
  -> LedgerEntry 2: debit FX Pool MYR $450,   credit Alice MYR $450
  -> LedgerEntry 3: debit Sam USD $1.50,      credit Revenue USD $1.50  (fee)
```

The bank's **FX Pool** (or nostro account) sits in between — it absorbs the currency mismatch. The pool's balance is the bank's FX exposure that treasury manages.

---

## Debit/Credit Semantics (Bank's Perspective)

Customer accounts are **liabilities** to the bank (the bank *owes* the customer). For liabilities:

| Action | Effect on customer balance |
|--------|---------------------------|
| **Debit** | Decreases (money departs) |
| **Credit** | Increases (money arrives) |

Mnemonic: **D**ebit = **D**eparts.

This maps directly to the codebase's `LedgerEntry` model:
- `debit_account_id` = money leaves this account
- `credit_account_id` = money arrives at this account

---

## What Production Systems Add

Real core banking ledgers (Thought Machine Vault, Mambu, or custom-built like Monzo's) add:

- **Journal** — a group of entries that must sum to zero (double-entry invariant enforced at DB level)
- **Entry types** — hold, release, settlement, fee, interest, reversal, adjustment
- **Effective date vs posted date** — "when did it economically happen" vs "when did we book it"
- **Separate services** — payments service owns Transfer; core banking/ledger service owns LedgerEntry; they communicate via internal API

---

## In Minibank

Currently Transfer and LedgerEntry have a 1:1 relationship. This is a valid simplification for learning. The moment fees, FX, or holds are introduced, it becomes 1:N and a `journal_id` column is needed to group related entries.

See also: [[p2p-transfer-deep-dive]], [[idempotency-client-guide]]
