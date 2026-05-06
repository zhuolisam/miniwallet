"""Tests for app/events/schemas.py — typed event contracts.

These are pure unit tests — no DB, no Kafka, no containers.
They verify that:
  1. Valid events parse into the correct typed envelope + payload.
  2. Missing or extra fields raise ValidationError.
  3. Unknown event types pass through without crashing (forward-compatible).
"""

import logging
import uuid

import pytest
from pydantic import ValidationError

from app.events.schemas import (
    AccountOpenedPayload,
    EventEnvelope,
    SeedCompletedPayload,
    TransferCompletedPayload,
    TransferFailedPayload,
    parse_event,
)
from tests.conftest import make_event


# --- parse_event: happy paths ---


def test_parse_transfer_completed():
    event = make_event("transfer.completed", {
        "transfer_id": str(uuid.uuid4()),
        "from_account_id": str(uuid.uuid4()),
        "to_account_id": str(uuid.uuid4()),
        "amount": "100.00000000",
        "entry_type": "transfer",
        "idempotency_key": "key-1",
    })
    envelope, payload = parse_event(event)
    assert isinstance(envelope, EventEnvelope)
    assert isinstance(payload, TransferCompletedPayload)
    assert payload.amount == "100.00000000"
    assert payload.entry_type == "transfer"


def test_parse_transfer_failed():
    event = make_event("transfer.failed", {
        "transfer_id": str(uuid.uuid4()),
        "from_account_id": str(uuid.uuid4()),
        "to_account_id": str(uuid.uuid4()),
        "amount": "50.00000000",
        "failure_code": "INSUFFICIENT_BALANCE",
        "entry_type": "transfer",
        "idempotency_key": "test-key-1",
    })
    envelope, payload = parse_event(event)
    assert isinstance(payload, TransferFailedPayload)
    assert payload.failure_code == "INSUFFICIENT_BALANCE"


def test_parse_account_opened():
    event = make_event("account.opened", {
        "account_id": str(uuid.uuid4()),
        "user_id": str(uuid.uuid4()),
        "status": "active",
    })
    envelope, payload = parse_event(event)
    assert isinstance(payload, AccountOpenedPayload)
    assert payload.status == "active"


def test_parse_seed_completed():
    event = make_event("seed.completed", {
        "account_id": str(uuid.uuid4()),
        "user_id": str(uuid.uuid4()),
        "amount": "1000.00000000",
        "entry_type": "seed",
    })
    envelope, payload = parse_event(event)
    assert isinstance(payload, SeedCompletedPayload)
    assert payload.entry_type == "seed"


# --- parse_event: envelope fields ---


def test_envelope_fields_preserved():
    actor = str(uuid.uuid4())
    event = make_event("transfer.completed", {
        "transfer_id": str(uuid.uuid4()),
        "from_account_id": str(uuid.uuid4()),
        "to_account_id": str(uuid.uuid4()),
        "amount": "1.00000000",
        "entry_type": "transfer",
        "idempotency_key": "k",
    }, actor_id=actor)
    envelope, _ = parse_event(event)
    assert envelope.event_id == event["event_id"]
    assert envelope.event_type == "transfer.completed"
    assert envelope.version == "1"
    assert envelope.actor_id == actor


# --- parse_event: validation errors ---


def test_missing_payload_field_raises():
    """A transfer.completed payload missing 'idempotency_key' must raise ValidationError."""
    event = make_event("transfer.completed", {
        "transfer_id": str(uuid.uuid4()),
        "from_account_id": str(uuid.uuid4()),
        "to_account_id": str(uuid.uuid4()),
        "amount": "100.00000000",
        "entry_type": "transfer",
        # idempotency_key intentionally missing
    })
    with pytest.raises(ValidationError):
        parse_event(event)


def test_missing_envelope_field_raises():
    """An event missing 'event_type' at the envelope level must raise ValidationError."""
    raw = {
        "event_id": str(uuid.uuid4()),
        # event_type intentionally missing
        "occurred_at": "2024-01-15T10:00:00Z",
        "version": "1",
        "actor_id": None,
        "payload": {},
    }
    with pytest.raises(ValidationError):
        parse_event(raw)


# --- parse_event: unknown event types (forward-compatible) ---


def test_unknown_event_type_passes_through_with_warning(caplog):
    """An unknown event type returns raw dict but logs a warning — operators must notice."""
    event = make_event("some.future.event", {"foo": "bar"})
    with caplog.at_level(logging.WARNING, logger="app.events.schemas"):
        envelope, payload = parse_event(event)
    assert isinstance(envelope, EventEnvelope)
    assert isinstance(payload, dict)
    assert payload["foo"] == "bar"
    assert "Unknown event_type=some.future.event" in caplog.text
