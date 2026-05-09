"""Typed event contracts for the Kafka event pipeline.

Every event published to Kafka has a fixed envelope (EventEnvelope) wrapping
a payload whose shape depends on the event_type. These Pydantic models are the
single source of truth for event structure — shared between producers (who
construct them) and consumers (who validate them via parse_event()).

A field typo at publish time is now a Pydantic ValidationError, not a silent
KeyError at 3 AM when a consumer tries to read a misspelled field.
"""

import logging

from pydantic import BaseModel

logger = logging.getLogger(__name__)


# --- Envelope ---

class EventEnvelope(BaseModel):
    """Common wrapper for all domain events."""
    event_id: str
    event_type: str
    occurred_at: str
    version: str
    actor_id: str | None
    payload: dict


# --- Payload models (one per event_type) ---

class EventPayload(BaseModel):
    """Base class for event payloads. Not strictly necessary, but useful for type hints."""
    pass

class TransferCompletedPayload(EventPayload):
    transfer_id: str
    from_account_id: str
    to_account_id: str
    amount: str
    currency: str
    entry_type: str
    idempotency_key: str


class TransferFailedPayload(EventPayload):
    transfer_id: str
    from_account_id: str
    to_account_id: str
    amount: str
    currency: str
    failure_code: str
    entry_type: str
    idempotency_key: str


class AccountOpenedPayload(EventPayload):
    account_id: str
    user_id: str
    status: str


class SeedCompletedPayload(EventPayload):
    account_id: str
    user_id: str
    amount: str
    currency: str
    entry_type: str


# --- Dispatch table ---

PAYLOAD_MODELS: dict[str, type[EventPayload]] = {
    "transfer.completed": TransferCompletedPayload,
    "transfer.failed": TransferFailedPayload,
    "account.opened": AccountOpenedPayload,
    "seed.completed": SeedCompletedPayload,
}


# --- Consumer-side parser ---

def parse_event(raw: dict) -> tuple[EventEnvelope, EventPayload | dict]:
    """Parse a raw event dict into a typed envelope + payload.

    Raises pydantic.ValidationError if the structure doesn't match the contract.

    Unknown event types return the envelope with the raw payload dict and log a
    warning. An unknown type in a pipeline with typed contracts is a signal —
    either a new producer shipped without updating schemas, or something is
    misconfigured. The consumer doesn't crash (forward-compatible), but operators
    are made aware.
    """
    envelope = EventEnvelope(**raw)
    model_cls = PAYLOAD_MODELS.get(envelope.event_type)
    if model_cls:
        payload = model_cls(**envelope.payload)
    else:
        logger.warning(
            "Unknown event_type=%s (event_id=%s). No payload schema registered — "
            "passing raw dict. If this is a new event type, add a model to PAYLOAD_MODELS.",
            envelope.event_type,
            envelope.event_id,
        )
        payload = envelope.payload
    return envelope, payload
