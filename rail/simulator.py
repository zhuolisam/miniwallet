"""Bank rail simulator — a controllable stand-in for the real payment rail.

In production, `send_withdrawal` would POST to a ClearBank / Modulr / Railsr
endpoint and await their response. Those services return either success (with
their own transaction reference, e.g. FPS end-to-end identifier) or a typed
failure code. They are ALSO unreliable in the same ways real external APIs
are unreliable: timeouts, connection resets, 5xx blips, validation rejections.

This simulator gives us the same surface area with two knobs for teaching:

1. `RAIL_FAILURE_RATE` (env var, 0.0–1.0): probabilistic random failures for
   load-testing the saga's compensation path. Default 0.0 for deterministic tests.

2. `force_next_outcome(outcome)`: test-only hook. Queue a specific result for
   the next call regardless of withdrawal ID. FIFO — multiple calls queue
   multiple outcomes consumed in order, so a test asserting "rail fails once,
   recovery retries, second call succeeds" is straightforward.

This is TEACHER-PROVIDED infrastructure — students implement the SAGA that
calls this, not the simulator itself. Treat it like a vendor SDK.
"""

import os
import random
import uuid
from dataclasses import dataclass


@dataclass
class RailResult:
    """Return value of a successful send. `reference` is the rail's own txn ID."""
    success: bool
    reference: str | None = None
    failure_code: str | None = None


@dataclass
class RailStatus:
    """Return value of a status query. `state` is the rail's view of the txn."""
    state: str  # "completed" | "failed" | "processing"
    reason: str | None = None


class RailError(Exception):
    """Raised when the rail rejects or times out a send.

    `code` is one of the failure codes documented in SYSTEM-DESIGN Section 4
    / PRD US-3.2: INVALID_ACCOUNT | BENEFICIARY_CLOSED | TIMEOUT | NETWORK_ERROR.
    """

    def __init__(self, code: str):
        self.code = code
        super().__init__(f"Rail error: {code}")


_DEFAULT_FAILURE_CODES = (
    "INVALID_ACCOUNT",
    "BENEFICIARY_CLOSED",
    "TIMEOUT",
    "NETWORK_ERROR",
)


class BankRailSimulator:
    """Single-process simulator. Instantiated once per API process via app.state."""

    def __init__(self):
        self.failure_rate = float(os.environ.get("RAIL_FAILURE_RATE", "0.0"))
        # FIFO queue of outcomes that fire regardless of withdrawal_id.
        # Consumed before checking failure_rate.
        self._forced_next: list[str] = []

    async def send_withdrawal(
        self,
        withdrawal_id,
        amount,
        destination,
    ) -> RailResult:
        """Simulate submitting a withdrawal to the external rail.

        Returns a RailResult on success. Raises RailError on failure — callers
        should NOT treat the two as symmetric; exceptions surface through the
        saga's try/except and drive compensation.
        """
        if self._forced_next:
            forced = self._forced_next.pop(0)
            if forced == "success":
                return RailResult(success=True, reference=f"RAIL-{uuid.uuid4().hex[:8]}")
            code = forced.split(":", 1)[1] if ":" in forced else "NETWORK_ERROR"
            raise RailError(code)

        if random.random() < self.failure_rate:
            raise RailError(random.choice(_DEFAULT_FAILURE_CODES))

        return RailResult(success=True, reference=f"RAIL-{uuid.uuid4().hex[:8]}")

    async def query_status(self, external_reference: str) -> RailStatus:
        """Query a previously submitted withdrawal's state.

        Used by the Week 12 saga recovery job when a withdrawal is stuck in
        `submitted` and we need to ask the rail whether it actually landed.
        For the Weeks 10–11 scope, withdrawals transition `submitted → completed`
        synchronously in the same request so this is never called from the
        happy path — it exists here so the rail interface is complete.
        """
        return RailStatus(state="completed")

    def force_next_outcome(self, outcome: str) -> None:
        """Test-only: queue an outcome for the next send_withdrawal() call regardless of ID.

        `outcome` is "success" or "fail:<CODE>", e.g. "fail:TIMEOUT".
        FIFO — multiple calls queue multiple outcomes consumed in order.
        """
        self._forced_next.append(outcome)
