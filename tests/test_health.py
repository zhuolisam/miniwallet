"""Tests for GET /v1/health endpoint.

Verifies:
1. Returns 200 with circuit breaker state
2. Shows DB and Redis connectivity status
3. Circuit breaker state is accurately reflected
"""

import pytest
import pytest_asyncio
from datetime import datetime, timezone

from app.circuit_breaker import CircuitBreaker, _KEY_STATE, _KEY_FAILURE_COUNT
from app.dependencies import get_circuit_breaker


class TestHealthEndpoint:
    """Test the health check endpoint."""

    async def test_health_ok(self, client):
        response = await client.get("/v1/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ok"
        assert data["checks"]["database"] == "ok"
        assert data["checks"]["redis"] == "ok"
        assert data["circuit_breaker"]["state"] == "CLOSED"
        pass

    async def test_health_shows_circuit_breaker_open(self, client, redis_client):
        await redis_client.set(_KEY_STATE, "OPEN")
        await redis_client.set(_KEY_FAILURE_COUNT, "3")
        response = await client.get("/v1/health")
        assert response.status_code == 200
        data = response.json()
        assert data["circuit_breaker"]["state"] == "OPEN"
        assert data["circuit_breaker"]["failure_count"] == 3
        assert data["status"] == "ok"
        assert data["checks"]["database"] == "ok"
        assert data["checks"]["redis"] == "ok"
