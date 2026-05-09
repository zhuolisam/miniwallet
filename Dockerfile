FROM python:3.12-slim

WORKDIR /app

# Install uv for fast dependency resolution
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Install dependencies first (layer caching)
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

# Make venv's python the default — worker commands use `python -m ...`, not `uv run`.
# Without this, `python` resolves to the system Python which has no project deps.
ENV PATH="/app/.venv/bin:$PATH"

# Copy application code
COPY app/ app/
COPY consumers/ consumers/
COPY workers/ workers/
COPY management/ management/
COPY alembic/ alembic/
COPY alembic.ini .
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

# Default command — overridden per service in docker-compose.yml
CMD ["./entrypoint.sh"]
