#!/bin/sh
# Exit immediately if any command fails — if migrations fail, the container
# crashes instead of starting uvicorn against a broken schema. Docker's restart
# policy will surface the failure rather than silently serving 500s.
set -e

alembic upgrade head
exec uvicorn app.main:app --host 0.0.0.0 --port 8000
