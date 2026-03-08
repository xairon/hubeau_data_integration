# Dagster Orchestrator - Image LÉGÈRE
# Responsabilités : UI, scheduling, coordination
# ⚠️ NO USER CODE — runs execute on the worker (code location) via GRPC
# Uses uv for ultra-fast dependency installation (100x faster than pip)

FROM python:3.11-slim

# Minimal system dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        curl \
        postgresql-client \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install uv (ultra-fast Python package installer)
COPY --from=ghcr.io/astral-sh/uv:0.6.2 /uv /bin/uv

# Install ONLY Dagster orchestrator packages (no user code!)
# The webserver and daemon communicate with user code via GRPC.
# See: https://docs.dagster.io/deployment/oss/deployment-options/docker
COPY docker/orchestrator/requirements.txt /app/requirements.txt
RUN uv pip install --no-cache --system -r requirements.txt

# Copy only orchestrator configuration
COPY docker/orchestrator/dagster_home /app/dagster_home

ENV DAGSTER_HOME=/app/dagster_home

# Healthcheck
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:3000/server_info || exit 1

EXPOSE 3000

# Default command (override in docker-compose)
CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000", "-w", "/app/dagster_home/workspace.yaml"]
