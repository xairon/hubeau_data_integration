# Dagster Orchestrator - Image LÉGÈRE
# Responsabilités : UI, scheduling, coordination
# Taille cible : ~500MB
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
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Copy project files (need README.md for package metadata)
COPY pyproject.toml README.md /app/

# Install dependencies using uv (100x faster than pip!)
# --no-cache: Don't cache packages (smaller image)
# --system: Install to system Python (not venv)
# .[orchestrator]: Install orchestrator-only dependencies
RUN uv pip install --no-cache --system ".[orchestrator]"

# Copy only orchestrator configuration (pas le code métier)
COPY dagster_home /app/dagster_home
COPY configs /app/configs

ENV PYTHONPATH=/app
ENV DAGSTER_HOME=/app/dagster_home

# Healthcheck
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:3000/server_info || exit 1

EXPOSE 3000 4000

# Default command (override in docker-compose)
CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000", "-w", "/app/dagster_home/workspace.yaml"]
