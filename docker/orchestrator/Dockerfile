# Dagster Orchestrator - Image LÉGÈRE
# Responsabilités : UI, scheduling, coordination
# Taille cible : ~500MB

FROM python:3.11-slim

# Minimal system dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        curl \
        postgresql-client \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies (LÉGER - pas de GDAL/GEOS)
COPY requirements-orchestrator.txt /app/requirements-orchestrator.txt
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r /app/requirements-orchestrator.txt

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
