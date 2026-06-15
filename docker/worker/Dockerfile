# DLT Worker - Image COMPLÈTE
# Responsabilités : Extraction, transformation, chargement
# Taille cible : ~2GB (avec GDAL/GEOS)
# Uses uv for ultra-fast dependency installation (100x faster than pip)

FROM python:3.11-slim

# Full system dependencies (géospatial, database clients)
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        gdal-bin \
        libgdal-dev \
        libgeos-dev \
        libproj-dev \
        proj-bin \
        libspatialindex-dev \
        libpq-dev \
        curl \
        git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install uv (ultra-fast Python package installer)
COPY --from=ghcr.io/astral-sh/uv:0.6.2 /uv /bin/uv

# Copy project files needed for installation
# Cache bust: 2025-10-31-11:53
COPY pyproject.toml README.md /app/
COPY src /app/src

# Use /app/tmp for build temp files (avoids /tmp quota on fuse-overlayfs)
ENV TMPDIR=/app/tmp

# Install dependencies using uv (100x faster than pip!)
# --no-cache: Don't cache packages (smaller image)
# --system: Install to system Python (not venv)
# -e: Editable install (symlink to /app/src for hot reload)
# .[worker]: Install with worker extra dependencies
RUN mkdir -p /app/tmp && uv pip install --no-cache --system -e ".[worker]" && rm -rf /app/tmp

# Reset TMPDIR to default
ENV TMPDIR=/tmp

# Prevent Python from writing bytecode
ENV PYTHONDONTWRITEBYTECODE=1

# Copy remaining application code
COPY configs /app/configs

# Generate dbt manifest at build time (best practice for production)
# This ensures the manifest is always in sync with the dbt models
# and doesn't need to be regenerated at runtime
# Note: dbt parse doesn't require a DB connection, it only parses the project files
RUN cd /app/src/dbt_hubeau && \
    dbt deps --profiles-dir . --project-dir . && \
    dbt parse --profiles-dir . --project-dir . && \
    echo "✅ dbt manifest generated successfully"

ENV PYTHONPATH=/app/src:/app
ENV DAGSTER_HOME=/app/dagster_home

# Healthcheck (GRPC API)
HEALTHCHECK --interval=30s --timeout=10s --start-period=90s --retries=3 \
    CMD python -c "import socket; s=socket.socket(); s.connect(('localhost', 4000)); s.close()" || exit 1

EXPOSE 4000 9091

# Command is defined in docker-compose.yml for flexibility
# (allows hot reload with --watch flag in development)
