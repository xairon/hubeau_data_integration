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
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Copy project files needed for installation
# Cache bust: 2025-10-31-11:53
COPY pyproject.toml README.md /app/
COPY src /app/src

# Install dependencies using uv (100x faster than pip!)
# --no-cache: Don't cache packages (smaller image)
# --system: Install to system Python (not venv)
# -e: Editable install (symlink to /app/src for hot reload)
# .[worker]: Install with worker extra dependencies
RUN uv pip install --no-cache --system -e ".[worker]"

# Prevent Python from writing bytecode
ENV PYTHONDONTWRITEBYTECODE=1

# Copy remaining application code
COPY configs /app/configs
COPY scripts /app/scripts

# Make entrypoint executable
RUN chmod +x /app/scripts/worker_entrypoint.sh

ENV PYTHONPATH=/app/src:/app
ENV DAGSTER_HOME=/app/dagster_home

# Healthcheck (GRPC API)
HEALTHCHECK --interval=30s --timeout=10s --start-period=90s --retries=3 \
    CMD python -c "import socket; s=socket.socket(); s.connect(('localhost', 4000)); s.close()" || exit 1

EXPOSE 4000 9091

# Start worker with metrics server
CMD ["/app/scripts/worker_entrypoint.sh"]
