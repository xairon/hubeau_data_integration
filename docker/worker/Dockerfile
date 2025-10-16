# DLT Worker - Image COMPLÈTE
# Responsabilités : Extraction, transformation, chargement
# Taille cible : ~2GB (avec GDAL/GEOS)

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

# Install Python dependencies (COMPLET - toutes destinations DLT)
COPY requirements-worker.txt /app/requirements-worker.txt
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r /app/requirements-worker.txt

# Copy application code
COPY src /app/src
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
