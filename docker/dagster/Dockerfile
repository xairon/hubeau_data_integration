FROM python:3.11-slim

# Install system dependencies including uv
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
    && rm -rf /var/lib/apt/lists/*

# Install uv (ultra-fast Python package installer)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

WORKDIR /app

# Copy project files
COPY pyproject.toml README.md /app/
COPY src /app/src
COPY configs /app/configs

# Install dependencies using uv (100x faster than pip!)
# Install all dependencies (production + worker)
RUN uv pip install --no-cache --system .

ENV PYTHONPATH=/app/src:/app

EXPOSE 3000
