#!/bin/bash
# DLT Worker Entrypoint Script
# Starts Prometheus metrics server before launching Dagster GRPC API

set -e

echo "🚀 Starting DLT Worker..."

# Create Prometheus multiprocess directory
export PROMETHEUS_MULTIPROC_DIR=/tmp/prometheus
mkdir -p $PROMETHEUS_MULTIPROC_DIR
echo "✅ Created Prometheus multiprocess directory: $PROMETHEUS_MULTIPROC_DIR"

# Start Prometheus metrics HTTP server in background
echo "📊 Starting Prometheus metrics server on port 9091..."
python -m hubeau_pipeline.observability.metrics_server &
METRICS_PID=$!
echo "✅ Metrics server started (PID: $METRICS_PID)"

# Wait a bit for metrics server to start
sleep 2

# Start Dagster Code Server (GRPC API)
echo "🔧 Starting Dagster Code Server (GRPC API) on port 4000..."
exec dagster api grpc -h 0.0.0.0 -p 4000 -m hubeau_pipeline
