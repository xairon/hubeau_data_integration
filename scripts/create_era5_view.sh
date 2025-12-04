#!/bin/bash
# ============================================================================
# Create ERA5 Metadata View
# ============================================================================
# Creates a view without the netcdf_data (bytea) column to allow
# browsing ERA5 data in Adminer without memory issues.
#
# Usage:
#   bash scripts/create_era5_view.sh
# ============================================================================

set -e

CONTAINER_NAME="brgm-postgres"
PG_USER="postgres"
PG_DB="postgres"

echo ""
echo "=============================================="
echo "  ERA5 Metadata View Creation"
echo "=============================================="
echo ""

# Check if container is running
if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    echo "❌ Container ${CONTAINER_NAME} is not running"
    echo ""
    echo "Start PostgreSQL with:"
    echo "  docker compose up -d postgres"
    echo ""
    exit 1
fi

echo "✅ Container ${CONTAINER_NAME} is running"
echo ""

# Execute SQL script
echo "Creating view..."
echo ""

if docker exec -i ${CONTAINER_NAME} psql -U ${PG_USER} -d ${PG_DB} < scripts/create_era5_metadata_view.sql; then
    echo ""
    echo "=============================================="
    echo "  ✅ View Created Successfully"
    echo "=============================================="
    echo ""
    echo "In Adminer:"
    echo "  ❌ DO NOT open: era5_france_meteo_raw"
    echo "     (contains bytea - too large for browser)"
    echo ""
    echo "  ✅ USE this view: era5_france_meteo_raw_metadata"
    echo "     (metadata only - fast and browsable)"
    echo ""
    echo "=============================================="
    echo ""
else
    echo ""
    echo "❌ Failed to create view"
    exit 1
fi
