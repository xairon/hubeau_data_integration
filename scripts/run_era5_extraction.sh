#!/bin/bash
# ============================================================================
# ERA5 Time Series Extraction - Wrapper Script
# ============================================================================
# Extracts ERA5 NetCDF data from PostgreSQL bytea to normalized time series
#
# Usage:
#   bash scripts/run_era5_extraction.sh [OPTIONS]
#
# Options:
#   --file-id FILE_ID    Process specific file only
#   --batch-size SIZE    Batch size for inserts (default: 10000)
#   --force              Reprocess already extracted files
#
# Examples:
#   # Extract all files
#   bash scripts/run_era5_extraction.sh
#
#   # Extract specific file
#   bash scripts/run_era5_extraction.sh --file-id era5_france_2020_2021
#
#   # Extract all files with smaller batches
#   bash scripts/run_era5_extraction.sh --batch-size 5000
# ============================================================================

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo ""
echo "=============================================="
echo "  ERA5 Time Series Extraction"
echo "=============================================="
echo ""

# Check if Docker is running
if ! docker ps > /dev/null 2>&1; then
    echo -e "${RED}❌ Docker is not running${NC}"
    echo ""
    echo "Please start Docker Desktop and try again."
    exit 1
fi

# Check if postgres container is running
if ! docker ps --format '{{.Names}}' | grep -q "^brgm-postgres$"; then
    echo -e "${RED}❌ PostgreSQL container is not running${NC}"
    echo ""
    echo "Start PostgreSQL with:"
    echo "  docker compose up -d postgres"
    exit 1
fi

echo -e "${GREEN}✅ Docker and PostgreSQL are running${NC}"
echo ""

# Load .env file
if [ -f .env ]; then
    echo -e "${GREEN}✅ Loading .env file${NC}"
    export $(grep -v '^#' .env | xargs)
else
    echo -e "${YELLOW}⚠️  No .env file found, using defaults${NC}"
fi

echo ""
echo "Running extraction inside dlt_worker container..."
echo ""
echo "This will:"
echo "  1. Read NetCDF files from staging.era5_france_meteo_raw (bytea)"
echo "  2. Extract time series data (time, lat, lon, temp, precip, evap)"
echo "  3. Insert into staging.era5_france_timeseries (normalized table)"
echo ""
echo "Expected:"
echo "  - ~38 files to process"
echo "  - ~7.3M rows per file"
echo "  - ~277M total rows"
echo "  - ~1-2 minutes per file"
echo "  - ~1 hour total runtime"
echo ""
echo -e "${YELLOW}⚠️  This is a long-running operation. Press Ctrl+C to cancel.${NC}"
echo ""
sleep 3

# Run extraction script inside dlt_worker container
# (dlt_worker has all required Python packages: psycopg2, pandas, xarray, h5netcdf)
docker exec -it brgm-dlt-worker python /app/scripts/extract_era5_timeseries.py "$@"

echo ""
echo "=============================================="
echo -e "${GREEN}✅ Extraction Complete${NC}"
echo "=============================================="
echo ""
echo "New table: staging.era5_france_timeseries"
echo ""
echo "Query examples:"
echo ""
echo "  # View sample data"
echo "  docker exec -it brgm-postgres psql -U postgres -d postgres -c \\"
echo "    \"SELECT time, latitude, longitude, temperature_2m, total_precipitation \\"
echo "     FROM staging.era5_france_timeseries LIMIT 10;\""
echo ""
echo "  # Check row count"
echo "  docker exec -it brgm-postgres psql -U postgres -d postgres -c \\"
echo "    \"SELECT COUNT(*) FROM staging.era5_france_timeseries;\""
echo ""
echo "  # Daily average temperature for France"
echo "  docker exec -it brgm-postgres psql -U postgres -d postgres -c \\"
echo "    \"SELECT DATE(time) AS date, AVG(temperature_2m) AS avg_temp_c \\"
echo "     FROM staging.era5_france_timeseries \\"
echo "     WHERE time >= '2023-01-01' AND time < '2024-01-01' \\"
echo "     GROUP BY DATE(time) ORDER BY date LIMIT 10;\""
echo ""
echo "Now you can browse this table in Adminer!"
echo ""
