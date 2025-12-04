# ERA5 Time Series Extraction

## Overview

This script extracts ERA5 NetCDF data stored in PostgreSQL bytea columns and creates a normalized time series table for SQL analytics.

**Problem**: Adminer cannot display `era5_france_meteo_raw` table (contains 50-100 MB bytea per row)

**Solution**: Extract NetCDF data into a normalized table `era5_france_timeseries` with one row per (time, lat, lon)

---

## Architecture

### Source Table

```
staging.era5_france_meteo_raw
├── 38 rows (files)
├── ~80 MB per row (NetCDF in bytea)
└── Total: ~3 GB

Each NetCDF file contains:
- 730 timesteps (2 years × 365 days)
- ~10,000 grid points (0.25° grid over France)
- 3 variables (temperature, precipitation, evaporation)
```

### Target Table

```
staging.era5_france_timeseries
├── ~277 million rows
├── ~50 bytes per row
└── Total: ~13 GB (with indexes)

Schema:
- time (TIMESTAMP)
- latitude (NUMERIC)
- longitude (NUMERIC)
- temperature_2m (NUMERIC) - Celsius
- total_precipitation (NUMERIC) - mm
- potential_evaporation (NUMERIC) - mm
- source_file_id (TEXT)
```

---

## Usage

### Quick Start

```bash
# Linux/macOS
bash scripts/run_era5_extraction.sh

# Windows
scripts\run_era5_extraction.bat
```

### Advanced Options

```bash
# Extract specific file only (for testing)
bash scripts/run_era5_extraction.sh --file-id era5_france_2020_2021

# Custom batch size (default: 10000)
bash scripts/run_era5_extraction.sh --batch-size 5000

# Reprocess already extracted files
bash scripts/run_era5_extraction.sh --force
```

### Direct Python Execution

```bash
# Inside dlt_worker container
docker exec -it brgm-dlt-worker python /app/scripts/extract_era5_timeseries.py

# With options
docker exec -it brgm-dlt-worker python /app/scripts/extract_era5_timeseries.py \
    --file-id era5_france_2020_2021 \
    --batch-size 5000
```

---

## Expected Runtime

| Metric | Value |
|--------|-------|
| **Files to process** | 38 |
| **Time per file** | ~1-2 minutes |
| **Total runtime** | ~1 hour |
| **Rows inserted** | ~277 million |
| **Final table size** | ~13 GB (with indexes) |

**Note**: First file is slower (table creation + index building). Subsequent files are faster.

---

## Verification

### Check Extraction Progress

```bash
# Check how many files have been processed
docker exec -it brgm-postgres psql -U postgres -d postgres -c "
SELECT
    COUNT(DISTINCT source_file_id) AS files_processed,
    COUNT(*) AS total_rows,
    MIN(time) AS oldest_date,
    MAX(time) AS newest_date
FROM staging.era5_france_timeseries;
"
```

### Sample Queries

```sql
-- View sample data
SELECT time, latitude, longitude, temperature_2m, total_precipitation
FROM staging.era5_france_timeseries
LIMIT 10;

-- Daily average temperature for France (2023)
SELECT
    DATE(time) AS date,
    AVG(temperature_2m) AS avg_temp_c,
    AVG(total_precipitation) AS avg_precip_mm
FROM staging.era5_france_timeseries
WHERE time >= '2023-01-01' AND time < '2024-01-01'
GROUP BY DATE(time)
ORDER BY date;

-- Temperature at specific location (Paris: 48.86°N, 2.35°E)
SELECT time, temperature_2m, total_precipitation
FROM staging.era5_france_timeseries
WHERE
    latitude BETWEEN 48.75 AND 48.95
    AND longitude BETWEEN 2.25 AND 2.45
    AND time >= '2023-01-01'
ORDER BY time;

-- Hottest day in France (entire dataset)
SELECT DATE(time) AS date, AVG(temperature_2m) AS avg_temp_c
FROM staging.era5_france_timeseries
GROUP BY DATE(time)
ORDER BY avg_temp_c DESC
LIMIT 1;
```

---

## Troubleshooting

### Problem: "Module xarray not found"

**Solution**: The script must run inside `brgm-dlt-worker` container (has all dependencies).

```bash
# Use the wrapper scripts (recommended)
bash scripts/run_era5_extraction.sh

# Or execute directly in container
docker exec -it brgm-dlt-worker python /app/scripts/extract_era5_timeseries.py
```

### Problem: "Out of memory" error

**Solution**: Reduce batch size

```bash
bash scripts/run_era5_extraction.sh --batch-size 5000
```

### Problem: Extraction is slow

**Causes**:
1. **Index building** (first file is slower)
2. **Network I/O** (if running on remote server)
3. **Disk I/O** (PostgreSQL writing large volume)

**Optimization**: Run on server with SSD and sufficient RAM (8+ GB recommended)

### Problem: Script interrupted, how to resume?

The script automatically skips already processed files.

```bash
# Resume extraction (skips already processed files)
bash scripts/run_era5_extraction.sh

# Force reprocessing (if data is corrupt)
bash scripts/run_era5_extraction.sh --force
```

---

## What Gets Created

### Target Table

```sql
staging.era5_france_timeseries
```

### Indexes (for performance)

```sql
idx_era5_time                -- ON (time)
idx_era5_location            -- ON (latitude, longitude)
idx_era5_time_location       -- ON (time, latitude, longitude)
idx_era5_source_file         -- ON (source_file_id)
```

### Permissions

```sql
GRANT SELECT ON staging.era5_france_timeseries TO readonly;
```

---

## Adminer Usage

After extraction, you can browse the data in Adminer:

1. Open: http://localhost:18081 (via SSH tunnel)
2. Login: `postgres` / `readonly_2024_secure`
3. Table: `staging.era5_france_timeseries` ✅ (fast, browsable)

**Do NOT open**: `staging.era5_france_meteo_raw` ❌ (still contains bytea - will crash)

---

## Storage Comparison

| Table | Rows | Size | Browsable in Adminer |
|-------|------|------|----------------------|
| `era5_france_meteo_raw` | 38 | 3 GB | ❌ No (bytea) |
| `era5_france_timeseries` | 277M | 13 GB | ✅ Yes (paginated) |

**Note**: The time series table is larger but fully queryable and browsable.

---

## Benefits

### Before (bytea storage)

❌ Cannot browse in Adminer
❌ Must extract NetCDF to query data
❌ Complex analytics require Python/xarray
❌ No SQL joins with other tables

### After (time series table)

✅ Browse in Adminer (paginated)
✅ Direct SQL queries
✅ JOIN with piezometry/hydrometry data
✅ Create materialized views for dashboards
✅ Fast spatial/temporal queries with indexes

---

## Example Use Cases

### 1. Temperature anomaly analysis

```sql
-- Compare 2023 temperatures to historical average
WITH historical AS (
    SELECT
        EXTRACT(MONTH FROM time) AS month,
        EXTRACT(DAY FROM time) AS day,
        AVG(temperature_2m) AS avg_temp
    FROM staging.era5_france_timeseries
    WHERE time >= '1991-01-01' AND time < '2021-01-01'  -- 30-year baseline
    GROUP BY EXTRACT(MONTH FROM time), EXTRACT(DAY FROM time)
),
current AS (
    SELECT
        EXTRACT(MONTH FROM time) AS month,
        EXTRACT(DAY FROM time) AS day,
        AVG(temperature_2m) AS avg_temp
    FROM staging.era5_france_timeseries
    WHERE time >= '2023-01-01' AND time < '2024-01-01'
    GROUP BY EXTRACT(MONTH FROM time), EXTRACT(DAY FROM time)
)
SELECT
    h.month,
    h.day,
    c.avg_temp - h.avg_temp AS temp_anomaly_c
FROM historical h
JOIN current c USING (month, day)
ORDER BY temp_anomaly_c DESC
LIMIT 10;
```

### 2. Drought analysis

```sql
-- Find consecutive days with low precipitation
SELECT
    DATE(time) AS date,
    AVG(total_precipitation) AS precip_mm,
    COUNT(*) OVER (
        ORDER BY DATE(time)
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS days_in_window,
    AVG(total_precipitation) OVER (
        ORDER BY DATE(time)
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS rolling_30d_avg
FROM staging.era5_france_timeseries
WHERE time >= '2022-01-01'
GROUP BY DATE(time)
ORDER BY rolling_30d_avg
LIMIT 10;
```

### 3. JOIN with piezometry data

```sql
-- Correlate groundwater levels with precipitation
SELECT
    DATE_TRUNC('month', p.date_mesure) AS month,
    AVG(p.niveau_nappe_ngf) AS avg_groundwater_level,
    AVG(e.total_precipitation) AS avg_precipitation_mm
FROM staging.piezometry_chroniques_raw p
JOIN staging.era5_france_timeseries e
    ON DATE_TRUNC('month', p.date_mesure) = DATE_TRUNC('month', e.time)
WHERE
    p.date_mesure >= '2020-01-01'
    AND e.latitude BETWEEN 48.0 AND 49.0
    AND e.longitude BETWEEN 2.0 AND 3.0
GROUP BY DATE_TRUNC('month', p.date_mesure)
ORDER BY month;
```

---

## Cleanup (if needed)

To remove the extracted table and start over:

```sql
-- Drop table and all indexes
DROP TABLE IF EXISTS staging.era5_france_timeseries CASCADE;

-- Run extraction again
bash scripts/run_era5_extraction.sh
```

**Note**: The original bytea table (`era5_france_meteo_raw`) is never modified.

---

## FAQ

### Q: Should I delete the bytea table after extraction?

**A**: No! Keep both tables:
- `era5_france_meteo_raw`: Archive/backup (original NetCDF files)
- `era5_france_timeseries`: Analytics/queries (normalized data)

### Q: How much disk space is needed?

**A**: ~16 GB total:
- Original: 3 GB (bytea table)
- Extracted: 13 GB (time series + indexes)

### Q: Can I run this in parallel?

**A**: Not recommended. The script is already optimized with batching. Parallel processing would cause lock contention in PostgreSQL.

### Q: What if I add more ERA5 data later?

**A**: Run the extraction script again. It automatically skips already processed files.

```bash
# Extract only new files
bash scripts/run_era5_extraction.sh
```

---

**Author**: Hub'Eau Pipeline Team
**Date**: 2025-01-04
**Related Docs**:
- `docs/ERA5_DATA_STORAGE.md` - Architecture overview
- `docs/DLT_BEST_PRACTICES.md` - DLT pipeline best practices
