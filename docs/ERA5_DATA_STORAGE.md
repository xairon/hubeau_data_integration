# ERA5 Data Storage - Architecture & Best Practices

## Overview

ERA5 data is currently stored as **raw NetCDF4 files in PostgreSQL bytea columns**. This approach has trade-offs that affect how you can interact with the data.

---

## Current Architecture

### Storage Format

```
Table: staging.era5_france_meteo_raw
├── file_id (TEXT)               # Unique identifier (e.g., "era5_france_1950_1951")
├── variables (JSON)             # List of variables in NetCDF
├── start_year (INTEGER)         # Start year of chunk
├── end_year (INTEGER)           # End year of chunk
├── area (JSON)                  # Bounding box [North, West, South, East]
├── netcdf_data (BYTEA)          # 🔴 RAW NetCDF4 file (50-100 MB each)
├── file_size_mb (NUMERIC)       # File size in MB
├── download_timestamp (TIMESTAMP) # When file was downloaded
└── file_metadata (JSON)         # Dataset, resolution, etc.
```

### Statistics

| Metric | Value |
|--------|-------|
| **Total files** | ~38 files (1950-2025, 2-year chunks) |
| **File size** | 50-100 MB each |
| **Total storage** | ~3-4 GB in PostgreSQL |
| **Variables per file** | 3 (temperature, precipitation, evaporation) |
| **Timesteps per file** | ~730 (2 years × 365 days) |
| **Grid points per file** | ~10,000 (0.25° grid over France) |

---

## Problem: Adminer Cannot Display Data

### Symptom

Opening `era5_france_meteo_raw` table in Adminer causes:
- ❌ Page hang/timeout
- ❌ Browser memory exhaustion
- ❌ Adminer crashes

### Root Cause

1. **Adminer loads ALL rows** into memory when displaying a table
2. **Each row contains 50-100 MB** of binary data (NetCDF in bytea)
3. **38 rows × 80 MB = 3+ GB** loaded into browser → crash

### Why This Design?

The bytea storage was chosen for:
- ✅ **Reproducibility**: Keep original NetCDF files for validation
- ✅ **Simplicity**: No external file system needed
- ✅ **Portability**: Entire dataset in one PostgreSQL backup
- ❌ **Trade-off**: Not browsable in web UIs like Adminer

---

## Solutions

### Solution 1: Use Metadata View (IMMEDIATE) ✅

**Action**: Create a view that excludes the `netcdf_data` column.

#### Create the view

```bash
# Run helper script
bash scripts/create_era5_view.sh

# Or manually
docker exec -i brgm-postgres psql -U postgres -d postgres < scripts/create_era5_metadata_view.sql
```

#### Result

New view: `staging.era5_france_meteo_raw_metadata`

```sql
-- Browsable in Adminer (no bytea)
SELECT * FROM staging.era5_france_meteo_raw_metadata;
```

**Columns available**:
- `file_id`, `variables`, `start_year`, `end_year`
- `area`, `file_size_mb`, `download_timestamp`, `file_metadata`
- DLT metadata: `_dlt_load_id`, `_dlt_id`

**What's excluded**:
- `netcdf_data` (the 50-100 MB bytea column)

#### Usage in Adminer

| Table | Status | Action |
|-------|--------|--------|
| `era5_france_meteo_raw` | ❌ **DO NOT OPEN** | Contains bytea - will crash |
| `era5_france_meteo_raw_metadata` | ✅ **USE THIS** | Metadata only - fast |

---

### Solution 2: Extract NetCDF to File System (MEDIUM-TERM)

**Concept**: Store NetCDF files on disk instead of PostgreSQL.

#### Architecture

```
/data/era5/
├── era5_france_1950_1951.nc  (80 MB)
├── era5_france_1952_1953.nc  (80 MB)
├── ...
└── era5_france_2024_2025.nc  (80 MB)

PostgreSQL table:
├── file_id
├── file_path: "/data/era5/era5_france_1950_1951.nc"
├── file_size_mb
└── metadata...
```

#### Pros/Cons

| Aspect | Assessment |
|--------|------------|
| PostgreSQL size | ✅ Much smaller (~1 MB vs 3 GB) |
| Adminer browsing | ✅ Fast, no issues |
| Backup complexity | ❌ Must backup DB + files |
| Docker volume | ❌ Requires persistent volume mount |
| Migration effort | ⚠️ Moderate (pipeline changes) |

---

### Solution 3: Store Extracted Time Series (LONG-TERM - RECOMMENDED)

**Concept**: Extract NetCDF data into normalized PostgreSQL tables.

#### Architecture

```sql
-- Original: 38 rows × 50 MB = 3 GB (bytea)
-- New: ~10 million rows × 100 bytes = 1 GB (structured)

CREATE TABLE staging.era5_france_timeseries (
    id BIGSERIAL PRIMARY KEY,
    time TIMESTAMP NOT NULL,
    latitude NUMERIC(6,3) NOT NULL,
    longitude NUMERIC(6,3) NOT NULL,
    temperature_2m NUMERIC(5,2),      -- Celsius
    total_precipitation NUMERIC(8,4),  -- mm
    potential_evaporation NUMERIC(8,4) -- mm
);

CREATE INDEX ON era5_france_timeseries (time);
CREATE INDEX ON era5_france_timeseries (latitude, longitude);
```

#### Example Pipeline Change

```python
import xarray as xr

# Open NetCDF
ds = xr.open_dataset(tmp_path)

# Extract to DataFrame
df = ds.to_dataframe().reset_index()

# Yield individual time series rows
for row in df.itertuples():
    yield {
        'time': row.time,
        'latitude': row.latitude,
        'longitude': row.longitude,
        'temperature_2m': row.t2m - 273.15,  # Convert K to °C
        'total_precipitation': row.tp * 1000,  # Convert m to mm
        'potential_evaporation': row.pev * 1000,
    }
```

#### Pros/Cons

| Aspect | Assessment |
|--------|------------|
| SQL queryability | ✅ Perfect (WHERE, JOIN, GROUP BY) |
| Adminer browsing | ✅ Fast, paginated |
| Analysis ready | ✅ No NetCDF processing needed |
| Storage size | ✅ Smaller (1 GB vs 3 GB) |
| Query performance | ✅ Excellent with indexes |
| Original data | ❌ Lost (unless stored separately) |
| Migration effort | ❌ High (major pipeline rewrite) |

#### Example Queries

```sql
-- Daily average temperature for Paris (48.86°N, 2.35°E)
SELECT
    DATE(time) AS date,
    AVG(temperature_2m) AS avg_temp_celsius
FROM staging.era5_france_timeseries
WHERE
    latitude BETWEEN 48.8 AND 48.9
    AND longitude BETWEEN 2.3 AND 2.4
    AND time >= '2023-01-01'
    AND time < '2024-01-01'
GROUP BY DATE(time)
ORDER BY date;

-- Total precipitation by month (France-wide average)
SELECT
    DATE_TRUNC('month', time) AS month,
    AVG(total_precipitation) AS avg_precip_mm
FROM staging.era5_france_timeseries
WHERE time >= '2020-01-01'
GROUP BY DATE_TRUNC('month', time)
ORDER BY month;
```

---

## Decision Matrix

| Criteria | Solution 1: View | Solution 2: File System | Solution 3: Extracted Time Series |
|----------|------------------|-------------------------|-----------------------------------|
| **Implementation time** | ⚡ 5 minutes | ⏱️ 2-3 hours | 🐌 1-2 days |
| **Adminer browsing** | ✅ Yes (metadata only) | ✅ Yes (all columns) | ✅ Yes (all data queryable) |
| **SQL analytics** | ❌ No (need to extract NetCDF) | ❌ No (need to extract NetCDF) | ✅ Yes (native SQL) |
| **Storage efficiency** | 🟡 Same (3 GB in DB) | 🟢 Better (1 MB in DB) | 🟢 Better (1 GB in DB) |
| **Original data preserved** | ✅ Yes | ✅ Yes | ❌ No (unless stored separately) |
| **Backup complexity** | 🟢 Simple (DB only) | 🔴 Complex (DB + files) | 🟢 Simple (DB only) |
| **Migration required** | 🟢 No | 🟡 Yes (pipeline) | 🔴 Yes (major rewrite) |

---

## Recommendation

### Immediate (Today)
✅ **Use Solution 1** (Metadata View)
- Run: `bash scripts/create_era5_view.sh`
- Browse: `era5_france_meteo_raw_metadata` in Adminer
- Original data still accessible via SQL:
  ```sql
  -- Get NetCDF file for a specific period
  SELECT netcdf_data
  FROM staging.era5_france_meteo_raw
  WHERE file_id = 'era5_france_2020_2021';
  ```

### Short-term (Next Sprint)
🔄 **Evaluate Solution 3** (Extracted Time Series)
- Better for analytics and dashboards
- Queryable without NetCDF processing
- Standard approach for climate data warehouses

### Keep in Mind
The current bytea approach is **valid for archival**, but not for **interactive analysis**.

---

## Commands Reference

### Create Metadata View

```bash
# Via helper script
bash scripts/create_era5_view.sh

# Manual SQL
docker exec -i brgm-postgres psql -U postgres -d postgres <<EOF
CREATE VIEW staging.era5_france_meteo_raw_metadata AS
SELECT
    file_id, variables, start_year, end_year, area,
    file_size_mb, download_timestamp, file_metadata,
    _dlt_load_id, _dlt_id
FROM staging.era5_france_meteo_raw;

GRANT SELECT ON staging.era5_france_meteo_raw_metadata TO readonly;
EOF
```

### Query Metadata (via psql)

```bash
# Via Docker
docker exec -it brgm-postgres psql -U postgres -d postgres

# Query metadata
SELECT file_id, start_year, end_year, file_size_mb, download_timestamp
FROM staging.era5_france_meteo_raw_metadata
ORDER BY start_year;

# Get NetCDF for specific period (warning: large output)
\o /tmp/era5_2020_2021.nc
SELECT netcdf_data FROM staging.era5_france_meteo_raw WHERE file_id = 'era5_france_2020_2021';
\o
```

### Export NetCDF File

```bash
# Export single NetCDF file to host
docker exec -i brgm-postgres psql -U postgres -d postgres -t -c "
  SELECT encode(netcdf_data, 'base64')
  FROM staging.era5_france_meteo_raw
  WHERE file_id = 'era5_france_2020_2021';
" | base64 -d > era5_france_2020_2021.nc
```

---

## Future Considerations

### If You Need Full SQL Analytics

Consider implementing Solution 3 (Extracted Time Series) with:

1. **Partitioning by year**:
   ```sql
   CREATE TABLE era5_france_timeseries_2020 PARTITION OF era5_france_timeseries
   FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');
   ```

2. **Compression**:
   ```sql
   -- TimescaleDB for time-series optimization
   SELECT create_hypertable('era5_france_timeseries', 'time');
   ```

3. **Aggregated views**:
   ```sql
   -- Daily aggregates (pre-computed)
   CREATE MATERIALIZED VIEW era5_daily_avg AS
   SELECT DATE(time) AS date, AVG(temperature_2m) AS avg_temp
   FROM era5_france_timeseries
   GROUP BY DATE(time);
   ```

---

## FAQ

### Q: Can I delete the original NetCDF data?

**A**: Only if you've extracted it to Solution 2 (file system) or Solution 3 (time series) and validated the extraction. The bytea storage is your only copy.

### Q: How do I access the NetCDF data for analysis?

**A**: Extract it via psql (see commands above) or use a Python script:

```python
import psycopg2
import io
import xarray as xr

# Connect to PostgreSQL
conn = psycopg2.connect("postgresql://postgres:password@localhost:5432/postgres")
cur = conn.cursor()

# Fetch NetCDF data
cur.execute("SELECT netcdf_data FROM staging.era5_france_meteo_raw WHERE file_id = 'era5_france_2020_2021'")
netcdf_bytes = cur.fetchone()[0]

# Load into xarray
ds = xr.open_dataset(io.BytesIO(netcdf_bytes))
print(ds)
```

### Q: Why not use PostGIS for spatial data?

**A**: ERA5 is raster (grid) data, not vector. PostGIS is optimized for vector geometries (points, lines, polygons). For raster time-series, normalized tables (Solution 3) + TimescaleDB is better.

---

**Last Updated**: 2025-01-04
**Author**: Hub'Eau Pipeline Team
