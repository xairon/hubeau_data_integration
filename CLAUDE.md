# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Hub'Eau Data Pipeline is a production-grade data warehouse for French hydrological data, following a **Medallion Architecture** (Bronze → Silver → Gold). It orchestrates data ingestion from Hub'Eau APIs and ERA5 climate data, transforms them using dbt, and serves analytics via Apache Superset.

**Key Technologies**: Dagster (orchestration), DLT (ingestion), dbt (transformation), PostgreSQL 16 + TimescaleDB + PostGIS

## Essential Commands

### 🚨 FIRST TIME SETUP - CRÉER LES VOLUMES (CRITIQUE)
```bash
# ⚠️ OBLIGATOIRE avant le premier "docker compose up"
# Les volumes externes protègent les données contre suppression accidentelle

# Linux/Mac:
bash scripts/init_volumes.sh

# Windows:
scripts\init_volumes.bat

# OU manuellement:
docker volume create brgm_postgres_data
docker volume create brgm_dagster_pg_data
docker volume create brgm_cloudbeaver_data
```

**IMPORTANT**: Les volumes sont maintenant **externes** - ils NE SERONT PAS supprimés par `docker compose down -v`. C'est voulu pour protéger vos données.

### Docker Operations
```bash
# Start the entire stack (volumes doivent exister!)
docker compose up -d --build

# Check service status
docker compose ps

# View logs (most useful for debugging)
docker compose logs -f dlt_worker
docker compose logs -f dagster_webserver

# Restart a service after code changes
docker compose restart dlt_worker

# Complete rebuild (when dependencies change)
docker compose down
docker compose build --no-cache
docker compose up -d

# Arrêt propre (SAFE - ne supprime PAS les volumes externes)
docker compose down

# ⚠️ Pour supprimer les données (DANGEREUX):
docker volume rm brgm_postgres_data brgm_dagster_pg_data brgm_cloudbeaver_data
```

### Database Access
```bash
# Connect to PostgreSQL
docker exec -it brgm-postgres psql -U postgres -d postgres

# Inside psql - inspect tables by schema
\dt bronze.*    # Raw data from APIs
\dt silver.*    # Cleaned data
\dt gold.*      # Analytics tables

# Count rows across all schemas
SELECT schemaname, tablename, n_live_tup AS rows
FROM pg_stat_user_tables
WHERE schemaname IN ('bronze', 'silver', 'gold')
ORDER BY n_live_tup DESC;
```

### dbt Commands (inside worker container)
```bash
# Run full transformation pipeline
docker exec brgm-dlt-worker dbt run

# Run specific models
docker exec brgm-dlt-worker dbt run --select stg_piezo_chroniques
docker exec brgm-dlt-worker dbt run --select int_daily_measurements+

# Run data quality tests
docker exec brgm-dlt-worker dbt test

# Check source freshness
docker exec brgm-dlt-worker dbt source freshness

# Generate documentation (catalog + manifest)
docker exec brgm-dlt-worker dbt docs generate

# Serve documentation locally (inside container)
docker exec brgm-dlt-worker dbt docs serve --port 8080

# Force rebuild of incremental models
docker exec brgm-dlt-worker dbt run --select int_station_era5_mapping+ --vars '{"recompute_station_era5_mapping": true}'
```

**dbt Documentation**: Auto-généré hebdomadairement via le job `dbt_docs_job` (schedule: dimanche 5h UTC)

### Dagster Operations
- **Web UI**: http://localhost:49500
- **Jobs** are launched via the Dagster UI (Launchpad tab)
- **Schedules** are configured in `src/hubeau_pipeline/schedules.py` and controlled by `DAGSTER_ENABLE_SCHEDULES` env var

**Available Jobs**:
- `full_bootstrap_job`: Complete database population (TME → stations → chroniques → ERA5 → dbt)
- `reference_data_bronze_job`: Load TME reference data only

**dbt Jobs (4-Stage Pipeline)**:
```
Stage 1: dbt_shared_staging_job     → ERA5 timeseries + grid points (prerequisite)
Stage 2: dbt_piezo_pipeline_job     → Piezometry domain (can run in PARALLEL)
         dbt_hydro_pipeline_job     → Hydrometry domain (can run in PARALLEL)
Stage 3: dbt_shared_dimensions_job  → dim_date, dim_geography (run LAST)
```
- `dbt_silver_gold_pipeline_job`: Run ALL dbt models (for bootstrap/full refresh)
- `dbt_test_job`: Run dbt data quality tests
- `dbt_docs_job`: Generate dbt documentation (hebdomadaire: dimanche 5h UTC)
- `dbt_quality_job`: Combined freshness + tests

**Daily Bronze Jobs**: `daily_piezometry_bronze_job`, `daily_hydrometry_bronze_job`, `era5_weekly_job`

### Initial Data Load
```bash
# Complete bootstrap (reference data → stations → chroniques → ERA5 → dbt)
# Launch the "full_bootstrap_job" from Dagster UI
# This is a sequential job that populates the entire database from scratch
```

**Important**: The `full_bootstrap_job` orchestrates the complete pipeline in the correct order. Don't run individual partitions manually unless you understand the dependencies.

## Architecture Overview

### Medallion Layers

**Bronze (Raw)**: DLT ingests data from APIs into `*_raw` tables
- Piezometry: `piezometry_stations_raw`, `piezometry_chroniques_raw`
- Hydrometry: `hydrometry_sites_raw`, `hydrometry_stations_raw`, `hydrometry_obs_elab_raw`
- Climate: `era5_france_timeseries`
- Reference: `tme_entites_hydrogeo`, `bdlisa_entites_raw`

**Silver (Clean)**: dbt staging models (`stg_*`) clean and type-cast data
- 7 staging models in `src/dbt_hubeau/models/staging/`
- Data validation via dbt tests
- Invalid records moved to `silver_rejects` schema

**Gold (Analytics)**: dbt intermediate + marts create analytics-ready tables
- **Intermediate** (7 models): Daily aggregations, spatial joins, ERA5 mappings
- **Marts** (14 models): Dimension tables (`dim_*`), fact tables (`fct_*`), aggregations (`agg_*`)
- **Main Fact Tables**: `hubeau_daily_chroniques` (piezometry + weather), `hydro_daily_chroniques` (hydrometry + weather)

### Key Asset Dependencies

```
Bronze (DLT assets)
  ↓
Silver (dbt staging models)
  ↓
Gold (dbt intermediate models)
  ↓
Gold (dbt marts models)
```

The pipeline has automatic dependency resolution. When Bronze data changes, downstream dbt models can be triggered via sensors or schedules.

## Code Organization

### Dagster Assets (`src/hubeau_pipeline/assets/`)
- `bronze/dlt_assets.py` - Hub'Eau API ingestion definitions
- `bronze/era5_assets.py` - ERA5 climate data ingestion
- `bronze/tme_entites_assets.py` - TME reference data
- `dbt_assets.py` - Bridges dbt models as Dagster assets

### Dagster Jobs (`src/hubeau_pipeline/jobs/`)
- `hubeau_jobs.py` - Station and chronique loading jobs (partitioned by year)
- `era5_jobs.py` - Climate data loading jobs
- `dbt_jobs.py` - Transformation pipeline jobs
- `full_bootstrap_job.py` - **Sequential** job for complete initialization
- `reference_data_jobs.py` - TME and BDLISA loading

### DLT Sources (`src/hubeau_pipeline/sources/`)
- `hubeau_csv_source.py` - Hub'Eau API pagination logic with retry mechanism
- `era5_source.py` - Copernicus CDS downloads via new CADS API client

### dbt Models (`src/dbt_hubeau/models/`)
- `staging/` - Silver layer (7 models)
- `intermediate/` - Gold layer transformations (7 models)
- `marts/` - Final analytics tables (14 models)
- `rejects/` - Data quality rejection tables

## Important Patterns

### DLT Ingestion Pattern
All DLT assets use:
- **Cursor-based pagination** for large API responses
- **Exponential backoff retry** (5 attempts) for 5xx errors
- **Automatic deduplication** via MERGE strategy
- **Year-based partitioning** for chroniques (1967-present for piezometry, 2000-present for hydrometry)

### dbt Materialization Strategy
- **Staging models**: `materialized: table` (full refresh)
- **Intermediate models**: `materialized: table` (some incremental)
- **Marts**: `materialized: table` (optimized for query performance)

### TimescaleDB Hypertables
- `bronze.piezometry_chroniques_raw` and `bronze.era5_france_timeseries` are hypertables partitioned by time
- Compression policies achieve 90%+ space savings on historical data
- Indexes are created via `on-run-start` hooks in `dbt_project.yml`

### Incremental Models
`int_station_era5_mapping` is incremental and may need forced rebuild:
```bash
docker exec brgm-dlt-worker dbt run --select int_station_era5_mapping+ --vars '{"recompute_station_era5_mapping": true}'
```

## Configuration Files

### YAML Configs (`/configs/`)
- `hubeau/piezometry_*.yml` - Piezometry API endpoint parameters
- `hubeau/hydrometry_*.yml` - Hydrometry API endpoint parameters
- `era5/era5_france_meteo.yml` - ERA5 download parameters (variables, grid, dates)

### Environment Variables (`.env`)
Key variables:
- `PG_HOST`, `PG_PORT`, `PG_DB`, `PG_USER`, `PG_PASSWORD` - Main database credentials
- `DAGSTER_PG_*` - Dagster metadata database credentials
- `DAGSTER_ENABLE_SCHEDULES=true|false` - Enable automated schedules
- `DAGSTER_ENABLE_SENSORS=true|false` - Enable event-driven sensors
- `DAGSTER_DBT_PARSE_PROJECT_ON_LOAD=1` - Force dbt manifest parsing (dev only)
- `BOOTSTRAP_PARTITIONS` - Filter partitions for `full_bootstrap_job` (e.g., `"piezometry_stations_job:*,piezometry_chroniques_job:2020,piezometry_chroniques_job:2021"`)
- `BOOTSTRAP_FORCE_RERUN=true` - Rerun completed partitions
- `BOOTSTRAP_CONTINUE_ON_ERROR=true` - Best-effort mode

### Docker Architecture
- **Worker Image** (`docker/worker/Dockerfile`): ~2GB, includes GDAL/GEOS for geospatial processing, runs DLT + dbt
- **Orchestrator Image** (`docker/orchestrator/Dockerfile`): ~500MB, lightweight, UI + scheduling only
- **GRPC Communication**: Orchestrator communicates with worker via GRPC (defined in `workspace.yaml`)

## Common Issues

### Missing TME Labels in Gold Tables
**Symptom**: `libelle_eh` is NULL in `stations_piezo_carte` or `int_station_era5_mapping`

**Cause**: Incremental model hasn't been rebuilt after TME data loaded

**Fix**:
```bash
docker exec brgm-dlt-worker dbt run --select int_station_era5_mapping+ --vars '{"recompute_station_era5_mapping": true}'
```

### Hub'Eau API 503 Errors
**Symptom**: Job fails with `HTTPError 503`

**Cause**: Hub'Eau API temporarily overloaded

**Fix**: Wait 15-30 minutes and retry via Dagster UI

### ERA5 CDS Timeouts
**Symptom**: `TimeoutError` during ERA5 download

**Fix**: Retry the job (has built-in exponential backoff). If persistent, check [CDS Status](https://cds.climate.copernicus.eu/)

### Duplicates in Bronze Tables
**Symptom**: More rows than expected after incremental load

**Cause**: 7-day overlap window in daily jobs

**Fix**: Silver layer automatically deduplicates. Run:
```bash
docker exec brgm-dlt-worker dbt run --select stg_piezo_chroniques stg_hydrometry_obs_elab
```

### Worker Container Won't Start
**Diagnosis**: Check logs with `docker compose logs dlt_worker`

**Common Fixes**:
- Port conflict: Change ports in `docker-compose.yml`
- Volume permissions: `docker compose down -v` and restart
- Stale image: `docker compose build --no-cache dlt_worker`

## Development Workflow

### Making Code Changes

1. **Python code** (`src/hubeau_pipeline/`): Mounted as volume, changes require `docker compose restart dlt_worker`
2. **dbt models** (`src/dbt_hubeau/models/`): Mounted as volume, auto-detected by Dagster
3. **Config files** (`configs/`): Mounted as volume, changes apply immediately

### Testing Changes

```bash
# Run dbt tests
docker exec brgm-dlt-worker dbt test

# Run specific model
docker exec brgm-dlt-worker dbt run --select your_model_name

# Test with fresh data
docker exec brgm-dlt-worker dbt run --full-refresh --select your_model_name
```

### Adding a New dbt Model

1. Create SQL file in `src/dbt_hubeau/models/{staging|intermediate|marts}/`
2. Define materialization in `dbt_project.yml` (or use folder defaults)
3. Run model: `docker exec brgm-dlt-worker dbt run --select your_model_name`
4. Add tests in `schema.yml` or separate test file
5. Run tests: `docker exec brgm-dlt-worker dbt test --select your_model_name`

### Adding a New DLT Source

1. Create YAML config in `configs/hubeau/` or `configs/era5/`
2. Add asset definition in `src/hubeau_pipeline/assets/bronze/`
3. Update `__init__.py` to export asset
4. Rebuild worker: `docker compose restart dlt_worker`
5. Asset appears in Dagster UI

## Performance Considerations

- **Bronze Layer**: Indexed on code_bss, date_mesure, coordinates (via `on-run-start` hooks)
- **Silver Layer**: Additional indexes created by dbt post-hooks
- **Gold Layer**: Materialized tables for fast queries
- **TimescaleDB**: Hypertables partition chroniques data by time (monthly chunks)
- **Compression**: Historical data (>90 days old) compressed at 90%+ ratio

## Documentation

- `docs/ARCHITECTURE.md` - Detailed system architecture
- `docs/SCHEMA_BDD.md` - Database schema reference
- `docs/CONFIGURATION.md` - Environment variables and settings
- `docs/ERA5_DATA_STORAGE.md` - ERA5 data storage strategy
- `docs/TIMESCALE_ET_INDEX.md` - TimescaleDB optimization details
- `docs/BDLISA_INTEGRATION.md` - BDLISA and TME reference data
- `docs/runbook.md` - Operational procedures and troubleshooting
- `docs/SUPERSET.md` - Business Intelligence setup

## Key Constraints

- **Python 3.11+** required
- **PostgreSQL 16** with PostGIS 3.4 and TimescaleDB extensions
- **dbt version pinned to 1.7.0** (compatibility with Dagster integration)
- **DLT version 0.4.12** (schema evolution features)
- **GDAL/GEOS** required in worker for geospatial processing (`pyogrio`, `geopandas`)

## Access Points

| Service | URL | Purpose |
|---------|-----|---------|
| Dagster UI | http://localhost:49500 | Pipeline orchestration and monitoring |
| Adminer | http://localhost:49501 | Lightweight PostgreSQL admin |
| PostgreSQL | localhost:49502 | Direct database access |
| CloudBeaver | http://localhost:49503 | Advanced SQL client |
| Superset | http://localhost:49504 | Business Intelligence dashboards |
| Netdata | http://localhost:49506 | Monitoring (containers + PostgreSQL) |
