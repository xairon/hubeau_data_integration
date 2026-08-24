# Architecture

How the pipeline is put together: data layers, orchestration, infrastructure.
Table-level detail lives in [DATABASE_SCHEMA.md](DATABASE_SCHEMA.md); day-to-day
operation in [OPERATIONS.md](OPERATIONS.md).

## Overview

```
External sources          Ingestion    Storage             Transformation   Consumption
────────────────          ─────────    ───────             ──────────────   ───────────
Hub'Eau (piezo, hydro)      DLT    →   PostgreSQL     →         dbt      →  Downstream apps
Copernicus CDS (ERA5)                  + TimescaleDB                        (SQL on Gold,
BDLISA (TME)                           + PostGIS                             e.g. the Junon
                                             ↑                                observatory)
                                    Dagster (orchestration)
```

- **DLT** ingests each source into the `bronze` schema (pagination, retry, MERGE dedup).
- **dbt** transforms Bronze → Silver → Gold (versioned SQL model DAG, data-quality tests).
- **Dagster** orchestrates the whole thing: scheduled ingestion, event-driven transformation.
- **PostgreSQL** stores everything; TimescaleDB handles time series, PostGIS the spatial side.
- **Gold tables** are queried directly in SQL by downstream applications. Those downstream
  dependencies are declared in `models/exposures.yml`.

## Medallion layers

Data crosses three layers of increasing quality, one PostgreSQL schema each.

| Layer | Schema | Tool | Materialization | Contents |
|-------|--------|------|-----------------|----------|
| Bronze | `bronze` | DLT | MERGE | Raw payloads as received (`text` columns) |
| Silver | `silver` | dbt `staging/` | table / incremental | Cleaned and typed data (8 models) |
| Rejects | `silver_rejects` | dbt `rejects/` | table | Filtered-out rows with a rejection reason (3 models) |
| Gold | `gold` | dbt `intermediate/` + `marts/` | table / incremental | Enriched analytical tables (6 + 12 models) |

Which schema a model lands in is decided by the `macros/generate_schema_name.sql` macro.

### Bronze — ingestion (DLT)

Each source is a Dagster asset running a DLT pipeline into `bronze`. DLT handles API
pagination, retries (exponential backoff on 5xx), schema inference and MERGE dedup on the
primary key. Time series are partitioned by year.

Main tables: `piezometry_stations_raw`, `piezometry_chroniques_raw`,
`hydrometry_sites_raw`, `hydrometry_stations_raw`, `hydrometry_obs_elab_raw`,
`era5_france_timeseries`, `tme_entites_hydrogeo`.

### Silver — cleaning (dbt staging)

Each `stg_*` model selects the useful columns from Bronze, casts types (`text` →
numeric/date/timestamp through the `cast_silver_*` macros), deduplicates (`DISTINCT ON`)
and drops invalid rows. Rejected rows go to `silver_rejects` with a `rejection_reason`
column for audit. Time-series models are incremental (`delete+insert`, 7-day lookback
window).

### Gold — analytics (dbt intermediate + marts)

- **Intermediate**: joins and preparation. Mapping a station to its nearest ERA5 grid point
  happens in `int_station_era5_mapping` / `int_hydro_station_era5_mapping` through a PostGIS
  KNN join (`CROSS JOIN LATERAL` + the `<->` operator).
- **Marts**: the final BI-facing tables.
  - Daily facts: `hubeau_daily_chroniques` (piezometry + weather), `hydro_daily_chroniques`
    (hydrometry + weather) — compressed TimescaleDB **hypertables**.
  - Aggregated facts: `fct_monthly_*`, `fct_yearly_*` (plain tables, `delete+insert`).
  - Dimensions: `dim_date`, `dim_geography`, `dim_piezo_stations`, `dim_hydro_stations`.
  - ERA5 grid: `fct_era5_monthly_grid`, `fct_era5_climatology_grid`.

### Tables built by Dagster, not dbt

Some Gold tables are produced by Python Dagster assets rather than dbt models. Looking for
them in `models/` is a dead end — this is the single most common way to get lost in this
repository.

| Table | Produced by | What it holds |
|-------|-------------|---------------|
| `gold.station_reference_stats` | `assets/*_index_assets.py` | Per-station reference baseline |
| `gold.fct_monthly_index` | `assets/*_index_assets.py` | Monthly standardized index (IPS / SSFI, 7 classes) |
| `gold.station_current_index` | `assets/*_index_assets.py` | Latest index value per station |
| `gold.fct_era5_indices_grid` | `assets/era5_indices_assets.py` | Gridded SPI / STI / SPEI |
| `gold.fct_era5_spei_climatology_grid` | `assets/era5_spei_climatology_assets.py` | SPEI reference distribution |

The index method itself is centralized in `ml/indices.py` (piezometry / hydrometry) and
`ml/era5_indices.py` (climate).

## Orchestration

Ingestion is **scheduled**. dbt transformation is **event-driven** (sensors), triggered as
soon as Bronze data lands — there is deliberately no time-based schedule for dbt.

### Schedules (ingestion only)

Enabled by `DAGSTER_ENABLE_SCHEDULES=true`. Times are UTC.

| Schedule | Cron | Job |
|----------|------|-----|
| ERA5 incremental update | `0 3 * * *` | `era5_weekly_job` |
| ERA5 daily temperature stats | `30 3 * * *` | `era5_daily_temp_update_job` |
| Bronze piezometry (last 7 days) | `0 4 * * *` | `daily_piezometry_bronze` |
| Bronze hydrometry (last 7 days) | `0 4 * * *` | `daily_hydrometry_bronze` |
| dbt documentation | `0 5 * * 0` (Sun) | `dbt_docs_job` |
| TME reference data (BDLISA) | `0 2 1 * *` (1st of month) | `reference_data_bronze` |
| Completeness check | `0 6 * * 1` (Mon) | `data_completeness_check` |
| IPS reference baseline | `0 7 * * 0` (Sun) | `station_reference_stats_refresh` |

### Sensors (transformation chain)

Enabled by `DAGSTER_ENABLE_SENSORS=true`.

```
Bronze (piezo + hydro) materialized
  └─ bronze_to_transform_sensor ──► dbt_transform        (all models, incremental)
       ├─ transform_to_index_sensor   ──► station_index_refresh  (IPS/SSFI)
       └─ transform_to_quality_sensor ──► dbt_quality_job        (freshness + dbt tests)
```

`dbt_transform` runs every dbt model; the `ref()` DAG orders staging → intermediate → marts
by itself. On success two sensors fire in parallel: the index refresh (data) and the quality
checks (non-blocking alerting — a failing test fails its own run but never blocks the data
refresh).

### Concurrency

Runs are **not** globally serialized. `dagster_home/dagster.yaml` sets
`max_concurrent_runs: 5` and then constrains what may overlap, per concurrency key:

| Key | Limit | Why |
|-----|-------|-----|
| each DLT pipeline (`piezometry_chroniques_bronze`, `daily_hydrometry_bronze`, …) | 1 | never two copies of the same ingestion at once |
| `dbt_pipeline` | 1 | the transform / quality / index / docs chain is never duplicated |
| `era5_weekly` | 1 | one daily grid update at a time |
| `era5_historical` | 3 | the ERA5 backfill may keep three CDS partitions in flight, for throughput |
| `era5_daily_temp_write` | 1 | **one writer only** on `bronze.era5_daily_temp_stats` |

That last key is shared deliberately between the nightly `era5_daily_temp_update_job` and the
`era5_daily_temp_historical_load` backfill. The table has no uniqueness constraint and its
DELETE+INSERT is not atomic, so when the current-year partition overlaps the nightly window the
two would duplicate rows — an incident that actually happened on 2026-07-07. The shared limit
of 1 makes the overlap impossible, and costs only one global slot out of five, so the backfill
never starves production.

The practical consequence: two *different* jobs will happily run at the same time. Do not
assume launching several jobs queues them behind one another.

## Docker infrastructure

Seven services in `docker-compose.yml`.

| Service | Container | Role |
|---------|-----------|------|
| `postgres` | `brgm-postgres` | PostgreSQL 16 + TimescaleDB + PostGIS (the data) |
| `postgres_tuning` | `brgm-postgres-tuning` | Applies PostgreSQL tuning parameters |
| `dagster_postgres` | `brgm-dagster-postgres` | Dagster metadata |
| `dlt_worker` | `brgm-dlt-worker` | All business code (assets, jobs, DLT, dbt) — gRPC code server |
| `dagster_webserver` | `brgm-dagster-webserver` | Dagster UI |
| `dagster_daemon` | `brgm-dagster-daemon` | Daemon (schedules, sensors, run queue) |
| `adminer` | `brgm-adminer` | PostgreSQL administration |

### Worker and orchestrator

Two separate images talking over **gRPC** (wired in `dagster_home/workspace.yaml`):

- **Worker** (`docker/worker/Dockerfile`, ~2 GB) holds all business code (Python plus
  GDAL/GEOS for geospatial). The dbt manifest is generated at build time
  (`dbt deps && dbt parse`). Every run executes here. Code is bind-mounted (hot reload).
- **Orchestrator** (`docker/orchestrator/Dockerfile`, ~500 MB) is webserver + daemon with
  **no business code** — Dagster packages only. It connects to the worker over gRPC. Rebuild
  it only when bumping the Dagster version.

### Volumes

Data volumes are **external**: `docker compose down -v` will not delete them. They must be
created once with `scripts/init_volumes.sh` before the first `docker compose up`.

### Hot reload

| Change | What to do |
|--------|------------|
| Python code (`src/hubeau_pipeline/`) | `docker compose restart dlt_worker`, then reload the code location in the Dagster UI |
| dbt models (`src/dbt_hubeau/models/`) | Reload definitions in the Dagster UI |
| YAML configs (`configs/`) | Nothing (bind-mounted) |
| Dependencies (`pyproject.toml`) | `docker compose build --no-cache dlt_worker && docker compose up -d` |

## Code layout

```
src/
├── hubeau_pipeline/              # Dagster pipeline (Python)
│   ├── definitions.py            # Wiring: assets, jobs, schedules, sensors, resources
│   ├── resources.py              # Connections (PostgreSQL, DLT, dbt)
│   ├── schedules.py              # 8 schedules (ingestion)
│   ├── sensors.py                # 3 sensors (dbt chain)
│   ├── assets/
│   │   ├── bronze/               # DLT ingestion assets (Hub'Eau, ERA5, TME)
│   │   ├── dbt_assets.py         # dbt → Dagster bridge (all models)
│   │   ├── *_index_assets.py     # IPS/SSFI indices (Python assets)
│   │   └── era5_*_assets.py      # Gridded climate indices (SPI/STI/SPEI)
│   ├── jobs/                     # Job definitions (ingestion, dbt, bootstrap, indices)
│   ├── sources/                  # Hub'Eau client only (hubeau_csv_source.py); the CDS
│   │                             # client lives in assets/bronze/era5_assets.py
│   ├── ml/                       # Index computation (indices.py, era5_indices.py)
│   └── io/                       # NoOpIOManager (DLT writes straight to PG)
│
└── dbt_hubeau/                   # dbt project
    ├── dbt_project.yml           # Config (vars, materialization, hooks)
    ├── profiles.yml              # PostgreSQL connection (env vars)
    ├── macros/                   # cast_silver, timescaledb, make_point, constraints, ...
    ├── seeds/                    # ref_stations_meteeau_bsn.csv
    └── models/{staging,rejects,intermediate,marts}/   # SQL models + schema.yml

configs/                          # Source configuration in YAML (hubeau, era5, bdlisa)
docker/                           # Dockerfiles, init SQL, per-service config
scripts/                          # init_volumes.sh, create_readonly_user.sh, server_deploy.sh, ...
```

## Data domains

- **Piezometry** — groundwater levels (stations + time series, from 1967).
- **Hydrometry** — river discharge (sites → stations → observations, from 2000).
- **Climate** — ERA5 reanalysis (temperature, precipitation, evaporation) on a ~0.1° grid
  over France; see [ERA5.md](ERA5.md).
- **Reference data** — TME hydrogeological entities (BDLISA), used to enrich stations.
