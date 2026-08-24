# Hub'Eau Data Pipeline

A data warehouse for French hydrological data: automated ingestion, transformation and
exposure. It collects piezometric levels (groundwater), hydrometric discharge (rivers), ERA5
climate reanalysis and the associated reference data, on a Medallion architecture
(Bronze → Silver → Gold) over PostgreSQL/TimescaleDB, orchestrated by Dagster. Gold tables are
consumed directly in SQL by downstream applications — chiefly the Junon observatory.

**Status** — active. Documentation verified on 2026-08-24 against commit `0360237`.

> **A downstream application depends on this stack's Docker network.** The Junon observatory
> (`time-serie-explo`) reads the Gold tables directly, joining the network Compose creates for
> this project — `hubeau_data_integration_default` — and reaching PostgreSQL at host
> `brgm-postgres`. Two consequences: bring this stack up **before** Junon, and keep the
> checkout directory named `hubeau_data_integration`, because Compose derives that network
> name from it. Renaming the directory or setting `COMPOSE_PROJECT_NAME` silently breaks Junon.

New here? Read this page, then [docs/README.md](docs/README.md) for the documentation map.

## Stack

| Component | Version | Role |
|-----------|---------|------|
| Dagster | 1.11.14 | Orchestration (schedules + sensors) |
| DLT | 0.4.12 | Ingestion (Hub'Eau and ERA5 APIs) |
| dbt | 1.7.0 | SQL transformation (staging → marts) |
| PostgreSQL | 16 | Database |
| TimescaleDB | — | Time series (hypertables, compression) |
| PostGIS | — | Geospatial (spatial joins) |

PostgreSQL, TimescaleDB and PostGIS all come from the single `timescale/timescaledb-ha:pg16`
image. That tag is rolling, so the extension versions move without the compose file changing —
a fresh pull on 2026-08-24 gave PostgreSQL 16.14, TimescaleDB 2.29.2 and PostGIS 3.6.4. Read
the versions from the database rather than trusting a table:

```bash
docker exec brgm-postgres psql -U postgres -tAc \
  "SELECT extname, extversion FROM pg_extension ORDER BY extname;"
```

## Getting started

### Requirements

- Docker with Compose v2
- ~10 GB RAM, ~50 GB disk for the full dataset
- A [Copernicus CDS](https://cds.climate.copernicus.eu/) API key for ERA5 ingestion

### Install

```bash
git clone https://scm.univ-tours.fr/ringuet/hubeau_data_integration.git
cd hubeau_data_integration

# 1. Create the external Docker volumes (required, once)
bash scripts/init_volumes.sh

# 2. Configure the environment
cp .env.example .env
# Edit .env: passwords and the Copernicus key

# 3. Build and start
docker compose up -d --build

# 4. Check the services (~60 s to come up)
docker compose ps
```

### Interfaces

| Service | URL | Role |
|---------|-----|------|
| Dagster | http://localhost:49500 | Orchestration and monitoring |
| Adminer | http://localhost:49501 | PostgreSQL administration |
| PostgreSQL | localhost:49502 | Direct database access |
| dbt docs | http://localhost:49505 | dbt documentation (started manually, see below) |

### Load the data

**Read [docs/QUICKSTART.md](docs/QUICKSTART.md) before launching anything.** It gives three
targets — a 15-minute smoke test needing no API key, a regional demo dataset where the climate
indices actually render, and the full production load — with the exact jobs to run, in order,
and how to verify each step.

The short version: `full_bootstrap` loads everything (reference data → stations → time series
by year → ERA5 → dbt) and is restartable, its progress persisted in `ops.bootstrap_state`.
Dagster UI → **Jobs** → `full_bootstrap` → **Launchpad** → **Launch Run**. Budget **days** and
tens of gigabytes: piezometry goes back to 1967, hydrometry to 2000, ERA5-Land to 1950. Almost
nobody wants that on a first install.

### Verify

```bash
# Row counts per schema
docker exec -it brgm-postgres psql -U postgres -d postgres -c "
SELECT schemaname, relname AS table_name, n_live_tup AS rows
FROM pg_stat_user_tables
WHERE schemaname IN ('bronze', 'silver', 'gold')
ORDER BY schemaname, n_live_tup DESC;"

# dbt data quality tests
docker exec -w /app/src/dbt_hubeau brgm-dlt-worker dbt test
```

## Everyday commands

```bash
# dbt (inside the worker container)
docker exec -w /app/src/dbt_hubeau brgm-dlt-worker dbt run                       # full pipeline
docker exec -w /app/src/dbt_hubeau brgm-dlt-worker dbt run --select model_name   # one model
docker exec -w /app/src/dbt_hubeau brgm-dlt-worker dbt test                      # quality tests
docker exec -w /app/src/dbt_hubeau brgm-dlt-worker dbt docs generate             # documentation
docker exec -w /app/src/dbt_hubeau brgm-dlt-worker dbt docs serve --port 8080    # served on :49505

# Docker
docker compose logs -f dlt_worker     # worker logs
docker compose restart dlt_worker     # after a Python change
docker compose build --no-cache dlt_worker && docker compose up -d   # full rebuild

# PostgreSQL
docker exec -it brgm-postgres psql -U postgres -d postgres
```

## Documentation

The full map, with what each document is for, is in **[docs/README.md](docs/README.md)**.

| Document | Contents |
|----------|----------|
| [Architecture](docs/ARCHITECTURE.md) | Medallion layers, orchestration, Docker infrastructure |
| [Configuration](docs/CONFIGURATION.md) | Environment variables, settings, production |
| [Database schema](docs/DATABASE_SCHEMA.md) | PostgreSQL tables (Bronze, Silver, Gold) |
| [Operations](docs/OPERATIONS.md) | Runbook: bootstrap, daily checks, incidents, backup |
| [ERA5](docs/ERA5.md) | Climate ingestion, PET and drought-index decisions |
| [TimescaleDB](docs/TIMESCALEDB.md) | Hypertables, compression, index types |
| [Sandbox deployment](docs/DEPLOY_SANDBOX.md) | Portainer + GitOps deployment |

`CLAUDE.md` at the root is the working guide for coding agents.

## License

MIT — see [LICENSE](LICENSE).
