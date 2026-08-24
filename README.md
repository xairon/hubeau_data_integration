# Hub'Eau Data Pipeline

A data warehouse for French hydrological data: automated ingestion, transformation and
exposure. It collects piezometric levels (groundwater), hydrometric discharge (rivers), ERA5
climate reanalysis and the associated reference data, on a Medallion architecture
(Bronze → Silver → Gold) over PostgreSQL/TimescaleDB, orchestrated by Dagster. Gold tables are
consumed directly in SQL by downstream applications — chiefly the Junon observatory.

**Status** — active. Documentation verified on 2026-08-24 against commit `0360237`.

New here? Read this page, then [docs/README.md](docs/README.md) for the documentation map.

## Stack

| Component | Version | Role |
|-----------|---------|------|
| Dagster | 1.11.14 | Orchestration (schedules + sensors) |
| DLT | 0.4.12 | Ingestion (Hub'Eau and ERA5 APIs) |
| dbt | 1.7.0 | SQL transformation (staging → marts) |
| PostgreSQL | 16 | Database |
| TimescaleDB | pg16 | Time series (hypertables, compression) |
| PostGIS | 3.4 | Geospatial (spatial joins) |

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

The `full_bootstrap` job loads everything: reference data → stations → time series by year →
ERA5 → dbt. It is restartable — progress is persisted in `ops.bootstrap_state`.

Dagster UI → **Jobs** → `full_bootstrap` → **Launchpad** → **Launch Run**.

A full bootstrap takes several hours: piezometry goes back to 1967, hydrometry to 2000. To
load a small subset instead, see
[docs/OPERATIONS.md](docs/OPERATIONS.md#restricting-what-the-bootstrap-loads) — note that the
restriction variables need more than an entry in `.env`.

### Verify

```bash
# Row counts per schema
docker exec -it brgm-postgres psql -U postgres -d postgres -c "
SELECT schemaname, tablename, n_live_tup AS rows
FROM pg_stat_user_tables
WHERE schemaname IN ('bronze', 'silver', 'gold')
ORDER BY schemaname, n_live_tup DESC;"

# dbt data quality tests
docker exec brgm-dlt-worker dbt test
```

## Everyday commands

```bash
# dbt (inside the worker container)
docker exec brgm-dlt-worker dbt run                       # full pipeline
docker exec brgm-dlt-worker dbt run --select model_name   # one model
docker exec brgm-dlt-worker dbt test                      # quality tests
docker exec brgm-dlt-worker dbt docs generate             # documentation
docker exec brgm-dlt-worker dbt docs serve --port 8080    # served on :49505

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
