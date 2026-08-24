# Configuration

Environment variables and pipeline settings. Values go in a `.env` file at the repository
root (template: `.env.example`), read by `docker-compose.yml`.

> **Read this before setting anything.** `docker compose` uses `.env` for *substitution
> inside the compose file*, not to inject variables into containers. The `dlt_worker`
> service has no `env_file:` — it lists its variables one by one under `environment:`.
> A variable that is not listed there **never reaches the worker process**, no matter what
> `.env` says. The table below marks which variables are forwarded today.

## PostgreSQL

| Variable | Description | Default | Forwarded to worker |
|----------|-------------|---------|---------------------|
| `PG_PASSWORD` | PostgreSQL password | — (required) | yes |
| `PG_HOST` | PostgreSQL host | `postgres` | yes |
| `PG_PORT` | Port (internal) | `5432` | yes |
| `PG_DB` | Database name | `postgres` | yes |
| `PG_USER` | User | `postgres` | yes |

Extensions (`postgis`, `timescaledb`) are enabled by `docker/postgres/init.sql`.

DLT's own destination credentials are derived from these
(`DESTINATION__POSTGRES__CREDENTIALS__*` ← `PG_*`) — nothing to set by hand.

## Dagster

| Variable | Description | Default in code | Default in the Docker stack |
|----------|-------------|-----------------|-----------------------------|
| `DAGSTER_PG_PASSWORD` | Password of the Dagster metadata database | — (required) | — |
| `DAGSTER_ENABLE_SCHEDULES` | Turn ingestion schedules on | `false` | **`true`** |
| `DAGSTER_ENABLE_SENSORS` | Turn the dbt sensor chain on | `false` | **`true`** |

The two defaults differ on purpose and this trips people up: `schedules.py:40` and
`sensors.py:48` default to `false`, but `docker-compose.yml` passes `${...:-true}`. Running
the Docker stack therefore starts with schedules and sensors **enabled**. Set them to
`false` in `.env` to get a quiet stack.

## ERA5 (Copernicus)

| Variable | Description | Default | Forwarded to worker |
|----------|-------------|---------|---------------------|
| `COPERNICUS_API_KEY` | Copernicus CDS API key | — (required for ERA5) | yes |
| `ERA5_AVAILABILITY_LAG_DAYS` | Days subtracted from "today" for the ERA5 end date | `5` | **no** — see below |

Copernicus publishes ERA5-Land with a few days of latency, so the ERA5 job only loads up to
`today − ERA5_AVAILABILITY_LAG_DAYS` (read at `assets/bronze/era5_assets.py:422`).

## Bootstrap (controlled initial load)

Read by `jobs/full_bootstrap_job.py` through `os.getenv`, inside the worker process.

| Variable | Description | Default | Forwarded to worker |
|----------|-------------|---------|---------------------|
| `BOOTSTRAP_PARTITIONS` | Allowlist of partitions to load (`job:partition`) | empty = everything | **no** — see below |
| `BOOTSTRAP_FORCE_RERUN` | Ignore completion state and re-run | `false` | **no** |
| `BOOTSTRAP_CONTINUE_ON_ERROR` | Keep going after an error (best effort) | `false` | **no** |

### Making the "no" variables actually work

`BOOTSTRAP_*` and `ERA5_AVAILABILITY_LAG_DAYS` are absent from `docker-compose.yml`. Putting
them in `.env` has no effect.

> The sandbox stack does not have this problem: `docker-compose.sandbox.yml:106-107` forwards
> `BOOTSTRAP_PARTITIONS` and `BOOTSTRAP_CONTINUE_ON_ERROR`, so on the sandbox the documented
> `.env` approach works. Only `BOOTSTRAP_FORCE_RERUN` is missing there too. See
> [DEPLOY_SANDBOX.md](DEPLOY_SANDBOX.md#5-run-the-initial-bootstrap).

For the main stack, pick one of:

```bash
# A. One-off, without touching the stack: run the job with the variable set
docker compose run --rm -e BOOTSTRAP_PARTITIONS=chroniques:piezometry:2020 dlt_worker \
  dagster job execute -m hubeau_pipeline.definitions -j full_bootstrap

# B. Permanent: add the line to the dlt_worker `environment:` block in docker-compose.yml,
#    then recreate the service
#      BOOTSTRAP_PARTITIONS: ${BOOTSTRAP_PARTITIONS:-}
docker compose up -d dlt_worker
```

Option B is the one to use if you bootstrap more than once — otherwise every operator hits
the same trap.

## The `.env` file

```bash
cp .env.example .env
# Edit .env: passwords and the Copernicus key
docker compose up -d --build
```

`.env` is in `.gitignore` and is never committed.

## dbt reprocessing (`--vars`)

Passed to a targeted `dbt run` to replay a window of data.

```bash
# Piezometry from a date
dbt run --select stg_piezo_chroniques --vars '{"piezometry_reprocess_from_date": "2020-01-01"}'

# Hydrometry from a date
dbt run --select stg_hydrometry_obs_elab --vars '{"hydrometry_reprocess_from_date": "2020-01-01"}'

# ERA5 from a timestamp
dbt run --select stg_era5_timeseries --vars '{"era5_reprocess_from_timestamp": "2020-01-01 00:00:00"}'

# Full recomputation of the station ↔ ERA5 mapping
dbt run --select int_station_era5_mapping+ --vars '{"recompute_station_era5_mapping": true}'
```

Incremental lookback windows are set in `dbt_project.yml`
(`piezometry_incremental_lookback_days: 7`, `streaming_lookback_days: 7`).

## Production

### GitLab CI/CD secrets

Set under **Settings → CI/CD → Variables** (Protected + Masked):
`PG_PASSWORD`, `DAGSTER_PG_PASSWORD`, `COPERNICUS_API_KEY`.

### External database

To use a managed PostgreSQL instead of the local container: point `PG_HOST`, `PG_PORT`,
`PG_DB`, `PG_USER`, `PG_PASSWORD` at the target and drop the `postgres` service from
`docker-compose.yml`. The target must provide PostGIS and TimescaleDB.

### Read-only user

```bash
bash scripts/create_readonly_user.sh
```

## Secret handling

- Strong passwords (16+ characters), one per service, never in the source.
- Store them in `.env` (local, gitignored) or a secret manager (production).
- Rotate every 90 days.
- TLS on any exposed connection; hand out read-only access wherever it is enough.
