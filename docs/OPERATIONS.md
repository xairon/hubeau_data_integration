# Operations runbook

Running the pipeline day to day: initial load, routine checks, code updates, reprocessing,
incidents, backup and restore.

1. [Initial bootstrap](#1-initial-bootstrap)
2. [Daily checks](#2-daily-checks)
3. [Updating the code](#3-updating-the-code)
4. [Reprocessing data](#4-reprocessing-data)
5. [Common incidents](#5-common-incidents)
6. [Backup and restore](#6-backup-and-restore)

---

## 1. Initial bootstrap

### Full bootstrap (empty database)

Run the `full_bootstrap` job from the Dagster UI:

1. Open http://localhost:49500 → **Jobs** → `full_bootstrap`
2. **Launchpad** → **Launch Run**

It loads in order: TME reference data (BDLISA) → stations → time series (year by year) →
ERA5 → dbt. It is restartable: progress is persisted in `ops.bootstrap_state`, so a
re-launch resumes instead of starting over.

**Budget several hours.** The full history means 1967 onward for piezometry, 2000 onward for
hydrometry.

### Progressive load (for a test environment)

1. `reference_data_bronze` — TME reference data
2. `piezometry_stations_bronze` + `hydrometry_stations_bronze` — station metadata
3. One time-series job for a recent year (`piezometry_chroniques_bronze` /
   `hydrometry_chroniques_bronze`)
4. `dbt_transform` — Silver + Gold transformations

### Restricting what the bootstrap loads

| Variable | Effect |
|----------|--------|
| `BOOTSTRAP_PARTITIONS` | Allowlist of `job:partition` (e.g. `chroniques:piezometry:2020,era5:1990-1991`) |
| `BOOTSTRAP_FORCE_RERUN` | Re-run even if already marked complete |
| `BOOTSTRAP_CONTINUE_ON_ERROR` | Keep going after an error (best effort) |

> These three are **not** forwarded to the worker container by `docker-compose.yml`. Setting
> them in `.env` does nothing. Pass them on the command line, or add them to the worker's
> `environment:` block — see [CONFIGURATION.md](CONFIGURATION.md#making-the-no-variables-actually-work).

```bash
docker compose run --rm \
  -e BOOTSTRAP_PARTITIONS=chroniques:piezometry:2020,era5:1990-1991 \
  -e BOOTSTRAP_FORCE_RERUN=true \
  dlt_worker dagster job execute -m hubeau_pipeline.definitions -j full_bootstrap
```

---

## 2. Daily checks

### Data freshness

```sql
SELECT 'piezometry' AS domain, MAX(date_mesure) AS latest_date, NOW() - MAX(date_mesure) AS lag
FROM bronze.piezometry_chroniques_raw
UNION ALL
SELECT 'hydrometry', MAX(date_obs_elab), NOW() - MAX(date_obs_elab)
FROM bronze.hydrometry_obs_elab_raw
UNION ALL
SELECT 'era5', MAX(time), NOW() - MAX(time)
FROM bronze.era5_france_timeseries;
```

An ERA5 lag of about five days is expected — see the incident section.

### dbt tests

```bash
docker exec -w /app/src/dbt_hubeau brgm-dlt-worker dbt test
```

### Logs

```bash
docker compose logs -f dlt_worker       # job execution
docker compose logs -f dagster_daemon   # schedules and sensors
```

---

## 3. Updating the code

| What changed | What to run |
|--------------|-------------|
| Python code | `docker compose restart dlt_worker` |
| YAML configs (`configs/`) | nothing — bind-mounted, re-read on each run |
| dbt models | `docker compose build dlt_worker && docker compose up -d` (regenerates the manifest) |
| `pyproject.toml` | `docker compose down && docker compose build --no-cache dlt_worker && docker compose up -d` |

After a dbt model change, re-run the pipeline from the Dagster UI: **Jobs** → `dbt_transform`
→ **Launch Run**.

If the *shape* of the schema changed and you want a clean rebuild:

```bash
docker exec -i brgm-postgres psql -U postgres -d postgres -c \
  "DROP SCHEMA IF EXISTS silver CASCADE; DROP SCHEMA IF EXISTS gold CASCADE; DROP SCHEMA IF EXISTS silver_rejects CASCADE;"
```

This drops derived data only — Bronze is untouched, so a `dbt_transform` run rebuilds
everything without re-ingesting from the APIs.

---

## 4. Reprocessing data

### Replay a time window

```bash
docker exec -w /app/src/dbt_hubeau brgm-dlt-worker dbt run --select stg_piezo_chroniques \
  --vars '{"piezometry_reprocess_from_date": "2020-01-01"}'

docker exec -w /app/src/dbt_hubeau brgm-dlt-worker dbt run --select stg_hydrometry_obs_elab \
  --vars '{"hydrometry_reprocess_from_date": "2020-01-01"}'
```

### Full refresh of one incremental model

```bash
docker exec -w /app/src/dbt_hubeau brgm-dlt-worker dbt run --full-refresh --select hubeau_daily_chroniques
```

### Recompute the station ↔ ERA5 mapping

Needed after TME data changes or when stations are added — the incremental mapping does not
revisit existing rows.

```bash
docker exec -w /app/src/dbt_hubeau brgm-dlt-worker dbt run --select int_station_era5_mapping+ \
  --vars '{"recompute_station_era5_mapping": true}'
```

---

## 5. Common incidents

### Hub'Eau API returns 503

Temporarily overloaded API. The built-in retry (5 attempts, exponential backoff) absorbs
most of it. Otherwise check [hubeau.eaufrance.fr](https://hubeau.eaufrance.fr/) for notices,
wait 15–30 minutes and re-launch the job from the Dagster UI.

### ERA5 CDS timeout

`TimeoutError` or `ConnectionError` while downloading. Re-launch (retry is built in), check
the [CDS status](https://cds.climate.copernicus.eu/), and if a wide range keeps failing,
re-run it as shorter periods.

### `tuple decompression limit exceeded`

Incremental DML against a compressed hypertable, past the default limit.

```sql
ALTER DATABASE postgres SET timescaledb.max_tuples_decompressed_per_dml_transaction = 0;
```

Then `docker compose restart dlt_worker`. This setting normally ships in
`docker/postgres/init.sql` — hitting the error means it did not apply.

### NULL TME labels in Gold tables

`libelle_eh` / `code_eh` NULL in `hubeau_daily_chroniques` or `int_station_era5_mapping`.
The incremental mapping does not recompute existing rows.

```bash
docker exec -it brgm-postgres psql -U postgres -d postgres -c \
  "SELECT COUNT(*), COUNT(libelle_eh) FROM bronze.tme_entites_hydrogeo;"

docker exec -w /app/src/dbt_hubeau brgm-dlt-worker dbt run --select int_station_era5_mapping+ \
  --vars '{"recompute_station_era5_mapping": true}'
```

### Piezo/hydro data stops short of today

The daily job did not run — scheduler off, container down, or a failed run.

```sql
SELECT MAX(date_mesure) FROM bronze.piezometry_chroniques_raw;
SELECT MAX(date_obs_elab::date) FROM bronze.hydrometry_obs_elab_raw;
```

Check **Runs** in the Dagster UI and re-launch the daily jobs by hand. If schedules are off,
see `DAGSTER_ENABLE_SCHEDULES` in [CONFIGURATION.md](CONFIGURATION.md#dagster).

### Latest ERA5 date is today minus five days

Expected. Copernicus CDS publishes with roughly five days of latency; the window is set by
`ERA5_AVAILABILITY_LAG_DAYS` (default 5).

### Gaps in ERA5

```sql
SELECT date_trunc('month', time), COUNT(*)
FROM bronze.era5_france_timeseries
GROUP BY 1 ORDER BY 1;
```

Identify the missing period and re-run `era5_historical_load` on the matching partition.

### Duplicates in Bronze

Expected — the ingestion window overlaps by 7 days. Silver deduplicates:

```bash
docker exec -w /app/src/dbt_hubeau brgm-dlt-worker dbt run --select stg_piezo_chroniques stg_hydrometry_obs_elab
```

### `dbt` says the project path is not found

```
Encountered an error: project path </app/dbt_project.yml> not found
```

The worker's WORKDIR is `/app`; the dbt project is in `/app/src/dbt_hubeau`. Add
`-w /app/src/dbt_hubeau` to the `docker exec`. Every dbt command in this documentation
already carries it — check yours against them.

### A container will not start

```bash
docker compose logs <service>
docker compose build --no-cache <service>
docker compose up -d <service>
```

Usual causes: port conflict, corrupted image.

### PostgreSQL connection refused

```bash
docker exec brgm-postgres pg_isready
docker compose logs postgres
docker compose restart postgres
```

### Disk full

```bash
docker system df
docker system prune
```

---

## 6. Backup and restore

### Daily backup (recommended)

```bash
# crontab: 02:00 daily, 7-day retention
0 2 * * * docker exec brgm-postgres pg_dumpall -c -U postgres | gzip > /backups/hubeau_$(date +\%Y\%m\%d).sql.gz
find /backups -name "hubeau_*.sql.gz" -mtime +7 -delete
```

### Manual backup

```bash
# Everything
docker exec brgm-postgres pg_dumpall -c -U postgres | gzip > backup_$(date +%Y%m%d).sql.gz

# One schema (much faster)
docker exec brgm-postgres pg_dump -U postgres -n bronze postgres | gzip > backup_bronze.sql.gz
docker exec brgm-postgres pg_dump -U postgres -n gold   postgres | gzip > backup_gold.sql.gz
```

### Restore

```bash
gunzip -c backup_20260305.sql.gz | docker exec -i brgm-postgres psql -U postgres
gunzip -c backup_bronze.sql.gz   | docker exec -i brgm-postgres psql -U postgres postgres
```

### Docker volumes

The data volumes are external (`brgm_postgres_data`, `brgm_dagster_pg_data`) and survive
`docker compose down -v`.

```bash
# Backup
docker compose stop
docker run --rm -v brgm_postgres_data:/data -v $(pwd):/backup alpine \
  tar czf /backup/postgres_data.tar.gz /data
docker compose up -d

# Restore
docker compose down
docker volume rm brgm_postgres_data && docker volume create brgm_postgres_data
docker run --rm -v brgm_postgres_data:/data -v $(pwd):/backup alpine \
  tar xzf /backup/postgres_data.tar.gz -C /
docker compose up -d
```

### Recovery objectives

| Scenario | RTO | RPO | Method |
|----------|-----|-----|--------|
| Container crash | 1 min | 0 | Docker auto-restart |
| Volume corruption | 30 min | 1 day | Restore from backup |
| Full rebuild | 4–8 h | n/a | Re-run every pipeline |
