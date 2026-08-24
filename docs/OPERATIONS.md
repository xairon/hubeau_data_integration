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

### `station_index_refresh` fails every night on a fresh install

```
pandas.errors.DatabaseError: Execution failed on sql '...'
relation "gold.fct_era5_spei_climatology_grid" does not exist
```

The nightly index job computes `fct_era5_indices_grid`, which joins the SPEI reference table.
That table is built by a **separate Python asset that is deliberately excluded from the nightly
job** — it is a fixed 1991–2020 reference and recomputing it every night would be waste.

The consequence nobody expects: on an installation where it was never materialized, the table
does not exist at all, and the nightly job crashes on it. Every night, until someone runs it by
hand:

```bash
docker exec brgm-dlt-worker dagster asset materialize \
  --select fct_era5_spei_climatology_grid -m hubeau_pipeline.definitions
```

Verified on 2026-08-24: `station_index_refresh` failed twice with the error above, then
succeeded immediately once the reference had been materialized. The table can legitimately be
**empty** — it needs 1991–2020 ERA5 to fit anything — but it has to exist.

Full SPEI rebuild order in [ERA5.md](ERA5.md#rebuilding-spei--order-matters).

### An incremental model produces nothing on a past dataset

Not an error — `dbt` reports success and the table simply does not grow. It affects the two
daily hypertable marts, `hubeau_daily_chroniques` and `hydro_daily_chroniques`.

Both their DELETE (through the `hypertable_delete` macro) and their SELECT are anchored on
`CURRENT_DATE - daily_recompute_window_days` (30 days by default). The two agree, which is why
there is no error — but on a dataset that stops in the past, the window covers nothing at all
and the incremental run has no rows to process.

This is a designed knob, not a defect. Widen it to cover your data:

```bash
docker exec -w /app/src/dbt_hubeau brgm-dlt-worker dbt run \
  --select hubeau_daily_chroniques --vars '{"daily_recompute_window_days": "22000"}'
```

22000 days is about sixty years, i.e. everything. The same variable drives the station-mart
rebuild in [ERA5.md](ERA5.md#rebuilding-the-station-marts).

> **Historical note.** Six other models — `int_daily_measurements`,
> `int_hydro_daily_measurements`, `int_era5_for_all_stations`, `fct_monthly_chroniques`,
> `fct_monthly_hydro`, `fct_era5_monthly_grid` — used to carry an `incremental_predicates`
> block anchored on `CURRENT_DATE` while their SELECT was anchored on the table's own
> `MAX(date)`. The two diverged as soon as the data was older than the delete window: the
> DELETE matched nothing, the INSERT repeated rows already present, and the run died on
> `duplicate key value violates unique constraint`. Since their DELETE already joins the
> incoming batch on the primary key, the predicate was a pruning optimization only — it was
> removed rather than repaired. If you are running an older checkout and hit that error, the
> workaround is the same widened window.

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

### Moving or archiving the whole warehouse

Use the two scripts. They were tested end to end on TimescaleDB 2.29.2 **with a compressed
chunk present**: `pg_restore` returned exit code 0 with zero errors, and the chunk came back
still compressed, the hypertable still a hypertable, the compression policy still registered.

```bash
bash scripts/dump_warehouse.sh ./backups          # -> backups/hubeau-warehouse-<stamp>.dump
bash scripts/restore_warehouse.sh ./backups/hubeau-warehouse-<stamp>.dump --into-throwaway
```

The second command is not optional ceremony. It starts a disposable container, restores into
it, prints the row counts and the hypertable state, then deletes the container. It touches
nothing. **A dump you have never restored is a hope, not a backup** — and it costs a minute.

To restore for real, into the running stack:

```bash
bash scripts/restore_warehouse.sh ./backups/hubeau-warehouse-<stamp>.dump
```

The dump is a PostgreSQL custom-format archive, so it carries everything: hypertables,
compression state and policies, PostGIS geometries, indexes, constraints, and the
`ops.bootstrap_state` progress table. Restore it on any host running the same
`timescale/timescaledb-ha:pg16` image and the stack starts where it left off.

**Sizing.** Measured ratio on this project: a dump weighs about **8 %** of the live database
(80 MB → 7.5 MB). ERA5 costs roughly **178 bytes per row** in the database. Measure your own
before you plan a transfer:

```bash
docker exec brgm-postgres psql -U postgres -d postgres -c "
SELECT pg_size_pretty(pg_database_size('postgres')) AS database,
       (SELECT pg_size_pretty(sum(total_bytes))
        FROM hypertable_detailed_size('bronze.era5_france_timeseries')) AS era5;"
```

**What is worth keeping.** Silver and Gold are derived — dbt rebuilds them from Bronze in
minutes. Bronze is what costs: Hub'Eau can be re-fetched but slowly, and ERA5 means going back
through the Copernicus queue, which is days. If you have to choose, Bronze is the layer to
save.

### Handing data to people who will not run PostgreSQL

```bash
bash scripts/export_flat.sh ./export tsv        # gzipped TSV, no dependency
bash scripts/export_flat.sh ./export parquet    # needs the duckdb binary on the host
```

This exports the Gold layer as flat files: readable by pandas, R, DuckDB, Excel. It is a
**delivery format, not a backup** — it loses hypertables, indexes, geometry types and any way
to restart the pipeline. Geometry columns come out as WKB hex; wrap them in `ST_AsText` on the
consumer side if that matters.

### Scheduled backup

```bash
# crontab: 02:00 daily, 7-day retention
0 2 * * * cd /path/to/hubeau_data_integration && bash scripts/dump_warehouse.sh /backups >> /var/log/hubeau-backup.log 2>&1
30 2 * * * find /backups -name 'hubeau-warehouse-*.dump' -mtime +7 -delete
```

### Per-schema dumps

Faster than a full dump when you only want one layer:

```bash
docker exec brgm-postgres pg_dump -U postgres -Fc -n bronze -f /tmp/bronze.dump postgres
docker exec brgm-postgres pg_dump -U postgres -Fc -n gold   -f /tmp/gold.dump   postgres
docker cp brgm-postgres:/tmp/bronze.dump ./backups/
```

> `pg_dump` prints `warning: there are circular foreign-key constraints on this table:
> continuous_agg`. It is harmless — it refers to TimescaleDB's own catalog, not to your data,
> and the restore is unaffected.

### Docker volumes

An alternative to a dump: copy the volume itself. It is faster on very large databases but
only restores onto the same PostgreSQL major version and the same TimescaleDB version.

The data volumes are external (`brgm_postgres_data`, `brgm_dagster_pg_data`) and survive
`docker compose down -v`.

```bash
# Backup — the stack must be stopped, or the copy is inconsistent
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
| Volume corruption | 30 min | 1 day | `restore_warehouse.sh` from the last dump |
| Host decommissioned | 1 h | last dump | Copy the dump, start the stack elsewhere, restore |
| Full rebuild from the sources | days | n/a | Re-run every pipeline, including the Copernicus queue |
