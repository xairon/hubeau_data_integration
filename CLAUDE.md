# CLAUDE.md

Working guide for coding agents in this repository. Reader-facing documentation lives in
[docs/](docs/README.md) — this file does not repeat it, it points at it.

## What this is

A production data warehouse for French hydrological data, on a Medallion architecture
(Bronze → Silver → Gold). It ingests the Hub'Eau APIs and the ERA5 climate reanalysis,
transforms with dbt, and exposes Gold tables to downstream applications — chiefly the Junon
observatory — directly in SQL.

Stack: Dagster (orchestration), DLT (ingestion), dbt 1.7.0 (transformation),
PostgreSQL 16 + TimescaleDB + PostGIS, Docker Compose.

Full picture in [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md); tables in
[docs/DATABASE_SCHEMA.md](docs/DATABASE_SCHEMA.md).

## Essential commands

```bash
# First time only: create the external volumes
bash scripts/init_volumes.sh

docker compose up -d --build          # start everything
docker compose restart dlt_worker     # after Python changes
docker compose down && docker compose build --no-cache && docker compose up -d   # after dependency changes
```

dbt runs inside the worker, and **every dbt command needs `-w /app/src/dbt_hubeau`**. The
worker's WORKDIR is `/app`, but the dbt project lives in `/app/src/dbt_hubeau`; without the
flag dbt reports `project path </app/dbt_project.yml> not found` and does nothing. With it,
`dbt debug` answers *All checks passed!*.


```bash
docker exec -w /app/src/dbt_hubeau brgm-dlt-worker dbt run                                     # full pipeline
docker exec -w /app/src/dbt_hubeau brgm-dlt-worker dbt run --select model_name                 # one model
docker exec -w /app/src/dbt_hubeau brgm-dlt-worker dbt run --select model_name+                # model + downstream
docker exec -w /app/src/dbt_hubeau brgm-dlt-worker dbt run --full-refresh --select model_name  # force rebuild
docker exec -w /app/src/dbt_hubeau brgm-dlt-worker dbt test                                    # data quality tests
docker exec -w /app/src/dbt_hubeau brgm-dlt-worker dbt source freshness
docker exec -w /app/src/dbt_hubeau brgm-dlt-worker dbt docs generate
```

Database: `docker exec -it brgm-postgres psql -U postgres -d postgres`
(`\dt bronze.*`, `\dt silver.*`, `\dt gold.*`).

Lint and types (config in `pyproject.toml`: line-length 120, target py311):

```bash
ruff check src/          # pycodestyle, pyflakes, isort, bugbear
ruff check --fix src/
black src/
mypy src/                # lenient: ignore_missing_imports = true
```

## Tests

Python unit tests cover the **pure computation layer only** — no database, no network.

| File | Covers |
|------|--------|
| `test_indices.py` | IPS/SSFI classification |
| `test_monthly_index.py`, `test_monthly_index_persistence.py` | Monthly index and its persistence |
| `test_reference_grid.py` | Fixed reference grid |
| `test_era5_indices.py` | SPI (gamma), STI (z-score), SPEI (GLO fit + CDF) |
| `test_era5_spei_climatology.py` | SPEI reference fitting and rejection accounting |
| `test_era5_indices_persistence.py` | SQL-constant contract of the indices table |
| `test_era5_daily_temp_aggregation.py` | Hourly → daily aggregation (needs `xarray`) |

**Nothing runs these tests automatically.** `.gitlab-ci.yml` has no test job, `tests/` is not
copied into the worker image, and `pytest` is not installed there either — it sits in a
`pyproject.toml` extra that the image does not install. The suite only runs when a human runs
it.

The reliable way, using the worker's real dependency set (numpy, scipy, xarray are all there):

```bash
docker cp tests brgm-dlt-worker:/app/tests
docker exec brgm-dlt-worker pip install -q pytest
docker exec -e PYTHONPATH=/app/src brgm-dlt-worker python3 -m pytest /app/tests -o addopts="" -q
```

Measured 2026-08-24: **57 passed in 4.67 s**.

On a bare host instead, `uv run pytest` does **not** work — it rebuilds psycopg2cffi, which
needs `pg_config`. Use the interpreter directly, and note that
`test_era5_daily_temp_aggregation.py` fails **at collection** without `xarray`:

```bash
PYTHONPATH=src python3 -m pytest tests/ -o addopts="" \
  --ignore=tests/test_era5_daily_temp_aggregation.py     # 37 tests
```

`tests/test_indices.py::GOLDEN_Z_TO_CLASS` is a **cross-repository sync contract**: the same
golden table exists in the Junon repository
(`time-serie-explo/tests/test_drought_classification_contract.py`), so the duplicated IPS math
cannot drift silently. Change one, change the other.

Everything else is covered by dbt tests (not_null, unique, relationships, accepted_values,
accepted_range) declared in the `schema.yml` files. Failing rows are persisted to the
`dbt_audit` schema (`+store_failures`).

## How the Dagster definitions wire together

Entry point `src/hubeau_pipeline/__init__.py` exports `defs` from `definitions.py`, which
assembles one `Definitions()`:

- **assets** — `all_assets`: 14 Bronze DLT assets, plus `hubeau_dbt_assets` which
  auto-discovers every dbt model from the manifest, plus the Python index assets
- **jobs** — `all_jobs` from `jobs/__init__.py`
- **schedules** — `all_schedules`: 8 schedules, gated by `DAGSTER_ENABLE_SCHEDULES`
- **sensors** — `all_sensors`: 3 sensors, gated by `DAGSTER_ENABLE_SENSORS`
- **resources** — `pg` (PostgreSQLResource), `dlt` (DagsterDltResource), `dbt`
  (DbtCliResource), `noop_io_manager` (DLT writes straight to PostgreSQL)

Schedules drive **ingestion only**. Sensors drive the whole dbt + index + quality chain;
there is deliberately no time-based dbt schedule. `dbt_transform_job` uses `.without_checks()`
so tests run in the quality job rather than inline: a failing test fails the quality run,
which is visible and alertable, but never blocks the data refresh.

`fct_era5_spei_climatology_grid` is deliberately **not** in the nightly index job — it is a
fixed 1991–2020 reference, materialized on demand.

## dbt layout

| Layer | Folder | Schema | Models |
|-------|--------|--------|--------|
| Bronze | (DLT, not dbt) | `bronze` | — |
| Silver | `staging/` | `silver` | 8 |
| Rejects | `rejects/` | `silver_rejects` | 3 |
| Gold intermediate | `intermediate/` | `gold` | 6 |
| Gold marts | `marts/` | `gold` | 12 |

Routing is handled by the `generate_schema_name.sql` macro plus the `dbt_project.yml` model
configs.

## Code organization

- `src/hubeau_pipeline/`
  - `assets/bronze/` — DLT ingestion assets, one file per source
  - `assets/dbt_assets.py` — bridges dbt models as Dagster assets (`@dbt_assets`)
  - `assets/{current_index,monthly_index,reference_stats}_assets.py` — station-level IPS/SSFI
  - `assets/era5_{indices,spei_climatology}_assets.py` — gridded SPI/STI/SPEI
  - `jobs/`, `sources/` (`hubeau_csv_source.py` only — the CDS client lives in `assets/bronze/era5_assets.py`, not here), `schedules.py` (8),
    `sensors.py` (3), `resources.py`, `io/io_managers.py`
  - `ml/` — index computation, pure and vectorized (numpy + scipy, no external index library):
    `indices.py` (IPS/SSFI), `era5_indices.py` (SPI gamma CDF, STI z-score, SPEI via
    `fit_glo_detailed` / `compute_spei_glo` — generalized logistic fitted by L-moments,
    Hosking estimators), plus the `*_persistence.py` modules
- `src/dbt_hubeau/` — `models/`, `macros/` (`cast_silver.sql`, `timescaledb.sql`,
  `make_point.sql`, `constraints.sql`, `generate_schema_name.sql`), `profiles.yml`,
  `packages.yml` (depends on `dbt_utils`)
- `configs/` — YAML for the API endpoints (`hubeau/`), ERA5 (`era5/`), BDLISA (`bdlisa/`)
- `docker/` — Dockerfiles, `postgres/init.sql`, Adminer, sandbox image variants
- `dagster_home/` — Dagster metadata and `workspace.yaml` (gRPC connection to the worker)

## Patterns worth knowing

- **DLT ingestion** — cursor pagination, exponential backoff (5 attempts on 5xx), MERGE dedup,
  year partitioning for time series.
- **dbt incremental** — staging uses delete+insert with configurable lookback (7 days by
  default, in `dbt_project.yml` vars). The `on-run-start` hooks create indexes and set the
  TimescaleDB decompression parameters.
- **ERA5 ingests three variables only**: `2m_temperature`, `total_precipitation`,
  `potential_evaporation` (`configs/era5/era5_france_meteo.yml`). No wind, no humidity — which
  is exactly why reference PET is computed by Hargreaves rather than Penman-Monteith. See
  [docs/ERA5.md](docs/ERA5.md).
- **Hot reload** — Python and dbt code is volume-mounted in the worker only. Restart the
  worker, then reload the code location in the Dagster UI. The orchestrator holds no user code
  and only needs rebuilding when Dagster itself is upgraded.

## Constraints

- Python 3.11+; dbt pinned to 1.7.0 (Dagster compatibility, enforced by `require-dbt-version`);
  DLT pinned to 0.4.12.
- Docker volumes are **external** — `docker compose down -v` does not delete them, and
  `scripts/init_volumes.sh` must run before the first `up`.
- GDAL/GEOS are required in the worker for `pyogrio` / `geopandas`.
- The `timescale/timescaledb-ha:pg16` image uses Patroni — do **not** add `command:` overrides
  in docker-compose.
- `BOOTSTRAP_*` and `ERA5_AVAILABILITY_LAG_DAYS` are **not** forwarded to the worker by the
  main `docker-compose.yml`; setting them in `.env` silently does nothing. See
  [docs/CONFIGURATION.md](docs/CONFIGURATION.md).

## Access points

| Service | URL |
|---------|-----|
| Dagster UI | http://localhost:49500 |
| Adminer | http://localhost:49501 |
| PostgreSQL | localhost:49502 |
| dbt docs | http://localhost:49505 (manual: `docker exec -w /app/src/dbt_hubeau brgm-dlt-worker dbt docs serve --port 8080`) |

## Traps that have already cost time

- **Phantom hypertables.** The `timescaledb_ddl_command_end` event trigger is **disabled** so
  dbt's `ALTER TABLE RENAME` cannot re-apply hypertable metadata. This is the permanent fix,
  versioned in `docker/postgres/init.sql` (idempotent DO block) so a from-scratch deploy
  reproduces it. If it somehow gets re-enabled:
  `ALTER EVENT TRIGGER timescaledb_ddl_command_end DISABLE;`. Manual cleanup is
  `DROP TABLE schema.table CASCADE` then `dbt run --select model_name` — **not**
  `--full-refresh`.
- **Zombie inserts from a killed CLI materialization.** `docker exec ... dagster asset
  materialize` bypasses the run queue; if the CLI client dies, the step keeps running in the
  worker and keeps inserting (duplicate keys observed on `era5_daily_temp_stats`). Launch runs
  from the Dagster UI or GraphQL so the QueuedRunCoordinator serializes them.
- **A daily mart that takes an hour instead of minutes.** Check whether `unique_key` is set on
  an `append` model: dbt 1.7.0 ignores the `append` strategy when `unique_key` is present and
  generates `DELETE ... USING`, which sequentially scans every hypertable chunk. Remove
  `unique_key`.
- **`operator does not exist: text >= date`.** DLT stores every Bronze column as `text`, so
  `_clean_recent_data()` must cast date columns explicitly (`{}::date >= CURRENT_DATE`).
- **Sensor crash loop (`trailing_unconsumed_events`).** A `multi_asset_sensor` must call
  `context.advance_all_cursors()` on **every** evaluation, not only when it yields a
  `RunRequest`. Otherwise events pile up to the 25-event limit and take down the daemon.
- **DLT `SchemaNotFoundError` / `CannotCoerceColumnException`.** DLT state lives in four
  places and all four must be cleaned for a fresh start: the filesystem
  (`rm -rf /var/dlt/pipelines/<name>` in the worker), `bronze._dlt_version`,
  `bronze._dlt_pipeline_state`, `bronze._dlt_loads`. Cleaning only some causes cascading
  errors.
- **FK violation on `int_hydro_daily_measurements` (orphan `code_station`).** Hub'Eau can
  return observations for stations missing from its stations endpoint. `stg_hydrometry_obs_elab`
  only filters on `code_site`, because `code_station` is nullable in 46 % of observations; the
  FK is enforced one level down by the `INNER JOIN stg_hydrometry_stations` inside
  `int_hydro_daily_measurements`. It self-heals once Hub'Eau catches up, within the 7-day
  lookback. Piezometry is unaffected — `stg_piezo_chroniques` filters at the staging layer.
- **Bronze duplicates are expected** (7-day overlap window); Silver deduplicates. Note that the
  `*_daily_raw` assets and the year-partitioned `*_chroniques_raw` / `*_obs_elab_raw` assets
  write the *same* Bronze table: daily cleans 7 days, the partition cleans a full year.
- **Missing TME labels in Gold**: rebuild the incremental mapping with
  `--vars '{"recompute_station_era5_mapping": true}'`.
- **A Bronze temperature backfill does not reach Silver on its own** — see the trap section in
  [docs/ERA5.md](docs/ERA5.md#backfilling-temperature--the-silver-trap).

More operational incidents in [docs/OPERATIONS.md](docs/OPERATIONS.md#5-common-incidents).
