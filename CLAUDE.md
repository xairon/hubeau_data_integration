# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Hub'Eau Data Pipeline: a production data warehouse for French hydrological data using a **Medallion Architecture** (Bronze → Silver → Gold). Ingests data from Hub'Eau APIs and ERA5 climate reanalysis, transforms via dbt, serves analytics via Apache Superset.

**Stack**: Dagster (orchestration), DLT (ingestion), dbt 1.7.0 (transformation), PostgreSQL 16 + TimescaleDB + PostGIS, Docker Compose

## Essential Commands

### Stack Operations
```bash
# First time: create external volumes (required before first docker compose up)
bash scripts/init_volumes.sh

# Start everything
docker compose up -d --build

# Restart worker after Python code changes (dbt model changes are auto-detected)
docker compose restart dlt_worker

# Full rebuild (when dependencies change)
docker compose down && docker compose build --no-cache && docker compose up -d
```

### dbt (run inside worker container)
```bash
docker exec brgm-dlt-worker dbt run                          # Full pipeline
docker exec brgm-dlt-worker dbt run --select model_name      # Single model
docker exec brgm-dlt-worker dbt run --select model_name+     # Model + downstream
docker exec brgm-dlt-worker dbt test                          # Data quality tests
docker exec brgm-dlt-worker dbt test --select model_name      # Test single model
docker exec brgm-dlt-worker dbt source freshness              # Check source freshness
docker exec brgm-dlt-worker dbt run --full-refresh --select model_name  # Force rebuild
docker exec brgm-dlt-worker dbt docs generate                 # Generate documentation

# Force rebuild of incremental ERA5 mapping (needed after TME data changes)
docker exec brgm-dlt-worker dbt run --select int_station_era5_mapping+ --vars '{"recompute_station_era5_mapping": true}'
```

### Database
```bash
docker exec -it brgm-postgres psql -U postgres -d postgres
# \dt bronze.*   \dt silver.*   \dt gold.*
```

### Linting & Formatting
```bash
# Configured in pyproject.toml: line-length=120, target=py311
ruff check src/                  # Lint (pycodestyle, pyflakes, isort, bugbear)
ruff check --fix src/            # Lint + auto-fix
black src/                       # Format
mypy src/                        # Type check (lenient: ignore_missing_imports=true)
```

### Testing
There are **no Python unit tests**. Data quality is managed entirely through dbt tests (not_null, unique, relationships, accepted_range) defined in `schema.yml` files. Run via `docker exec brgm-dlt-worker dbt test`.

pytest config exists in `pyproject.toml` (testpaths=`tests/`) but no test files are present.

## Architecture

### How Dagster Definitions Wire Together

Entry point: `src/hubeau_pipeline/__init__.py` → exports `defs` from `definitions.py`.

`definitions.py` assembles a single `Definitions()` object:
- **assets** = `all_assets` (14 bronze DLT assets + 1 `hubeau_dbt_assets` which auto-discovers all dbt models from the manifest)
- **jobs** = `all_jobs` (22 jobs from `jobs/__init__.py`)
- **schedules** = `all_schedules` (9 schedules, controlled by `DAGSTER_ENABLE_SCHEDULES` env var)
- **sensors** = `all_sensors` (3 sensors, controlled by `DAGSTER_ENABLE_SENSORS` env var)
- **resources** = `pg` (PostgreSQLResource), `dlt` (DagsterDltResource), `dbt` (DbtCliResource), `noop_io_manager` (NoOpIOManager for DLT assets that write directly to PostgreSQL)

### Docker Architecture

Two custom images, communicating via GRPC (defined in `dagster_home/workspace.yaml`):

- **Worker** (`docker/worker/Dockerfile`): ~2GB, includes GDAL/GEOS, runs `dagster code-server` on port 4000. Uses `uv` package manager for fast builds. Generates dbt manifest at build time via `dbt deps && dbt parse`. Source code hot-reloaded via volume mounts.
- **Orchestrator** (`docker/orchestrator/Dockerfile`): ~500MB, lightweight, runs `dagster-webserver` + `dagster-daemon`. No source code, connects to worker via GRPC.

### Medallion Layers & dbt Schema Mapping

| Layer | dbt folder | PostgreSQL schema | Materialization |
|-------|-----------|-------------------|-----------------|
| Bronze | (DLT, not dbt) | `bronze` | DLT MERGE |
| Silver | `staging/` (7 models) | `silver` | table |
| Rejects | `rejects/` (3 models) | `silver_rejects` | table |
| Gold (intermediate) | `intermediate/` (7 models) | `gold` | table (some incremental) |
| Gold (marts) | `marts/` (14 models) | `gold` | table |

Schema mapping is controlled by `generate_schema_name.sql` macro and `dbt_project.yml` model configs.

### Data Domains

- **Piezometry**: Groundwater level stations + chroniques (year-partitioned 1967-present)
- **Hydrometry**: River flow sites → stations → observations (year-partitioned 2000-present)
- **Climate**: ERA5 reanalysis data (temperature, precip, wind, humidity) on a France-wide grid
- **Reference**: TME hydrogeo entities, SANDRE nomenclatures, geographic referentials (regions, departments, hydrological zones)

### Pipeline Flow

```
Bronze (DLT assets) → Silver (dbt staging/) → Gold (dbt intermediate/ → marts/)
```

**Main fact tables**: `hubeau_daily_chroniques` (piezometry + ERA5 weather), `hydro_daily_chroniques` (hydrometry + ERA5 weather)

The spatial join between stations and ERA5 grid is done in `int_station_era5_mapping` (KNN nearest grid point). This model is incremental with special rebuild logic via `recompute_station_era5_mapping` var.

### 4-Stage Daily Schedule (UTC)

```
3h00  ERA5 Smart Update (Bronze)
4h00  Hub'Eau Bronze: piezometry + hydrometry (parallel)
5h00  dbt Stage 1: shared staging (ERA5 timeseries + grid points)
6h30  dbt Stage 2: domain pipelines (piezo + hydro, parallel)
8h00  dbt Stage 3: shared dimensions (dim_date, dim_geography)
```

Monthly (1st, 2h00): Reference data refresh. Weekly (Sunday, 5h00): dbt docs generation.

Schedules are time-based with buffer gaps. Sensors (`sensors.py`) provide event-driven alternative: `bronze_to_silver_sensor` triggers dbt when Bronze chroniques materialize (5 min cooldown).

### Bootstrap

`full_bootstrap_job` orchestrates complete initial load: reference data → stations → chroniques (iterates year partitions) → ERA5 → dbt. Tracks state in `ops.bootstrap_state` table for restartability. Controlled by env vars: `BOOTSTRAP_PARTITIONS`, `BOOTSTRAP_FORCE_RERUN`, `BOOTSTRAP_CONTINUE_ON_ERROR`.

## Code Organization

### Key Directories

- `src/hubeau_pipeline/` - Dagster pipeline (Python)
  - `assets/bronze/` - DLT ingestion assets (one file per data source)
  - `assets/dbt_assets.py` - Bridges dbt models as Dagster assets via `@dbt_assets` decorator
  - `jobs/` - 22 job definitions (bronze, dbt staged pipelines, bootstrap)
  - `sources/` - DLT source clients: `hubeau_csv_source.py` (API pagination + retry), `era5_source.py` (CADS API + NetCDF)
  - `schedules.py` - 9 cron schedules
  - `sensors.py` - 3 event-driven sensors
  - `resources.py` - PostgreSQLResource (Pydantic-based), DLT resource
  - `io/io_managers.py` - NoOpIOManager for DLT (data goes to PostgreSQL, not filesystem)
- `src/dbt_hubeau/` - dbt project
  - `models/{staging,intermediate,marts,rejects}/` - SQL models with `schema.yml` tests
  - `macros/` - `cast_silver.sql` (type helpers), `timescaledb.sql` (hypertable creation), `make_point.sql` (PostGIS geometry), `constraints.sql` (PK/FK), `generate_schema_name.sql`
  - `profiles.yml` - PostgreSQL connection (env var templated)
  - `packages.yml` - depends on `dbt_utils`
- `configs/` - YAML configs for API endpoints (`hubeau/`), ERA5 parameters (`era5/`), BDLISA (`bdlisa/`)
- `docker/` - Dockerfiles, init SQL, monitoring (Grafana dashboards, Prometheus), Superset config
- `dagster_home/` - Dagster metadata, `workspace.yaml` (GRPC connection to worker)

### CI/CD

GitLab CI (`.gitlab-ci.yml`): Generates dbt documentation and deploys to GitLab Pages on `main` branch pushes.

## Important Patterns

### DLT Ingestion
All DLT assets use cursor-based pagination, exponential backoff retry (5 attempts for 5xx), MERGE deduplication, and year-based partitioning for chroniques data.

### dbt Incremental Strategy
Staging models use delete+insert with configurable lookback windows (default 7 days, set in `dbt_project.yml` vars). The `on-run-start` hooks in `dbt_project.yml` create indexes and configure TimescaleDB decompression settings.

### TimescaleDB
`bronze.piezometry_chroniques_raw` and `bronze.era5_france_timeseries` are hypertables. Compression policies achieve 90%+ savings on data >90 days old. The `timescaledb.sql` macro handles hypertable creation.

### Hot Reload
- **Python code** (`src/hubeau_pipeline/`): Volume-mounted in worker only. After changes:
  1. `docker compose restart dlt_worker` (picks up volume-mounted code)
  2. **Rebuild orchestrator image**: `docker compose build dagster_webserver` then recreate daemon+webserver (the orchestrator image copies `src/` at build time — the daemon executes runs using its own baked-in code, NOT the worker's volume mount)
  3. Reload code location via Dagster UI or GraphQL `reloadRepositoryLocation`
- **dbt models** (`src/dbt_hubeau/models/`): Volume-mounted, auto-detected by Dagster (reload definitions in UI)
- **Config files** (`configs/`): Volume-mounted, changes apply immediately

> **WARNING**: The daemon container has its own copy of `src/` baked into the `hubeau-orchestrator` image. Restarting only the worker is NOT sufficient for Python code changes — the daemon will continue using stale code from its image.

## Key Constraints

- Python 3.11+, dbt pinned to 1.7.0 (Dagster compatibility), DLT pinned to 0.4.12
- Docker volumes are **external** (not deleted by `docker compose down -v`) - must be created first via `scripts/init_volumes.sh`
- GDAL/GEOS required in worker for geospatial processing (`pyogrio`, `geopandas`)
- The `timescale/timescaledb-ha:pg16` image uses Patroni - do NOT add `command:` overrides in docker-compose

## Access Points

| Service | URL | Notes |
|---------|-----|-------|
| Dagster UI | http://localhost:49500 | Pipeline orchestration |
| PostgreSQL | localhost:49502 | Direct DB access |
| Adminer | http://localhost:49501 | Lightweight DB admin |
| CloudBeaver | http://localhost:49503 | Advanced SQL client |
| Superset | http://localhost:49504 | BI dashboards |
| dbt docs | http://localhost:49505 | Manual: `docker exec brgm-dlt-worker dbt docs serve --port 8080` |
| Grafana | http://localhost:49507 | Monitoring (admin/admin) |
| Prometheus | http://localhost:49508 | Metrics |

## Common Issues

- **Missing TME labels in Gold tables**: Rebuild incremental mapping: `docker exec brgm-dlt-worker dbt run --select int_station_era5_mapping+ --vars '{"recompute_station_era5_mapping": true}'`
- **Hub'Eau API 503**: API overloaded, wait 15-30 min and retry via Dagster UI
- **ERA5 CDS timeouts**: Retry (built-in backoff). Check [CDS Status](https://cds.climate.copernicus.eu/) if persistent
- **Bronze duplicates**: Expected from 7-day overlap window. Silver layer deduplicates automatically
- **Worker won't start**: Check `docker compose logs dlt_worker`. Common: port conflict, stale image (`docker compose build --no-cache dlt_worker`)
- **Phantom hypertables (table is hypertable but shouldn't be)**: The event trigger `timescaledb_ddl_command_end` has been **DISABLED** (2026-03-06) to prevent dbt's `ALTER TABLE RENAME` from re-applying hypertable metadata. This is the permanent fix. If somehow re-enabled: `ALTER EVENT TRIGGER timescaledb_ddl_command_end DISABLE;`. Manual cleanup: `DROP TABLE schema.table CASCADE` then `dbt run --select model_name` (NOT `--full-refresh`). Clean orphaned policies: `SELECT delete_job(id) FROM _timescaledb_config.bgw_job WHERE ...`
- **dbt daily mart runs 1h+ instead of minutes**: Check if `unique_key` is set on an `append` model — dbt 1.7.0 ignores `append` strategy when `unique_key` is present and generates `DELETE...USING` which seq-scans all hypertable chunks. Fix: remove `unique_key` from the model config
- **Bronze daily cleanup fails with `text >= date`**: DLT stores all Bronze columns as `text`. The `_clean_recent_data()` function must cast date columns explicitly (`{}::date >= CURRENT_DATE`). If you see `operator does not exist: text >= date`, check that the `::date` cast is present
- **Dagster sensor crash loop (trailing_unconsumed_events)**: `multi_asset_sensor` must call `context.advance_all_cursors()` on EVERY evaluation, not just when yielding a RunRequest. Without this, events accumulate to the 25-event limit and crash the daemon

## Skills Reference

Skills are invoked via `/skill-name` or the Skill tool. Always check if a skill applies before starting work.

### Development Workflow Skills

These skills form a structured development pipeline. Follow the chain that matches your task.

**Typical feature chain**: `/brainstorming` → `/writing-plans` → `/executing-plans` or `/subagent-driven-development` → `/verification-before-completion` → `/commit` → `/requesting-code-review`

| Skill | Invoke when... | Why |
|-------|---------------|-----|
| `superpowers:brainstorming` | Starting any new feature, design, or creative task | Turns ideas into validated designs via collaborative Q&A. Produces a design doc in `docs/plans/`. **Must precede implementation.** |
| `superpowers:writing-plans` | You have a spec/design and need implementation steps | Creates a step-by-step plan file from the design. Follows brainstorming. |
| `superpowers:executing-plans` | You have a written plan ready to implement | Executes plans step by step with checkpoints. For sequential work. |
| `superpowers:subagent-driven-development` | A plan has independent tasks that can run in parallel | Dispatches subagents for parallel execution. Ideal for multi-model dbt changes or independent asset + job work. |
| `superpowers:dispatching-parallel-agents` | Facing 2+ independent tasks (research, implementation) | Parallel agent coordination. Use when tasks have zero dependencies between them. |
| `superpowers:test-driven-development` | Implementing any feature or bugfix | Write tests first, then implementation. For this project: write dbt `schema.yml` tests before model SQL, or Python tests before asset code. |
| `superpowers:systematic-debugging` | Any bug, test failure, or unexpected behavior | Structured root-cause analysis. Use when dbt tests fail, jobs error, or data quality issues appear. |
| `superpowers:verification-before-completion` | About to claim work is complete | Final checklist before marking done. Prevents missed edge cases. **Always use before committing.** |
| `superpowers:finishing-a-development-branch` | Implementation is complete, all tests pass | Prepares branch for merge: cleanup, final review, squash guidance. |
| `superpowers:using-git-worktrees` | Starting feature work that needs isolation | Creates a git worktree so you can work without affecting main. Useful for multi-day features. |

### Code Review & Git Skills

| Skill | Invoke when... | Why |
|-------|---------------|-----|
| `commit-commands:commit` (`/commit`) | Ready to commit staged changes | Creates well-formatted git commits with proper messages. |
| `commit-commands:commit-push-pr` (`/commit-push-pr`) | Ready to commit, push, and open a PR in one step | Full workflow: commit → push → create PR with summary. |
| `commit-commands:clean_gone` | Branches have piled up locally | Cleans all local branches whose remote tracking branch is `[gone]`. |
| `code-review:code-review` | Need to review a PR (yours or someone else's) | Structured code review with checklist. |
| `superpowers:requesting-code-review` | Completed a feature, want review | Prepares code for review, highlights key changes. |
| `superpowers:receiving-code-review` | Received code review feedback | Structured approach to addressing reviewer comments. |

### Feature Development

| Skill | Invoke when... | Why |
|-------|---------------|-----|
| `feature-dev:feature-dev` | Developing a feature with guided codebase exploration | Full guided workflow: understand codebase → design → implement → test. Combines exploration with implementation. Good starting point when you're not sure which specific workflow to use. |

### Scientific & Data Skills (relevant to this project)

This project handles hydrological time series, geospatial data, and climate reanalysis. These skills are directly useful:

**Geospatial & Data Processing**

| Skill | Invoke when... | Why for this project |
|-------|---------------|---------------------|
| `scientific-skills:geopandas` | Working with spatial data, geometries, PostGIS | Project uses geopandas extensively: BDLISA GeoPackage loading, station coordinates, spatial joins. Use for any `assets/bronze/bdlisa_*.py` or PostGIS work. |
| `scientific-skills:polars` | Processing large DataFrames (alternative to pandas) | Faster than pandas for large chroniques datasets. Consider for new DLT sources or data transformation scripts. |
| `scientific-skills:dask` | Data too large for single-machine pandas | Distributed processing for full historical chroniques (millions of rows). |
| `scientific-skills:zarr-python` | Working with chunked N-D arrays, cloud storage | Useful if ERA5 data needs intermediate storage in chunked format before PostgreSQL load. |

**Analysis & Visualization**

| Skill | Invoke when... | Why for this project |
|-------|---------------|---------------------|
| `scientific-skills:exploratory-data-analysis` | Exploring data quality, distributions, anomalies | Run EDA on Gold tables to validate data pipeline output, detect outliers in chroniques, check ERA5 coverage. |
| `scientific-skills:statistical-analysis` | Trend analysis, seasonality, hypothesis testing | Analyze piezometric trends (`agg_station_trends`), validate seasonal patterns, statistical tests on data quality. |
| `scientific-skills:statsmodels` | Regression, time series decomposition | Trend detection in water levels, seasonal decomposition of hydrometric data, autocorrelation analysis. |
| `scientific-skills:scikit-learn` | Anomaly detection, clustering, classification | Detect anomalous measurements in chroniques, cluster stations by behavior, classify data quality. |
| `scientific-skills:plotly` | Interactive charts and maps | Interactive station maps, time series exploration. Superset uses Plotly internally. |
| `scientific-skills:matplotlib` | Static publication-quality plots | Detailed custom plots when Plotly is overkill. |
| `scientific-skills:seaborn` | Statistical visualization with pandas | Heatmaps of data coverage, distribution plots, correlation matrices between hydro variables. |
| `scientific-skills:scientific-visualization` | Publication-ready multi-panel figures | Meta-skill for combining matplotlib/seaborn/plotly into polished figures. |
| `scientific-skills:networkx` | Graph/network analysis | Model hydrological networks: upstream/downstream station relationships, aquifer connectivity. |

**Research & Documentation**

| Skill | Invoke when... | Why for this project |
|-------|---------------|---------------------|
| `scientific-skills:scientific-writing` | Writing technical reports or documentation | Document data pipeline methodology, data quality reports, architecture decisions. |
| `scientific-skills:hypothesis-generation` | Formulating research questions from data | Generate hypotheses about groundwater trends, climate impact on hydrology. |
| `scientific-skills:literature-review` | Reviewing scientific literature | Research Hub'Eau API changes, ERA5 methodology, hydrological analysis methods. |

**Export & Reporting**

| Skill | Invoke when... | Why for this project |
|-------|---------------|---------------------|
| `scientific-skills:xlsx` | Creating/editing spreadsheets | Export Gold table summaries, station inventories, data quality reports to Excel. |
| `scientific-skills:pdf` | PDF generation or manipulation | Generate PDF reports from pipeline data, extract data from PDF sources. |
| `scientific-skills:docx` | Creating Word documents | Technical documentation, data delivery reports. |
| `scientific-skills:pptx` | Creating presentations | Pipeline status presentations, data analysis results. |
| `scientific-skills:scientific-slides` | Research talk slide decks | Present hydro data analysis findings, pipeline architecture talks. |
| `scientific-skills:markitdown` | Converting files to Markdown | Convert existing docs to Markdown for the repository. |

### Frontend & BI

| Skill | Invoke when... | Why for this project |
|-------|---------------|---------------------|
| `frontend-design:frontend-design` | Building web interfaces | Custom dashboards beyond Superset, data quality monitoring UIs, admin panels. |

### Meta & Maintenance Skills

| Skill | Invoke when... | Why |
|-------|---------------|-----|
| `claude-md-management:revise-claude-md` | End of a productive session with new learnings | Updates this CLAUDE.md with patterns discovered during work. |
| `claude-md-management:claude-md-improver` | CLAUDE.md feels outdated or incomplete | Audits and improves CLAUDE.md files. |
| `claude-code-setup:claude-automation-recommender` | Want to set up hooks/automations | Analyzes codebase and recommends Claude Code automations (pre-commit hooks, etc.). |
| `superpowers:writing-skills` | Creating or editing custom skills | For building project-specific skills (e.g., a "dbt-model-generator" skill). |

### Common Workflow Examples

**Adding a new data source (e.g., new Hub'Eau API endpoint)**:
1. `/brainstorming` — design the asset, schema, API config
2. `/writing-plans` — plan: YAML config → DLT source → Dagster asset → dbt staging model → tests
3. `/test-driven-development` — write `schema.yml` tests first, then model SQL
4. `/verification-before-completion` — verify end-to-end
5. `/commit-push-pr` — ship it

**Debugging a dbt test failure**:
1. `/systematic-debugging` — trace from failing test → model SQL → source data → root cause

**Analyzing data quality on Gold tables**:
1. `scientific-skills:exploratory-data-analysis` — profile data distributions
2. `scientific-skills:statistical-analysis` — test for anomalies and trends
3. `scientific-skills:plotly` or `seaborn` — visualize findings

**Creating a data export/report**:
1. `scientific-skills:xlsx` or `pdf` — generate formatted output from Gold tables
2. `scientific-skills:scientific-writing` — write methodology section if needed
