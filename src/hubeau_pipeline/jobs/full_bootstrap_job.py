"""
Full Bootstrap Job - Complete Database Population (Sequential)

A single job that populates the ENTIRE database from scratch:
0. Load reference data (TME uniquement) — requis pour stg_tme_entites
1. Load all station metadata
2. Load chroniques SEQUENTIALLY (1990-present, one year at a time)
3. Load ERA5 SEQUENTIALLY (1990-present, 2-year chunks)
4. Run dbt Silver/Gold transformations

Restartability:
- Persists partition status in ops.bootstrap_state
- Supports BOOTSTRAP_PARTITIONS (allowlist), BOOTSTRAP_FORCE_RERUN, BOOTSTRAP_CONTINUE_ON_ERROR

IMPORTANT: This job runs partitions SEQUENTIALLY to avoid API rate limits!
WARNING: This job will take MANY HOURS (possibly days) to complete!
"""

import os
import time
from datetime import datetime
from dagster import (
    job,
    op,
    In,
    Out,
    Nothing,
    OpExecutionContext,
)
import psycopg2
from psycopg2 import sql as psql
import yaml
from contextlib import contextmanager


# Configuration
START_YEAR = 1990
DELAY_BETWEEN_PARTITIONS = 5  # seconds between API calls


def _get_current_year() -> int:
    """Return current year at runtime (not import time)."""
    return datetime.now().year


# ==============================================================================
# HELPER: Get DB connection
# ==============================================================================

@contextmanager
def get_db_connection():
    conn = psycopg2.connect(
        host=os.getenv('PG_HOST', 'postgres'),
        port=os.getenv('PG_PORT', '5432'),
        database=os.getenv('PG_DB', 'postgres'),
        user=os.getenv('PG_USER', 'postgres'),
        password=os.getenv('PG_PASSWORD'),
        sslmode=os.getenv('PG_SSLMODE', 'prefer')
    )
    try:
        yield conn
    finally:
        conn.close()


def try_acquire_bootstrap_lock(conn, context) -> bool:
    """
    Try to acquire an exclusive advisory lock for bootstrap job.
    Uses pg_try_advisory_lock (non-blocking) to detect concurrent runs.
    Returns True if lock acquired, False if already held by another session.
    Note: Lock is released when the connection closes.
    """
    LOCK_ID = 123456789  # Arbitrary unique lock ID for bootstrap
    with conn.cursor() as cur:
        context.log.info("🔒 Checking bootstrap advisory lock...")
        cur.execute("SELECT pg_try_advisory_lock(%s)", (LOCK_ID,))
        acquired = cur.fetchone()[0]
    conn.commit()
    if acquired:
        context.log.info("✅ Bootstrap lock acquired")
    else:
        context.log.warning("⚠️ Bootstrap lock already held by another session")
    return acquired


from hubeau_pipeline.utils import env_true as _env_true


def _selected_partitions() -> set[str] | None:
    raw = os.getenv("BOOTSTRAP_PARTITIONS")
    if not raw:
        return None
    return {p.strip() for p in raw.split(",") if p.strip()}


def _ensure_bootstrap_state_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS ops")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS ops.bootstrap_state (
                job_name TEXT NOT NULL,
                partition_key TEXT NOT NULL,
                status TEXT NOT NULL,
                started_at TIMESTAMPTZ,
                completed_at TIMESTAMPTZ,
                error TEXT,
                PRIMARY KEY (job_name, partition_key)
            )
            """
        )
    conn.commit()


def _partition_is_completed(conn, job_name: str, partition_key: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT status FROM ops.bootstrap_state WHERE job_name = %s AND partition_key = %s",
            (job_name, partition_key),
        )
        row = cur.fetchone()
    return bool(row and row[0] == "completed")


def _mark_partition_start(conn, job_name: str, partition_key: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ops.bootstrap_state (job_name, partition_key, status, started_at)
            VALUES (%s, %s, 'running', NOW())
            ON CONFLICT (job_name, partition_key)
            DO UPDATE SET status = 'running', started_at = EXCLUDED.started_at, completed_at = NULL, error = NULL
            """,
            (job_name, partition_key),
        )
    conn.commit()


def _mark_partition_success(conn, job_name: str, partition_key: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE ops.bootstrap_state
            SET status = 'completed', completed_at = NOW(), error = NULL
            WHERE job_name = %s AND partition_key = %s
            """,
            (job_name, partition_key),
        )
    conn.commit()


def _mark_partition_failed(conn, job_name: str, partition_key: str, error: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE ops.bootstrap_state
            SET status = 'failed', completed_at = NOW(), error = %s
            WHERE job_name = %s AND partition_key = %s
            """,
            (error[:2000], job_name, partition_key),
        )
    conn.commit()


def _should_skip_partition(job_name: str, partition_key: str) -> bool:
    selected = _selected_partitions()
    if selected is not None:
        # Explicit allowlist: only run matching partitions, regardless of completion
        return f"{job_name}:{partition_key}" not in selected
    if _env_true("BOOTSTRAP_FORCE_RERUN", "false"):
        return False
    with get_db_connection() as conn:
        _ensure_bootstrap_state_table(conn)
        return _partition_is_completed(conn, job_name, partition_key)


def _continue_on_error() -> bool:
    return _env_true("BOOTSTRAP_CONTINUE_ON_ERROR", "false")


# ==============================================================================
# OPS - Sequential steps
# ==============================================================================

@op(out=Out(Nothing))
def bootstrap_start(context: OpExecutionContext) -> Nothing:
    """Initialize bootstrap job and acquire exclusive lock."""
    context.log.info("🚀 ═══════════════════════════════════════════════════════════")
    context.log.info("🚀 FULL BOOTSTRAP JOB STARTED")
    context.log.info("🚀 ═══════════════════════════════════════════════════════════")

    # Check for concurrent bootstrap via advisory lock.
    # Lock is released when connection closes, so this is a best-effort check.
    # Primary concurrency protection is Dagster's max_concurrent_runs tag.
    with get_db_connection() as conn:
        if not try_acquire_bootstrap_lock(conn, context):
            raise RuntimeError(
                "Another bootstrap job is already running. "
                "Wait for it to finish or kill the other session."
            )
    context.log.info(f"📅 Period: {START_YEAR} → {_get_current_year()}")
    context.log.info(f"⏱️  Started at: {datetime.now().isoformat()}")
    context.log.info("⚠️  This job will take MANY HOURS to complete!")
    context.log.info("⚠️  Do NOT run other data jobs while this is running!")


@op(ins={"start": In(Nothing)}, out=Out(Nothing), required_resource_keys={"pg"})
def load_reference_data(context: OpExecutionContext) -> Nothing:
    """Load TME reference data (entités hydrogéologiques uniquement)."""
    from ..assets.bronze.tme_entites_assets import load_tme_entites_to_bronze

    context.log.info("📚 STEP 0/5: Loading TME reference data (entités hydrogéologiques)...")
    job_name = "reference_data"
    partition_key = "all"
    if _should_skip_partition(job_name, partition_key):
        context.log.info("📚 TME reference data already loaded. Skipping.")
        return
    with get_db_connection() as conn:
        _mark_partition_start(conn, job_name, partition_key)
    pg = context.resources.pg
    try:
        context.log.info("  → Loading TME entities...")
        load_tme_entites_to_bronze(context, pg)
    except Exception as e:
        with get_db_connection() as conn:
            _mark_partition_failed(conn, job_name, partition_key, str(e))
        raise
    with get_db_connection() as conn:
        _mark_partition_success(conn, job_name, partition_key)
    context.log.info("📚 TME reference data loaded.")


@op(ins={"ref_data": In(Nothing)}, out=Out(Nothing))
def load_all_stations(context: OpExecutionContext) -> Nothing:
    """Load ALL station metadata (non-partitioned)."""
    from ..sources.hubeau_csv_source import hubeau_stations
    from dlt.sources.helpers.rest_client import RESTClient
    
    import dlt
    from dlt.destinations import postgres

    context.log.info("📍 ═══════════════════════════════════════════════════════════")
    context.log.info("📍 STEP 1/5: Loading ALL station metadata (via DLT)...")
    context.log.info("📍 ═══════════════════════════════════════════════════════════")
    job_name = "stations"
    partition_key = "all"
    if _should_skip_partition(job_name, partition_key):
        context.log.info("📍 Stations already loaded. Skipping.")
        return
    with get_db_connection() as conn:
        _mark_partition_start(conn, job_name, partition_key)

    # Credentials from standard env vars (available in all containers)
    db_creds = {
        "database": os.getenv("PG_DB", "postgres"),
        "password": os.getenv("PG_PASSWORD", ""),
        "username": os.getenv("PG_USER", "postgres"),
        "host": os.getenv("PG_HOST", "postgres"),
        "port": int(os.getenv("PG_PORT", "5432")),
    }

    # Configure DLT Pipeline
    pipeline = dlt.pipeline(
        pipeline_name="hubeau_stations_bootstrap",
        destination=postgres(credentials=db_creds),
        dataset_name="bronze",
    )

    # Load configs and run pipeline
    # IMPORTANT: Les noms doivent correspondre aux assets DLT (*_raw) pour que dbt fonctionne
    configs = [
        ("piezometry_stations_raw", "configs/hubeau/piezometry_stations.yml"),
        ("hydrometry_sites_raw", "configs/hubeau/hydrometry_sites.yml"),
        ("hydrometry_stations_raw", "configs/hubeau/hydrometry_stations.yml"),
    ]
    
    try:
        for table_name, config_path in configs:
            context.log.info(f"  📍 Loading {table_name}...")
            with open(config_path) as f:
                config = yaml.safe_load(f)
            
            # Wrap the generator in a DLT resource
            @dlt.resource(name=table_name, write_disposition="replace")
            def resource_wrapper():
                yield from hubeau_stations(config, dagster_context=context)

            # RUN PIPELINE
            info = pipeline.run(resource_wrapper())
            context.log.info(f"  ✅ {table_name}: {info}")
            
            time.sleep(DELAY_BETWEEN_PARTITIONS)
    except Exception as e:
        context.log.error(f"  ❌ stations failed: {e}")
        with get_db_connection() as conn:
            _mark_partition_failed(conn, job_name, partition_key, str(e))
        raise
    with get_db_connection() as conn:
        _mark_partition_success(conn, job_name, partition_key)
    
    context.log.info("📍 Station loading complete!")


@op(ins={"stations": In(Nothing)}, out=Out(Nothing))
def load_all_chroniques_sequential(context: OpExecutionContext) -> Nothing:
    """Load ALL chroniques SEQUENTIALLY (1990-present)."""
    from ..sources.hubeau_csv_source import hubeau_chroniques_year
    
    import dlt
    from dlt.destinations import postgres

    context.log.info("📊 ═══════════════════════════════════════════════════════════")
    context.log.info("📊 STEP 2/5: Loading ALL chroniques (SEQUENTIAL, via DLT)...")
    context.log.info(f"📊 Period: {START_YEAR} → {_get_current_year()}")
    context.log.info("📊 ═══════════════════════════════════════════════════════════")
    
    # Credentials from standard env vars
    db_creds = {
        "database": os.getenv("PG_DB", "postgres"),
        "password": os.getenv("PG_PASSWORD", ""),
        "username": os.getenv("PG_USER", "postgres"),
        "host": os.getenv("PG_HOST", "postgres"),
        "port": int(os.getenv("PG_PORT", "5432")),
    }

    # Configure DLT Pipeline
    pipeline = dlt.pipeline(
        pipeline_name="hubeau_chroniques_bootstrap",
        destination=postgres(credentials=db_creds),
        dataset_name="bronze",
    )

    # Load station codes first
    apis = [
        {
            "name": "piezometry",
            "stations_table": "piezometry_stations_raw",
            "chroniques_config": "configs/hubeau/piezometry_chroniques.yml",
            "station_code_field": "code_bss",
            "table_name": "piezometry_chroniques_raw"
        },
        {
            "name": "hydrometry",
            "stations_table": "hydrometry_sites_raw",  # code_site vient des sites, pas stations
            "chroniques_config": "configs/hubeau/hydrometry_obs_elab.yml",
            "station_code_field": "code_site",
            "table_name": "hydrometry_obs_elab_raw"
        },
    ]
    
    for api in apis:
        context.log.info(f"📊 Processing {api['name']} chroniques...")
        
        # Load station codes FROM DATABASE (stations already loaded in step 1)
        station_codes = []
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(psql.SQL(
                    'SELECT DISTINCT {col} FROM bronze.{tbl} WHERE {col} IS NOT NULL'
                ).format(
                    col=psql.Identifier(api["station_code_field"]),
                    tbl=psql.Identifier(api["stations_table"]),
                ))
                station_codes = [row[0] for row in cur.fetchall()]
        
        context.log.info(f"  📍 Found {len(station_codes):,} stations in database")
        
        # Load chroniques year by year
        with open(api["chroniques_config"]) as f:
            chroniques_config = yaml.safe_load(f)
        
        for year in range(START_YEAR, _get_current_year() + 1):
            context.log.info(f"  📅 {api['name']} - Year {year}...")
            job_name = "chroniques"
            partition_key = f"{api['name']}:{year}"
            if _should_skip_partition(job_name, partition_key):
                context.log.info(f"  ↪️  Skipping {partition_key} (already completed)")
                continue
            with get_db_connection() as conn:
                _mark_partition_start(conn, job_name, partition_key)
            try:
                # Wrap generator in DLT resource
                @dlt.resource(name=api["table_name"], write_disposition="append")
                def resource_wrapper():
                    yield from hubeau_chroniques_year(
                        chroniques_config, 
                        str(year), 
                        station_codes, 
                        dagster_context=context
                    )

                # RUN PIPELINE
                info = pipeline.run(resource_wrapper())
                context.log.info(f"  ✅ {year}: {info}")
                with get_db_connection() as conn:
                    _mark_partition_success(conn, job_name, partition_key)

                # Rate limit: wait between years
                time.sleep(DELAY_BETWEEN_PARTITIONS)
                
            except Exception as e:
                context.log.error(f"  ❌ {year} failed: {e}")
                with get_db_connection() as conn:
                    _mark_partition_failed(conn, job_name, partition_key, str(e))
                if _continue_on_error():
                    context.log.warning("  ↪️  Continuing after error (BOOTSTRAP_CONTINUE_ON_ERROR=true)")
                    continue
                raise
    
    context.log.info("📊 Chroniques loading complete!")


@op(ins={"chroniques": In(Nothing)}, out=Out(Nothing))
def load_all_era5_sequential(context: OpExecutionContext) -> Nothing:
    """Load ALL ERA5 data SEQUENTIALLY (1990-present), chunked by ERA5_YEARS_PER_CHUNK."""
    # Import direct function + chunk size (source unique de vérité, partagée avec
    # ERA5_PARTITIONS_DEF utilisé par l'asset weekly/historique).
    from ..assets.bronze.era5_assets import ERA5_YEARS_PER_CHUNK, process_era5_range_to_timeseries

    context.log.info("🌤️  ═══════════════════════════════════════════════════════════")
    context.log.info(f"🌤️  STEP 3/5: Loading ALL ERA5 data (SEQUENTIAL, {ERA5_YEARS_PER_CHUNK}-year chunks)...")
    context.log.info(f"🌤️  Period: {START_YEAR} → {_get_current_year()}")
    context.log.info("🌤️  ═══════════════════════════════════════════════════════════")
    
    current_year = _get_current_year()
    year = START_YEAR
    while year <= current_year:
        chunk_end = min(year + ERA5_YEARS_PER_CHUNK - 1, current_year)

        start_date = datetime(year, 1, 1)
        end_date = datetime(chunk_end, 12, 31)
        file_id = f"era5_bootstrap_{year}_{chunk_end}"

        context.log.info(f"  🌤️  Loading ERA5 {year}-{chunk_end}...")
        job_name = "era5"
        partition_key = f"{year}-{chunk_end}"
        if _should_skip_partition(job_name, partition_key):
            context.log.info(f"  ↪️  Skipping era5 {partition_key} (already completed)")
            year += ERA5_YEARS_PER_CHUNK
            continue
        with get_db_connection() as conn:
            _mark_partition_start(conn, job_name, partition_key)

        try:
            # Direct insertion function (handles DB connection internally)
            rows = process_era5_range_to_timeseries(context, start_date, end_date, file_id)
            context.log.info(f"  ✅ {year}-{chunk_end}: {rows:,} rows inserted")
            with get_db_connection() as conn:
                _mark_partition_success(conn, job_name, partition_key)
        except Exception as e:
            context.log.error(f"  ❌ {year}-{chunk_end} failed: {e}")
            with get_db_connection() as conn:
                _mark_partition_failed(conn, job_name, partition_key, str(e))
            if not _continue_on_error():
                raise
        
        # Rate limit: wait between chunks (longer for ERA5)
        time.sleep(DELAY_BETWEEN_PARTITIONS * 2)

        year += ERA5_YEARS_PER_CHUNK  # Next chunk (granularité partagée avec ERA5_PARTITIONS_DEF)
    
    context.log.info("🌤️  ERA5 loading complete!")


@op(ins={"era5": In(Nothing)}, out=Out(Nothing))
def run_dbt_full(context: OpExecutionContext) -> Nothing:
    """Run full dbt transformation pipeline."""
    import subprocess
    
    context.log.info("🔄 ═══════════════════════════════════════════════════════════")
    context.log.info("🔄 STEP 4/5: Running dbt Silver/Gold transformations...")
    context.log.info("🔄 ═══════════════════════════════════════════════════════════")
    
    job_name = "dbt"
    partition_key = "full"
    if _should_skip_partition(job_name, partition_key):
        context.log.info("🔄 dbt already completed. Skipping.")
        return
    with get_db_connection() as conn:
        _mark_partition_start(conn, job_name, partition_key)
    try:
        dbt_project_dir = os.getenv("DBT_PROJECT_DIR", "/app/src/dbt_hubeau")
        result = subprocess.run(
            ["dbt", "run", "--project-dir", dbt_project_dir, "--profiles-dir", dbt_project_dir],
            capture_output=True,
            text=True,
            timeout=3600  # 1 hour timeout
        )
        context.log.info(result.stdout)
        if result.returncode != 0:
            context.log.error(result.stderr)
            raise RuntimeError(f"dbt run failed with code {result.returncode}")
    except Exception as e:
        with get_db_connection() as conn:
            _mark_partition_failed(conn, job_name, partition_key, str(e))
        raise
    with get_db_connection() as conn:
        _mark_partition_success(conn, job_name, partition_key)
    
    context.log.info("🔄 dbt transformations complete!")


@op(ins={"dbt": In(Nothing)})
def bootstrap_complete(context: OpExecutionContext):
    """Log job completion and release lock."""
    context.log.info("🎉 ═══════════════════════════════════════════════════════════")
    context.log.info("🎉 FULL BOOTSTRAP JOB COMPLETED!")
    context.log.info("🎉 ═══════════════════════════════════════════════════════════")

    # Advisory lock is automatically released when connection closes
    # (connection was opened in bootstrap_start)
    context.log.info("✅ Bootstrap lock will be released on connection close")
    context.log.info(f"📅 Completed at: {datetime.now().isoformat()}")


# ==============================================================================
# JOB - Sequential pipeline
# ==============================================================================

@job(
    name="full_bootstrap",
    description=(
        "🚀 FULL DATABASE BOOTSTRAP - Complete data population from 1990 to present. "
        "Runs SEQUENTIALLY to respect API rate limits. "
        "⚠️ WARNING: This job will take MANY HOURS (possibly days) to complete!"
    ),
    tags={
        "dagster/priority": "1",
        "environment": "bootstrap",
    },
    hooks=set(),
)
def full_bootstrap_job():
    """Full sequential bootstrap pipeline (reference data: TME uniquement)."""
    start = bootstrap_start()
    ref_data = load_reference_data(start=start)
    stations = load_all_stations(ref_data=ref_data)
    chroniques = load_all_chroniques_sequential(stations=stations)
    era5 = load_all_era5_sequential(chroniques=chroniques)
    dbt = run_dbt_full(era5=era5)
    bootstrap_complete(dbt=dbt)
