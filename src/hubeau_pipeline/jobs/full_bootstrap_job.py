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
    Config,
    RunConfig,
)
import psycopg2
import yaml


# Configuration
START_YEAR = 1990
CURRENT_YEAR = datetime.now().year
DELAY_BETWEEN_PARTITIONS = 5  # seconds between API calls


# ==============================================================================
# HELPER: Get DB connection
# ==============================================================================

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv('PG_HOST', 'postgres'),
        port=os.getenv('PG_PORT', '5432'),
        database=os.getenv('PG_DB', 'postgres'),
        user=os.getenv('PG_USER', 'postgres'),
        password=os.getenv('PG_PASSWORD'),
        sslmode=os.getenv('PG_SSLMODE', 'prefer')
    )


def _env_true(name: str, default: str = "false") -> bool:
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes", "y"}


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
    """Initialize bootstrap job."""
    context.log.info("🚀 ═══════════════════════════════════════════════════════════")
    context.log.info("🚀 FULL BOOTSTRAP JOB STARTED")
    context.log.info("🚀 ═══════════════════════════════════════════════════════════")
    context.log.info(f"📅 Period: {START_YEAR} → {CURRENT_YEAR}")
    context.log.info(f"⏱️  Started at: {datetime.now().isoformat()}")
    context.log.info("⚠️  This job will take MANY HOURS to complete!")
    context.log.info("⚠️  Do NOT run other data jobs while this is running!")


@op(ins={"start": In(Nothing)}, out=Out(Nothing), required_resource_keys={"pg"})
def load_reference_data(context: OpExecutionContext) -> Nothing:
    """Load reference data: TME (entités hydrogéologiques). Required for stg_tme_entites."""
    from ..assets.bronze.tme_entites_assets import load_tme_entites_to_bronze

    context.log.info("📚 STEP 0/5: Loading reference data (TME)...")
    job_name = "reference_data"
    partition_key = "all"
    if _should_skip_partition(job_name, partition_key):
        context.log.info("📚 Reference data already loaded. Skipping.")
        return
    with get_db_connection() as conn:
        _mark_partition_start(conn, job_name, partition_key)
    pg = context.resources.pg
    try:
        load_tme_entites_to_bronze(context, pg)
    except Exception as e:
        with get_db_connection() as conn:
            _mark_partition_failed(conn, job_name, partition_key, str(e))
        raise
    with get_db_connection() as conn:
        _mark_partition_success(conn, job_name, partition_key)
    context.log.info("📚 Reference data loaded.")


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
        "password": os.getenv("PG_PASSWORD", "postgres"),
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
    configs = [
        ("piezometry_stations", "configs/hubeau/piezometry_stations.yml"),
        ("hydrometry_sites", "configs/hubeau/hydrometry_sites.yml"),
        ("hydrometry_stations", "configs/hubeau/hydrometry_stations.yml"),
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
    from ..sources.hubeau_csv_source import hubeau_chroniques_year, hubeau_stations
    
    import dlt
    from dlt.destinations import postgres

    context.log.info("📊 ═══════════════════════════════════════════════════════════")
    context.log.info("📊 STEP 2/5: Loading ALL chroniques (SEQUENTIAL, via DLT)...")
    context.log.info(f"📊 Period: {START_YEAR} → {CURRENT_YEAR}")
    context.log.info("📊 ═══════════════════════════════════════════════════════════")
    
    # Credentials from standard env vars
    db_creds = {
        "database": os.getenv("PG_DB", "postgres"),
        "password": os.getenv("PG_PASSWORD", "postgres"),
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
            "stations_config": "configs/hubeau/piezometry_stations.yml",
            "chroniques_config": "configs/hubeau/piezometry_chroniques.yml",
            "station_code_field": "code_bss",
            "table_name": "piezometry_chroniques_raw"
        },
        {
            "name": "hydrometry",
            "stations_config": "configs/hubeau/hydrometry_stations.yml",
            "chroniques_config": "configs/hubeau/hydrometry_obs_elab.yml",
            "station_code_field": "code_site",
            "table_name": "hydrometry_obs_elab_raw"
        },
    ]
    
    for api in apis:
        context.log.info(f"📊 Processing {api['name']} chroniques...")
        
        # Load station codes
        with open(api["stations_config"]) as f:
            stations_config = yaml.safe_load(f)
        
        station_codes = []
        for record in hubeau_stations(stations_config, dagster_context=context):
            code = record.get(api["station_code_field"])
            if code:
                station_codes.append(code)
        
        context.log.info(f"  📍 Found {len(station_codes):,} stations")
        
        # Load chroniques year by year
        with open(api["chroniques_config"]) as f:
            chroniques_config = yaml.safe_load(f)
        
        for year in range(START_YEAR, CURRENT_YEAR + 1):
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
    """Load ALL ERA5 data SEQUENTIALLY (1990-present, 2-year chunks)."""
    # Fix: Import direct function, not asset
    from ..assets.bronze.era5_assets import process_era5_range_to_timeseries
    
    context.log.info("🌤️  ═══════════════════════════════════════════════════════════")
    context.log.info("🌤️  STEP 3/5: Loading ALL ERA5 data (SEQUENTIAL, 2-year chunks)...")
    context.log.info(f"🌤️  Period: {START_YEAR} → {CURRENT_YEAR}")
    context.log.info("🌤️  ═══════════════════════════════════════════════════════════")
    
    year = START_YEAR
    while year <= CURRENT_YEAR:
        chunk_end = min(year + 1, CURRENT_YEAR)  # 2-year chunk
        
        start_date = datetime(year, 1, 1)
        end_date = datetime(chunk_end, 12, 31)
        file_id = f"era5_bootstrap_{year}_{chunk_end}"
        
        context.log.info(f"  🌤️  Loading ERA5 {year}-{chunk_end}...")
        job_name = "era5"
        partition_key = f"{year}-{chunk_end}"
        if _should_skip_partition(job_name, partition_key):
            context.log.info(f"  ↪️  Skipping era5 {partition_key} (already completed)")
            year += 2
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
        
        year += 2  # Move to next 2-year chunk
    
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
        result = subprocess.run(
            ["dbt", "run", "--project-dir", "/app/src/dbt_hubeau", "--profiles-dir", "/app/src/dbt_hubeau"],
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
    """Log job completion."""
    context.log.info("🎉 ═══════════════════════════════════════════════════════════")
    context.log.info("🎉 FULL BOOTSTRAP JOB COMPLETED!")
    context.log.info("🎉 ═══════════════════════════════════════════════════════════")
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
