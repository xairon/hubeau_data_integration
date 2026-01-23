"""
Full Bootstrap Job - Complete Database Population (Sequential)

A single job that populates the ENTIRE database from scratch:
1. Load all station metadata
2. Load chroniques SEQUENTIALLY (1990-present, one year at a time)
3. Load ERA5 SEQUENTIALLY (1990-present, 2-year chunks)
4. Run dbt Silver/Gold transformations

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

from ..hooks import log_failure_hook, slack_failure_hook, email_failure_hook


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


@op(ins={"start": In(Nothing)}, out=Out(Nothing))
def load_all_stations(context: OpExecutionContext) -> Nothing:
    """Load ALL station metadata (non-partitioned)."""
    from ..sources.hubeau_csv_source import hubeau_stations
    from dlt.sources.helpers.rest_client import RESTClient
    
    import dlt
    from dlt.destinations import postgres

    context.log.info("📍 ═══════════════════════════════════════════════════════════")
    context.log.info("📍 STEP 1/4: Loading ALL station metadata (via DLT)...")
    context.log.info("📍 ═══════════════════════════════════════════════════════════")
    
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
    
    for table_name, config_path in configs:
        context.log.info(f"  📍 Loading {table_name}...")
        try:
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
            context.log.error(f"  ❌ {table_name} failed: {e}")
            raise e
    
    context.log.info("📍 Station loading complete!")


@op(ins={"stations": In(Nothing)}, out=Out(Nothing))
def load_all_chroniques_sequential(context: OpExecutionContext) -> Nothing:
    """Load ALL chroniques SEQUENTIALLY (1990-present)."""
    from ..sources.hubeau_csv_source import hubeau_chroniques_year, hubeau_stations
    
    import dlt
    from dlt.destinations import postgres

    context.log.info("📊 ═══════════════════════════════════════════════════════════")
    context.log.info("📊 STEP 2/4: Loading ALL chroniques (SEQUENTIAL, via DLT)...")
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
            "table_name": "piezometry_chroniques"
        },
        {
            "name": "hydrometry",
            "stations_config": "configs/hubeau/hydrometry_stations.yml",
            "chroniques_config": "configs/hubeau/hydrometry_obs_elab.yml",
            "station_code_field": "code_site",
            "table_name": "hydrometry_observations"
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
                
                # Rate limit: wait between years
                time.sleep(DELAY_BETWEEN_PARTITIONS)
                
            except Exception as e:
                context.log.error(f"  ❌ {year} failed: {e}")
                # Continue to next year
    
    context.log.info("📊 Chroniques loading complete!")


@op(ins={"chroniques": In(Nothing)}, out=Out(Nothing))
def load_all_era5_sequential(context: OpExecutionContext) -> Nothing:
    """Load ALL ERA5 data SEQUENTIALLY (1990-present, 2-year chunks)."""
    # Fix: Import direct function, not asset
    from ..assets.bronze.era5_assets import process_era5_range_to_timeseries
    
    context.log.info("🌤️  ═══════════════════════════════════════════════════════════")
    context.log.info("🌤️  STEP 3/4: Loading ALL ERA5 data (SEQUENTIAL, 2-year chunks)...")
    context.log.info(f"🌤️  Period: {START_YEAR} → {CURRENT_YEAR}")
    context.log.info("🌤️  ═══════════════════════════════════════════════════════════")
    
    year = START_YEAR
    while year <= CURRENT_YEAR:
        chunk_end = min(year + 1, CURRENT_YEAR)  # 2-year chunk
        
        start_date = datetime(year, 1, 1)
        end_date = datetime(chunk_end, 12, 31)
        file_id = f"era5_bootstrap_{year}_{chunk_end}"
        
        context.log.info(f"  🌤️  Loading ERA5 {year}-{chunk_end}...")
        
        try:
            # Direct insertion function (handles DB connection internally)
            rows = process_era5_range_to_timeseries(context, start_date, end_date, file_id)
            context.log.info(f"  ✅ {year}-{chunk_end}: {rows:,} rows inserted")
        except Exception as e:
            context.log.error(f"  ❌ {year}-{chunk_end} failed: {e}")
        
        # Rate limit: wait between chunks (longer for ERA5)
        time.sleep(DELAY_BETWEEN_PARTITIONS * 2)
        
        year += 2  # Move to next 2-year chunk
    
    context.log.info("🌤️  ERA5 loading complete!")


@op(ins={"era5": In(Nothing)}, out=Out(Nothing))
def run_dbt_full(context: OpExecutionContext) -> Nothing:
    """Run full dbt transformation pipeline."""
    import subprocess
    
    context.log.info("🔄 ═══════════════════════════════════════════════════════════")
    context.log.info("🔄 STEP 4/4: Running dbt Silver/Gold transformations...")
    context.log.info("🔄 ═══════════════════════════════════════════════════════════")
    
    try:
        result = subprocess.run(
            ["dbt", "run", "--project-dir", "/app/src/dbt_hubeau"],
            capture_output=True,
            text=True,
            timeout=3600  # 1 hour timeout
        )
        context.log.info(result.stdout)
        if result.returncode != 0:
            context.log.error(result.stderr)
    except Exception as e:
        context.log.error(f"dbt failed: {e}")
    
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
    """Full sequential bootstrap pipeline."""
    start = bootstrap_start()
    stations = load_all_stations(start=start)
    chroniques = load_all_chroniques_sequential(stations=stations)
    era5 = load_all_era5_sequential(chroniques=chroniques)
    dbt = run_dbt_full(era5=era5)
    bootstrap_complete(dbt=dbt)
