"""
Bronze Layer DLT Assets - Hub'Eau Data Pipeline

This module defines 22 Dagster assets that load raw Hub'Eau data into PostgreSQL
using DLT (Data Load Tool) with standard patterns:

PATTERN 1: STATIONS (8 assets) - FULL mode
- Uses hubeau_stations() resource with write_disposition="replace"
- Replaces all data on each run

PATTERN 2: CHRONIQUES (14 assets) - MODE partition + INCREMENTAL
- With partition "full": Uses hubeau_chroniques_year() to load ALL historical data
- With partition "2020"-"2025": Uses hubeau_chroniques_year() for specific year backfills
- Without partition: Uses hubeau_chroniques_incremental() for ongoing updates
- Uses delete_year_data() for idempotence in partition modes
"""

import os
import yaml
import dlt
from dagster import asset, StaticPartitionsDefinition
from typing import Dict, Any

from hubeau_pipeline.utils.db_helpers import delete_year_data
from hubeau_pipeline.sources.hubeau_csv_source import (
    hubeau_stations,
    hubeau_chroniques_year,
    hubeau_chroniques_incremental,
)

# ============================================================================
# PARTITIONS DEFINITION
# ============================================================================

# MODE_PARTITIONS: FULL (all data) + YEAR-based backfills (2020-2025)
# - "full" = Load ALL data (no date filter) - for production initial load
# - "2020"-"2025" = Load specific year - for testing or targeted backfills
# - NO PARTITION (incremental) = Load since last date - for ongoing updates (DLT managed)
MODE_PARTITIONS = StaticPartitionsDefinition([
    "full",  # Load ALL data (production mode)
    "2020", "2021", "2022", "2023", "2024", "2025"  # Year-specific backfills (testing/targeted)
])


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def extract_load_info_metrics(load_info, table_name: str) -> Dict[str, Any]:
    """
    Extract serializable metrics from DLT load_info object.
    
    Args:
        load_info: DLT LoadInfo object
        table_name: Name of the table that was loaded
    
    Returns:
        Dictionary with serializable metrics
    """
    rows_loaded = 0
    jobs_count = 0
    
    try:
        for package in load_info.load_packages:
            if package.jobs:
                for job in package.jobs:
                    jobs_count += 1
                    items = job.metrics.get("items", 0)
                    if isinstance(items, (int, float)):
                        rows_loaded += int(items)
    except Exception:
        pass
    
    return {
        "rows_loaded": rows_loaded,
        "table_name": table_name,
        "jobs_count": jobs_count,
        "status": "success"
    }


# ============================================================================
# HELPER FUNCTION: DLT Pipeline Creation
# ============================================================================

def _create_dlt_pipeline(pipeline_name: str, context=None) -> dlt.Pipeline:
    """
    Create OPTIMIZED DLT pipeline with PostgreSQL destination.

    CRITICAL FIX: Uses unique pipeline_name per Dagster run to prevent
    file system conflicts when multiple runs execute concurrently.

    DLT FEATURES ENABLED:
    - Automatic schema detection and evolution
    - Automatic column name normalization
    - Type inference and validation
    - Batch processing for large datasets
    - Error handling and retry mechanisms
    - Progress tracking and detailed metrics
    - Schema normalization

    OPTIMIZATIONS:
    1. Metadata in separate schema (_dlt_metadata) - keeps staging schema clean
    2. CSV loader for fast PostgreSQL COPY operations
    3. Batch size: 10,000 records per write
    4. Detailed progress logging enabled
    5. Isolated working directory per run (prevents concurrent run conflicts)

    Args:
        pipeline_name: Base name of the DLT pipeline
        context: Dagster context (optional) - used to isolate concurrent runs

    Returns:
        Configured DLT pipeline instance with unique working directory
    """
    import logging
    logger = logging.getLogger(__name__)

    # CRITICAL: Make pipeline name unique per Dagster run to avoid file conflicts
    # When multiple runs of the same asset execute concurrently, they would otherwise
    # try to write to the same DLT working directory (/var/dlt/pipelines/<name>/)
    # causing FileNotFoundError and data corruption
    if context and hasattr(context, 'run_id'):
        # Include first 8 chars of run_id to create isolated DLT working directory
        unique_pipeline_name = f"{pipeline_name}_{context.run_id[:8]}"
        logger.info(f"Creating ISOLATED DLT pipeline: {unique_pipeline_name} (run: {context.run_id[:8]})")
    else:
        unique_pipeline_name = pipeline_name
        logger.info(f"Creating DLT pipeline: {unique_pipeline_name} (WARNING: not isolated)")

    # Create postgres destination with credentials
    from dlt.destinations import postgres

    logger.info(f"Creating postgres destination...")
    destination = postgres(
        credentials={
            "database": os.getenv("PG_DB", "postgres"),
            "username": os.getenv("PG_USER", "postgres"),  # DLT uses "username" not "user"
            "password": os.getenv("PG_PASSWORD"),
            "host": os.getenv("PG_HOST", "localhost"),
            "port": int(os.getenv("PG_PORT", "5432"))
        }
    )

    logger.info(f"Creating pipeline instance...")
    # Configure pipeline with unique name for isolation
    # DLT automatically handles:
    # - Schema detection and evolution
    # - Type inference
    # - Column normalization
    # - Error handling
    # - Batch optimization
    # All performance settings are in .dlt/config.toml:
    # - Batch writes every 10,000 records (buffer_max_items)
    # - Direct INSERT for fast loading (loader_file_format)
    # - Detailed JSON logging (log_format, log_level)
    # - Workers (extract, normalize, load)
    pipeline = dlt.pipeline(
        pipeline_name=unique_pipeline_name,  # UNIQUE per run - prevents conflicts
        destination=destination,
        dataset_name="staging",  # Schema: staging (contains all *_raw tables + _dlt_* metadata)
        progress="log"  # Enable progress logging
    )

    logger.info(f"Pipeline created successfully: {unique_pipeline_name}")
    return pipeline


# ============================================================================
# PATTERN 1: STATIONS ASSETS (FULL MODE)
# ============================================================================

@asset(
    compute_kind="dlt",
    group_name="temperature",
    io_manager_key="noop_io_manager"
)
def temperature_stations_raw(context):
    """
    Temperature stations - FULL load (replace all)
    
    Uses DLT advanced features:
    - Automatic schema detection and evolution
    - Type inference and validation
    - Column normalization
    - Error handling and retries
    """
    config_path = "configs/hubeau/temperature_stations.yml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    pipeline = _create_dlt_pipeline("hubeau_temperature_stations", context)

    load_info = pipeline.run(
        hubeau_stations(config),
        table_name="temperature_stations_raw"
    )

    # Extract comprehensive metrics from DLT
    rows_loaded = 0
    try:
        for package in load_info.load_packages:
            if package.jobs:
                for job in package.jobs:
                    items = job.metrics.get("items", 0)
                    if isinstance(items, (int, float)):
                        rows_loaded += int(items)
                    context.log.info(f"DLT Job metrics: {job.metrics}")
    except Exception as e:
        context.log.warning(f"Could not extract detailed metrics: {e}")

    # Log schema information
    try:
        schema = pipeline.default_schema
        context.log.info(f"DLT Schema: {len(schema.tables)} tables detected")
    except Exception as e:
        context.log.debug(f"Could not extract schema info: {e}")

    context.log.info(f"✅ Loaded {rows_loaded:,} rows to temperature_stations_raw")
    
    # Return serializable dict instead of DLT object
    return extract_load_info_metrics(load_info, "temperature_stations_raw")


@asset(
    compute_kind="dlt",
    group_name="piezometry",
    io_manager_key="noop_io_manager"
)
def piezometry_stations_raw(context):
    """
    Piezometry stations - FULL load (replace all)
    """
    config_path = "configs/hubeau/piezometry_stations.yml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    pipeline = _create_dlt_pipeline("hubeau_piezometry_stations", context)

    load_info = pipeline.run(
        hubeau_stations(config),
        table_name="piezometry_stations_raw"
    )

    # Extract metrics and return serializable dict
    metrics = extract_load_info_metrics(load_info, "piezometry_stations_raw")
    context.log.info(f"✅ Loaded {metrics['rows_loaded']:,} rows to piezometry_stations_raw")
    return metrics


@asset(
    compute_kind="dlt",
    group_name="hydrometry",
    io_manager_key="noop_io_manager"
)
def hydrometry_sites_raw(context):
    """
    Hydrometry sites - FULL load (replace all)
    """
    config_path = "configs/hubeau/hydrometry_sites.yml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    pipeline = _create_dlt_pipeline("hubeau_hydrometry_sites", context)

    load_info = pipeline.run(
        hubeau_stations(config),
        table_name="hydrometry_sites_raw"
    )

    # Extract metrics and return serializable dict
    table_name = context.asset_key.path[-1].replace("_raw", "")
    metrics = extract_load_info_metrics(load_info, f"{table_name}_raw")
    context.log.info(f"✅ Loaded {metrics['rows_loaded']:,} rows to {metrics['table_name']}")
    return metrics


@asset(
    compute_kind="dlt",
    group_name="hydrometry",
    io_manager_key="noop_io_manager"
)
def hydrometry_stations_raw(context):
    """
    Hydrometry stations - FULL load (replace all)
    """
    config_path = "configs/hubeau/hydrometry_stations.yml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    pipeline = _create_dlt_pipeline("hubeau_hydrometry_stations", context)

    load_info = pipeline.run(
        hubeau_stations(config),
        table_name="hydrometry_stations_raw"
    )

    # Extract metrics and return serializable dict
    table_name = context.asset_key.path[-1].replace("_raw", "")
    metrics = extract_load_info_metrics(load_info, f"{table_name}_raw")
    context.log.info(f"✅ Loaded {metrics['rows_loaded']:,} rows to {metrics['table_name']}")
    return metrics


@asset(
    compute_kind="dlt",
    group_name="hydrobio",
    io_manager_key="noop_io_manager"
)
def hydrobio_stations_raw(context):
    """
    Hydrobiology stations - FULL load (replace all)
    """
    config_path = "configs/hubeau/hydrobio_stations.yml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    pipeline = _create_dlt_pipeline("hubeau_hydrobio_stations", context)

    load_info = pipeline.run(
        hubeau_stations(config),
        table_name="hydrobio_stations_raw"
    )

    # Extract metrics and return serializable dict
    table_name = context.asset_key.path[-1].replace("_raw", "")
    metrics = extract_load_info_metrics(load_info, f"{table_name}_raw")
    context.log.info(f"✅ Loaded {metrics['rows_loaded']:,} rows to {metrics['table_name']}")
    return metrics


@asset(
    compute_kind="dlt",
    group_name="quality_rivers",
    io_manager_key="noop_io_manager"
)
def quality_rivers_stations_raw(context):
    """
    River quality stations - FULL load (replace all)
    """
    config_path = "configs/hubeau/quality_rivers_stations.yml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    pipeline = _create_dlt_pipeline("hubeau_quality_rivers_stations", context)

    load_info = pipeline.run(
        hubeau_stations(config),
        table_name="quality_rivers_stations_raw"
    )

    # Extract metrics and return serializable dict
    table_name = context.asset_key.path[-1].replace("_raw", "")
    metrics = extract_load_info_metrics(load_info, f"{table_name}_raw")
    context.log.info(f"✅ Loaded {metrics['rows_loaded']:,} rows to {metrics['table_name']}")
    return metrics


@asset(
    compute_kind="dlt",
    group_name="quality_groundwater",
    io_manager_key="noop_io_manager"
)
def quality_groundwater_stations_raw(context):
    """
    Groundwater quality stations - FULL load (replace all)
    """
    config_path = "configs/hubeau/quality_groundwater_stations.yml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    pipeline = _create_dlt_pipeline("hubeau_quality_groundwater_stations", context)

    load_info = pipeline.run(
        hubeau_stations(config),
        table_name="quality_groundwater_stations_raw"
    )

    # Extract metrics and return serializable dict
    table_name = context.asset_key.path[-1].replace("_raw", "")
    metrics = extract_load_info_metrics(load_info, f"{table_name}_raw")
    context.log.info(f"✅ Loaded {metrics['rows_loaded']:,} rows to {metrics['table_name']}")
    return metrics


@asset(
    compute_kind="dlt",
    group_name="ecoulement",
    io_manager_key="noop_io_manager"
)
def ecoulement_stations_raw(context):
    """
    Flow (ecoulement) stations - FULL load (replace all)
    """
    config_path = "configs/hubeau/ecoulement_stations.yml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    pipeline = _create_dlt_pipeline("hubeau_ecoulement_stations", context)

    load_info = pipeline.run(
        hubeau_stations(config),
        table_name="ecoulement_stations_raw"
    )

    # Extract metrics and return serializable dict
    table_name = context.asset_key.path[-1].replace("_raw", "")
    metrics = extract_load_info_metrics(load_info, f"{table_name}_raw")
    context.log.info(f"✅ Loaded {metrics['rows_loaded']:,} rows to {metrics['table_name']}")
    return metrics


# ============================================================================
# PATTERN 2: CHRONIQUES ASSETS (MODE PARTITION + INCREMENTAL)
# ============================================================================

@asset(
    compute_kind="dlt",
    group_name="temperature",
    partitions_def=MODE_PARTITIONS,
    io_manager_key="noop_io_manager"
)
def temperature_chroniques_raw(context):
    """
    Temperature chroniques - MODE partition + INCREMENTAL
    - With partition "full": Load ALL historical data
    - With partition "2020"-"2025": Load specific year (backfill/testing)
    - Without partition: Incremental from last date
    """
    config_path = "configs/hubeau/temperature_chroniques.yml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    pipeline = _create_dlt_pipeline("hubeau_temperature_chroniques", context)

    if context.has_partition_key:
        # ===== YEAR PARTITION MODE =====
        year = context.partition_key
        context.log.info(f"YEAR PARTITION: {year}")

        # Delete existing data for this year (idempotence)
        deleted = delete_year_data(
            "temperature_chroniques_raw",
            year,
            "date_mesure_temp"
        )
        context.log.info(f"Deleted {deleted} records for year {year}")

        # Load year data
        load_info = pipeline.run(
            hubeau_chroniques_year(config, year=year),
            table_name="temperature_chroniques_raw"
        )
    else:
        # ===== INCREMENTAL MODE =====
        context.log.info("INCREMENTAL mode")

        # Use DLT incremental (tracks last date automatically)
        load_info = pipeline.run(
            hubeau_chroniques_incremental(
                config,
                last_date=dlt.sources.incremental("date_mesure_temp")
            ),
            table_name="temperature_chroniques_raw"
        )

    # Extract metrics and return serializable dict
    metrics = extract_load_info_metrics(load_info, "temperature_chroniques_raw")
    context.log.info(f"✅ Loaded {metrics['rows_loaded']:,} rows to temperature_chroniques_raw")
    return metrics


@asset(
    compute_kind="dlt",
    group_name="piezometry",
    partitions_def=MODE_PARTITIONS,
    io_manager_key="noop_io_manager"
)
def piezometry_chroniques_raw(context):
    """
    Piezometry chroniques - MODE partition + INCREMENTAL
    - With partition "full": Load ALL historical data
    - With partition "2020"-"2025": Load specific year (backfill/testing)
    - Without partition: Incremental from last date
    """
    config_path = "configs/hubeau/piezometry_chroniques.yml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    pipeline = _create_dlt_pipeline("hubeau_piezometry_chroniques", context)

    if context.has_partition_key:
        # ===== YEAR PARTITION MODE =====
        year = context.partition_key
        context.log.info(f"YEAR PARTITION: {year}")

        # Delete existing data for this year (idempotence)
        deleted = delete_year_data(
            "piezometry_chroniques_raw",
            year,
            "date_mesure"
        )
        context.log.info(f"Deleted {deleted} records for year {year}")

        # Load year data
        load_info = pipeline.run(
            hubeau_chroniques_year(config, year=year),
            table_name="piezometry_chroniques_raw"
        )
    else:
        # ===== INCREMENTAL MODE =====
        context.log.info("INCREMENTAL mode")

        # Use DLT incremental (tracks last date automatically)
        load_info = pipeline.run(
            hubeau_chroniques_incremental(
                config,
                last_date=dlt.sources.incremental("date_mesure")
            ),
            table_name="piezometry_chroniques_raw"
        )

    # Extract metrics and return serializable dict
    metrics = extract_load_info_metrics(load_info, "piezometry_chroniques_raw")
    context.log.info(f"✅ Loaded {metrics['rows_loaded']:,} rows to piezometry_chroniques_raw")
    return metrics


@asset(
    compute_kind="dlt",
    group_name="hydrometry",
    partitions_def=MODE_PARTITIONS,
    io_manager_key="noop_io_manager"
)
def hydrometry_obs_elab_raw(context):
    """
    Hydrometry observations elaborated - MODE partition + INCREMENTAL
    - With partition "full": Load ALL historical data
    - With partition "2020"-"2025": Load specific year (backfill/testing)
    - Without partition: Incremental from last date
    """
    config_path = "configs/hubeau/hydrometry_obs_elab.yml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    pipeline = _create_dlt_pipeline("hubeau_hydrometry_obs_elab", context)

    if context.has_partition_key:
        # ===== YEAR PARTITION MODE =====
        year = context.partition_key
        context.log.info(f"YEAR PARTITION: {year}")

        # Delete existing data for this year (idempotence)
        deleted = delete_year_data(
            "hydrometry_obs_elab_raw",
            year,
            "date_obs_elab"
        )
        context.log.info(f"Deleted {deleted} records for year {year}")

        # Load year data
        load_info = pipeline.run(
            hubeau_chroniques_year(config, year=year),
            table_name="hydrometry_obs_elab_raw"
        )
    else:
        # ===== INCREMENTAL MODE =====
        context.log.info("INCREMENTAL mode")

        # Use DLT incremental (tracks last date automatically)
        load_info = pipeline.run(
            hubeau_chroniques_incremental(
                config,
                last_date=dlt.sources.incremental("date_obs_elab")
            ),
            table_name="hydrometry_obs_elab_raw"
        )

    # Extract metrics and return serializable dict
    metrics = extract_load_info_metrics(load_info, "hydrometry_obs_elab_raw")
    context.log.info(f"✅ Loaded {metrics['rows_loaded']:,} rows to hydrometry_obs_elab_raw")
    return metrics


@asset(
    compute_kind="dlt",
    group_name="hydrobio",
    partitions_def=MODE_PARTITIONS,
    io_manager_key="noop_io_manager"
)
def hydrobio_indices_raw(context):
    """
    Hydrobiology indices - MODE partition + INCREMENTAL
    - With partition "full": Load ALL historical data
    - With partition "2020"-"2025": Load specific year (backfill/testing)
    - Without partition: Incremental from last date
    """
    config_path = "configs/hubeau/hydrobio_indices.yml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    pipeline = _create_dlt_pipeline("hubeau_hydrobio_indices", context)

    if context.has_partition_key:
        # ===== YEAR PARTITION MODE =====
        year = context.partition_key
        context.log.info(f"YEAR PARTITION: {year}")

        # Delete existing data for this year (idempotence)
        deleted = delete_year_data(
            "hydrobio_indices_raw",
            year,
            "date_prelevement"
        )
        context.log.info(f"Deleted {deleted} records for year {year}")

        # Load year data
        load_info = pipeline.run(
            hubeau_chroniques_year(config, year=year),
            table_name="hydrobio_indices_raw"
        )
    else:
        # ===== INCREMENTAL MODE =====
        context.log.info("INCREMENTAL mode")

        # Use DLT incremental (tracks last date automatically)
        load_info = pipeline.run(
            hubeau_chroniques_incremental(
                config,
                last_date=dlt.sources.incremental("date_prelevement")
            ),
            table_name="hydrobio_indices_raw"
        )

    # Extract metrics and return serializable dict
    metrics = extract_load_info_metrics(load_info, "hydrobio_indices_raw")
    context.log.info(f"✅ Loaded {metrics['rows_loaded']:,} rows to hydrobio_indices_raw")
    return metrics


@asset(
    compute_kind="dlt",
    group_name="hydrobio",
    partitions_def=MODE_PARTITIONS,
    io_manager_key="noop_io_manager"
)
def hydrobio_taxons_raw(context):
    """
    Hydrobiology taxons - MODE partition + INCREMENTAL
    - With partition "full": Load ALL historical data
    - With partition "2020"-"2025": Load specific year (backfill/testing)
    - Without partition: Incremental from last date
    """
    config_path = "configs/hubeau/hydrobio_taxons.yml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    pipeline = _create_dlt_pipeline("hubeau_hydrobio_taxons", context)

    if context.has_partition_key:
        # ===== YEAR PARTITION MODE =====
        year = context.partition_key
        context.log.info(f"YEAR PARTITION: {year}")

        # Delete existing data for this year (idempotence)
        deleted = delete_year_data(
            "hydrobio_taxons_raw",
            year,
            "date_prelevement"
        )
        context.log.info(f"Deleted {deleted} records for year {year}")

        # Load year data
        load_info = pipeline.run(
            hubeau_chroniques_year(config, year=year),
            table_name="hydrobio_taxons_raw"
        )
    else:
        # ===== INCREMENTAL MODE =====
        context.log.info("INCREMENTAL mode")

        # Use DLT incremental (tracks last date automatically)
        load_info = pipeline.run(
            hubeau_chroniques_incremental(
                config,
                last_date=dlt.sources.incremental("date_prelevement")
            ),
            table_name="hydrobio_taxons_raw"
        )

    # Extract metrics and return serializable dict
    metrics = extract_load_info_metrics(load_info, "hydrobio_taxons_raw")
    context.log.info(f"✅ Loaded {metrics['rows_loaded']:,} rows to hydrobio_taxons_raw")
    return metrics


@asset(
    compute_kind="dlt",
    group_name="quality_rivers",
    partitions_def=MODE_PARTITIONS,
    io_manager_key="noop_io_manager"
)
def quality_rivers_analyses_raw(context):
    """
    River quality analyses - MODE partition + INCREMENTAL
    - With partition "full": Load ALL historical data
    - With partition "2020"-"2025": Load specific year (backfill/testing)
    - Without partition: Incremental from last date
    """
    config_path = "configs/hubeau/quality_rivers_analyses.yml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    pipeline = _create_dlt_pipeline("hubeau_quality_rivers_analyses", context)

    if context.has_partition_key:
        # ===== YEAR PARTITION MODE =====
        year = context.partition_key
        context.log.info(f"YEAR PARTITION: {year}")

        # Delete existing data for this year (idempotence)
        deleted = delete_year_data(
            "quality_rivers_analyses_raw",
            year,
            "date_prelevement"
        )
        context.log.info(f"Deleted {deleted} records for year {year}")

        # Load year data
        load_info = pipeline.run(
            hubeau_chroniques_year(config, year=year),
            table_name="quality_rivers_analyses_raw"
        )
    else:
        # ===== INCREMENTAL MODE =====
        context.log.info("INCREMENTAL mode")

        # Use DLT incremental (tracks last date automatically)
        load_info = pipeline.run(
            hubeau_chroniques_incremental(
                config,
                last_date=dlt.sources.incremental("date_prelevement")
            ),
            table_name="quality_rivers_analyses_raw"
        )

    # Extract metrics and return serializable dict
    metrics = extract_load_info_metrics(load_info, "quality_rivers_analyses_raw")
    context.log.info(f"✅ Loaded {metrics['rows_loaded']:,} rows to quality_rivers_analyses_raw")
    return metrics


@asset(
    compute_kind="dlt",
    group_name="quality_rivers",
    partitions_def=MODE_PARTITIONS,
    io_manager_key="noop_io_manager"
)
def quality_rivers_conditions_raw(context):
    """
    River quality conditions - MODE partition + INCREMENTAL
    - With partition "full": Load ALL historical data
    - With partition "2020"-"2025": Load specific year (backfill/testing)
    - Without partition: Incremental from last date
    """
    config_path = "configs/hubeau/quality_rivers_conditions.yml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    pipeline = _create_dlt_pipeline("hubeau_quality_rivers_conditions", context)

    if context.has_partition_key:
        # ===== YEAR PARTITION MODE =====
        year = context.partition_key
        context.log.info(f"YEAR PARTITION: {year}")

        # Delete existing data for this year (idempotence)
        deleted = delete_year_data(
            "quality_rivers_conditions_raw",
            year,
            "date_prelevement"
        )
        context.log.info(f"Deleted {deleted} records for year {year}")

        # Load year data
        load_info = pipeline.run(
            hubeau_chroniques_year(config, year=year),
            table_name="quality_rivers_conditions_raw"
        )
    else:
        # ===== INCREMENTAL MODE =====
        context.log.info("INCREMENTAL mode")

        # Use DLT incremental (tracks last date automatically)
        load_info = pipeline.run(
            hubeau_chroniques_incremental(
                config,
                last_date=dlt.sources.incremental("date_prelevement")
            ),
            table_name="quality_rivers_conditions_raw"
        )

    # Extract metrics and return serializable dict
    metrics = extract_load_info_metrics(load_info, "quality_rivers_conditions_raw")
    context.log.info(f"✅ Loaded {metrics['rows_loaded']:,} rows to quality_rivers_conditions_raw")
    return metrics


@asset(
    compute_kind="dlt",
    group_name="quality_rivers",
    partitions_def=MODE_PARTITIONS,
    io_manager_key="noop_io_manager"
)
def quality_rivers_operations_raw(context):
    """
    River quality operations - MODE partition + INCREMENTAL
    - With partition "full": Load ALL historical data
    - With partition "2020"-"2025": Load specific year (backfill/testing)
    - Without partition: Incremental from last date
    """
    config_path = "configs/hubeau/quality_rivers_operations.yml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    pipeline = _create_dlt_pipeline("hubeau_quality_rivers_operations", context)

    if context.has_partition_key:
        # ===== YEAR PARTITION MODE =====
        year = context.partition_key
        context.log.info(f"YEAR PARTITION: {year}")

        # Delete existing data for this year (idempotence)
        deleted = delete_year_data(
            "quality_rivers_operations_raw",
            year,
            "date_prelevement"
        )
        context.log.info(f"Deleted {deleted} records for year {year}")

        # Load year data
        load_info = pipeline.run(
            hubeau_chroniques_year(config, year=year),
            table_name="quality_rivers_operations_raw"
        )
    else:
        # ===== INCREMENTAL MODE =====
        context.log.info("INCREMENTAL mode")

        # Use DLT incremental (tracks last date automatically)
        load_info = pipeline.run(
            hubeau_chroniques_incremental(
                config,
                last_date=dlt.sources.incremental("date_prelevement")
            ),
            table_name="quality_rivers_operations_raw"
        )

    # Extract metrics and return serializable dict
    metrics = extract_load_info_metrics(load_info, "quality_rivers_operations_raw")
    context.log.info(f"✅ Loaded {metrics['rows_loaded']:,} rows to quality_rivers_operations_raw")
    return metrics


@asset(
    compute_kind="dlt",
    group_name="quality_groundwater",
    partitions_def=MODE_PARTITIONS,
    io_manager_key="noop_io_manager"
)
def quality_groundwater_analyses_raw(context):
    """
    Groundwater quality analyses - MODE partition + INCREMENTAL
    - With partition "full": Load ALL historical data
    - With partition "2020"-"2025": Load specific year (backfill/testing)
    - Without partition: Incremental from last date
    """
    config_path = "configs/hubeau/quality_groundwater_analyses.yml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    pipeline = _create_dlt_pipeline("hubeau_quality_groundwater_analyses", context)

    if context.has_partition_key:
        # ===== YEAR PARTITION MODE =====
        year = context.partition_key
        context.log.info(f"YEAR PARTITION: {year}")

        # Delete existing data for this year (idempotence)
        deleted = delete_year_data(
            "quality_groundwater_analyses_raw",
            year,
            "date_prelevement"
        )
        context.log.info(f"Deleted {deleted} records for year {year}")

        # Load year data
        load_info = pipeline.run(
            hubeau_chroniques_year(config, year=year),
            table_name="quality_groundwater_analyses_raw"
        )
    else:
        # ===== INCREMENTAL MODE =====
        context.log.info("INCREMENTAL mode")

        # Use DLT incremental (tracks last date automatically)
        load_info = pipeline.run(
            hubeau_chroniques_incremental(
                config,
                last_date=dlt.sources.incremental("date_prelevement")
            ),
            table_name="quality_groundwater_analyses_raw"
        )

    # Extract metrics and return serializable dict
    metrics = extract_load_info_metrics(load_info, "quality_groundwater_analyses_raw")
    context.log.info(f"✅ Loaded {metrics['rows_loaded']:,} rows to quality_groundwater_analyses_raw")
    return metrics


@asset(
    compute_kind="dlt",
    group_name="ecoulement",
    io_manager_key="noop_io_manager"
)
def ecoulement_campagnes_raw(context):
    """
    Flow (ecoulement) campaigns - FULL load (replace all)
    """
    config_path = "configs/hubeau/ecoulement_campagnes.yml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    pipeline = _create_dlt_pipeline("hubeau_ecoulement_campagnes", context)

    load_info = pipeline.run(
        hubeau_stations(config),
        table_name="ecoulement_campagnes_raw"
    )

    # Extract metrics and return serializable dict
    table_name = context.asset_key.path[-1].replace("_raw", "")
    metrics = extract_load_info_metrics(load_info, f"{table_name}_raw")
    context.log.info(f"✅ Loaded {metrics['rows_loaded']:,} rows to {metrics['table_name']}")
    return metrics


@asset(
    compute_kind="dlt",
    group_name="ecoulement",
    partitions_def=MODE_PARTITIONS,
    io_manager_key="noop_io_manager"
)
def ecoulement_observations_raw(context):
    """
    Flow observations - MODE partition + INCREMENTAL
    - With partition "full": Load ALL historical data
    - With partition "2020"-"2025": Load specific year (backfill/testing)
    - Without partition: Incremental from last date
    """
    config_path = "configs/hubeau/ecoulement_observations.yml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    pipeline = _create_dlt_pipeline("hubeau_ecoulement_observations", context)

    if context.has_partition_key:
        # ===== YEAR PARTITION MODE =====
        year = context.partition_key
        context.log.info(f"YEAR PARTITION: {year}")

        # Delete existing data for this year (idempotence)
        deleted = delete_year_data(
            "ecoulement_observations_raw",
            year,
            "date_observation"
        )
        context.log.info(f"Deleted {deleted} records for year {year}")

        # Load year data
        load_info = pipeline.run(
            hubeau_chroniques_year(config, year=year),
            table_name="ecoulement_observations_raw"
        )
    else:
        # ===== INCREMENTAL MODE =====
        context.log.info("INCREMENTAL mode")

        # Use DLT incremental (tracks last date automatically)
        load_info = pipeline.run(
            hubeau_chroniques_incremental(
                config,
                last_date=dlt.sources.incremental("date_observation")
            ),
            table_name="ecoulement_observations_raw"
        )

    # Extract metrics and return serializable dict
    metrics = extract_load_info_metrics(load_info, "ecoulement_observations_raw")
    context.log.info(f"✅ Loaded {metrics['rows_loaded']:,} rows to ecoulement_observations_raw")
    return metrics


@asset(
    compute_kind="dlt",
    group_name="prelevements",
    partitions_def=MODE_PARTITIONS,
    io_manager_key="noop_io_manager"
)
def prelevements_chroniques_raw(context):
    """
    Water withdrawals chroniques - MODE partition + INCREMENTAL
    - With partition "full": Load ALL historical data
    - With partition "2020"-"2025": Load specific year (backfill/testing)
    - Without partition: Incremental from last date
    """
    config_path = "configs/hubeau/prelevements_chroniques.yml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    pipeline = _create_dlt_pipeline("hubeau_prelevements_chroniques", context)

    if context.has_partition_key:
        # ===== YEAR PARTITION MODE =====
        year = context.partition_key
        context.log.info(f"YEAR PARTITION: {year}")

        # Delete existing data for this year (idempotence)
        deleted = delete_year_data(
            "prelevements_chroniques_raw",
            year,
            "annee"
        )
        context.log.info(f"Deleted {deleted} records for year {year}")

        # Load year data
        load_info = pipeline.run(
            hubeau_chroniques_year(config, year=year),
            table_name="prelevements_chroniques_raw"
        )
    else:
        # ===== INCREMENTAL MODE =====
        context.log.info("INCREMENTAL mode")

        # Use DLT incremental (tracks last date automatically)
        load_info = pipeline.run(
            hubeau_chroniques_incremental(
                config,
                last_date=dlt.sources.incremental("annee")
            ),
            table_name="prelevements_chroniques_raw"
        )

    # Extract metrics and return serializable dict
    metrics = extract_load_info_metrics(load_info, "prelevements_chroniques_raw")
    context.log.info(f"✅ Loaded {metrics['rows_loaded']:,} rows to prelevements_chroniques_raw")
    return metrics


@asset(
    compute_kind="dlt",
    group_name="prelevements",
    io_manager_key="noop_io_manager"
)
def prelevements_ouvrages_raw(context):
    """
    Water withdrawal structures (ouvrages) - FULL load (replace all)
    """
    config_path = "configs/hubeau/prelevements_ouvrages.yml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    pipeline = _create_dlt_pipeline("hubeau_prelevements_ouvrages", context)

    load_info = pipeline.run(
        hubeau_stations(config),
        table_name="prelevements_ouvrages_raw"
    )

    # Extract metrics and return serializable dict
    table_name = context.asset_key.path[-1].replace("_raw", "")
    metrics = extract_load_info_metrics(load_info, f"{table_name}_raw")
    context.log.info(f"✅ Loaded {metrics['rows_loaded']:,} rows to {metrics['table_name']}")
    return metrics


@asset(
    compute_kind="dlt",
    group_name="prelevements",
    io_manager_key="noop_io_manager"
)
def prelevements_points_raw(context):
    """
    Water withdrawal points - FULL load (replace all)
    """
    config_path = "configs/hubeau/prelevements_points.yml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    pipeline = _create_dlt_pipeline("hubeau_prelevements_points", context)

    load_info = pipeline.run(
        hubeau_stations(config),
        table_name="prelevements_points_raw"
    )

    # Extract metrics and return serializable dict
    table_name = context.asset_key.path[-1].replace("_raw", "")
    metrics = extract_load_info_metrics(load_info, f"{table_name}_raw")
    context.log.info(f"✅ Loaded {metrics['rows_loaded']:,} rows to {metrics['table_name']}")
    return metrics
