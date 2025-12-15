"""
Bronze Layer DLT Assets - Hub'Eau Data Pipeline

Pattern: @asset + pipeline.run()

This pattern is used because @dlt_assets evaluates the pipeline at module 
load time, before environment variables are available in Dagster's execution
context. Using @asset gives us runtime control.

DOMAINS:
- Piezometry (stations + chroniques)
- Hydrometry (sites + stations + observations)
"""

import os
import yaml
import dlt
from dagster import asset, AssetExecutionContext, StaticPartitionsDefinition
from datetime import datetime

from hubeau_pipeline.sources.hubeau_csv_source import (
    hubeau_stations,
    hubeau_chroniques_year,
)

# ============================================================================
# PARTITIONS
# ============================================================================

CURRENT_YEAR = datetime.now().year
OLDEST_YEAR = 1967
YEAR_PARTITIONS = [str(year) for year in range(OLDEST_YEAR, CURRENT_YEAR + 1)]
MODE_PARTITIONS = StaticPartitionsDefinition(YEAR_PARTITIONS)


# ============================================================================
# HELPERS
# ============================================================================

def _load_config(name: str) -> dict:
    with open(f"configs/hubeau/{name}.yml") as f:
        return yaml.safe_load(f)


def _create_pipeline(name: str) -> dlt.Pipeline:
    """Create DLT pipeline with explicit credentials at RUNTIME."""
    credentials = (
        f"postgresql://"
        f"{os.environ.get('PG_USER', 'postgres')}:"
        f"{os.environ.get('PG_PASSWORD', 'postgres')}@"
        f"{os.environ.get('PG_HOST', 'postgres')}:"
        f"{os.environ.get('PG_PORT', '5432')}/"
        f"{os.environ.get('PG_DB', 'postgres')}"
    )
    return dlt.pipeline(
        pipeline_name=name,
        destination=dlt.destinations.postgres(credentials),
        dataset_name=os.environ.get("DLT_BRONZE_DATASET", "staging"),
        progress="log",
    )


def _extract_metrics(load_info, context) -> int:
    rows = 0
    try:
        for pkg in getattr(load_info, "load_packages", []) or []:
            for job in getattr(pkg, "jobs", []) or []:
                metrics = getattr(job, "metrics", None) or {}
                rows += metrics.get("items", 0)
    except Exception as e:
        context.log.warning(f"Metrics extraction failed: {e}")
    return rows


# ============================================================================
# PIEZOMETRY ASSETS
# ============================================================================

@asset(compute_kind="dlt", group_name="piezometry_stations")
def piezometry_stations_raw(context: AssetExecutionContext):
    """Piezometry stations - FULL load."""
    config = _load_config("piezometry_stations")
    pipeline = _create_pipeline("hubeau_piezometry_stations")
    
    load_info = pipeline.run(
        hubeau_stations(config, dagster_context=context),
        table_name="piezometry_stations_raw"
    )
    
    rows = _extract_metrics(load_info, context)
    context.log.info(f"✅ Loaded {rows:,} piezometry stations")
    return {"rows_loaded": rows}


@asset(
    compute_kind="dlt",
    group_name="piezometry_chroniques",
    partitions_def=MODE_PARTITIONS,
)
def piezometry_chroniques_raw(context: AssetExecutionContext):
    """Piezometry chroniques - Partitioned by year."""
    year = context.partition_key
    context.log.info(f"📅 Year: {year}")
    
    config = _load_config("piezometry_chroniques")
    # Unique pipeline name per partition to avoid DLT file conflicts
    pipeline = _create_pipeline(f"hubeau_piezometry_chroniques_{year}")
    
    load_info = pipeline.run(
        hubeau_chroniques_year(config, year=year, dagster_context=context),
        table_name="piezometry_chroniques_raw"
    )
    
    rows = _extract_metrics(load_info, context)
    context.log.info(f"✅ Loaded {rows:,} rows for {year}")
    return {"rows_loaded": rows, "year": year}


# ============================================================================
# HYDROMETRY ASSETS
# ============================================================================

@asset(compute_kind="dlt", group_name="hydrometry_sites")
def hydrometry_sites_raw(context: AssetExecutionContext):
    """Hydrometry sites - FULL load."""
    config = _load_config("hydrometry_sites")
    pipeline = _create_pipeline("hubeau_hydrometry_sites")
    
    load_info = pipeline.run(
        hubeau_stations(config, dagster_context=context),
        table_name="hydrometry_sites_raw"
    )
    
    rows = _extract_metrics(load_info, context)
    context.log.info(f"✅ Loaded {rows:,} hydrometry sites")
    return {"rows_loaded": rows}


@asset(compute_kind="dlt", group_name="hydrometry_stations")
def hydrometry_stations_raw(context: AssetExecutionContext):
    """Hydrometry stations - FULL load."""
    config = _load_config("hydrometry_stations")
    pipeline = _create_pipeline("hubeau_hydrometry_stations")
    
    load_info = pipeline.run(
        hubeau_stations(config, dagster_context=context),
        table_name="hydrometry_stations_raw"
    )
    
    rows = _extract_metrics(load_info, context)
    context.log.info(f"✅ Loaded {rows:,} hydrometry stations")
    return {"rows_loaded": rows}


@asset(
    compute_kind="dlt",
    group_name="hydrometry_chroniques",
    partitions_def=MODE_PARTITIONS,
)
def hydrometry_obs_elab_raw(context: AssetExecutionContext):
    """Hydrometry observations - Partitioned by year."""
    year = context.partition_key
    context.log.info(f"📅 Year: {year}")
    
    config = _load_config("hydrometry_obs_elab")
    # Unique pipeline name per partition to avoid DLT file conflicts
    pipeline = _create_pipeline(f"hubeau_hydrometry_obs_elab_{year}")
    
    load_info = pipeline.run(
        hubeau_chroniques_year(config, year=year, dagster_context=context),
        table_name="hydrometry_obs_elab_raw"
    )
    
    rows = _extract_metrics(load_info, context)
    context.log.info(f"✅ Loaded {rows:,} rows for {year}")
    return {"rows_loaded": rows, "year": year}
