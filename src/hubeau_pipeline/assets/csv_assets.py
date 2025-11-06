"""
Generic CSV Ingestion Assets - Config-Driven

ONE ASSET PER CONFIG FILE (not per CSV file!)

Add new CSV ingestion:
1. Drop CSV in /app/data/csv_inbox/
2. Create YAML config in /app/configs/csv_ingestion/
3. Asset is auto-generated (no code changes needed)

Example configs:
- piezometers.yml → creates staging_piezometers table
- meteo_data.yml → creates staging_meteo_data table
"""

import os
from pathlib import Path
from typing import Dict, Any
from dagster import asset, AssetExecutionContext, Output, MetadataValue
import dlt

from hubeau_pipeline.sources.csv_source import (
    csv_to_staging,
    get_csv_configs,
    CSVIngestionConfig
)


def _create_dlt_pipeline(pipeline_name: str, context=None) -> dlt.Pipeline:
    """
    Create DLT pipeline for CSV ingestion.
    Same pattern as other DLT assets.
    """
    import logging
    logger = logging.getLogger(__name__)

    # Unique pipeline name per run (avoid concurrent conflicts)
    if context and hasattr(context, 'run_id'):
        unique_pipeline_name = f"{pipeline_name}_{context.run_id[:8]}"
        logger.info(f"Creating DLT pipeline: {unique_pipeline_name}")
    else:
        unique_pipeline_name = pipeline_name

    from dlt.destinations import postgres

    destination = postgres(
        credentials={
            "database": os.getenv("PG_DB", "postgres"),
            "username": os.getenv("PG_USER", "postgres"),
            "password": os.getenv("PG_PASSWORD"),
            "host": os.getenv("PG_HOST", "localhost"),
            "port": int(os.getenv("PG_PORT", "5432"))
        }
    )

    # CSV ingestion goes to "staging" schema
    pipeline = dlt.pipeline(
        pipeline_name=unique_pipeline_name,
        destination=destination,
        dataset_name="staging",  # All CSVs land in staging schema
        progress="log"
    )

    return pipeline


def extract_load_info_metrics(load_info, table_name: str) -> Dict[str, Any]:
    """
    Extract serializable metrics from DLT load_info object.
    Handles cases where package.jobs might contain unexpected data types.

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
                    # Check if job has metrics attribute (some jobs might be strings or other types)
                    if hasattr(job, 'metrics') and callable(getattr(job, 'metrics', None)) or isinstance(getattr(job, 'metrics', None), dict):
                        if hasattr(job.metrics, 'get'):
                            items = job.metrics.get("items", 0)
                        elif isinstance(job.metrics, dict):
                            items = job.metrics.get("items", 0)
                        else:
                            items = 0

                        if isinstance(items, (int, float)):
                            rows_loaded += int(items)
    except Exception as e:
        # Silently continue if metrics extraction fails
        # The data is still loaded successfully, we just can't report the count
        pass

    return {
        "rows_loaded": rows_loaded,
        "table_name": table_name,
        "jobs_count": jobs_count,
        "status": "success"
    }


# ==============================================================================
# DYNAMIC ASSET GENERATION
# ==============================================================================

def create_csv_asset(config_name: str):
    """
    Factory function to create a Dagster asset for a CSV config.

    Args:
        config_name: Name of YAML config file (without .yml)

    Returns:
        Dagster asset function
    """

    @asset(
        name=f"csv_{config_name}",
        description=f"Ingest CSV data from config: {config_name}.yml",
        compute_kind="dlt",
        group_name="csv_ingestion",
        metadata={
            "config": config_name,
            "destination_schema": "staging"
        }
    )
    def csv_ingestion_asset(context: AssetExecutionContext) -> Output:
        """
        Generic CSV ingestion asset.
        Reads config, finds CSV, auto-detects schema, loads to PostgreSQL.
        """
        # Load config
        config = CSVIngestionConfig(config_name)

        context.log.info(f"=== CSV INGESTION: {config_name} ===")
        context.log.info(f"File pattern: {config.file_pattern}")
        context.log.info(f"Destination: staging.{config.table_name}")
        context.log.info(f"Write mode: {config.write_disposition}")

        # Create DLT pipeline
        pipeline = _create_dlt_pipeline(f"csv_{config_name}", context)

        # Run ingestion
        load_info = pipeline.run(
            csv_to_staging(config_name=config_name),
            table_name=config.table_name,
            write_disposition=config.write_disposition,
            primary_key=config.primary_key
        )

        # Extract metrics using robust helper function
        metrics = extract_load_info_metrics(load_info, config.table_name)
        rows_loaded = metrics["rows_loaded"]

        context.log.info(f"✅ Loaded {rows_loaded:,} rows to staging.{config.table_name}")

        return Output(
            value={
                "rows_loaded": rows_loaded,
                "table_name": config.table_name,
                "schema": "staging",
                "config": config_name,
                "status": metrics["status"]
            },
            metadata={
                "rows_loaded": MetadataValue.int(rows_loaded),
                "destination_table": MetadataValue.text(f"staging.{config.table_name}"),
                "write_mode": MetadataValue.text(config.write_disposition),
                "jobs_count": MetadataValue.int(metrics["jobs_count"]),
            }
        )

    return csv_ingestion_asset


# ==============================================================================
# AUTO-GENERATE ASSETS FROM CONFIGS
# ==============================================================================

# Scan /app/configs/csv_ingestion/ and create assets dynamically
_csv_configs = get_csv_configs()

# Create one asset per config file
csv_assets = [
    create_csv_asset(config_name)
    for config_name in _csv_configs
]

# Export all assets
__all__ = ["csv_assets"] + [f"csv_{c}" for c in _csv_configs]


# ==============================================================================
# MANUAL FALLBACK (if auto-discovery doesn't work)
# ==============================================================================

# If you prefer explicit asset definitions, uncomment this:

# @asset(
#     name="csv_piezometers",
#     description="Ingest piezometers CSV data",
#     compute_kind="dlt",
#     group_name="csv_ingestion"
# )
# def csv_piezometers(context: AssetExecutionContext) -> Output:
#     """Ingest piezometers CSV"""
#     config = CSVIngestionConfig("piezometers")
#     pipeline = _create_dlt_pipeline("csv_piezometers", context)
#     load_info = pipeline.run(
#         csv_to_staging(config_name="piezometers"),
#         table_name=config.table_name,
#         write_disposition=config.write_disposition
#     )
#     rows_loaded = sum(p.jobs[0].metrics.get("items", 0) for p in load_info.load_packages if p.jobs)
#     return Output(value={"rows_loaded": rows_loaded})
