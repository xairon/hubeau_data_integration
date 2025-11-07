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

from hubeau_pipeline.sources.csv_source import (
    csv_to_staging,
    get_csv_configs,
    CSVIngestionConfig
)
from hubeau_pipeline.utils.dlt_batching import (
    create_dlt_pipeline,
    run_dlt_resource,
)
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
        pipeline = create_dlt_pipeline(
            f"csv_{config_name}",
            context=context,
            dataset_name="staging",
        )

        # Run ingestion via utilitaire commun
        metrics = run_dlt_resource(
            pipeline=pipeline,
            resource=csv_to_staging(config_name=config_name),
            context=context,
            table_name=config.table_name,
            write_disposition=config.write_disposition,
            extra_metadata={"schema": "staging"},
            primary_key=config.primary_key,
        )

        rows_loaded = metrics.get("rows_loaded", 0)

        context.log.info(
            f"✅ Loaded {rows_loaded:,} rows to staging.{config.table_name}"
        )

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
