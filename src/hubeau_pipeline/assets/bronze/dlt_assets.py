from typing import Any, Dict

import dlt
from dagster import AssetExecutionContext, asset, DailyPartitionsDefinition, StaticPartitionsDefinition
from dlt.common.typing import TSecretValue

from pipelines.dlt.hubeau_generic import run_pipeline

# Partitions pour les données historiques (annuelles depuis 2020)
YEARLY_PARTITIONS = StaticPartitionsDefinition(
    [str(year) for year in range(2020, 2026)]  # 2020-2025
)

# Partitions pour les données temps réel (30 derniers jours)
DAILY_PARTITIONS = DailyPartitionsDefinition(start_date="2022-01-01")

# ====================================
# Generic dlt Ingestion Asset
# ====================================

def ingest_dlt(context: AssetExecutionContext, config_path: str) -> Dict[str, Any]:
    """
    Generic function to run a dlt pipeline based on a YAML configuration file.
    This is used internally by the dlt assets.
    """
    import os
    import yaml
    from datetime import datetime
    
    context.log.info(f"🚀 Starting dlt ingestion for config: {config_path}")

    # Load configuration from YAML
    full_path = os.path.join("/app", config_path)
    with open(full_path, 'r') as f:
        cfg = yaml.safe_load(f)
    
    # Get partition key if available
    partition_key = context.partition_key if context.has_partition_key else None
    if partition_key:
        context.log.info(f"📅 Partition: {partition_key}")
        
        # Update slicer dates based on partition
        if cfg.get("slicer", {}).get("mode") == "datetime":
            # Determine if it's a year or a date
            try:
                # Try to parse as year (YYYY format)
                if len(partition_key) == 4 and partition_key.isdigit():
                    year = int(partition_key)
                    cfg["slicer"]["start_date"] = f"{year}-01-01"
                    cfg["slicer"]["end_date"] = f"{year}-12-31"
                    context.log.info(f"🗓️ Ingestion pour l'année {year}")
                else:
                    # Parse as date (YYYY-MM-DD format)
                    date_obj = datetime.strptime(partition_key, "%Y-%m-%d")
                    cfg["slicer"]["start_date"] = partition_key
                    cfg["slicer"]["end_date"] = partition_key
                    context.log.info(f"🗓️ Ingestion pour le jour {partition_key}")
            except ValueError:
                context.log.warning(f"⚠️ Could not parse partition key: {partition_key}")
    
    context.log.info(f"Loaded dlt config: {cfg['name']}")

    # Build MinIO credentials for dlt
    import os
    minio_user = os.getenv("MINIO_USER", "admin")
    minio_pass = os.getenv("MINIO_PASS", "BrgmMinio2024!")
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    minio_region = os.getenv("MINIO_REGION", "us-east-1")
    
    credentials = {
        "aws_access_key_id": TSecretValue(minio_user),
        "aws_secret_access_key": TSecretValue(minio_pass),
        "endpoint_url": minio_endpoint,
        "region_name": minio_region,
    }

    # Run the dlt pipeline
    load_info = run_pipeline(
        cfg,
        bucket_url=f"s3://bronze",
        credentials=credentials,
        dataset_name=cfg.get("dataset_name", "bronze"),
        file_format=cfg.get("file_format", "json"),
        layout=cfg.get("layout", "{table_name}/{curr_date}/data.json"),
        state_fs_options={
            "aws_access_key_id": TSecretValue(minio_user),
            "aws_secret_access_key": TSecretValue(minio_pass),
            "endpoint_url": minio_endpoint,
            "region_name": minio_region,
            "bucket_name": "dlt-state",
        }
    )

    context.log.info(f"✅ dlt pipeline for {cfg['name']} finished.")

    # Extract relevant metrics
    row_count = 0
    if hasattr(load_info, 'load_packages') and load_info.load_packages:
        for package in load_info.load_packages:
            if hasattr(package, 'jobs'):
                for job in package.jobs:
                    if hasattr(job, 'job_file_type') and job.job_file_type == "data":
                        if hasattr(job, 'records_count'):
                            row_count += job.records_count

    return {"stream": cfg["name"], "rows": row_count}

# ====================================
# Hub'Eau Specific dlt Assets
# ====================================

@asset(group_name="hubeau_hydrobiology", partitions_def=YEARLY_PARTITIONS)
def hydrobio_taxons(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests hydrobiology taxons data using dlt."""
    return ingest_dlt(context, "configs/hubeau/hydrobio_taxons.yml")

@asset(group_name="hubeau_hydrobiology", partitions_def=YEARLY_PARTITIONS)
def hydrobio_indices(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests hydrobiology indices data using dlt."""
    return ingest_dlt(context, "configs/hubeau/hydrobio_indices.yml")

@asset(group_name="hubeau_hydrometry")
def hydrometry_observations(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests hydrometry observations data using dlt (30 derniers jours)."""
    return ingest_dlt(context, "configs/hubeau/hydrometry_observations.yml")

@asset(group_name="hubeau_piezometry", partitions_def=DAILY_PARTITIONS)
def piezometry_chroniques(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests piezometry chroniques data using dlt."""
    return ingest_dlt(context, "configs/hubeau/piezometry_chroniques.yml")

@asset(group_name="hubeau_quality_rivers", partitions_def=YEARLY_PARTITIONS)
def quality_rivers_analyses(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests superficial waterbodies quality analyses data using dlt."""
    return ingest_dlt(context, "configs/hubeau/quality_rivers_analyses.yml")

@asset(group_name="hubeau_quality_groundwater", partitions_def=YEARLY_PARTITIONS)
def quality_groundwater_analyses(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests groundwater quality analyses data using dlt."""
    return ingest_dlt(context, "configs/hubeau/quality_groundwater_analyses.yml")

@asset(group_name="hubeau_ecoulement", partitions_def=YEARLY_PARTITIONS)
def ecoulement_observations(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests ecoulement observations data using dlt."""
    return ingest_dlt(context, "configs/hubeau/ecoulement_observations.yml")

@asset(group_name="hubeau_prelevements", partitions_def=YEARLY_PARTITIONS)
def prelevements_chroniques(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests prelevements chroniques data using dlt."""
    return ingest_dlt(context, "configs/hubeau/prelevements_chroniques.yml")

@asset(group_name="hubeau_temperature", partitions_def=YEARLY_PARTITIONS)
def temperature_chroniques(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests temperature chroniques data using dlt."""
    return ingest_dlt(context, "configs/hubeau/temperature_chroniques.yml")

@asset(group_name="hubeau_temperature")
def temperature_stations_reference(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests temperature stations reference data using dlt (pas de partition)."""
    return ingest_dlt(context, "configs/reference/temperature_stations.yml")
