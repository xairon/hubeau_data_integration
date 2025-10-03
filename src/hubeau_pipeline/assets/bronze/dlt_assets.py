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
        
        # Update temporal_filter if present (pour APIs avec slicer=dept + filtre temporel)
        if cfg.get("temporal_filter") and len(partition_key) == 4 and partition_key.isdigit():
            year = int(partition_key)
            # Pour les filtres temporels annuels (ex: prelevements, quality)
            if "annee" in cfg["temporal_filter"].get("start_param", ""):
                cfg["temporal_filter"]["start_date"] = str(year)
                if cfg["temporal_filter"].get("end_param"):
                    cfg["temporal_filter"]["end_date"] = str(year)
            else:
                # Pour les filtres temporels avec dates complètes
                cfg["temporal_filter"]["start_date"] = f"{year}-01-01"
                if cfg["temporal_filter"].get("end_param"):
                    cfg["temporal_filter"]["end_date"] = f"{year}-12-31"
            context.log.info(f"🗓️ Filtre temporel mis à jour pour l'année {year}")
    
    context.log.info(f"🚀 Starting DLT ingestion for: {cfg['name']}")
    context.log.info(f"📊 Configuration loaded: {cfg.get('base_url', '')}{cfg.get('path', '')}")
    context.log.info(f"🔑 Primary keys: {cfg.get('primary_keys', [])}")
    context.log.info(f"📅 Replication key: {cfg.get('replication_key', 'N/A')}")
    context.log.info(f"🗓️ Slicer mode: {cfg.get('slicer', {}).get('mode', 'N/A')}")
    context.log.info(f"📈 Date range: {cfg.get('slicer', {}).get('start_date', 'N/A')} to {cfg.get('slicer', {}).get('end_date', 'N/A')}")

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

    # Test API connectivity and get sample data first
    context.log.info(f"🔍 Testing API connectivity for {cfg['name']}...")
    try:
        import requests
        import time
        
        test_params = cfg.get("params_default", {}).copy()
        test_params.update({
            "size": 10,  # Small test batch
            "format": "json"
        })
        
        test_url = f"{cfg.get('base_url', '')}{cfg.get('path', '')}"
        test_start = time.time()
        
        response = requests.get(test_url, params=test_params, timeout=30)
        test_duration = time.time() - test_start
        
        context.log.info(f"🌐 API test response: {response.status_code} in {test_duration:.2f}s")
        
        if response.status_code in [200, 206]:  # 206 = Partial Content (normal pour pagination)
            data = response.json()
            if cfg.get("records_path"):
                import jsonpath_ng
                jsonpath_expr = jsonpath_ng.parse(cfg["records_path"])
                matches = [match.value for match in jsonpath_expr.find(data)]
                context.log.info(f"📊 Test data sample: {len(matches)} records found")
                if matches:
                    context.log.info(f"📋 Sample record fields: {list(matches[0].keys()) if isinstance(matches[0], dict) else 'N/A'}")
            else:
                context.log.info(f"📊 Test data: {len(data) if isinstance(data, list) else 'single record'}")
        else:
            context.log.error(f"❌ API test failed with status {response.status_code}: {response.text[:200]}")
            
    except Exception as e:
        context.log.warning(f"⚠️ API connectivity test failed: {str(e)}")

    # Run the dlt pipeline
    context.log.info(f"🏃 Starting DLT pipeline execution...")
    pipeline_start_time = time.time()
    
    # Capture all logs from DLT pipeline and display them in Dagster
    import io
    import sys
    from contextlib import redirect_stdout, redirect_stderr
    
    # Store reference to built-in print function
    import builtins
    original_print = builtins.print
    
    # Custom print function that sends to Dagster
    def dagster_print(*args, **kwargs):
        message = ' '.join(str(arg) for arg in args)
        context.log.info(f"DLT: {message}")
    
    # Monkey patch print to use Dagster logger
    builtins.print = dagster_print
    
    try:
        # Execute DLT pipeline with monkey-patched print
        # Get state store from config or use default
        state_store = cfg.get("state_store", "s3://bronze/_state")
        
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
            },
            dagster_log=None  # Use monkey-patched print instead
        )
    finally:
        # Restore original print function
        builtins.print = original_print

    pipeline_duration = time.time() - pipeline_start_time
    context.log.info(f"✅ DLT pipeline for {cfg['name']} finished in {pipeline_duration:.2f}s")

    # Extract detailed metrics and statistics
    # Note: DLT LoadInfo doesn't contain detailed metrics, so we rely on DLT's internal logs
    # which are displayed via our monkey-patched print function
    stats = {
        "stream": cfg["name"],
        "rows": 0,  # Will be updated from DLT logs if available
        "files": 0,
        "packages": 0,
        "duration_seconds": pipeline_duration,
        "load_packages": [],
        "errors": [],
        "warnings": [],
        "dlt_logs_available": True  # Flag to indicate we have DLT logs
    }
    
    if hasattr(load_info, 'load_packages') and load_info.load_packages:
        stats["packages"] = len(load_info.load_packages)
        context.log.info(f"📦 Processed {len(load_info.load_packages)} load packages")
        
        # DLT LoadInfo doesn't contain detailed job metrics, but we know data was written
        # based on the presence of load packages and the DLT logs showing successful processing
        stats["files"] = len(load_info.load_packages)  # Each package typically represents one file
        
        for package in load_info.load_packages:
            package_stats = {
                "load_id": package.load_id,
                "jobs": [],
                "total_records": "unknown",  # Not available in LoadInfo
                "total_files": 1
            }
            
            if hasattr(package, 'jobs'):
                context.log.info(f"📄 Package {package.load_id}: {len(package.jobs)} jobs")
                
                for job in package.jobs:
                    # Handle both string and object job types
                    if isinstance(job, str):
                        job_stats = {
                            "job_id": job,
                            "job_file_type": "unknown",
                            "records_count": "unknown",
                            "file_size": "unknown"
                        }
                        job_display_id = job
                    else:
                        job_stats = {
                            "job_id": getattr(job, 'job_id', str(job)),
                            "job_file_type": getattr(job, 'job_file_type', 'unknown'),
                            "records_count": getattr(job, 'records_count', 'unknown'),
                            "file_size": getattr(job, 'file_size', 'unknown')
                        }
                        job_display_id = job_stats["job_id"]
                    
                    package_stats["jobs"].append(job_stats)
                    
                    context.log.info(f"📊 Job {job_display_id}: type={job_stats['job_file_type']}")
            
            stats["load_packages"].append(package_stats)

    # Log final statistics
    context.log.info(f"🎉 Ingestion {cfg['name']} completed!")
    context.log.info(f"📊 Final statistics:")
    context.log.info(f"   • Load packages: {stats['packages']}")
    context.log.info(f"   • Files written: {stats['files']}")
    context.log.info(f"   • Duration: {pipeline_duration:.2f}s")
    context.log.info(f"   • Data written to MinIO: ✅ (see DLT logs above for detailed metrics)")
    
    # Check if we have load packages (indicates successful data ingestion)
    if stats['packages'] > 0:
        context.log.info(f"✅ Data successfully ingested for {cfg['name']}")
        context.log.info(f"   • Detailed metrics available in DLT logs above")
        context.log.info(f"   • Files stored in MinIO bucket: bronze/{cfg.get('dataset_name', 'bronze')}")
        stats["rows"] = "see_dlt_logs"  # Indicate that metrics are in DLT logs
    else:
        context.log.warning(f"⚠️ No data ingested for {cfg['name']}! This might indicate:")
        context.log.warning(f"   • API returned empty results")
        context.log.warning(f"   • Date range has no data")
        context.log.warning(f"   • API endpoint might be incorrect")
        context.log.warning(f"   • Authentication issues")
        stats["warnings"].append("No data ingested - check API endpoint and date range")

    return stats

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
    return ingest_dlt(context, "configs/hubeau/temperature_stations.yml")
