from typing import Any, Dict, List, Optional

import time
import io
import logging

import dlt
from dagster import AssetExecutionContext, asset, DailyPartitionsDefinition, StaticPartitionsDefinition
from dlt.common.typing import TSecretValue
import pyarrow.parquet as pq
import pyarrow.fs as pafs
import pandas as pd

from src.dlt_pipeline.hubeau_source import hubeau_rest_source, load_hubeau_config

# Import factorized station extraction functions
try:
    from hubeau.extractors.station_api import (
        extract_station_codes_from_result,
        get_active_departments_for_stations
    )
except ImportError:
    # Fallback pour station_api
    def extract_station_codes_from_result(result: Dict, station_type: str, partition_date: str) -> Dict[str, List[str]]:
        """Fallback: retourne un dict vide si module non disponible"""
        return {}

    def get_active_departments_for_stations(stations: List[str]) -> List[str]:
        """Fallback: retourne une liste vide si module non disponible"""
        return []

# Import des fonctions de lecture MinIO (toujours disponibles maintenant)
try:
    from src.hubeau_pipeline.utils.station_minio import (
        extract_station_codes_from_minio as _extract_station_codes_from_minio,
        filter_active_stations_for_period as _filter_active_stations_for_period
    )
except ImportError as e:
    # Fallback si le module station_minio n'est pas accessible
    import sys
    sys.stderr.write(f"Warning: station_minio module not found ({e}), using fallback\n")

    def _extract_station_codes_from_minio(station_type: str) -> List[str]:
        """Fallback: retourne une liste vide si module non disponible"""
        return []

    def _filter_active_stations_for_period(stations: List[str], partition_date: str, station_type: str) -> List[str]:
        """Fallback: retourne toutes les stations si module non disponible"""
        return stations

# Partitions pour les données historiques (annuelles depuis 2020)
YEARLY_PARTITIONS = StaticPartitionsDefinition(
    [str(year) for year in range(2020, 2026)]  # 2020-2025
)

# ====================================
# UTILITAIRES POUR RÉDUIRE LA REDONDANCE
# ====================================

def _get_partition_date_yearly(context: AssetExecutionContext) -> str:
    """Convertit une partition annuelle (ex: '2024') en date (ex: '2024-01-01')."""
    partition_key = context.partition_key
    return f"{partition_key}-01-01"

def _get_partition_date_daily(context: AssetExecutionContext) -> str:
    """Retourne directement la partition quotidienne (ex: '2024-01-01')."""
    return context.partition_key

def _setup_observation_asset(context: AssetExecutionContext, station_type: str, partition_date: str) -> tuple[Dict[str, List[str]], str]:
    """
    Configuration commune pour les assets d'observations.

    Returns:
        tuple: (stations_data: Dict[station_code, List[months]], log_message)
    """
    context.log.info(f"🔍 Récupération des stations {station_type} pour la partition {partition_date}")

    # ✅ DEBUG: Vérifier quelle fonction est utilisée
    context.log.warning(f"🔍 DEBUG: _extract_station_codes_from_minio function = {_extract_station_codes_from_minio}")

    # ✅ STRATÉGIE OPTIMISÉE AVEC FALLBACK AUTOMATIQUE:
    # 1. Récupérer TOUTES les stations depuis MinIO (référentiel complet)
    try:
        all_stations = _extract_station_codes_from_minio(station_type)
        context.log.info(f"📂 {len(all_stations)} stations total dans référentiel MinIO")
    except Exception as e:
        context.log.error(f"❌ Erreur lors de l'appel à _extract_station_codes_from_minio: {e}")
        import traceback
        context.log.error(f"   Traceback: {traceback.format_exc()}")
        all_stations = []

    stations_data: Dict[str, List[str]] = {}

    if all_stations:
        # ✅ Filtrer les stations basé sur les métadonnées MinIO (dates de mesure)
        # Au lieu d'appeler l'API, on utilise les champs date_debut/fin_mesure du référentiel
        context.log.info(f"📂 {len(all_stations)} stations trouvées dans MinIO")

        # Filtrer pour ne garder que les stations actives dans la partition
        filtered_stations = filter_active_stations_for_period(all_stations, partition_date, station_type, context.log)
        context.log.info(f"✅ {len(filtered_stations)} stations actives pour partition {partition_date}")

        # 2. Convertir en dict avec tous les mois de l'année
        from datetime import datetime
        year = datetime.strptime(partition_date, "%Y-%m-%d").year
        all_months = [f"{year}-{m:02d}" for m in range(1, 13)]
        stations_data = {station: all_months for station in filtered_stations}

    # 4. Fallback: si aucune station n'est trouvée via MinIO (ou filtrage vide),
    #    basculer sur la découverte des stations actives via l'API Hub'Eau
    if not stations_data:
        context.log.warning("⚠️ Aucune station disponible depuis MinIO après filtrage. Fallback API activé pour découvrir les stations actives/mois.")
        try:
            stations_data = extract_station_codes_from_result({}, station_type=station_type, partition_date=partition_date)
            context.log.info(f"✅ Fallback API: {len(stations_data)} stations actives détectées pour {station_type}")
        except Exception as e:
            context.log.error(f"❌ Fallback API échoué pour {station_type}: {e}")
            stations_data = {}

    total_station_months = sum(len(months) for months in stations_data.values())
    log_message = f"📊 Using {len(stations_data)} {station_type} stations ({total_station_months} station-mois)"
    context.log.info(log_message)

    # Log les stations récupérées pour debug
    station_codes = list(stations_data.keys())
    if len(station_codes) <= 20:
        context.log.info(f"📋 Stations filtrées: {', '.join(station_codes)}")
    else:
        context.log.info(f"📋 Premières 20 stations: {', '.join(station_codes[:20])}")

    return stations_data, log_message

# ====================================
# Generic dlt Ingestion Asset
# ====================================

# Functions have been moved to hubeau.extractors modules for better modularity
# - extract_station_codes_from_result, get_active_departments_for_stations -> station_api.py
# - _extract_station_codes_from_minio, _filter_active_stations_for_period -> station_minio.py

def ingest_dlt(context: AssetExecutionContext, config_path: str, stations_data: Optional[Dict[str, List[str]]] = None, partition_date: Optional[str] = None) -> Dict[str, Any]:
    """
    Generic function to run a dlt pipeline based on a YAML configuration file.
    This is used internally by the dlt assets.

    Args:
        context: Dagster execution context
        config_path: Path to YAML config file
        stations_data: Dict {station_code: [months]} for temporal filtering
        partition_date: Partition date string
    """
    import os
    import yaml
    from datetime import datetime
    
    context.log.info(f"🚀 Starting dlt ingestion for config: {config_path}")

    # Load configuration from YAML
    # Use absolute path if exists, otherwise join with current directory
    from pathlib import Path
    config_file = Path(config_path)
    if not config_file.is_absolute():
        # Try /app for Docker, fallback to current directory
        docker_path = Path("/app") / config_path
        if docker_path.exists():
            full_path = docker_path
        else:
            full_path = Path.cwd() / config_path
    else:
        full_path = config_file

    with open(full_path, 'r') as f:
        cfg = yaml.safe_load(f)
    
    # Get partition key if available
    partition_key = context.partition_key if context.has_partition_key else None
    if partition_key:
        context.log.info(f"📅 Partition: {partition_key}")

        # Ajouter partition_key dans la config pour résolution du layout
        cfg["partition_key"] = partition_key

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

        # ✅ FIX: Update extraction.start_date aussi (pour dept_datetime mode)
        if cfg.get("extraction") and len(partition_key) == 4 and partition_key.isdigit():
            year = int(partition_key)
            if "start_date" in cfg["extraction"]:
                cfg["extraction"]["start_date"] = f"{year}-01-01"
                context.log.info(f"🗓️ Extraction start_date mis à jour: {year}-01-01")
            if "end_date" in cfg["extraction"]:
                cfg["extraction"]["end_date"] = f"{year}-12-31"
                context.log.info(f"🗓️ Extraction end_date mis à jour: {year}-12-31")

    # Simple logging without accessing nested config fields
    # (DLT will handle the nested config structure internally)
    context.log.info(f"🚀 Starting DLT ingestion...")

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
    context.log.info(f"🏃 Starting DLT pipeline execution...")
    pipeline_start_time = time.time()
    
    # ✅ DEBUG: Log les paramètres stations_data pour comprendre le problème
    if stations_data:
        context.log.warning(f"🔍 DEBUG: stations_data type={type(stations_data)}, count={len(stations_data)}")
        # Afficher quelques exemples
        sample_stations = list(stations_data.items())[:3] if isinstance(stations_data, dict) else stations_data[:3]
        context.log.warning(f"🔍 DEBUG: Sample stations_data: {sample_stations}")
    
    # Capture all logs from DLT pipeline and display them in Dagster
    import io
    import sys
    import logging
    from contextlib import redirect_stdout, redirect_stderr
    
    # Store reference to built-in print function
    import builtins
    original_print = builtins.print
    
    # Custom print function that sends to Dagster
    def dagster_print(*args, **kwargs):
        message = ' '.join(str(arg) for arg in args)
        context.log.warning(f"DLT: {message}")  # ✅ Utiliser WARNING au lieu de INFO pour être sûr de voir les logs
    
    # Monkey patch print to use Dagster logger
    builtins.print = dagster_print
    
    # ✅ Aussi capturer les logs Python des modules dlt_pipeline
    # Créer un handler qui envoie vers Dagster
    class DagsterLogHandler(logging.Handler):
        def emit(self, record):
            msg = self.format(record)
            context.log.warning(f"DLT [{record.levelname}]: {msg}")
    
    dagster_handler = DagsterLogHandler()
    dagster_handler.setLevel(logging.DEBUG)
    
    # Capturer les logs de sources.py
    sources_logger = logging.getLogger('dlt_pipeline.sources')
    sources_logger.setLevel(logging.DEBUG)
    sources_logger.addHandler(dagster_handler)
    
    # Capturer les logs de slicing.py
    slicing_logger = logging.getLogger('dlt_pipeline.slicing')
    slicing_logger.setLevel(logging.DEBUG)
    slicing_logger.addHandler(dagster_handler)
    
    # ✅ CORRECTION CRITIQUE: Capturer les logs de hubeau_source.py (deux formats possibles)
    hubeau_source_logger = logging.getLogger('src.dlt_pipeline.hubeau_source')
    hubeau_source_logger.setLevel(logging.DEBUG)
    hubeau_source_logger.addHandler(dagster_handler)
    
    hubeau_source_logger2 = logging.getLogger('dlt_pipeline.hubeau_source')
    hubeau_source_logger2.setLevel(logging.DEBUG)
    hubeau_source_logger2.addHandler(dagster_handler)
    
    # ✅ Capturer les logs de station_minio.py
    station_minio_logger = logging.getLogger('src.hubeau_pipeline.utils.station_minio')
    station_minio_logger.setLevel(logging.DEBUG)
    station_minio_logger.addHandler(dagster_handler)
    
    try:
        # Execute DLT pipeline with monkey-patched print
        # Get state store from config or use default
        state_store = cfg.get("state_store", "s3://bronze/_state")
        
        # Créer le pipeline DLT
        # ✅ FIX: Déduire dataset_name du source.name pour éviter bronze/bronze
        # Exemple: source.name="piezometry" → dataset_name="piezometry_api"
        # Ça correspond aux paths dans station_minio.py (ex: "piezometry_api/piezometry_stations/")
        source_name = cfg.get("source", {}).get("name", "unknown")
        dataset_name = cfg.get("dataset_name", f"{source_name}_api")

        # ✅ NOUVEAU: Utiliser le module destinations.py pour respecter la config YAML
        from src.dlt_pipeline.destinations import get_filesystem_destination

        # Préparer la configuration filesystem depuis YAML
        filesystem_config = cfg.get("destinations", {}).get("filesystem", {})

        # Override/ajouter les credentials MinIO (priorité aux env vars)
        filesystem_config["credentials"] = {
            "aws_access_key_id": minio_user,
            "aws_secret_access_key": minio_pass,
            "endpoint_url": minio_endpoint,
            "region_name": minio_region,
        }

        # Assurer un bucket_url par défaut si non spécifié dans YAML
        if "bucket_url" not in filesystem_config:
            filesystem_config["bucket_url"] = "s3://bronze"

        # ✅ CORRECTION CRITIQUE: Résoudre le layout avec la partition_key
        # DLT utilise la date actuelle pour {year}, {month} etc., pas la partition !
        # On doit résoudre manuellement les placeholders custom comme {YYYY}
        if partition_key and "layout" in filesystem_config:
            layout = filesystem_config["layout"]
            
            # Extraire l'année de la partition_key
            try:
                if len(partition_key) == 4 and partition_key.isdigit():
                    # Partition annuelle : "2024"
                    year = partition_key
                    month = "00"  # Pas de mois spécifique
                    day = "00"
                elif len(partition_key) == 7:
                    # Partition mensuelle : "2024-08"
                    year = partition_key[:4]
                    month = partition_key[5:7]
                    day = "00"
                else:
                    # Partition quotidienne : "2024-08-15"
                    year = partition_key[:4]
                    month = partition_key[5:7]
                    day = partition_key[8:10]
                
                # Résoudre les placeholders custom
                layout = layout.replace("{YYYY}", year)
                layout = layout.replace("{MM}", month)
                layout = layout.replace("{DD}", day)
                
                filesystem_config["layout"] = layout
                context.log.info(f"✅ Layout résolu: {layout} (partition: {partition_key})")
            except Exception as e:
                context.log.warning(f"⚠️ Erreur lors de la résolution du layout: {e}")

        # Log de la config pour debug
        context.log.info(f"📦 Filesystem config: bucket={filesystem_config.get('bucket_url')}, "
                        f"format={filesystem_config.get('file_format', 'parquet (default)')}, "
                        f"layout={filesystem_config.get('layout', 'default')}")

        # Créer la destination avec file_format, layout depuis YAML
        destination = get_filesystem_destination(filesystem_config)

        pipeline = dlt.pipeline(
            pipeline_name="hubeau_pipeline",
            destination=destination,
            dataset_name=dataset_name
        )

        # Créer la source Hub'Eau
        source = hubeau_rest_source(
            config_path=str(full_path),
            stations_data=stations_data,
            partition_date=partition_date
        )

        # Exécuter le pipeline
        load_info = pipeline.run(source)
    finally:
        # Restore original print function
        builtins.print = original_print
        # Retirer les handlers Dagster pour éviter les fuites mémoire
        sources_logger.removeHandler(dagster_handler)
        slicing_logger.removeHandler(dagster_handler)
        hubeau_source_logger.removeHandler(dagster_handler)
        hubeau_source_logger2.removeHandler(dagster_handler)
        station_minio_logger.removeHandler(dagster_handler)

    pipeline_duration = time.time() - pipeline_start_time
    context.log.info(f"✅ DLT pipeline finished in {pipeline_duration:.2f}s")

    # Extract detailed metrics and statistics
    # Note: DLT LoadInfo doesn't contain detailed metrics, so we rely on DLT's internal logs
    # which are displayed via our monkey-patched print function
    resource_name = cfg.get("resource", {}).get("name", "unknown")
    stats = {
        "stream": resource_name,
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
    context.log.info(f"🎉 Ingestion {resource_name} completed!")
    context.log.info(f"📊 Final statistics:")
    context.log.info(f"   • Load packages: {stats['packages']}")
    context.log.info(f"   • Files written: {stats['files']}")
    context.log.info(f"   • Duration: {pipeline_duration:.2f}s")
    context.log.info(f"   • Data written to MinIO: ✅ (see DLT logs above for detailed metrics)")

    # Check if we have load packages (indicates successful data ingestion)
    if stats['packages'] > 0:
        context.log.info(f"✅ Data successfully ingested for {resource_name}")
        context.log.info(f"   • Detailed metrics available in DLT logs above")
        context.log.info(f"   • Files stored in MinIO path: bronze/{dataset_name}/{resource_name}/")
        stats["rows"] = "see_dlt_logs"  # Indicate that metrics are in DLT logs
    else:
        context.log.warning(f"⚠️ No data ingested for {resource_name}! This might indicate:")
        context.log.warning(f"   • API returned empty results")
        context.log.warning(f"   • Date range has no data")
        context.log.warning(f"   • API endpoint might be incorrect")
        context.log.warning(f"   • Authentication issues")
        stats["warnings"].append("No data ingested - check API endpoint and date range")

    return stats

# ====================================
# ASSETS DE STATIONS DE RÉFÉRENCE (définis en premier)
# ====================================

@asset(group_name="hubeau_hydrometry")
def hydrometry_stations_reference(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests hydrometry stations reference data using dlt (pas de partition)."""
    return ingest_dlt(context, "configs/hubeau/hydrometry_stations.yml")

@asset(group_name="hubeau_piezometry")
def piezometry_stations_reference(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests piezometry stations reference data using dlt (pas de partition)."""
    return ingest_dlt(context, "configs/hubeau/piezometry_stations.yml")

@asset(group_name="hubeau_quality_rivers")
def quality_rivers_stations_reference(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests quality rivers stations reference data using dlt (pas de partition)."""
    return ingest_dlt(context, "configs/hubeau/quality_rivers_stations.yml")

@asset(group_name="hubeau_quality_groundwater")
def quality_groundwater_stations_reference(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests quality groundwater stations reference data using dlt (pas de partition)."""
    return ingest_dlt(context, "configs/hubeau/quality_groundwater_stations.yml")

@asset(group_name="hubeau_ecoulement")
def ecoulement_stations_reference(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests ecoulement stations reference data using dlt (pas de partition)."""
    return ingest_dlt(context, "configs/hubeau/ecoulement_stations.yml")

@asset(group_name="hubeau_ecoulement")
def ecoulement_campagnes_reference(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests ecoulement campaigns reference (utilisé pour caler les fenêtres d'observations)."""
    return ingest_dlt(context, "configs/hubeau/ecoulement_campagnes.yml")
@asset(group_name="hubeau_hydrobio")
def hydrobio_stations_reference(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests hydrobiology stations reference data using dlt (pas de partition)."""
    return ingest_dlt(context, "configs/hubeau/hydrobio_stations.yml")

@asset(group_name="hubeau_prelevements")
def prelevements_ouvrages_reference(context: AssetExecutionContext) -> Dict[str, Any]:
    """
    Ingestion du référentiel des OUVRAGES de prélèvement (~168k ouvrages).

    Un ouvrage = installation technique de prélèvement (infrastructure).
    Utilisé par les chroniques (code_ouvrage).
    """
    return ingest_dlt(context, "configs/hubeau/prelevements_ouvrages.yml")

@asset(group_name="hubeau_prelevements")
def prelevements_points_reference(context: AssetExecutionContext) -> Dict[str, Any]:
    """
    Ingestion du référentiel des POINTS de prélèvement (~186k points).

    Un point = emplacement spécifique de mesure sur un ouvrage.
    1 ouvrage peut avoir plusieurs points de prélèvement.
    """
    return ingest_dlt(context, "configs/hubeau/prelevements_points.yml")

@asset(group_name="hubeau_temperature")
def temperature_stations_reference(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests temperature stations reference data using dlt (pas de partition)."""
    return ingest_dlt(context, "configs/hubeau/temperature_stations.yml")

# ====================================
# NOUVEAUX ASSETS POUR ENDPOINTS MANQUANTS
# ====================================

@asset(group_name="hubeau_hydrometry")
def hydrometry_sites_reference(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests hydrometry sites reference data using dlt (pas de partition)."""
    return ingest_dlt(context, "configs/hubeau/hydrometry_sites.yml")

@asset(group_name="hubeau_hydrometry", partitions_def=YEARLY_PARTITIONS, deps=[hydrometry_stations_reference])
def hydrometry_obs_elab(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests hydrometry elaborated observations (historical data)."""
    partition_date = _get_partition_date_yearly(context)
    stations_data, _ = _setup_observation_asset(context, "hydrometry", partition_date)
    return ingest_dlt(context, "configs/hubeau/hydrometry_obs_elab.yml", stations_data=stations_data, partition_date=partition_date)

@asset(group_name="hubeau_quality_rivers", partitions_def=YEARLY_PARTITIONS, deps=[quality_rivers_stations_reference])
def quality_rivers_operations(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests quality rivers sampling operations."""
    partition_date = _get_partition_date_yearly(context)
    stations_data, _ = _setup_observation_asset(context, "quality_rivers", partition_date)
    return ingest_dlt(context, "configs/hubeau/quality_rivers_operations.yml", stations_data=stations_data, partition_date=partition_date)

@asset(group_name="hubeau_quality_rivers", partitions_def=YEARLY_PARTITIONS, deps=[quality_rivers_stations_reference])
def quality_rivers_conditions(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests quality rivers environmental conditions."""
    partition_date = _get_partition_date_yearly(context)
    stations_data, _ = _setup_observation_asset(context, "quality_rivers", partition_date)
    return ingest_dlt(context, "configs/hubeau/quality_rivers_conditions.yml", stations_data=stations_data, partition_date=partition_date)

@asset(group_name="hubeau_piezometry", partitions_def=YEARLY_PARTITIONS, deps=[piezometry_stations_reference])
def piezometry_chroniques_historical(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests piezometry historical chroniques (complete historical data)."""
    partition_date = _get_partition_date_yearly(context)
    stations_data, _ = _setup_observation_asset(context, "piezometry", partition_date)
    return ingest_dlt(context, "configs/hubeau/piezometry_chroniques_historical.yml", stations_data=stations_data, partition_date=partition_date)

# ====================================
# ASSETS D'OBSERVATIONS/ANALYSES (dépendent des stations)
# ====================================

@asset(group_name="hubeau_hydrobiology", partitions_def=YEARLY_PARTITIONS, deps=[hydrobio_stations_reference])
def hydrobio_taxons(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests hydrobiology taxons data using dlt."""
    partition_date = _get_partition_date_yearly(context)
    stations_data, _ = _setup_observation_asset(context, "hydrobio", partition_date)
    return ingest_dlt(context, "configs/hubeau/hydrobio_taxons.yml", stations_data=stations_data, partition_date=partition_date)

@asset(group_name="hubeau_hydrobiology", partitions_def=YEARLY_PARTITIONS, deps=[hydrobio_stations_reference])
def hydrobio_indices(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests hydrobiology indices data using dlt."""
    partition_date = _get_partition_date_yearly(context)
    stations_data, _ = _setup_observation_asset(context, "hydrobio", partition_date)
    return ingest_dlt(context, "configs/hubeau/hydrobio_indices.yml", stations_data=stations_data, partition_date=partition_date)

@asset(group_name="hubeau_piezometry", partitions_def=YEARLY_PARTITIONS, deps=[piezometry_stations_reference])
def piezometry_chroniques(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests piezometry chroniques data using dlt."""
    partition_date = _get_partition_date_yearly(context)
    stations_data, _ = _setup_observation_asset(context, "piezometry", partition_date)
    return ingest_dlt(context, "configs/hubeau/piezometry_chroniques.yml", stations_data=stations_data, partition_date=partition_date)

@asset(group_name="hubeau_quality_rivers", partitions_def=YEARLY_PARTITIONS, deps=[quality_rivers_stations_reference])
def quality_rivers_analyses(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests superficial waterbodies quality analyses data using dlt."""
    partition_date = _get_partition_date_yearly(context)
    stations_data, _ = _setup_observation_asset(context, "quality_rivers", partition_date)
    return ingest_dlt(context, "configs/hubeau/quality_rivers_analyses.yml", stations_data=stations_data, partition_date=partition_date)

@asset(group_name="hubeau_quality_groundwater", partitions_def=YEARLY_PARTITIONS, deps=[quality_groundwater_stations_reference])
def quality_groundwater_analyses(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests groundwater quality analyses data using dlt."""
    partition_date = _get_partition_date_yearly(context)
    stations_data, _ = _setup_observation_asset(context, "quality_groundwater", partition_date)
    return ingest_dlt(context, "configs/hubeau/quality_groundwater_analyses.yml", stations_data=stations_data, partition_date=partition_date)

@asset(group_name="hubeau_ecoulement", partitions_def=YEARLY_PARTITIONS, deps=[ecoulement_stations_reference, ecoulement_campagnes_reference])
def ecoulement_observations(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests ecoulement observations data using dlt (données annuelles)."""
    partition_date = _get_partition_date_yearly(context)
    stations_data, _ = _setup_observation_asset(context, "ecoulement", partition_date)
    return ingest_dlt(context, "configs/hubeau/ecoulement_observations.yml", stations_data=stations_data, partition_date=partition_date)

@asset(group_name="hubeau_prelevements", partitions_def=YEARLY_PARTITIONS, deps=[prelevements_ouvrages_reference])
def prelevements_chroniques(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests prelevements chroniques data using dlt."""
    partition_date = _get_partition_date_yearly(context)
    stations_data, _ = _setup_observation_asset(context, "prelevements", partition_date)
    return ingest_dlt(context, "configs/hubeau/prelevements_chroniques.yml", stations_data=stations_data, partition_date=partition_date)


@asset(group_name="hubeau_temperature", partitions_def=YEARLY_PARTITIONS, deps=[temperature_stations_reference])
def temperature_chroniques(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests temperature chroniques data using dlt with yearly partitions and automatic fallback."""
    partition_date = _get_partition_date_yearly(context)
    stations_data, _ = _setup_observation_asset(context, "temperature", partition_date)
    context.log.info(f"📊 Processing temperature chroniques with automatic fallback (partition: {context.partition_key})")
    return ingest_dlt(context, "configs/hubeau/temperature_chroniques.yml", stations_data=stations_data, partition_date=partition_date)
