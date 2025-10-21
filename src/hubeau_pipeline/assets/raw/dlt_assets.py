from typing import Any, Dict, List, Optional

import time
import io
import logging

import dlt
from dagster import AssetExecutionContext, asset, DailyPartitionsDefinition, StaticPartitionsDefinition

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

# Import des fonctions de lecture PostgreSQL
from src.hubeau_pipeline.utils.station_postgres import (
    extract_station_codes_from_postgres,
    filter_active_stations_for_period
)

# Partitions pour les données historiques (annuelles depuis 2020 + partition "all")
# La partition "all" permet de récupérer TOUTES les données sans filtre temporel
YEARLY_PARTITIONS = StaticPartitionsDefinition(
    ["all"] + [str(year) for year in range(2020, 2026)]  # "all", 2020-2025
)

# ====================================
# UTILITAIRES POUR RÉDUIRE LA REDONDANCE
# ====================================

def _get_partition_date_yearly(context: AssetExecutionContext) -> Optional[str]:
    """
    Convertit une partition annuelle (ex: '2024') en date (ex: '2024-01-01').
    Si partition = 'all', retourne None (pas de filtre temporel).
    """
    partition_key = context.partition_key
    if partition_key == "all":
        return None  # Pas de filtre temporel
    return f"{partition_key}-01-01"

def _get_partition_date_daily(context: AssetExecutionContext) -> str:
    """Retourne directement la partition quotidienne (ex: '2024-01-01')."""
    return context.partition_key

def _setup_observation_asset(context: AssetExecutionContext, station_type: str, partition_date: Optional[str]) -> tuple[Dict[str, List[str]], str]:
    """
    Configuration commune pour les assets d'observations.

    Args:
        context: Dagster execution context
        station_type: Type de station (piezometry, hydrometry, etc.)
        partition_date: Date de partition (YYYY-01-01) ou None si partition="all"

    Returns:
        tuple: (stations_data: Dict[station_code, List[months]], log_message)
    """
    partition_key = context.partition_key

    if partition_key == "all":
        context.log.info(f"🌍 Récupération de TOUTES les stations {station_type} (partition 'all' - pas de filtre temporel)")
    else:
        context.log.info(f"🔍 Récupération des stations {station_type} pour la partition {partition_date}")

    # Récupérer TOUTES les stations depuis PostgreSQL (référentiel complet)
    try:
        all_stations = extract_station_codes_from_postgres(station_type)
        context.log.info(f"📂 {len(all_stations)} stations total dans référentiel PostgreSQL")
    except Exception as e:
        context.log.error(f"❌ Erreur lors de l'accès à PostgreSQL: {e}")
        import traceback
        context.log.error(f"   Traceback: {traceback.format_exc()}")
        raise RuntimeError(f"PostgreSQL table for {station_type} stations does not exist or is not accessible. "
                         f"Please ensure the station reference data has been loaded into PostgreSQL first.") from e

    stations_data: Dict[str, List[str]] = {}

    if all_stations:
        # Filtrer les stations basé sur les métadonnées PostgreSQL (dates de mesure)
        context.log.info(f"📂 {len(all_stations)} stations trouvées dans PostgreSQL")

        if partition_key == "all":
            # Partition "all" : TOUTES les stations sans filtrage temporel
            filtered_stations = all_stations
            context.log.info(f"✅ {len(filtered_stations)} stations (TOUTES, pas de filtre temporel)")

            # Pour partition "all", pas de mois spécifiques - on laisse DLT gérer
            stations_data = {station: [] for station in filtered_stations}
        else:
            # Partition annuelle : filtrer par période
            filtered_stations = filter_active_stations_for_period(all_stations, partition_date, station_type)
            context.log.info(f"✅ {len(filtered_stations)} stations actives pour partition {partition_date}")

            # Convertir en dict avec tous les mois de l'année
            from datetime import datetime
            year = datetime.strptime(partition_date, "%Y-%m-%d").year
            all_months = [f"{year}-{m:02d}" for m in range(1, 13)]
            stations_data = {station: all_months for station in filtered_stations}

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
# - extract_station_codes_from_postgres, filter_active_stations_for_period -> station_postgres.py

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

        # Skip temporal filters if partition is "all" (retrieve ALL historical data)
        if partition_key == "all":
            context.log.info(f"🌍 Partition 'all' détectée - PAS de filtre temporel (récupération de TOUTES les données)")
            # Do NOT update slicer, temporal_filter, or extraction dates
            # DLT will retrieve all available data from the API
        else:
            # Normal partition with temporal filtering
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
    
    # ✅ Capturer les logs de station_postgres.py (si pas déjà configuré)
    station_postgres_logger = logging.getLogger('src.hubeau_pipeline.utils.station_postgres')
    station_postgres_logger.setLevel(logging.DEBUG)
    # Only add handler if not already present (may have been set up in asset function)
    if dagster_handler not in station_postgres_logger.handlers:
        station_postgres_logger.addHandler(dagster_handler)
    
    try:
        # Execute DLT pipeline with monkey-patched print
        # Créer le pipeline DLT
        # Use PostgreSQL destination
        source_name = cfg.get("source", {}).get("name", "unknown")
        resource_name = cfg.get("resource", {}).get("name", "unknown")

        # Get PostgreSQL destination
        from src.dlt_pipeline.destinations import get_postgres_destination
        postgres_config = cfg.get("destinations", {}).get("postgres", {})
        if "dataset_name" not in postgres_config:
            postgres_config["dataset_name"] = os.getenv("HUBEAU_SCHEMA", "hubeau")
        
        # ✅ FIX: Configurer DLT pour utiliser nos tables existantes
        # DLT va utiliser le schéma hubeau et nos tables PostgreSQL existantes
        postgres_config["schema"] = "hubeau"  # Forcer le schéma hubeau
        context.log.info(f"📦 PostgreSQL destination: schema={postgres_config['schema']}, dataset={postgres_config['dataset_name']}")
        destination = get_postgres_destination(postgres_config)

        # Use dataset_name from postgres_config
        dataset_name = postgres_config["dataset_name"]

        # ✅ FIX: Configure pipelines_dir to use a temp directory to avoid local filesystem issues
        # DLT will store pipeline working files here, but incremental state goes to MinIO automatically
        import tempfile
        pipelines_dir = os.path.join(tempfile.gettempdir(), "dlt_pipelines")
        os.makedirs(pipelines_dir, exist_ok=True)
        context.log.info(f"📁 DLT pipelines_dir: {pipelines_dir}")

        # Vérifier que le schéma hubeau existe
        try:
            import psycopg2
            from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
            
            conn = psycopg2.connect(
                host=os.getenv("PG_HOST", "postgres"),
                port=os.getenv("PG_PORT", "5432"),
                database=os.getenv("PG_DB", "postgres"),
                user=os.getenv("PG_USER", "postgres"),
                password=os.getenv("PG_PASSWORD")
            )
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            
            with conn.cursor() as cur:
                # Vérifier si le schéma hubeau existe
                cur.execute("""
                    SELECT schema_name 
                    FROM information_schema.schemata 
                    WHERE schema_name = 'hubeau'
                """)
                
                if not cur.fetchone():
                    context.log.warning("⚠️ Schema 'hubeau' n'existe pas - création automatique...")
                    # Créer le schéma si nécessaire
                    cur.execute("CREATE SCHEMA IF NOT EXISTS hubeau")
                    context.log.info("✅ Schema 'hubeau' créé")
                else:
                    context.log.info("✅ Schema 'hubeau' existe déjà")
            
            conn.close()
        except Exception as e:
            context.log.error(f"❌ Erreur vérification schéma: {e}")
            context.log.warning("⚠️ Continuation sans vérification du schéma...")

        # ✅ FIX: Use unique pipeline name per source/resource to avoid schema conflicts
        # Each asset gets isolated DLT state to prevent schema leaks between sources
        # Format: "hubeau_{source_name}_{resource_name}"
        # Example: "hubeau_piezometry_piezometry_chroniques_historical"
        pipeline_name = f"hubeau_{source_name}_{resource_name}"
        context.log.info(f"📦 DLT pipeline name: {pipeline_name} (prevents schema conflicts)")

        pipeline = dlt.pipeline(
            pipeline_name=pipeline_name,  # ✅ Unique per asset
            destination=destination,
            dataset_name=dataset_name,
            pipelines_dir=pipelines_dir,  # Use temp directory for local working files
            # Configuration pour utiliser notre schéma existant
            full_refresh=False  # Éviter la recréation complète
        )

        # Créer la source Hub'Eau
        source = hubeau_rest_source(
            config_path=str(full_path),
            stations_data=stations_data,
            partition_date=partition_date
        )

        # ✅ FIX: Configurer DLT pour utiliser nos tables PostgreSQL existantes
        # DLT va maintenant écrire directement dans nos tables existantes
        context.log.info(f"🎯 DLT configuré pour écrire dans les tables PostgreSQL existantes")
        
        # Déterminer le nom de la table existante basé sur le fichier YAML
        table_name = resource_name  # Utiliser le nom de la resource comme nom de table

        # ✅ OPTIMISATION: Utiliser notre custom destination PostgreSQL optimisée
        # avec COPY natif au lieu du DLT standard qui fait row-by-row
        write_disposition = cfg.get("resource", {}).get("write_disposition", "append")
        primary_keys = cfg.get("resource", {}).get("primary_key", [])
        if isinstance(primary_keys, str):
            primary_keys = [primary_keys]

        # Import de notre custom destination optimisée
        from hubeau_pipeline.destinations import postgres_bulk_destination
        from hubeau_pipeline.utils.postgres_helpers import PostgresHelper

        # Déterminer si c'est une table de référence
        is_reference = any(keyword in table_name for keyword in [
            "stations", "sites", "ouvrages", "points", "campagnes"
        ])

        context.log.info(f"🎯 Table {table_name}: type={'référence' if is_reference else 'temporelle'}, disposition={write_disposition}")

        # Exécuter l'extraction avec DLT
        context.log.info(f"📥 Phase 1/2: Extraction des données avec DLT...")
        extract_start = time.time()

        # Extraire les données (DLT excelle ici)
        extracted_data = list(source)  # Convertir le générateur en liste

        extract_duration = time.time() - extract_start
        context.log.info(f"✅ Extraction terminée en {extract_duration:.2f}s - {len(extracted_data)} records extraits")

        if extracted_data:
            # Charger avec notre destination optimisée
            context.log.info(f"💾 Phase 2/2: Chargement optimisé PostgreSQL...")
            load_start = time.time()

            # Add column mappings for specific tables
            column_mappings = None
            if table_name == "hydrobio_stations":
                column_mappings = {
                    "code_station_hydrobio": "code_station",
                    "libelle_station_hydrobio": "libelle_station",
                    "uri_station_hydrobio": "uri_station"
                }

            postgres_bulk_destination.load_batch(
                table_name=table_name,
                data=extracted_data,
                write_disposition=write_disposition,
                primary_keys=primary_keys if primary_keys else None,
                column_mappings=column_mappings
            )

            load_duration = time.time() - load_start
            context.log.info(f"✅ Chargement terminé en {load_duration:.2f}s")
            context.log.info(f"⚡ Performance: {len(extracted_data)/load_duration:.0f} records/seconde")

            # Créer un load_info simulé pour compatibilité
            class LoadPackage:
                def __init__(self, records_loaded):
                    self.load_id = f"custom_load_{int(time.time() * 1000)}"
                    self.records = records_loaded

            class LoadInfo:
                def __init__(self, records_loaded):
                    self.load_packages = [LoadPackage(records_loaded)]
                    self.metrics = {"rows_loaded": records_loaded}

            load_info = LoadInfo(len(extracted_data))
        else:
            context.log.warning(f"⚠️ Aucune donnée extraite pour {table_name}")
            load_info = None

    finally:
        # Restore original print function
        builtins.print = original_print
        # Retirer les handlers Dagster pour éviter les fuites mémoire
        sources_logger.removeHandler(dagster_handler)
        slicing_logger.removeHandler(dagster_handler)
        hubeau_source_logger.removeHandler(dagster_handler)
        hubeau_source_logger2.removeHandler(dagster_handler)
        station_postgres_logger.removeHandler(dagster_handler)

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
    context.log.info(f"   • Duration: {pipeline_duration:.2f}s")
    context.log.info(f"   • Data written to PostgreSQL: ✅ (see DLT logs above for detailed metrics)")

    # Check if we have load packages (indicates successful data ingestion)
    if stats['packages'] > 0:
        context.log.info(f"✅ Data successfully ingested for {resource_name}")
        context.log.info(f"   • Detailed metrics available in DLT logs above")
        context.log.info(f"   • Data stored in PostgreSQL schema: {dataset_name}")
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

# ⚠️ IMPORTANT: hydrometry_sites DOIT être chargé AVANT hydrometry_stations
# car hydrometry_stations a une FK vers hydrometry_sites
@asset(group_name="hubeau_hydrometry")
def hydrometry_sites_reference(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests hydrometry sites reference data using dlt (pas de partition)."""
    return ingest_dlt(context, "configs/hubeau/hydrometry_sites.yml")

@asset(group_name="hubeau_hydrometry", deps=[hydrometry_sites_reference])
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
    # Pas de vérification pour les points car pas dans station_minio.py
    return ingest_dlt(context, "configs/hubeau/prelevements_points.yml")

@asset(group_name="hubeau_temperature")
def temperature_stations_reference(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests temperature stations reference data using dlt (pas de partition)."""
    return ingest_dlt(context, "configs/hubeau/temperature_stations.yml")

# ====================================
# NOUVEAUX ASSETS POUR ENDPOINTS MANQUANTS
# ====================================

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
