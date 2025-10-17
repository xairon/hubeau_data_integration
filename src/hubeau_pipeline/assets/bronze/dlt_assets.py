from typing import Any, Dict, List, Optional

import time
import io
import logging

import dlt
from dagster import AssetExecutionContext, asset, DailyPartitionsDefinition, StaticPartitionsDefinition, MaterializeResult
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

# ✅ DEBUG: Vérifier quelle fonction est importée
import sys
sys.stderr.write(f"🔍 DEBUG IMPORT: _extract_station_codes_from_minio = {_extract_station_codes_from_minio}\n")
sys.stderr.write(f"🔍 DEBUG IMPORT: _extract_station_codes_from_minio module = {_extract_station_codes_from_minio.__module__}\n")

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
        filtered_stations = _filter_active_stations_for_period(all_stations, partition_date, station_type)
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

def _setup_station_minio_logging(context: AssetExecutionContext):
    """
    Configure le logger station_minio pour capturer les logs dans Dagster.
    Doit être appelé avant check_stations_need_update().
    """
    import logging

    class DagsterLogHandler(logging.Handler):
        def emit(self, record):
            msg = self.format(record)
            # Use warning level to ensure visibility
            context.log.warning(f"MINIO [{record.levelname}]: {msg}")

    handler = DagsterLogHandler()
    handler.setLevel(logging.DEBUG)

    # Configure station_minio logger
    station_minio_logger = logging.getLogger('src.hubeau_pipeline.utils.station_minio')
    station_minio_logger.setLevel(logging.DEBUG)

    # Remove existing handlers to avoid duplicates
    station_minio_logger.handlers.clear()

    # Add our Dagster handler
    station_minio_logger.addHandler(handler)

    return handler

def check_stations_need_update(context: AssetExecutionContext, station_type: str) -> bool:
    """
    Vérifie si les stations de référence existent déjà dans MinIO.
    Si des fichiers existent, on skip l'ingestion.

    Args:
        context: Dagster execution context
        station_type: Type de stations ("piezometry", "temperature", etc.)

    Returns:
        bool: True si une mise à jour est nécessaire, False sinon
    """
    import pyarrow.fs as pafs
    import os

    # Mapping des types vers les chemins MinIO
    dataset_mapping = {
        "piezometry": ("piezometry_api", "piezometry_stations"),
        "hydrometry": ("hydrometry_api", "hydrometry_stations"),
        "quality_rivers": ("quality_api", "quality_rivers_stations"),
        "quality_groundwater": ("quality_api", "quality_groundwater_stations"),
        "hydrobio": ("hydrobio_api", "hydrobio_stations"),
        "ecoulement": ("ecoulement_api", "ecoulement_stations"),
        "prelevements": ("prelevements_api", "prelevements_ouvrages"),
        "temperature": ("temperature_api", "temperature_stations")
    }

    if station_type not in dataset_mapping:
        context.log.warning(f"⚠️ Type de station non supporté: {station_type}, lancement de l'intégration")
        return True

    try:
        # Configuration S3/MinIO
        minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://srv991054.hstgr.cloud:9000")
        minio_user = os.getenv("MINIO_ROOT_USER", "minioadmin")
        minio_pass = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")

        s3 = pafs.S3FileSystem(
            endpoint_override=minio_endpoint.replace("http://", "").replace("https://", ""),
            access_key=minio_user,
            secret_key=minio_pass,
            scheme="http" if "http://" in minio_endpoint else "https"
        )

        # Construire le chemin
        source_name, resource_name = dataset_mapping[station_type]
        path = f"bronze/{source_name}/{resource_name}/"

        context.log.info(f"🔍 Checking for existing files in: {path}")

        # Vérifier si des fichiers .parquet existent
        try:
            files = s3.get_file_info(pafs.FileSelector(path, recursive=False))
            parquet_files = [f for f in files if f.path.endswith('.parquet') and f.type == pafs.FileType.File]

            if parquet_files:
                context.log.info(f"✅ Fichiers trouvés dans MinIO: {len(parquet_files)} fichier(s)")
                context.log.info(f"   ⏭️  L'asset sera SKIPPED (données déjà présentes)")
                return False
            else:
                context.log.info(f"📂 Aucun fichier dans MinIO, lancement de l'intégration")
                return True

        except Exception as read_error:
            context.log.info(f"📂 Path n'existe pas dans MinIO: {path}")
            context.log.info(f"   Lancement de l'intégration initiale")
            return True

    except Exception as e:
        context.log.error(f"❌ Erreur lors de la vérification: {e}")
        import traceback
        context.log.error(f"📋 Traceback: {traceback.format_exc()}")
        context.log.info(f"🔄 Lancement de l'intégration par précaution")
        return True


def consolidate_parquet_files(
    context: AssetExecutionContext,
    source_name: str,
    resource_name: str,
    bucket_url: str,
    credentials: Dict
):
    """
    Consolidate multiple parquet files into a single file.

    This prevents accumulation of files when using merge write_disposition.
    Reads all parquet files in the directory, deduplicates, and writes back as single file.

    Args:
        context: Dagster execution context
        source_name: Source name (e.g., "piezometry")
        resource_name: Resource name (e.g., "piezometry_stations")
        bucket_url: MinIO bucket URL
        credentials: MinIO credentials
    """
    import pyarrow.parquet as pq
    import pyarrow as pa
    import pyarrow.fs as pafs
    from datetime import datetime

    dataset_name = f"{source_name}_api"
    path = f"{bucket_url.replace('s3://', '')}/{dataset_name}/{resource_name}/"

    context.log.info(f"🔄 Consolidating parquet files in {path}")

    # Create S3 filesystem
    endpoint = credentials["endpoint_url"].replace("http://", "").replace("https://", "")
    s3 = pafs.S3FileSystem(
        access_key=credentials["aws_access_key_id"],
        secret_key=credentials["aws_secret_access_key"],
        endpoint_override=endpoint,
        scheme="http"
    )

    try:
        # List all parquet files
        files = s3.get_file_info(pafs.FileSelector(path, recursive=True))
        parquet_files = [f for f in files if f.path.endswith('.parquet') and f.type == pafs.FileType.File]

        if len(parquet_files) <= 1:
            context.log.info(f"✅ Only {len(parquet_files)} file(s), no consolidation needed")
            return

        context.log.info(f"📚 Found {len(parquet_files)} parquet files to consolidate")

        # Read all files into a single table
        tables = []
        for file_info in parquet_files:
            try:
                table = pq.read_table(file_info.path, filesystem=s3)
                tables.append(table)
                context.log.debug(f"   Read {file_info.path}: {table.num_rows} rows")
            except Exception as e:
                context.log.warning(f"   ⚠️ Could not read {file_info.path}: {e}")

        if not tables:
            context.log.warning(f"⚠️ No tables could be read, skipping consolidation")
            return

        # ✅ FIX: Unify schemas before concatenation to handle nullable differences
        # DLT may create different nullable constraints between runs
        if len(tables) > 1:
            # Create unified schema with all fields nullable
            base_schema = tables[0].schema
            unified_fields = [pa.field(f.name, f.type, nullable=True) for f in base_schema]
            unified_schema = pa.schema(unified_fields)

            # Cast all tables to unified schema
            tables = [table.cast(unified_schema) for table in tables]
            context.log.info(f"✅ Unified {len(tables)} schemas (all fields nullable)")

        # Concatenate all tables
        combined_table = pa.concat_tables(tables)
        initial_rows = combined_table.num_rows

        context.log.info(f"📊 Combined {len(tables)} tables: {initial_rows} total rows")

        # Deduplicate based on primary key (if exists in schema)
        # For station data, this would be code_station or code_bss
        schema = combined_table.schema
        primary_key_candidates = ["code_station", "code_bss", "code_ouvrage"]
        primary_key = None

        for pk in primary_key_candidates:
            if pk in schema.names:
                primary_key = pk
                break

        if primary_key:
            # Convert to pandas for easy deduplication
            df = combined_table.to_pandas()
            df_dedup = df.drop_duplicates(subset=[primary_key], keep='last')
            combined_table = pa.Table.from_pandas(df_dedup)

            rows_removed = initial_rows - combined_table.num_rows
            context.log.info(f"🧹 Deduplicated by '{primary_key}': removed {rows_removed} duplicate rows")

        # Write consolidated file with timestamp
        timestamp = datetime.now().timestamp()
        consolidated_path = f"{path}{timestamp}.parquet"

        pq.write_table(combined_table, consolidated_path, filesystem=s3)
        context.log.info(f"✅ Wrote consolidated file: {consolidated_path}")
        context.log.info(f"   Final row count: {combined_table.num_rows}")

        # Delete old files
        for file_info in parquet_files:
            try:
                s3.delete_file(file_info.path)
                context.log.debug(f"   🗑️ Deleted old file: {file_info.path}")
            except Exception as e:
                context.log.warning(f"   ⚠️ Could not delete {file_info.path}: {e}")

        context.log.info(f"✅ Consolidation complete: {len(parquet_files)} files → 1 file")

    except Exception as e:
        context.log.error(f"❌ Consolidation failed: {e}")
        import traceback
        context.log.error(f"   Traceback: {traceback.format_exc()}")
        raise


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

    # ✅ FAIL FAST: Load MinIO credentials from environment (NO DEFAULTS)
    minio_user = os.getenv("MINIO_USER")
    minio_pass = os.getenv("MINIO_PASS")
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    minio_region = os.getenv("MINIO_REGION", "us-east-1")

    # Validate required credentials
    if not minio_user or not minio_pass:
        error_msg = (
            "❌ CRITICAL: MinIO credentials not set!\n"
            f"   MINIO_USER: {'NOT SET' if not minio_user else 'SET'}\n"
            f"   MINIO_PASS: {'NOT SET' if not minio_pass else 'SET'}\n"
            "These MUST be defined in GitLab CI/CD Variables and properly exported.\n"
            "Check .gitlab-ci.yml exports and ensure variables are not 'Protected' "
            "or branch is protected."
        )
        context.log.error(error_msg)
        raise ValueError(error_msg)

    # Log MinIO credentials for debugging (masked password)
    context.log.info(f"🔐 MinIO credentials:")
    context.log.info(f"   - MINIO_USER: {minio_user}")
    context.log.info(f"   - MINIO_PASS: SET")
    context.log.info(f"   - MINIO_ENDPOINT: {minio_endpoint}")
    context.log.info(f"   - MINIO_REGION: {minio_region}")

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
    
    # ✅ Capturer les logs de station_minio.py (si pas déjà configuré)
    station_minio_logger = logging.getLogger('src.hubeau_pipeline.utils.station_minio')
    station_minio_logger.setLevel(logging.DEBUG)
    # Only add handler if not already present (may have been set up in asset function)
    if dagster_handler not in station_minio_logger.handlers:
        station_minio_logger.addHandler(dagster_handler)
    
    try:
        # Execute DLT pipeline with monkey-patched print
        # Créer le pipeline DLT
        # ✅ FIX: Déduire dataset_name du source.name pour éviter bronze/bronze
        # Exemple: source.name="piezometry" → dataset_name="piezometry_api"
        # Ça correspond aux paths dans station_minio.py (ex: "piezometry_api/piezometry_stations/")
        source_name = cfg.get("source", {}).get("name", "unknown")
        resource_name = cfg.get("resource", {}).get("name", "unknown")
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

        # ✅ FIX: Configure pipelines_dir to use a temp directory to avoid local filesystem issues
        # DLT will store pipeline working files here, but incremental state goes to MinIO automatically
        import tempfile
        pipelines_dir = os.path.join(tempfile.gettempdir(), "dlt_pipelines")
        os.makedirs(pipelines_dir, exist_ok=True)
        context.log.info(f"📁 DLT pipelines_dir: {pipelines_dir}")

        pipeline = dlt.pipeline(
            pipeline_name="hubeau_pipeline",
            destination=destination,
            dataset_name=dataset_name,
            pipelines_dir=pipelines_dir  # Use temp directory for local working files
        )

        # Créer la source Hub'Eau
        source = hubeau_rest_source(
            config_path=str(full_path),
            stations_data=stations_data,
            partition_date=partition_date
        )

        # Extract file format from config (default to parquet)
        file_format = cfg.get("destinations", {}).get("filesystem", {}).get("file_format", "parquet")
        context.log.info(f"🗂️ Using file format: {file_format}")

        # Exécuter le pipeline avec format explicite
        load_info = pipeline.run(source, loader_file_format=file_format)

        # ✅ POST-INGESTION: Consolidate multiple parquet files into one
        # This prevents accumulation of files with merge mode
        if load_info and hasattr(load_info, 'load_packages') and len(load_info.load_packages) > 0:
            try:
                consolidate_parquet_files(
                    context,
                    source_name=source_name,
                    resource_name=resource_name,
                    bucket_url=filesystem_config.get("bucket_url", "s3://bronze"),
                    credentials=filesystem_config["credentials"]
                )
            except Exception as consolidation_error:
                context.log.warning(f"⚠️ File consolidation failed: {consolidation_error}")
                context.log.warning(f"   Data is still valid but multiple files may accumulate")

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
def hydrometry_stations_reference(context: AssetExecutionContext) -> MaterializeResult:
    """Ingests hydrometry stations reference data using dlt (pas de partition)."""
    # ✅ Setup logging BEFORE checking MinIO
    _setup_station_minio_logging(context)

    if not check_stations_need_update(context, "hydrometry"):
        return MaterializeResult(
            metadata={
                "status": "skipped",
                "reason": "Données inchangées - aucune ingestion nécessaire",
                "minio_path": "bronze/hydrometry_api/hydrometry_stations/"
            }
        )

    ingest_dlt(context, "configs/hubeau/hydrometry_stations.yml")
    return MaterializeResult(
        metadata={
            "status": "completed",
            "info": "Ingestion terminée avec succès"
        }
    )

@asset(group_name="hubeau_piezometry")
def piezometry_stations_reference(context: AssetExecutionContext) -> MaterializeResult:
    """Ingests piezometry stations reference data using dlt (pas de partition)."""
    # ✅ Setup logging BEFORE checking MinIO
    _setup_station_minio_logging(context)

    if not check_stations_need_update(context, "piezometry"):
        return MaterializeResult(
            metadata={
                "status": "skipped",
                "reason": "Données inchangées - aucune ingestion nécessaire",
                "minio_path": "bronze/piezometry_api/piezometry_stations/"
            }
        )

    ingest_dlt(context, "configs/hubeau/piezometry_stations.yml")
    return MaterializeResult(
        metadata={
            "status": "completed",
            "info": "Ingestion terminée avec succès"
        }
    )

@asset(group_name="hubeau_quality_rivers")
def quality_rivers_stations_reference(context: AssetExecutionContext) -> MaterializeResult:
    """Ingests quality rivers stations reference data using dlt (pas de partition)."""
    # ✅ Setup logging BEFORE checking MinIO
    _setup_station_minio_logging(context)

    if not check_stations_need_update(context, "quality_rivers"):
        return MaterializeResult(
            metadata={
                "status": "skipped",
                "reason": "Données inchangées - aucune ingestion nécessaire",
                "minio_path": "bronze/quality_api/quality_rivers_stations/"
            }
        )

    ingest_dlt(context, "configs/hubeau/quality_rivers_stations.yml")
    return MaterializeResult(
        metadata={
            "status": "completed",
            "info": "Ingestion terminée avec succès"
        }
    )

@asset(group_name="hubeau_quality_groundwater")
def quality_groundwater_stations_reference(context: AssetExecutionContext) -> MaterializeResult:
    """Ingests quality groundwater stations reference data using dlt (pas de partition)."""
    # ✅ Setup logging BEFORE checking MinIO
    _setup_station_minio_logging(context)

    if not check_stations_need_update(context, "quality_groundwater"):
        return MaterializeResult(
            metadata={
                "status": "skipped",
                "reason": "Données inchangées - aucune ingestion nécessaire",
                "minio_path": "bronze/quality_api/quality_groundwater_stations/"
            }
        )

    ingest_dlt(context, "configs/hubeau/quality_groundwater_stations.yml")
    return MaterializeResult(
        metadata={
            "status": "completed",
            "info": "Ingestion terminée avec succès"
        }
    )

@asset(group_name="hubeau_ecoulement")
def ecoulement_stations_reference(context: AssetExecutionContext) -> MaterializeResult:
    """Ingests ecoulement stations reference data using dlt (pas de partition)."""
    # ✅ Setup logging BEFORE checking MinIO
    _setup_station_minio_logging(context)

    if not check_stations_need_update(context, "ecoulement"):
        return MaterializeResult(
            metadata={
                "status": "skipped",
                "reason": "Données inchangées - aucune ingestion nécessaire",
                "minio_path": "bronze/ecoulement_api/ecoulement_stations/"
            }
        )

    ingest_dlt(context, "configs/hubeau/ecoulement_stations.yml")
    return MaterializeResult(
        metadata={
            "status": "completed",
            "info": "Ingestion terminée avec succès"
        }
    )

@asset(group_name="hubeau_ecoulement")
def ecoulement_campagnes_reference(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests ecoulement campaigns reference (utilisé pour caler les fenêtres d'observations)."""
    return ingest_dlt(context, "configs/hubeau/ecoulement_campagnes.yml")
@asset(group_name="hubeau_hydrobio")
def hydrobio_stations_reference(context: AssetExecutionContext) -> MaterializeResult:
    """Ingests hydrobiology stations reference data using dlt (pas de partition)."""
    # ✅ Setup logging BEFORE checking MinIO
    _setup_station_minio_logging(context)

    if not check_stations_need_update(context, "hydrobio"):
        return MaterializeResult(
            metadata={
                "status": "skipped",
                "reason": "Données inchangées - aucune ingestion nécessaire",
                "minio_path": "bronze/hydrobio_api/hydrobio_stations/"
            }
        )

    ingest_dlt(context, "configs/hubeau/hydrobio_stations.yml")
    return MaterializeResult(
        metadata={
            "status": "completed",
            "info": "Ingestion terminée avec succès"
        }
    )

@asset(group_name="hubeau_prelevements")
def prelevements_ouvrages_reference(context: AssetExecutionContext) -> MaterializeResult:
    """
    Ingestion du référentiel des OUVRAGES de prélèvement (~168k ouvrages).

    Un ouvrage = installation technique de prélèvement (infrastructure).
    Utilisé par les chroniques (code_ouvrage).
    """
    # ✅ Setup logging BEFORE checking MinIO
    _setup_station_minio_logging(context)

    if not check_stations_need_update(context, "prelevements"):
        return MaterializeResult(
            metadata={
                "status": "skipped",
                "reason": "Données inchangées - aucune ingestion nécessaire",
                "minio_path": "bronze/prelevements_api/prelevements_ouvrages/"
            }
        )

    ingest_dlt(context, "configs/hubeau/prelevements_ouvrages.yml")
    return MaterializeResult(
        metadata={
            "status": "completed",
            "info": "Ingestion terminée avec succès"
        }
    )

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
def temperature_stations_reference(context: AssetExecutionContext) -> MaterializeResult:
    """Ingests temperature stations reference data using dlt (pas de partition)."""
    # ✅ Setup logging BEFORE checking MinIO
    _setup_station_minio_logging(context)

    if not check_stations_need_update(context, "temperature"):
        return MaterializeResult(
            metadata={
                "status": "skipped",
                "reason": "Données inchangées - aucune ingestion nécessaire",
                "minio_path": "bronze/temperature_api/temperature_stations/"
            }
        )

    ingest_dlt(context, "configs/hubeau/temperature_stations.yml")
    return MaterializeResult(
        metadata={
            "status": "completed",
            "info": "Ingestion terminée avec succès"
        }
    )

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
