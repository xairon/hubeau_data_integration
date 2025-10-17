"""
Utility functions for Hub'Eau data assets.

This module contains all the business logic extracted from the original dlt_assets.py:
- Station management from MinIO
- Parquet file consolidation
- Skip logic for existing data
- Partition date helpers
- Logging configuration

These utilities are used by dagster-dlt assets to preserve complex business logic.
"""

from typing import Any, Dict, List, Optional, Tuple
import os
import time
import logging
from datetime import datetime
from pathlib import Path

import dlt
from dagster import AssetExecutionContext
from dlt.common.typing import TSecretValue
import pyarrow.parquet as pq
import pyarrow as pa
import pyarrow.fs as pafs


# ====================================
# STATION EXTRACTION FROM MINIO
# ====================================

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
    import sys
    sys.stderr.write(f"Warning: station_minio module not found ({e}), using fallback\n")

    def _extract_station_codes_from_minio(station_type: str) -> List[str]:
        """Fallback: retourne une liste vide si module non disponible"""
        return []

    def _filter_active_stations_for_period(stations: List[str], partition_date: str, station_type: str) -> List[str]:
        """Fallback: retourne toutes les stations si module non disponible"""
        return stations


# ====================================
# PARTITION HELPERS
# ====================================

def get_partition_date_yearly(context: AssetExecutionContext) -> str:
    """Convertit une partition annuelle (ex: '2024') en date (ex: '2024-01-01')."""
    partition_key = context.partition_key
    return f"{partition_key}-01-01"


def get_partition_date_daily(context: AssetExecutionContext) -> str:
    """Retourne directement la partition quotidienne (ex: '2024-01-01')."""
    return context.partition_key


# ====================================
# STATION SETUP FOR OBSERVATION ASSETS
# ====================================

def setup_observation_asset(
    context: AssetExecutionContext,
    station_type: str,
    partition_date: str
) -> Tuple[Dict[str, List[str]], str]:
    """
    Configuration commune pour les assets d'observations.

    Returns:
        tuple: (stations_data: Dict[station_code, List[months]], log_message)
    """
    context.log.info(f"🔍 Récupération des stations {station_type} pour la partition {partition_date}")

    # STRATÉGIE OPTIMISÉE AVEC FALLBACK AUTOMATIQUE:
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
        # Filtrer les stations basé sur les métadonnées MinIO (dates de mesure)
        # Au lieu d'appeler l'API, on utilise les champs date_debut/fin_mesure du référentiel
        context.log.info(f"📂 {len(all_stations)} stations trouvées dans MinIO")

        # Filtrer pour ne garder que les stations actives dans la partition
        filtered_stations = _filter_active_stations_for_period(all_stations, partition_date, station_type)
        context.log.info(f"✅ {len(filtered_stations)} stations actives pour partition {partition_date}")

        # 2. Convertir en dict avec tous les mois de l'année
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
# LOGGING CONFIGURATION
# ====================================

def setup_station_minio_logging(context: AssetExecutionContext):
    """
    Configure le logger station_minio pour capturer les logs dans Dagster.
    Doit être appelé avant check_stations_need_update().
    """
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


def setup_dlt_logging(context: AssetExecutionContext):
    """
    Configure tous les loggers DLT pour capturer les logs dans Dagster.
    Retourne les handlers pour cleanup ultérieur.
    """
    class DagsterLogHandler(logging.Handler):
        def emit(self, record):
            msg = self.format(record)
            context.log.warning(f"DLT [{record.levelname}]: {msg}")

    handler = DagsterLogHandler()
    handler.setLevel(logging.DEBUG)

    # Configure all DLT-related loggers
    loggers_to_configure = [
        'dlt_pipeline.sources',
        'dlt_pipeline.slicing',
        'src.dlt_pipeline.hubeau_source',
        'dlt_pipeline.hubeau_source',
        'src.hubeau_pipeline.utils.station_minio'
    ]

    configured_loggers = []
    for logger_name in loggers_to_configure:
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.DEBUG)
        if handler not in logger.handlers:
            logger.addHandler(handler)
        configured_loggers.append(logger)

    return handler, configured_loggers


# ====================================
# SKIP LOGIC FOR EXISTING DATA
# ====================================

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


# ====================================
# PARQUET CONSOLIDATION
# ====================================

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

        # FIX: Unify schemas before concatenation to handle nullable differences
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


# ====================================
# MINIO CREDENTIALS HELPER
# ====================================

def get_minio_credentials() -> Dict:
    """Get MinIO credentials from environment variables."""
    minio_user = os.getenv("MINIO_USER")
    minio_pass = os.getenv("MINIO_PASS")
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    minio_region = os.getenv("MINIO_REGION", "us-east-1")

    # ✅ FAIL FAST: Validate credentials
    if not minio_user or not minio_pass:
        raise ValueError(
            f"CRITICAL: MinIO credentials not set! "
            f"MINIO_USER={'NOT SET' if not minio_user else 'SET'}, "
            f"MINIO_PASS={'NOT SET' if not minio_pass else 'SET'}"
        )

    return {
        "aws_access_key_id": minio_user,
        "aws_secret_access_key": minio_pass,
        "endpoint_url": minio_endpoint,
        "region_name": minio_region,
    }
