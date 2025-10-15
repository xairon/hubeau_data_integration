"""
Station MinIO extraction logic for Hub'Eau data ingestion.
Handles reading station reference data from MinIO storage and filtering active stations.
"""
import calendar
import json
import gzip
import logging
import os
import traceback
from datetime import date, datetime, timedelta
from typing import List, Optional

import boto3
import httpx
import pandas as pd
import pyarrow.fs as pafs
import pyarrow.parquet as pq


def extract_station_codes_from_minio(
    station_type: str = "temperature",
    logger: Optional[logging.Logger] = None
) -> List[str]:
    """
    Extrait les codes de stations depuis MinIO (liste potentiellement incomplète).
    Cette fonction est un fallback si l'API Hub'Eau n'est pas disponible.

    Args:
        station_type: Type de stations à extraire
        logger: Optional logger instance. If not provided, uses the module logger.

    Returns:
        List[str]: Liste des codes de stations
    """
    # Use provided logger or create a module-level one
    if logger is None:
        logger = logging.getLogger(__name__)

    # Configuration MinIO depuis les variables d'environnement
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    minio_user = os.getenv("MINIO_USER", "minioadmin")
    minio_pass = os.getenv("MINIO_PASS", "minioadmin")
    bucket_name = os.getenv("MINIO_BRONZE_BUCKET", "bronze")

    try:
        # Client S3 pour MinIO
        s3_client = boto3.client(
            's3',
            endpoint_url=minio_endpoint,
            aws_access_key_id=minio_user,
            aws_secret_access_key=minio_pass,
            region_name='us-east-1'
        )

        # Déterminer le préfixe selon le type de stations (correspondant aux dataset_name dans les configs YAML)
        station_prefixes = {
            "temperature": "temperature_api/temperature_stations/",
            "hydrometry": "hydrometry_api/hydrometry_stations/",
            "piezometry": "piezometry_api/piezometry_stations/",
            "quality_rivers": "quality_rivers_api/quality_rivers_stations/",
            "quality_groundwater": "quality_groundwater_api/quality_groundwater_stations/",
            "ecoulement": "ecoulement_api/ecoulement_stations/",
            "hydrobio": "hydrobio_api/hydrobio_stations/",
            "prelevements": "prelevements_api/prelevements_ouvrages/"  # ✅ CORRECTION: Utiliser ouvrages (chroniques utilisent code_ouvrage)
        }

        prefix = station_prefixes.get(station_type, f"{station_type}_api/{station_type}_stations/")

        # Chercher les fichiers de stations les plus récents (préfixe standard)
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=prefix
        )

        # Si rien trouvé, essayer des variantes possibles (avec dataset en préfixe)
        if 'Contents' not in response or not response.get('Contents'):
            logger.warning(f"⚠️ No files found in {prefix}, trying alternative paths...")
            alt_prefixes = [
                f"{station_type}_stations/",
                f"bronze/{station_type}_stations/",
                # Variantes avec datasets (ex: quality_groundwater_api/quality_groundwater_stations/)
                f"quality_groundwater_api/{station_type}_stations/",
                f"quality_rivers_api/{station_type}_stations/",
                f"temperature_api/{station_type}_stations/",
                f"hydrometry_api/{station_type}_stations/",
                f"piezometry_api/{station_type}_stations/",
                f"ecoulement_api/{station_type}_stations/",
                f"hydrobio_api/{station_type}_stations/",
                f"prelevements_api/{station_type}_stations/",
            ]
            for alt_prefix in alt_prefixes:
                response = s3_client.list_objects_v2(
                    Bucket=bucket_name,
                    Prefix=alt_prefix
                )
                if response.get('Contents'):
                    break

        # Si toujours rien, dernier recours: lister et filtrer par motif "/{folder}/"
        if 'Contents' not in response or not response.get('Contents'):
            try:
                logger.warning("⚠️ Broad scan of bucket to locate station files (may be slow)...")
                all_objs = s3_client.list_objects_v2(Bucket=bucket_name)
                if 'Contents' in all_objs:
                    folder = station_prefixes.get(station_type, f"{station_type}_stations/").rstrip('/')
                    filtered = [o for o in all_objs['Contents'] if f"/{folder}/" in o['Key'] or o['Key'].startswith(folder + "/")]
                    if filtered:
                        response = {'Contents': filtered}
            except Exception:
                pass

        if 'Contents' not in response:
            raise ValueError(f"No {station_type} stations data found in MinIO")

        # Filtrer pour ne prendre que les fichiers Parquet (pas les dossiers)
        parquet_files = [f for f in response['Contents'] if f['Key'].endswith('.parquet')]

        if not parquet_files:
            raise ValueError(f"No Parquet files found in {station_type}_stations folder")

        # Prendre le fichier le plus récent
        latest_file = max(parquet_files, key=lambda x: x['LastModified'])
        file_key = latest_file['Key']

        logger.info(f"📂 Reading {station_type} stations from Parquet: {file_key}")

        # Déterminer le champ de clé selon le type de stations
        station_key_fields = {
            "temperature": "code_station",
            "hydrometry": "code_station",
            "piezometry": "code_bss",
            "quality_rivers": "code_station",
            "quality_groundwater": "code_bss",
            "ecoulement": "code_station",
            "hydrobio": "code_station_hydrobio",
            "prelevements": "code_ouvrage"  # ✅ CORRECTION: Chroniques utilisent code_ouvrage (ouvrages)
        }

        key_field = station_key_fields.get(station_type, "code_station")

        try:
            # Créer un filesystem S3 pour pyarrow (compatible MinIO)
            endpoint_host = minio_endpoint.replace('http://', '').replace('https://', '')
            scheme = 'http' if minio_endpoint.startswith('http://') else 'https'

            s3_fs = pafs.S3FileSystem(
                access_key=minio_user,
                secret_key=minio_pass,
                endpoint_override=endpoint_host,
                scheme=scheme
            )

            # Construire le chemin S3 complet : bucket/key
            s3_path = f"{bucket_name}/{file_key}"

            # Lire le Parquet directement via le filesystem S3
            table = pq.read_table(s3_path, filesystem=s3_fs)
            df = table.to_pandas()

            # Extraire les codes de stations
            if key_field in df.columns:
                # Filtrer seulement les stations encore actives
                # (avec une date de fin de service récente ou nulle)
                if 'date_fin_service' in df.columns:
                    # Filtrer stations actives (date_fin_service nulle ou >= 2024-01-01)
                    df_active = df[
                        (df['date_fin_service'].isna()) |
                        (df['date_fin_service'] >= "2024-01-01")
                    ]
                    stations = df_active[key_field].dropna().unique().tolist()
                else:
                    # Pas de colonne date_fin_service, prendre toutes les stations
                    stations = df[key_field].dropna().unique().tolist()
            else:
                logger.warning(f"⚠️ Colonne {key_field} non trouvée dans le Parquet. Colonnes disponibles: {df.columns.tolist()}")
                stations = []

        except Exception as e:
            logger.error(f"⚠️ Erreur lors de la lecture du fichier Parquet: {e}")
            logger.error(traceback.format_exc())
            stations = []

        logger.info(f"✅ Extracted {len(stations)} {station_type} station codes from MinIO Parquet")
        return stations

    except Exception as e:
        logger.error(f"⚠️ Erreur lors de la lecture des {station_type} stations depuis MinIO: {e}")
        logger.error(traceback.format_exc())
        return []


def filter_active_stations_for_period(
    stations: List[str],
    partition_date: str,
    station_type: str,
    logger: Optional[logging.Logger] = None
) -> List[str]:
    """
    Filtre les stations pour ne garder que celles qui ont des données pour la période donnée.
    Utilise les métadonnées (dates) depuis MinIO au lieu d'appeler l'API.

    Args:
        stations: Liste des codes de stations
        partition_date: Date de partition (ex: "2024-01-01")
        station_type: Type de stations ("temperature", etc.)
        logger: Optional logger instance. If not provided, uses the module logger.

    Returns:
        List[str]: Liste filtrée des stations actives pour la période
    """
    # Use provided logger or create a module-level one
    if logger is None:
        logger = logging.getLogger(__name__)

    try:
        # Construire la période (année complète)
        year = datetime.strptime(partition_date, "%Y-%m-%d").year
        start_date_year = date(year, 1, 1)
        end_date_year = date(year, 12, 31)

        logger.info(f"🔍 Filtrage des stations pour l'année {year} basé sur métadonnées MinIO")

        # Configuration MinIO depuis les variables d'environnement
        minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
        minio_user = os.getenv("MINIO_USER", "minioadmin")
        minio_pass = os.getenv("MINIO_PASS", "minioadmin")
        bucket_name = os.getenv("MINIO_BRONZE_BUCKET", "bronze")

        # Client S3 pour MinIO
        s3_client = boto3.client(
            's3',
            endpoint_url=minio_endpoint,
            aws_access_key_id=minio_user,
            aws_secret_access_key=minio_pass,
            region_name='us-east-1'
        )

        # Déterminer le préfixe selon le type de stations
        station_prefixes = {
            "temperature": "temperature_api/temperature_stations/",
            "hydrometry": "hydrometry_api/hydrometry_stations/",
            "piezometry": "piezometry_api/piezometry_stations/",
            "quality_rivers": "quality_rivers_api/quality_rivers_stations/",
            "quality_groundwater": "quality_groundwater_api/quality_groundwater_stations/",
            "ecoulement": "ecoulement_api/ecoulement_stations/",
            "hydrobio": "hydrobio_api/hydrobio_stations/",
            "prelevements": "prelevements_api/prelevements_ouvrages/"
        }

        prefix = station_prefixes.get(station_type, f"{station_type}_api/{station_type}_stations/")

        # Chercher les fichiers de stations
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=prefix
        )

        if 'Contents' not in response:
            logger.warning(f"⚠️ Aucun fichier trouvé dans {prefix}, retour de toutes les stations")
            return stations

        # Filtrer pour ne prendre que les fichiers Parquet
        parquet_files = [f for f in response['Contents'] if f['Key'].endswith('.parquet')]

        if not parquet_files:
            logger.warning(f"⚠️ Aucun fichier Parquet trouvé, retour de toutes les stations")
            return stations

        # Prendre le fichier le plus récent
        latest_file = max(parquet_files, key=lambda x: x['LastModified'])
        file_key = latest_file['Key']

        logger.info(f"📂 Lecture des métadonnées depuis: {file_key}")

        # Déterminer le champ de clé selon le type de stations
        station_key_fields = {
            "temperature": "code_station",
            "hydrometry": "code_station",
            "piezometry": "code_bss",
            "quality_rivers": "code_station",
            "quality_groundwater": "code_bss",
            "ecoulement": "code_station",
            "hydrobio": "code_station_hydrobio",
            "prelevements": "code_ouvrage"
        }

        key_field = station_key_fields.get(station_type, "code_station")

        # Créer un filesystem S3 pour pyarrow (compatible MinIO)
        endpoint_host = minio_endpoint.replace('http://', '').replace('https://', '')
        scheme = 'http' if minio_endpoint.startswith('http://') else 'https'

        s3_fs = pafs.S3FileSystem(
            access_key=minio_user,
            secret_key=minio_pass,
            endpoint_override=endpoint_host,
            scheme=scheme
        )

        # Construire le chemin S3 complet
        s3_path = f"{bucket_name}/{file_key}"

        # Lire le Parquet directement via le filesystem S3
        table = pq.read_table(s3_path, filesystem=s3_fs)
        df = table.to_pandas()

        logger.info(f"📊 {len(df)} stations dans le référentiel MinIO")

        # Filtrer selon les dates de mesure
        # Les champs de dates varient selon l'API
        date_debut_fields = ["date_debut_mesure", "date_debut_obs", "date_debut_service"]
        date_fin_fields = ["date_fin_mesure", "date_fin_obs", "date_fin_service"]

        # Trouver les champs de dates présents
        debut_field = None
        fin_field = None

        for field in date_debut_fields:
            if field in df.columns:
                debut_field = field
                break

        for field in date_fin_fields:
            if field in df.columns:
                fin_field = field
                break

        if not debut_field or not fin_field:
            logger.warning(f"⚠️ Champs de dates non trouvés (colonnes: {df.columns.tolist()}), retour de toutes les stations")
            return stations

        logger.info(f"📅 Utilisation des champs: {debut_field}, {fin_field}")

        # Convertir les dates en format datetime
        df[debut_field] = pd.to_datetime(df[debut_field], errors='coerce')
        df[fin_field] = pd.to_datetime(df[fin_field], errors='coerce')

        # Filtrer les stations actives pour l'année demandée
        # Une station est active si:
        # - date_debut_mesure <= end_date_year (a commencé avant ou pendant l'année)
        # - ET (date_fin_mesure >= start_date_year OU date_fin_mesure est NULL) (est toujours active ou s'est arrêtée après le début de l'année)
        df_active = df[
            (df[debut_field] <= pd.Timestamp(end_date_year)) &
            ((df[fin_field].isna()) | (df[fin_field] >= pd.Timestamp(start_date_year)))
        ]

        # Extraire les codes des stations actives
        if key_field in df_active.columns:
            active_station_codes = set(df_active[key_field].dropna().unique().tolist())
        else:
            logger.warning(f"⚠️ Champ {key_field} non trouvé, retour de toutes les stations")
            return stations

        # Filtrer la liste originale
        filtered_stations = [s for s in stations if s in active_station_codes]

        logger.info(f"✅ {len(filtered_stations)} stations actives pour {year} (sur {len(stations)} total)")

        return filtered_stations

    except Exception as e:
        logger.error(f"⚠️ Erreur lors du filtrage basé sur métadonnées MinIO: {e}")
        logger.error(traceback.format_exc())
        logger.warning("⚠️ Retour de toutes les stations sans filtrage")
        return stations  # En cas d'erreur, retourner toutes les stations


# Backward compatibility aliases
_extract_station_codes_from_minio = extract_station_codes_from_minio
_filter_active_stations_for_period = filter_active_stations_for_period