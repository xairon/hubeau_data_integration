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

    Args:
        stations: Liste des codes de stations
        partition_date: Date de partition (ex: "2024-01-01")
        station_type: Type de stations ("temperature", etc.)
        logger: Optional logger instance. If not provided, uses the module logger.

    Returns:
        List[str]: Liste filtrée des stations actives
    """
    # Use provided logger or create a module-level one
    if logger is None:
        logger = logging.getLogger(__name__)

    try:
        # Construire la période (année complète)
        year = datetime.strptime(partition_date, "%Y-%m-%d").year
        start_date = f"{year}-01-01"
        end_date = f"{year}-12-31"

        logger.info(f"🔍 Test des stations pour la période {start_date} à {end_date}")

        # Configuration des endpoints par type
        api_configs = {
            "temperature": {
                "base_url": "https://hubeau.eaufrance.fr/api/v1/temperature",
                # Utiliser l'endpoint stations pour détecter l'activité sans risque de troncature sur les données
                "path": "/station",
                "start_param": "date_debut_mesure",
                "end_param": "date_fin_mesure",
                "station_field": "code_station"
            },
            "hydrometry": {
                "base_url": "https://hubeau.eaufrance.fr/api/v2/hydrometrie",
                "path": "/obs_elab",
                "start_param": "date_debut_obs_elab",
                "end_param": "date_fin_obs_elab",
                "station_field": "code_station"
            },
            "piezometry": {
                "base_url": "https://hubeau.eaufrance.fr/api/v1/niveaux_nappes",
                "path": "/stations",  # ✅ Utiliser /stations au lieu de /chroniques (ne nécessite pas code_bss)
                "start_param": "date_recherche",  # ✅ /stations utilise date_recherche
                "end_param": None,  # ✅ /stations n'a pas de paramètre de fin
                "station_field": "code_bss"
            },
            "quality_rivers": {
                "base_url": "https://hubeau.eaufrance.fr/api/v2/qualite_rivieres",
                "path": "/station_pc",  # ✅ Utiliser /station_pc au lieu de /analyse_pc (pas de limite 20K)
                "start_param": "date_debut_prelevement",
                "end_param": "date_fin_prelevement",
                "station_field": "code_station"
            },
            "quality_groundwater": {
                "base_url": "https://hubeau.eaufrance.fr/api/v1/qualite_nappes",
                "path": "/analyses",
                "start_param": "date_debut_prelevement",
                "end_param": "date_fin_prelevement",
                "station_field": "code_bss"
            },
            "ecoulement": {
                "base_url": "https://hubeau.eaufrance.fr/api/v1/ecoulement",
                "path": "/observations",
                "start_param": "date_debut_obs",
                "end_param": "date_fin_obs",
                "station_field": "code_station"
            },
            "hydrobio": {
                "base_url": "https://hubeau.eaufrance.fr/api/v1/indicateurs_services",
                "path": "/indices",
                "start_param": "date_debut_campagne",
                "end_param": "date_fin_campagne",
                "station_field": "code_station_hydrobio"
            },
            "prelevements": {
                "base_url": "https://hubeau.eaufrance.fr/api/v1/prelevements",
                "path": "/chroniques",
                "start_param": "annee_min",
                "end_param": "annee_max",
                "station_field": "code_ouvrage"  # ⚠️ Chroniques utilisent code_ouvrage, pas code_point_prelevement
            }
        }

        if station_type not in api_configs:
            logger.warning(f"⚠️ Type de station non supporté pour le filtrage: {station_type}")
            return stations

        config = api_configs[station_type]

        # ✅ CORRECTIF: Récupérer TOUTES les stations actives avec pagination
        active_stations = set()
        page = 1
        max_pages = 500  # Limite augmentée (quality_groundwater: 8.5M÷20K=425 pages)

        while page <= max_pages:
            params = {
                "format": "json",
                "size": 20000,  # Taille maximale pour récupérer plus de stations
                "page": page,
            }

            # ✅ Gestion spéciale pour piezometry: utiliser date_recherche avec milieu d'année
            if station_type == "piezometry":
                # Utiliser milieu d'année pour maximiser la couverture
                mid_year_date = f"{year}-06-15"
                params["date_recherche"] = mid_year_date
            else:
                # Pour les autres types, utiliser les paramètres start/end
                params[config["start_param"]] = start_date
                if config["end_param"] is not None:
                    params[config["end_param"]] = end_date

            response = httpx.get(f"{config['base_url']}{config['path']}", params=params, timeout=30)

            if response.status_code not in [200, 206]:
                logger.warning(f"⚠️ Erreur API lors du filtrage (page {page}): {response.status_code}")
                break

            data = response.json()
            records = data.get("data", [])

            if not records:
                # Plus de données
                break

            # Extraire les stations qui ont des données
            station_field = config["station_field"]
            for record in records:
                if station_field in record:
                    active_stations.add(record[station_field])

            logger.info(f"🔍 Page {page}: {len(records)} records, {len(active_stations)} stations uniques trouvées")

            # ✅ Continuer tant que l'API fournit un lien next
            next_link = data.get("next")
            if next_link is None:
                logger.info(f"✅ Dernière page atteinte (next=None)")
                break

            page += 1

        # Filtrer la liste originale pour ne garder que les stations actives
        filtered_stations = [station for station in stations if station in active_stations]

        logger.info(f"📊 Résultat du filtrage: {len(filtered_stations)} stations actives sur {len(stations)} total")
        logger.info(f"📊 Stations actives trouvées dans l'API: {len(active_stations)}")

        return filtered_stations

    except Exception as e:
        logger.error(f"⚠️ Erreur lors du filtrage des stations: {e}")
        return stations  # En cas d'erreur, retourner toutes les stations


# Backward compatibility aliases
_extract_station_codes_from_minio = extract_station_codes_from_minio
_filter_active_stations_for_period = filter_active_stations_for_period