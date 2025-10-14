"""
Module pour extraire et filtrer les codes de stations depuis MinIO.

Ce module fournit les fonctions nécessaires pour:
1. Lire les données de référence des stations depuis MinIO (fichiers parquet)
2. Extraire les codes de stations
3. Filtrer les stations actives pour une période donnée
"""

import os
import logging
from typing import List
from datetime import datetime

import pandas as pd
import pyarrow.parquet as pq
import pyarrow.fs as pafs

logger = logging.getLogger(__name__)


def _get_station_code_column(station_type: str, columns: List[str]) -> str:
    """
    Détermine le nom de la colonne contenant le code station.

    Différents endpoints utilisent différents noms:
    - piezometry: code_bss
    - hydrometry: code_station
    - quality_rivers: code_station
    - etc.

    Args:
        station_type: Type de station ("piezometry", "hydrometry", etc.)
        columns: Liste des colonnes disponibles dans le DataFrame

    Returns:
        Nom de la colonne contenant le code station
    """
    # Mapping station_type -> colonne de code
    station_code_mapping = {
        "piezometry": "code_bss",
        "hydrometry": "code_station",
        "quality_rivers": "code_station",
        "quality_groundwater": "code_station",
        "hydrobio": "code_station",
        "ecoulement": "code_station",
        "prelevements": "code_ouvrage",
        "temperature": "code_station"
    }

    expected_column = station_code_mapping.get(station_type, "code_station")

    # Vérifier que la colonne existe
    if expected_column not in columns:
        logger.warning(f"⚠️ Colonne attendue '{expected_column}' non trouvée. Colonnes disponibles: {columns}")
        # Essayer de trouver une colonne contenant 'code'
        code_columns = [col for col in columns if 'code' in col.lower()]
        if code_columns:
            logger.info(f"✅ Utilisation de la colonne: {code_columns[0]}")
            return code_columns[0]

    return expected_column


def _get_minio_path(station_type: str) -> str:
    """
    Construit le chemin MinIO pour un type de station donné.

    Args:
        station_type: Type de station ("piezometry", "hydrometry", etc.)

    Returns:
        Chemin dans MinIO (ex: "bronze/piezometry_api/piezometry_stations/")
    """
    # Mapping des types de stations vers les noms de datasets
    dataset_mapping = {
        "piezometry": ("piezometry_api", "piezometry_stations"),
        "hydrometry": ("hydrometry_api", "hydrometry_stations"),
        "quality_rivers": ("quality_rivers_api", "quality_rivers_stations"),
        "quality_groundwater": ("quality_groundwater_api", "quality_groundwater_stations"),
        "hydrobio": ("hydrobio_api", "hydrobio_stations"),
        "ecoulement": ("ecoulement_api", "ecoulement_stations"),
        "prelevements": ("prelevements_api", "prelevements_ouvrages"),
        "temperature": ("temperature_api", "temperature_stations")
    }

    dataset_name, resource_name = dataset_mapping.get(
        station_type,
        (f"{station_type}_api", f"{station_type}_stations")
    )

    return f"bronze/{dataset_name}/{resource_name}/"


def _create_s3_filesystem():
    """
    Crée un système de fichiers S3 pour accéder à MinIO.

    Returns:
        pyarrow.fs.S3FileSystem configuré pour MinIO
    """
    # Configuration MinIO depuis les variables d'environnement
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    minio_user = os.getenv("MINIO_USER", "admin")
    minio_pass = os.getenv("MINIO_PASS", "BrgmMinio2024!")

    # Nettoyer l'endpoint (enlever http://)
    endpoint = minio_endpoint.replace("http://", "").replace("https://", "")

    # Créer le système de fichiers S3
    s3 = pafs.S3FileSystem(
        access_key=minio_user,
        secret_key=minio_pass,
        endpoint_override=endpoint,
        scheme="http"
    )

    return s3


def extract_station_codes_from_minio(station_type: str) -> List[str]:
    """
    Extrait les codes de stations depuis les fichiers parquet dans MinIO.

    Cette fonction lit les données de référence des stations depuis MinIO
    et extrait la liste complète des codes de stations disponibles.

    Args:
        station_type: Type de station ("piezometry", "hydrometry", etc.)

    Returns:
        Liste des codes de stations (ex: ["06595X0016/S", "06882X0213/F1", ...])
        Liste vide si aucune donnée n'est trouvée ou en cas d'erreur

    Example:
        >>> stations = extract_station_codes_from_minio("piezometry")
        >>> print(f"Trouvé {len(stations)} stations piezometry")
    """
    minio_path = _get_minio_path(station_type)

    try:
        # Créer le système de fichiers S3
        s3 = _create_s3_filesystem()

        # Lister les fichiers parquet
        try:
            files = s3.get_file_info(pafs.FileSelector(f"bronze/{minio_path}", recursive=True))
        except Exception:
            # Si le path n'existe pas, essayer sans le prefix "bronze/"
            files = s3.get_file_info(pafs.FileSelector(minio_path, recursive=True))

        parquet_files = [f.path for f in files if f.path.endswith('.parquet') and f.type == pafs.FileType.File]

        if not parquet_files:
            logger.warning(f"⚠️ Aucun fichier parquet trouvé dans {minio_path}")
            return []

        logger.info(f"📂 Trouvé {len(parquet_files)} fichiers parquet dans {minio_path}")

        # Lire tous les fichiers parquet et extraire les codes de stations
        all_stations = set()
        for file_path in parquet_files:
            try:
                table = pq.read_table(file_path, filesystem=s3)
                df = table.to_pandas()

                # Déterminer le nom de la colonne de code station
                station_code_column = _get_station_code_column(station_type, df.columns.tolist())

                if station_code_column in df.columns:
                    stations = df[station_code_column].dropna().unique().tolist()
                    all_stations.update(stations)
                    logger.debug(f"  ✅ Extrait {len(stations)} stations uniques de {file_path}")
                else:
                    logger.warning(f"  ⚠️ Colonne {station_code_column} non trouvée dans {file_path}")

            except Exception as e:
                logger.error(f"  ❌ Erreur lors de la lecture de {file_path}: {e}")
                continue

        logger.info(f"✅ Extrait {len(all_stations)} stations uniques depuis MinIO pour {station_type}")
        return sorted(list(all_stations))

    except Exception as e:
        logger.error(f"❌ Erreur lors de la lecture depuis MinIO pour {station_type}: {e}")
        logger.debug(f"   Path essayé: {minio_path}")
        return []


def filter_active_stations_for_period(stations: List[str], partition_date: str, station_type: str) -> List[str]:
    """
    Filtre les stations actives pour une période donnée.

    Cette fonction lit les métadonnées des stations depuis MinIO et filtre
    celles qui étaient actives pendant la période spécifiée (année).

    Le filtrage se base sur les champs date_debut et date_fin:
    - Une station est considérée active si: date_debut <= fin_annee ET (date_fin >= debut_annee OU date_fin is NULL)

    Args:
        stations: Liste complète des codes de stations
        partition_date: Date de partition (format: "YYYY-MM-DD", ex: "2024-01-01")
        station_type: Type de station ("piezometry", "hydrometry", etc.)

    Returns:
        Liste des stations actives pendant la période
        Retourne toutes les stations en entrée si:
        - Aucune donnée n'est trouvée dans MinIO
        - Les champs de dates ne sont pas disponibles
        - Une erreur se produit

    Example:
        >>> all_stations = ["06595X0016/S", "06882X0213/F1", ...]
        >>> active_2024 = filter_active_stations_for_period(all_stations, "2024-01-01", "piezometry")
        >>> print(f"{len(active_2024)}/{len(all_stations)} stations actives en 2024")
    """
    # Parse la partition_date pour extraire l'année
    try:
        year = datetime.strptime(partition_date, "%Y-%m-%d").year
    except ValueError:
        logger.error(f"❌ Format de date invalide: {partition_date}. Attendu: YYYY-MM-DD")
        return stations

    minio_path = _get_minio_path(station_type)

    try:
        # Créer le système de fichiers S3
        s3 = _create_s3_filesystem()

        # Lister les fichiers parquet
        try:
            files = s3.get_file_info(pafs.FileSelector(f"bronze/{minio_path}", recursive=True))
        except Exception:
            files = s3.get_file_info(pafs.FileSelector(minio_path, recursive=True))

        parquet_files = [f.path for f in files if f.path.endswith('.parquet') and f.type == pafs.FileType.File]

        if not parquet_files:
            logger.warning(f"⚠️ Aucun fichier parquet trouvé dans {minio_path}, retour de toutes les stations")
            return stations

        logger.info(f"📂 Filtrage des stations actives pour l'année {year} depuis {len(parquet_files)} fichiers")

        # Lire et filtrer
        active_stations = set()
        has_date_fields = False

        for file_path in parquet_files:
            try:
                table = pq.read_table(file_path, filesystem=s3)
                df = table.to_pandas()

                station_code_column = _get_station_code_column(station_type, df.columns.tolist())

                # Vérifier la présence de champs de dates
                # Possibilités: date_debut/date_fin, date_ouverture/date_fermeture, etc.
                date_start_col = None
                date_end_col = None

                for start_name in ["date_debut", "date_ouverture", "date_debut_mesure"]:
                    if start_name in df.columns:
                        date_start_col = start_name
                        break

                for end_name in ["date_fin", "date_fermeture", "date_fin_mesure"]:
                    if end_name in df.columns:
                        date_end_col = end_name
                        break

                if date_start_col and date_end_col:
                    has_date_fields = True

                    # Convertir en datetime si nécessaire
                    if df[date_start_col].dtype == 'object':
                        df[date_start_col] = pd.to_datetime(df[date_start_col], errors='coerce')
                    if df[date_end_col].dtype == 'object':
                        df[date_end_col] = pd.to_datetime(df[date_end_col], errors='coerce')

                    # Définir les bornes de l'année
                    year_start = pd.Timestamp(year=year, month=1, day=1)
                    year_end = pd.Timestamp(year=year, month=12, day=31)

                    # Filtrer les stations actives pendant l'année
                    # Station active si: date_debut <= year_end ET (date_fin >= year_start OU date_fin is NULL)
                    active_df = df[
                        (df[date_start_col] <= year_end) &
                        ((df[date_end_col].isna()) | (df[date_end_col] >= year_start))
                    ]

                    if station_code_column in active_df.columns:
                        stations_in_period = active_df[station_code_column].dropna().unique().tolist()
                        active_stations.update(stations_in_period)
                        logger.debug(f"  ✅ Trouvé {len(stations_in_period)} stations actives dans {file_path}")
                else:
                    # Pas de champs de dates, ajouter toutes les stations du fichier
                    if station_code_column in df.columns:
                        all_file_stations = df[station_code_column].dropna().unique().tolist()
                        active_stations.update(all_file_stations)

            except Exception as e:
                logger.error(f"  ❌ Erreur lors du filtrage de {file_path}: {e}")
                continue

        if not has_date_fields:
            logger.warning(f"⚠️ Pas de champs date_debut/date_fin pour {station_type}, retour de toutes les stations")
            return stations

        # Filtrer pour ne garder que les stations qui étaient dans la liste d'entrée
        filtered_stations = [s for s in stations if s in active_stations]

        logger.info(f"✅ Filtré {len(filtered_stations)}/{len(stations)} stations actives pour l'année {year}")
        return filtered_stations

    except Exception as e:
        logger.error(f"❌ Erreur lors du filtrage des stations: {e}")
        logger.debug(f"   Retour de toutes les stations ({len(stations)} stations)")
        return stations
