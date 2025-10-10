from typing import Any, Dict, List, Optional

import time
import io

import dlt
from dagster import AssetExecutionContext, asset, DailyPartitionsDefinition, StaticPartitionsDefinition
from dlt.common.typing import TSecretValue
import pyarrow.parquet as pq
import pyarrow.fs as pafs
import pandas as pd

from pipelines.dlt.hubeau_generic import run_pipeline

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

    # ✅ STRATÉGIE OPTIMISÉE AVEC FALLBACK AUTOMATIQUE:
    # 1. Récupérer TOUTES les stations depuis MinIO (référentiel complet)
    all_stations = _extract_station_codes_from_minio(station_type)
    context.log.info(f"📂 {len(all_stations)} stations total dans référentiel MinIO")

    stations_data: Dict[str, List[str]] = {}

    if all_stations:
        # 2. Filtrer pour ne garder que les stations actives dans la partition
        filtered_stations = _filter_active_stations_for_period(all_stations, partition_date, station_type)
        context.log.info(f"✅ {len(filtered_stations)} stations actives pour partition {partition_date}")

        # 3. Convertir en dict avec tous les mois de l'année
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

def extract_station_codes_from_result(result: Dict[str, Any], station_type: str = "temperature", partition_date: str = None) -> Dict[str, List[str]]:
    """
    Extrait les codes de stations actives avec leurs mois de données depuis l'API Hub'Eau.

    Args:
        result: Résultat de l'asset upstream (peut être vide {})
        station_type: Type de stations ("temperature", "hydrometry", "piezometry", etc.)
        partition_date: Date de partition pour filtrer les stations actives (REQUIS)

    Returns:
        Dict[str, List[str]]: Dictionnaire {code_station: [liste des mois "YYYY-MM" avec données]}
    """
    import httpx
    from datetime import datetime, date
    import calendar
    
    if not partition_date:
        print("⚠️ Pas de partition_date fournie, récupération depuis MinIO (liste potentiellement incomplète)")
        # Convertir la liste en dict avec tous les mois de l'année courante
        stations_list = _extract_station_codes_from_minio(station_type)
        year = datetime.now().year
        all_months = [f"{year}-{m:02d}" for m in range(1, 13)]
        return {station: all_months for station in stations_list}
    
    # ✅ SOLUTION: Récupérer directement les stations actives depuis l'API Hub'Eau
    print(f"🔍 Récupération des stations actives depuis l'API Hub'Eau pour {station_type} (partition: {partition_date})")
    
    try:
        # ✅ CORRECTION: Utiliser la même logique que DLT pour la période
        # DLT utilise month_windows() qui génère des fenêtres mensuelles exactes
        year = datetime.strptime(partition_date, "%Y-%m-%d").year
        
        # ✅ NOUVELLE APPROCHE: Tester chaque mois comme le fait DLT
        # Au lieu d'une seule requête sur toute l'année, faire une requête par mois
        print(f"🔍 Test des stations actives pour l'année {year}")

        # Configuration des endpoints par type
        # ✅ STRATÉGIE: Utiliser l'endpoint de STATIONS si filtrage temporel supporté
        # ❌ SINON: Utiliser l'endpoint de DONNÉES avec approche hybride (mois + département si limite 20K)
        api_configs = {
            "temperature": {
                "base_url": "https://hubeau.eaufrance.fr/api/v1/temperature",
                "path": "/station",  # ✅ Endpoint stations supporte filtrage temporel
                "start_param": "date_debut_mesure",
                "end_param": "date_fin_mesure",
                "station_field": "code_station"
            },
            "hydrometry": {
                "base_url": "https://hubeau.eaufrance.fr/api/v2/hydrometrie",
                "path": "/obs_elab",  # ✅ Observations élaborées (historique complet)
                "start_param": "date_debut_obs_elab",
                "end_param": "date_fin_obs_elab",
                "station_field": "code_station"
            },
            "piezometry": {
                "base_url": "https://hubeau.eaufrance.fr/api/v1/niveaux_nappes",
                "path": "/chroniques",  # ❌ /stations ne supporte pas filtrage, utiliser données
                "start_param": "date_debut_mesure",
                "end_param": "date_fin_mesure",
                "station_field": "code_bss"
            },
            "quality_rivers": {
                "base_url": "https://hubeau.eaufrance.fr/api/v2/qualite_rivieres",
                "path": "/station_pc",  # ✅ Endpoint stations supporte filtrage temporel (4431 vs 24324)
                "start_param": "date_debut_prelevement",
                "end_param": "date_fin_prelevement",
                "station_field": "code_station"
            },
            "quality_groundwater": {
                "base_url": "https://hubeau.eaufrance.fr/api/v1/qualite_nappes",
                "path": "/analyses",  # ❌ /stations ne supporte pas filtrage, utiliser données
                "start_param": "date_debut_prelevement",
                "end_param": "date_fin_prelevement",
                "station_field": "code_bss"
            },
            "ecoulement": {
                "base_url": "https://hubeau.eaufrance.fr/api/v1/ecoulement",
                "path": "/observations",  # ❌ /stations ne supporte pas filtrage, utiliser données
                "start_param": "date_observation_min",  # ✅ Corrigé selon API
                "end_param": "date_observation_max",    # ✅ Corrigé selon API
                "station_field": "code_station"
            },
            "hydrobio": {
                "base_url": "https://hubeau.eaufrance.fr/api/v1/hydrobio",
                "path": "/indices",  # ❌ Pas d'endpoint stations avec filtrage, utiliser données
                "start_param": "date_debut_prelevement",  # ✅ Corrigé selon API
                "end_param": "date_fin_prelevement",      # ✅ Corrigé selon API
                "station_field": "code_station_hydrobio"
            },
            "prelevements": {
                "base_url": "https://hubeau.eaufrance.fr/api/v1/prelevements",
                "path": "/chroniques",  # ❌ Pas d'endpoint stations avec filtrage, utiliser données
                "start_param": "annee",  # ⚠️ Chroniques: filtrage par année uniquement (pas de dates précises)
                "end_param": "annee",    # ⚠️ Utiliser la même année pour start/end
                "station_field": "code_ouvrage"
            }
        }
        
        if station_type not in api_configs:
            print(f"⚠️ Type de station non supporté: {station_type}, fallback sur MinIO")
            return _extract_station_codes_from_minio(station_type)
        
        config = api_configs[station_type]
        
        # ✅ NOUVELLE APPROCHE: Tester chaque mois comme le fait DLT
        # Utiliser month_windows() pour générer les mêmes fenêtres que DLT
        def month_windows(start: date, end: date):
            """Génère les mêmes fenêtres mensuelles que DLT"""
            cursor = date(start.year, start.month, 1)
            final = date(end.year, end.month, calendar.monthrange(end.year, end.month)[1])
            while cursor <= final:
                last_day = calendar.monthrange(cursor.year, cursor.month)[1]
                period_end = date(cursor.year, cursor.month, last_day)
                yield cursor, min(period_end, final)
                if cursor.month == 12:
                    cursor = date(cursor.year + 1, 1, 1)
                else:
                    cursor = date(cursor.year, cursor.month + 1, 1)
        
        # Générer les fenêtres mensuelles pour l'année
        start_date_obj = date(year, 1, 1)
        end_date_obj = date(year, 12, 31)

        # ✅ NOUVELLE STRUCTURE: Dict[station_code, List[month_str]]
        stations_months: Dict[str, set] = {}  # Utiliser set temporairement pour éviter doublons
        total_requests = 0

        # ✅ NOUVELLE APPROCHE: Toujours utiliser découpage mensuel pour tracker les mois actifs
        print(f"🔍 Interrogation API par mois pour {station_type} ({year})...")

        station_field = config["station_field"]

        # Découpage mensuel pour tous les types : permet de tracker les mois actifs par station
        for month_start, month_end in month_windows(start_date_obj, end_date_obj):
            month_str = month_start.strftime('%Y-%m')
            page = 1
            month_stations = set()

            # ✅ PAGINATION: Continue jusqu'à ce qu'on ait toutes les stations du mois
            while True:
                params = {
                    "format": "json",
                    "size": 20000,
                    "page": page,
                    config["start_param"]: month_start.isoformat(),
                    config["end_param"]: month_end.isoformat()
                }

                try:
                    response = httpx.get(f"{config['base_url']}{config['path']}", params=params, timeout=60)
                    total_requests += 1

                    if response.status_code not in [200, 206]:
                        break

                    data = response.json()
                    records = data.get("data", [])

                    if len(records) == 0:
                        break

                    # Extraire les stations du mois
                    for record in records:
                        if station_field in record:
                            station_code = record[station_field]
                            month_stations.add(station_code)

                    # Si moins de 20K records, c'est la dernière page
                    if len(records) < 20000:
                        break

                    page += 1

                except Exception as e:
                    break

            # Ajouter ce mois à chaque station trouvée
            for station_code in month_stations:
                if station_code not in stations_months:
                    stations_months[station_code] = set()
                stations_months[station_code].add(month_str)

            if len(month_stations) > 0:
                print(f"  Mois {month_str}: {len(month_stations)} stations ({page} page(s))")

        # Convertir les sets en listes triées
        stations_months_dict = {
            station: sorted(list(months))
            for station, months in stations_months.items()
        }

        total_stations = len(stations_months_dict)
        total_station_months = sum(len(months) for months in stations_months_dict.values())
        print(f"✅ {total_stations} stations actives trouvées pour {station_type} (année {year})")
        print(f"📊 Total station-mois: {total_station_months} ({total_requests} requêtes)")

        return stations_months_dict
        
    except Exception as e:
        print(f"⚠️ Erreur lors de la récupération des stations depuis l'API: {e}")
        import traceback
        traceback.print_exc()
        print("⚠️ Fallback sur la liste MinIO (potentiellement incomplète)")
        # Convertir la liste en dict avec tous les mois de l'année
        stations_list = _extract_station_codes_from_minio(station_type)
        year = datetime.strptime(partition_date, "%Y-%m-%d").year if partition_date else datetime.now().year
        all_months = [f"{year}-{m:02d}" for m in range(1, 13)]
        return {station: all_months for station in stations_list}


def get_active_departments_for_stations(stations_data: Dict[str, List[str]], station_type: str) -> list[str]:
    """
    Extrait les départements ayant des stations actives.

    Args:
        stations_data: Dict des stations actives {code_station: [mois]}
        station_type: Type de stations

    Returns:
        Liste unique et triée des départements ayant des stations
    """
    import httpx

    if not stations_data:
        return []

    # Convertir le dict en liste de codes stations
    station_codes = list(stations_data.keys())

    # Configuration des endpoints pour récupérer les métadonnées des stations
    api_configs = {
        "temperature": {
            "url": "https://hubeau.eaufrance.fr/api/v1/temperature/station",
            "station_field": "code_station",
            "dept_field": "code_departement"
        },
        "hydrometry": {
            "url": "https://hubeau.eaufrance.fr/api/v2/hydrometrie/referentiel/stations",
            "station_field": "code_station",
            "dept_field": "code_departement"
        },
        "piezometry": {
            "url": "https://hubeau.eaufrance.fr/api/v1/niveaux_nappes/stations",
            "station_field": "code_bss",
            "dept_field": "code_departement"
        },
        "quality_rivers": {
            "url": "https://hubeau.eaufrance.fr/api/v2/qualite_rivieres/station_pc",
            "station_field": "code_station",
            "dept_field": "code_departement"
        },
        "quality_groundwater": {
            "url": "https://hubeau.eaufrance.fr/api/v1/qualite_nappes/stations",
            "station_field": "code_bss",
            "dept_field": "code_departement"
        },
        "hydrobio": {
            "url": "https://hubeau.eaufrance.fr/api/v1/hydrobio/stations_hydrobio",
            "station_field": "code_station_hydrobio",
            "dept_field": "code_departement"
        },
        "ecoulement": {
            "url": "https://hubeau.eaufrance.fr/api/v1/ecoulement/stations",
            "station_field": "code_station",
            "dept_field": "code_departement"
        },
        "prelevements": {
            "url": "https://hubeau.eaufrance.fr/api/v1/prelevements/referentiel/ouvrages",  # ✅ CORRECTION: Utiliser ouvrages (chroniques utilisent code_ouvrage)
            "station_field": "code_ouvrage",  # ✅ CORRECTION: code_ouvrage pour les chroniques
            "dept_field": "code_departement"
        }
    }

    if station_type not in api_configs:
        print(f"⚠️ Type de station non supporté pour filtrage départements: {station_type}")
        return []

    config = api_configs[station_type]
    departments = set()

    try:
        # Stratégie: Récupérer en une seule fois toutes les stations
        # (plus efficace que requête par station)
        print(f"🔍 Récupération départements pour {len(station_codes)} stations {station_type}")

        # Construire requête avec toutes les stations (si API supporte multi-stations)
        # Sinon, faire par batches
        station_field = config["station_field"]
        dept_field = config["dept_field"]

        # Stratégie batch : récupérer toutes les stations en plusieurs requêtes
        batch_size = 100  # Limiter par sécurité
        for i in range(0, len(station_codes), batch_size):
            batch = station_codes[i:i + batch_size]

            # Requête pour ce batch
            params = {
                "format": "json",
                "size": 1000,
                station_field: ",".join(batch)
            }

            try:
                response = httpx.get(config["url"], params=params, timeout=30)
                if response.status_code == 200:
                    data = response.json()
                    for record in data.get("data", []):
                        dept = record.get(dept_field)
                        if dept:
                            departments.add(dept)
            except Exception as e:
                print(f"⚠️ Erreur lors de la requête batch {i//batch_size + 1}: {e}")
                continue

        departments_list = sorted(list(departments))
        print(f"✅ {len(departments_list)} départements avec stations actives trouvés")
        return departments_list

    except Exception as e:
        print(f"⚠️ Erreur lors du filtrage départements: {e}")
        import traceback
        traceback.print_exc()
        return []


def _extract_station_codes_from_minio(station_type: str = "temperature") -> list[str]:
    """
    Extrait les codes de stations depuis MinIO (liste potentiellement incomplète).
    Cette fonction est un fallback si l'API Hub'Eau n'est pas disponible.
    """
    import boto3
    import json
    import gzip
    import os
    
    # Configuration MinIO depuis les variables d'environnement
    minio_endpoint = os.getenv("AWS_ENDPOINT_URL", "http://minio:9000")
    minio_user = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
    minio_pass = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
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
            print(f"⚠️ No files found in {prefix}, trying alternative paths...")
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
                print("⚠️ Broad scan of bucket to locate station files (may be slow)...")
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

        print(f"📂 Reading {station_type} stations from Parquet: {file_key}")

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
                print(f"⚠️ Colonne {key_field} non trouvée dans le Parquet. Colonnes disponibles: {df.columns.tolist()}")
                stations = []

        except Exception as e:
            print(f"⚠️ Erreur lors de la lecture du fichier Parquet: {e}")
            import traceback
            traceback.print_exc()
            stations = []

        print(f"✅ Extracted {len(stations)} {station_type} station codes from MinIO Parquet")
        return stations
        
    except Exception as e:
        print(f"⚠️ Erreur lors de la lecture des {station_type} stations depuis MinIO: {e}")
        import traceback
        traceback.print_exc()
        return []


def _filter_active_stations_for_period(stations: list[str], partition_date: str, station_type: str) -> list[str]:
    """
    Filtre les stations pour ne garder que celles qui ont des données pour la période donnée.
    
    Args:
        stations: Liste des codes de stations
        partition_date: Date de partition (ex: "2024-01-01")
        station_type: Type de stations ("temperature", etc.)
    """
    import httpx
    from datetime import datetime, timedelta
    
    try:
        # Construire la période (année complète)
        year = datetime.strptime(partition_date, "%Y-%m-%d").year
        start_date = f"{year}-01-01"
        end_date = f"{year}-12-31"
        
        print(f"🔍 Test des stations pour la période {start_date} à {end_date}")
        
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
                "path": "/chroniques",
                "start_param": "date_debut_mesure",
                "end_param": "date_fin_mesure",
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
            print(f"⚠️ Type de station non supporté pour le filtrage: {station_type}")
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
                config["start_param"]: start_date,
                config["end_param"]: end_date
            }
            
            response = httpx.get(f"{config['base_url']}{config['path']}", params=params, timeout=30)
            
            if response.status_code not in [200, 206]:
                print(f"⚠️ Erreur API lors du filtrage (page {page}): {response.status_code}")
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
            
            print(f"🔍 Page {page}: {len(records)} records, {len(active_stations)} stations uniques trouvées")

            # ✅ Continuer tant que l'API fournit un lien next
            next_link = data.get("next")
            if next_link is None:
                print(f"✅ Dernière page atteinte (next=None)")
                break

            page += 1
        
        # Filtrer la liste originale pour ne garder que les stations actives
        filtered_stations = [station for station in stations if station in active_stations]
        
        print(f"📊 Résultat du filtrage: {len(filtered_stations)} stations actives sur {len(stations)} total")
        print(f"📊 Stations actives trouvées dans l'API: {len(active_stations)}")
        
        return filtered_stations
        
    except Exception as e:
        print(f"⚠️ Erreur lors du filtrage des stations: {e}")
        return stations  # En cas d'erreur, retourner toutes les stations

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
    full_path = os.path.join("/app", config_path)
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
            file_format=cfg.get("file_format", "parquet"),
            layout=cfg.get("layout", "{table_name}/{load_id}.{file_id}.parquet"),
            state_fs_options={
                "aws_access_key_id": TSecretValue(minio_user),
                "aws_secret_access_key": TSecretValue(minio_pass),
                "endpoint_url": minio_endpoint,
                "region_name": minio_region,
            },
            dagster_log=None,  # Use monkey-patched print instead
            stations_data=stations_data,
            partition_date=partition_date
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
