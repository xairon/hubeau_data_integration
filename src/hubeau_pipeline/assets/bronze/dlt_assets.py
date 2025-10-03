from typing import Any, Dict, Optional

import time

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
# UTILITAIRES POUR RÉDUIRE LA REDONDANCE
# ====================================

def _get_partition_date_yearly(context: AssetExecutionContext) -> str:
    """Convertit une partition annuelle (ex: '2024') en date (ex: '2024-01-01')."""
    partition_key = context.partition_key
    return f"{partition_key}-01-01"

def _get_partition_date_daily(context: AssetExecutionContext) -> str:
    """Retourne directement la partition quotidienne (ex: '2024-01-01')."""
    return context.partition_key

def _setup_observation_asset(context: AssetExecutionContext, station_type: str, partition_date: str) -> tuple[list[str], str]:
    """
    Configuration commune pour les assets d'observations.
    
    Returns:
        tuple: (stations_data, log_message)
    """
    stations_data = extract_station_codes_from_result({}, station_type, partition_date)
    log_message = f"📊 Using {len(stations_data)} {station_type} stations for observations"
    context.log.info(log_message)
    return stations_data, log_message

# ====================================
# Generic dlt Ingestion Asset
# ====================================

def extract_station_codes_from_result(result: Dict[str, Any], station_type: str = "temperature", partition_date: str = None) -> list[str]:
    """
    Extrait les codes de stations depuis le résultat d'un asset upstream.
    Cette fonction lit les données depuis MinIO et extrait les codes de stations.
    
    Args:
        result: Résultat de l'asset upstream
        station_type: Type de stations ("temperature", "hydrometry", "piezometry", etc.)
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
        
        # Déterminer le préfixe selon le type de stations
        station_prefixes = {
            "temperature": "hubeau/temperature_stations/",
            "hydrometry": "hubeau/hydrometry_stations/",
            "piezometry": "hubeau/piezometry_stations/",
            "quality_rivers": "hubeau/quality_rivers_stations/",
            "quality_groundwater": "hubeau/quality_groundwater_stations/",
            "ecoulement": "hubeau/ecoulement_stations/",
            "hydrobio": "hubeau/hydrobio_stations/",
            "prelevements": "hubeau/prelevements_stations/"
        }
        
        prefix = station_prefixes.get(station_type, f"hubeau/{station_type}_stations/")
        
        # Chercher les fichiers de stations les plus récents
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=prefix
        )
        
        if 'Contents' not in response:
            print(f"⚠️ No files found in {prefix}, trying alternative paths...")
            # Essayer d'autres chemins possibles
            for alt_prefix in [f"{station_type}_stations/", f"bronze/{station_type}_stations/"]:
                response = s3_client.list_objects_v2(
                    Bucket=bucket_name,
                    Prefix=alt_prefix
                )
                if 'Contents' in response:
                    break
        
        if 'Contents' not in response:
            raise ValueError(f"No {station_type} stations data found in MinIO")
        
        # Filtrer pour ne prendre que les fichiers JSON (pas les dossiers)
        json_files = [f for f in response['Contents'] if f['Key'].endswith('.json')]
        
        if not json_files:
            raise ValueError(f"No JSON files found in {station_type}_stations folder")
        
        # Prendre le fichier le plus récent
        latest_file = max(json_files, key=lambda x: x['LastModified'])
        file_key = latest_file['Key']
        
        print(f"📂 Reading {station_type} stations from: {file_key}")
        
        # Lire le fichier JSON (DLT stocke les données compressées)
        obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        
        # Essayer de décompresser d'abord
        try:
            compressed_data = obj['Body'].read()
            decompressed_data = gzip.decompress(compressed_data)
            content = decompressed_data.decode('utf-8')
        except (gzip.BadGzipFile, UnicodeDecodeError):
            # Si ce n'est pas compressé, lire directement
            content = obj['Body'].read().decode('utf-8')
        
        # DLT stocke les données en JSONL (une ligne par record)
        stations = []
        
        # Déterminer le champ de clé selon le type de stations
        station_key_fields = {
            "temperature": "code_station",
            "hydrometry": "code_station", 
            "piezometry": "code_bss",
            "quality_rivers": "code_station",
            "quality_groundwater": "code_bss",
            "ecoulement": "code_station",
            "hydrobio": "code_station_hydrobio",
            "prelevements": "code_point_prelevement"
        }
        
        key_field = station_key_fields.get(station_type, "code_station")
        
        # Parser comme JSONL (format dlt par défaut)
        for line in content.strip().split('\n'):
            if line.strip():
                try:
                    record = json.loads(line)
                    if key_field in record:
                        # Filtrer seulement les stations encore actives
                        # (avec une date de fin de service récente ou nulle)
                        date_fin_service = record.get('date_fin_service')
                        if not date_fin_service or date_fin_service >= "2024-01-01":
                            stations.append(record[key_field])
                except json.JSONDecodeError:
                    pass
        
        # Si JSONL n'a pas fonctionné, essayer comme JSON normal
        if not stations:
            try:
                data = json.loads(content)
                if isinstance(data, list):
                    # Format direct : liste de stations
                    stations = [station.get(key_field) for station in data if isinstance(station, dict) and key_field in station]
                elif isinstance(data, dict) and 'data' in data:
                    # Format API Hub'Eau : {"data": [...]}
                    stations = [station.get(key_field) for station in data['data'] if station.get(key_field)]
            except json.JSONDecodeError:
                pass
        
        print(f"✅ Extracted {len(stations)} {station_type} station codes from MinIO")
        
        # Si une partition_date est fournie, filtrer les stations qui ont des données pour cette période
        if partition_date and station_type in ["temperature", "hydrometry", "piezometry", "quality_rivers", "quality_groundwater", "ecoulement", "hydrobio", "prelevements"]:
            print(f"🔍 Filtrage des stations pour la période {partition_date}")
            active_stations = _filter_active_stations_for_period(stations, partition_date, station_type)
            print(f"✅ Stations actives pour {partition_date}: {len(active_stations)} sur {len(stations)}")
            return active_stations
        
        return stations
        
    except Exception as e:
        # En cas d'erreur, retourner une liste vide pour éviter de bloquer le pipeline
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
                "path": "/chronique",
                "start_param": "date_debut_mesure",
                "end_param": "date_fin_mesure",
                "station_field": "code_station"
            },
            "hydrometry": {
                "base_url": "https://hubeau.eaufrance.fr/api/v1/hydrometrie",
                "path": "/observations_tr",
                "start_param": "date_debut_obs",
                "end_param": "date_fin_obs",
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
                "base_url": "https://hubeau.eaufrance.fr/api/v1/qualite_rivieres",
                "path": "/analyses",
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
                "start_param": "date_debut_prelevement",
                "end_param": "date_fin_prelevement",
                "station_field": "code_point_prelevement"
            }
        }
        
        if station_type not in api_configs:
            print(f"⚠️ Type de station non supporté pour le filtrage: {station_type}")
            return stations
        
        config = api_configs[station_type]
        
        # Faire une requête test pour identifier les stations actives
        params = {
            "format": "json",
            "size": 1000,  # Récupérer un échantillon
            config["start_param"]: start_date,
            config["end_param"]: end_date
        }
        
        response = httpx.get(f"{config['base_url']}{config['path']}", params=params, timeout=30)
        
        if response.status_code not in [200, 206]:
            print(f"⚠️ Erreur API lors du filtrage: {response.status_code}")
            return stations
        
        data = response.json()
        records = data.get("data", [])
        
        # Extraire les stations qui ont des données
        active_stations = set()
        station_field = config["station_field"]
        for record in records:
            if station_field in record:
                active_stations.add(record[station_field])
        
        # Filtrer la liste originale pour ne garder que les stations actives
        filtered_stations = [station for station in stations if station in active_stations]
        
        print(f"📊 Résultat du filtrage: {len(filtered_stations)} stations actives sur {len(stations)} total")
        
        return filtered_stations
        
    except Exception as e:
        print(f"⚠️ Erreur lors du filtrage des stations: {e}")
        return stations  # En cas d'erreur, retourner toutes les stations

def ingest_dlt(context: AssetExecutionContext, config_path: str, stations_data: Optional[list[str]] = None, partition_date: Optional[str] = None) -> Dict[str, Any]:
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

@asset(group_name="hubeau_hydrobio")
def hydrobio_stations_reference(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests hydrobiology stations reference data using dlt (pas de partition)."""
    return ingest_dlt(context, "configs/hubeau/hydrobio_stations.yml")

@asset(group_name="hubeau_prelevements")
def prelevements_stations_reference(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests prelevements stations reference data using dlt (pas de partition)."""
    return ingest_dlt(context, "configs/hubeau/prelevements_stations.yml")

@asset(group_name="hubeau_temperature")
def temperature_stations_reference(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests temperature stations reference data using dlt (pas de partition)."""
    return ingest_dlt(context, "configs/hubeau/temperature_stations.yml")

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

@asset(group_name="hubeau_hydrometry", partitions_def=DAILY_PARTITIONS, deps=[hydrometry_stations_reference])
def hydrometry_observations(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests hydrometry observations data using dlt (30 derniers jours)."""
    partition_date = _get_partition_date_daily(context)
    stations_data, _ = _setup_observation_asset(context, "hydrometry", partition_date)
    return ingest_dlt(context, "configs/hubeau/hydrometry_observations.yml", stations_data=stations_data)

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

@asset(group_name="hubeau_ecoulement", partitions_def=DAILY_PARTITIONS, deps=[ecoulement_stations_reference])
def ecoulement_observations(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests ecoulement observations data using dlt (30 derniers jours)."""
    partition_date = _get_partition_date_daily(context)
    stations_data, _ = _setup_observation_asset(context, "ecoulement", partition_date)
    return ingest_dlt(context, "configs/hubeau/ecoulement_observations.yml", stations_data=stations_data)

@asset(group_name="hubeau_prelevements", partitions_def=YEARLY_PARTITIONS, deps=[prelevements_stations_reference])
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
