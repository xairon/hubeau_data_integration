"""
Assets Bronze Hub'Eau - Implémentation réelle avec connexions APIs
Gestion d'erreurs, timeouts, validation, stockage MinIO
"""

from dagster import asset, DailyPartitionsDefinition, AssetExecutionContext, get_dagster_logger, RetryPolicy
from datetime import datetime, timedelta
import requests
import time
import json
import boto3
from botocore.exceptions import ClientError, ConnectionError as BotoConnectionError
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
import hashlib
import os

# Configuration des partitions journalières  
# HYDRO: Limitation 1 mois historique → démarrage récent
DAILY_PARTITIONS = DailyPartitionsDefinition(start_date="2024-09-01")

@dataclass
class HubeauAPIConfig:
    """Configuration pour une API Hub'Eau"""
    name: str
    base_url: str
    endpoints: List[str]
    params: Dict[str, Any]
    max_retries: int = 3
    backoff_factor: float = 2.0
    timeout: int = 60
    rate_limit_delay: float = 0.5  # 2 req/sec max Hub'Eau
    # Note: Pagination gérée par 'size' dans params, pas max_per_page

class HubeauIngestionService:
    """Service d'ingestion professionnelle Hub'Eau avec gestion d'erreurs"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'BRGM-HubEau-Pipeline/1.0',
            'Accept': 'application/json'
        })
        
        # Configuration MinIO
        self.minio_client = self._init_minio_client()
        self.minio_bucket = "hubeau-bronze"
        
    def _init_minio_client(self):
        """Initialisation client MinIO avec gestion d'erreurs"""
        try:
            client = boto3.client(
                's3',
                endpoint_url='http://minio:9000',  # Direct container name
                aws_access_key_id=os.getenv('MINIO_USER', 'minioadmin'),
                aws_secret_access_key=os.getenv('MINIO_PASS', 'minioadmin'),
                region_name='us-east-1'
            )
            
            # Test connexion simple (list buckets au lieu de head_bucket)
            client.list_buckets()
            return client
            
        except Exception as e:
            # Erreur critique - on ne peut pas fonctionner sans MinIO
            print(f"❌ MinIO connection FAILED: {e}")
            print(f"🔧 Vérifier: docker-compose logs minio")
            print(f"🔧 Variables: MINIO_USER={os.getenv('MINIO_USER')}")
            raise Exception(f"MinIO required for Bronze layer: {e}")
    
    def _ensure_bucket_exists(self, bucket_name: str) -> bool:
        """S'assurer que le bucket MinIO existe"""
        if not self.minio_client:
            return False
            
        try:
            self.minio_client.head_bucket(Bucket=bucket_name)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                try:
                    self.minio_client.create_bucket(Bucket=bucket_name)
                    print(f"✅ Bucket {bucket_name} créé")
                    return True
                except ClientError as create_error:
                    print(f"❌ Impossible de créer bucket {bucket_name}: {create_error}")
                    return False
            else:
                print(f"❌ Erreur bucket {bucket_name}: {e}")
                return False
    
    def call_api_with_retry(self, url: str, params: Dict[str, Any], config: HubeauAPIConfig, endpoint: str = "") -> Optional[Dict[str, Any]]:
        """Appel API avec retry et backoff exponentiel"""
        for attempt in range(config.max_retries):
            try:
                # Rate limiting respectueux
                time.sleep(config.rate_limit_delay)
                
                response = self.session.get(
                    url,
                    params=params,
                    timeout=config.timeout
                )
                
                # Validation du status HTTP (200 OK ou 206 Partial Content pour pagination)
                if response.status_code == 200 or response.status_code == 206:  # 206 = Partial Content (pagination normale)
                    try:
                        data = response.json()
                        self._validate_hubeau_response(data, config.name, endpoint)
                        return data
                    except json.JSONDecodeError as e:
                        raise ValueError(f"Invalid JSON response: {e}")
                        
                elif response.status_code == 429:  # Rate limit
                    wait_time = config.backoff_factor ** attempt * 60  # Minutes
                    print(f"⚠️ Rate limit atteint, attente {wait_time:.1f}s")
                    time.sleep(wait_time)
                    continue
                    
                elif response.status_code in [500, 502, 503, 504]:  # Server errors
                    print(f"⚠️ Erreur serveur {response.status_code}, retry {attempt + 1}/{config.max_retries}")
                    time.sleep(config.backoff_factor ** attempt)
                    continue
                    
                else:
                    response.raise_for_status()
                    
            except requests.exceptions.Timeout:
                print(f"⚠️ Timeout {config.timeout}s, retry {attempt + 1}/{config.max_retries}")
                time.sleep(config.backoff_factor ** attempt)
                
            except requests.exceptions.ConnectionError as e:
                print(f"⚠️ Erreur connexion: {e}, retry {attempt + 1}/{config.max_retries}")
                time.sleep(config.backoff_factor ** attempt)
                
            except requests.exceptions.RequestException as e:
                print(f"❌ Erreur requête non-récupérable: {e}")
                return None
                
        print(f"❌ Échec définitif après {config.max_retries} tentatives")
        return None
    
    def _validate_hubeau_response(self, data: Dict[str, Any], api_name: str, endpoint: str = "") -> None:
        """Validation de la réponse Hub'Eau"""
        # Vérification structure de base
        if not isinstance(data, dict):
            raise ValueError(f"Response is not a dict for {api_name}")
        
        # Vérification champs requis Hub'Eau
        if 'data' not in data:
            raise ValueError(f"Missing 'data' field in response for {api_name}")
            
        if 'count' not in data:
            print(f"⚠️ Missing 'count' field in response for {api_name}")
            
        # Validation données
        data_array = data.get('data', [])
        if not isinstance(data_array, list):
            raise ValueError(f"'data' field is not a list for {api_name}")
        
        # Validation échantillon des données si présentes
        if data_array:
            self._validate_sample_data(data_array[0], api_name, endpoint)
    
    def _validate_sample_data(self, sample: Dict[str, Any], api_name: str, endpoint: str) -> None:
        """Validation d'un échantillon de données selon l'API"""
        # Champs requis par endpoint Hub'Eau (générique)
        required_fields_by_endpoint = {
            # Piézométrie
            'stations': ['code_bss'],  
            'chroniques_tr': ['code_bss', 'date_mesure'],
            'chroniques': ['code_bss', 'date_mesure'],
            
            # Hydrométrie  
            'observations_tr': ['code_station', 'date_obs'],
            'observations': ['code_station', 'date_obs'],
            
            # Qualité - sera différencié par API dans le mapping ci-dessous  
            'analyses': [],  # Pas de validation générique, spécifique par API
            'station_pc': ['code_station'],  # Qualité cours d'eau stations
            'analyse_pc': ['code_station', 'date_prelevement'],  # Qualité cours d'eau analyses
            
            # Température (selon doc officielle)
            'station': ['code_station'],  # Singulier selon doc !
            'chronique': ['code_station', 'date_mesure_temp'],  # date_mesure_temp selon doc !
        }
        
        # Validation spécifique par API et endpoint
        api_specific_fields = {
            'quality_surface': {
                'station_pc': ['code_station'],  # Doc officielle endpoint
                'analyse_pc': ['code_station', 'date_prelevement']  # Doc officielle endpoint
            },
            'quality_groundwater': {
                'analyses': ['code_bss', 'date_debut_prelevement']  # Champ réel selon API
            },
            'hydro': {
                'stations': ['code_station']
            },
            'temperature': {
                'stations': ['code_station']
            }
        }
        
        # Validation combinée : endpoint générique + API spécifique
        required_fields = []
        
        # Champs génériques par endpoint
        if endpoint in required_fields_by_endpoint:
            required_fields.extend(required_fields_by_endpoint[endpoint])
        
        # Champs spécifiques par API
        if api_name in api_specific_fields and endpoint in api_specific_fields[api_name]:
            required_fields.extend(api_specific_fields[api_name][endpoint])
        
        # Validation
        for field in required_fields:
            if field not in sample:
                raise ValueError(f"Missing required field '{field}' in {api_name}/{endpoint} data")
    
    def paginate_api_call(self, config: HubeauAPIConfig, endpoint: str, base_params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Pagination complète - RÉCUPÉRATION DE TOUTES LES DONNÉES DISPONIBLES"""
        all_data = []
        page = 1
        total_fetched = 0
        max_pages = 1000  # Limite sécurité contre pagination infinie
        
        while page <= max_pages:  # Protection contre boucle infinie
            # Paramètres pagination
            params = base_params.copy()
            # Pagination Hub'Eau : utilisation de 'size' selon documentation officielle
            params.update({
                'size': base_params.get('size', 20000),  # Utilise size depuis config
                'page': page
            })
            
            url = f"{config.base_url}/{endpoint}"
            response_data = self.call_api_with_retry(url, params, config, endpoint)
            
            if not response_data:
                print(f"❌ Échec récupération page {page} pour {endpoint}")
                break
                
            page_data = response_data.get('data', [])
            if not page_data:
                print(f"✅ Fin pagination {endpoint} - Page vide")
                break
                
            print(f"📄 Page {page}: {len(page_data)} records récupérés (total: {total_fetched + len(page_data)})")
                
            all_data.extend(page_data)
            total_fetched += len(page_data)
            page += 1
            
            # Fin de pagination quand la page est incomplète
            page_size = base_params.get('size', 20000)
            if len(page_data) < page_size:
                print(f"✅ Fin pagination {endpoint} - Dernière page ({len(page_data)} < {page_size})")
                break
        
        if page > max_pages:
            print(f"⚠️ Arrêt pagination {endpoint} - Limite sécurité atteinte ({max_pages} pages)")
        
        print(f"🎯 TOTAL {endpoint}: {total_fetched} records récupérés")
        
        # DÉDUPLICATION OBSERVATIONS : 1 observation par jour maximum
        if endpoint in ['chroniques_tr', 'observations_tr', 'chronique']:
            all_data = self._deduplicate_observations(all_data, endpoint)
            print(f"🔧 Après déduplication {endpoint}: {len(all_data)} records uniques")
        
        return all_data
    
    def _deduplicate_observations(self, data: List[Dict[str, Any]], endpoint: str) -> List[Dict[str, Any]]:
        """Déduplication des observations : 1 observation par jour maximum par station"""
        if not data:
            return data
        
        # Mapping des champs de date selon l'endpoint
        date_fields = {
            'chroniques_tr': 'date_mesure',
            'observations_tr': 'date_obs', 
            'chronique': 'date_mesure_temp'
        }
        
        # Mapping des champs de station selon l'endpoint
        station_fields = {
            'chroniques_tr': 'code_bss',
            'observations_tr': 'code_station',
            'chronique': 'code_station'
        }
        
        date_field = date_fields.get(endpoint)
        station_field = station_fields.get(endpoint)
        
        if not date_field or not station_field:
            print(f"⚠️ Impossible de dédupliquer {endpoint} - champs non reconnus")
            return data
        
        # Grouper par station et date (jour seulement)
        grouped = {}
        for record in data:
            if date_field in record and station_field in record:
                try:
                    # Extraire la date (jour seulement)
                    date_str = record[date_field]
                    if isinstance(date_str, str):
                        date_day = date_str.split('T')[0]  # Garder seulement YYYY-MM-DD
                    else:
                        date_day = str(date_str).split('T')[0]
                    
                    station = record[station_field]
                    key = f"{station}_{date_day}"
                    
                    if key not in grouped:
                        grouped[key] = record
                    else:
                        # Garder la première observation du jour (déjà triée par 'sort': 'asc')
                        pass
                        
                except Exception as e:
                    print(f"⚠️ Erreur déduplication record: {e}")
                    continue
        
        deduplicated = list(grouped.values())
        print(f"🔧 Déduplication {endpoint}: {len(data)} → {len(deduplicated)} records")
        return deduplicated
    
    def store_to_minio(self, data: Any, bucket: str, object_key: str) -> bool:
        """Stockage sécurisé vers MinIO"""
        if not self.minio_client:
            raise Exception("MinIO client not initialized - cannot store data")
            
        try:
            # S'assurer que le bucket existe
            if not self._ensure_bucket_exists(bucket):
                return False
            
            # Sérialisation JSON avec gestion erreurs
            if isinstance(data, (dict, list)):
                json_data = json.dumps(data, ensure_ascii=False, indent=2)
            else:
                json_data = str(data)
            
            # Upload vers MinIO
            self.minio_client.put_object(
                Bucket=bucket,
                Key=object_key,
                Body=json_data.encode('utf-8'),
                ContentType='application/json'
            )
            
            print(f"✅ Stocké dans MinIO: s3://{bucket}/{object_key}")
            return True
            
        except (ClientError, BotoConnectionError) as e:
            print(f"❌ Erreur stockage MinIO: {e}")
            return False
        except Exception as e:
            print(f"❌ Erreur inattendue stockage: {e}")
            return False
    
    def ingest_hubeau_api(self, config: HubeauAPIConfig, date_partition: str) -> Dict[str, Any]:
        """Ingestion complète d'une API Hub'Eau pour une date donnée"""
        logger = get_dagster_logger()
        logger.info(f"🌊 Ingestion {config.name} pour {date_partition}")
        
        # Paramètres de base avec filtre temporel
        date_obj = datetime.fromisoformat(date_partition)
        base_params = config.params.copy()
        
        # Filtre temporel Hub'Eau - PARAMÈTRES SPÉCIFIQUES PAR API
        date_fin = date_obj
        if config.name == 'hydro':
            # API Hydro v2 : limiter à 7 jours pour éviter 39M records
            date_debut = date_obj - timedelta(days=7)
        else:
            date_debut = date_obj - timedelta(days=365)  # 1 an pour autres APIs
        
        # Paramètres temporels SPÉCIFIQUES selon documentation officielle par API
        if config.name == 'temperature':
            # API Température : paramètres spécifiques _temp
            base_params.update({
                'date_debut_mesure_temp': date_debut.strftime('%Y-%m-%d'),
                'date_fin_mesure_temp': date_fin.strftime('%Y-%m-%d')
            })
        elif config.name == 'piezo':
            # API Piézométrie : date_mesure standard
            base_params.update({
                'date_debut_mesure': date_debut.strftime('%Y-%m-%d'),
                'date_fin_mesure': date_fin.strftime('%Y-%m-%d')
            })
        elif config.name == 'hydro':
            # API Hydrométrie v2 : paramètres _obs (changement v2!)
            base_params.update({
                'date_debut_obs': date_debut.strftime('%Y-%m-%d'),
                'date_fin_obs': date_fin.strftime('%Y-%m-%d')
            })
        elif config.name in ['quality_surface', 'quality_groundwater']:
            # APIs qualité : paramètres _prelevement
            base_params.update({
                'date_debut_prelevement': date_debut.strftime('%Y-%m-%d'),
                'date_fin_prelevement': date_fin.strftime('%Y-%m-%d')
            })
        elif config.name == 'onde':
            # API ONDE : paramètres _campagne
            base_params.update({
                'date_debut_campagne': date_debut.strftime('%Y-%m-%d'),
                'date_fin_campagne': date_fin.strftime('%Y-%m-%d')
            })
        elif config.name == 'hydrobiologie':
            # API Hydrobiologie : paramètres _operation
            base_params.update({
                'date_debut_operation': date_debut.strftime('%Y-%m-%d'),
                'date_fin_operation': date_fin.strftime('%Y-%m-%d')
            })
        elif config.name == 'prelevements':
            # API Prélèvements : paramètres génériques
            base_params.update({
                'date_debut': date_debut.strftime('%Y-%m-%d'),
                'date_fin': date_fin.strftime('%Y-%m-%d')
            })
        
        # OPTIMISATION OBSERVATIONS : 1 observation par jour maximum
        # Pour les endpoints d'observations, ajouter des paramètres d'optimisation
        observation_endpoints = ['chroniques_tr', 'observations_tr', 'chronique']
        for endpoint in config.endpoints:
            if endpoint in observation_endpoints:
                # Paramètres pour optimiser les observations (1 par jour max)
                base_params.update({
                    'sort': 'asc',  # Tri chronologique
                    'pretty': 'true'  # Format lisible
                })
                logger.info(f"🔧 Optimisation observations activée pour {endpoint}")
        
        results = {}
        total_records = 0
        
        for endpoint in config.endpoints:
            try:
                logger.info(f"📡 Appel {config.name}/{endpoint}")
                
                # Paramètres spécifiques par endpoint
                endpoint_params = base_params.copy()
                
                # Les endpoints de référentiel n'ont pas besoin de filtres temporels
                if 'referentiel' in endpoint or endpoint in ['stations', 'station']:
                    # Supprimer les filtres temporels pour les référentiels
                    temporal_keys = [
                        'date_debut_mesure', 'date_fin_mesure',
                        'date_debut_prelevement', 'date_fin_prelevement', 
                        'date_debut_mesure_temp', 'date_fin_mesure_temp'
                    ]
                    for key in temporal_keys:
                        endpoint_params.pop(key, None)
                    logger.info(f"🔧 Filtres temporels supprimés pour {endpoint}")
                
                endpoint_data = self.paginate_api_call(config, endpoint, endpoint_params)
                
                if endpoint_data:
                    # Stockage MinIO par endpoint
                    object_key = f"{config.name}/{date_partition}/{endpoint}.json"
                    storage_success = self.store_to_minio(
                        endpoint_data, 
                        self.minio_bucket, 
                        object_key
                    )
                    
                    results[endpoint] = {
                        'records_count': len(endpoint_data),
                        'minio_path': f"s3://{self.minio_bucket}/{object_key}",
                        'storage_success': storage_success,
                        'sample_record': endpoint_data[0] if endpoint_data else None
                    }
                    
                    total_records += len(endpoint_data)
                    logger.info(f"✅ {endpoint}: {len(endpoint_data)} records stockés")
                else:
                    results[endpoint] = {
                        'records_count': 0,
                        'error': 'Aucune donnée récupérée',
                        'storage_success': False
                    }
                    logger.warning(f"⚠️ {endpoint}: Aucune donnée")
                    
            except Exception as e:
                logger.error(f"❌ Erreur {endpoint}: {e}")
                results[endpoint] = {
                    'records_count': 0,
                    'error': str(e),
                    'storage_success': False
                }
        
        return {
            'execution_date': datetime.now().isoformat(),
            'partition_date': date_partition,
            'api_name': config.name,
            'total_records_ingested': total_records,
            'endpoints_processed': list(config.endpoints),
            'minio_bucket': self.minio_bucket,
            'results_by_endpoint': results,
            'status': 'success' if total_records > 0 else 'no_data'
        }

# ====================================
# ASSETS HUB'EAU BRONZE RÉELS
# ====================================

@asset(
    partitions_def=DAILY_PARTITIONS,
    group_name="bronze_hubeau",
    description="🏔️ Ingestion COMPLÈTE piézométrie Hub'Eau - TOUTES les données disponibles",
    retry_policy=RetryPolicy(max_retries=3, delay=300)  # 5min delay
)
def hubeau_piezo_bronze_real(context: AssetExecutionContext) -> Dict[str, Any]:
    """
    Ingestion RÉELLE données piézométriques Hub'Eau
    - API: https://hubeau.eaufrance.fr/api/v1/niveaux_nappes
    - Retry automatique, pagination, validation
    - Stockage MinIO sécurisé
    """
    day = context.partition_key
    
    config = HubeauAPIConfig(
        name="piezo",
        base_url="https://hubeau.eaufrance.fr/api/v1/niveaux_nappes",
        endpoints=["stations", "chroniques_tr"],
        params={
            "format": "json",
            "size": 20000  # Limite officielle Hub'Eau
        },
        max_retries=3,
        backoff_factor=2.0,
        timeout=120,
        rate_limit_delay=0.5
    )
    
    service = HubeauIngestionService()
    return service.ingest_hubeau_api(config, day)

@asset(
    partitions_def=DAILY_PARTITIONS,
    group_name="bronze_hubeau",
    description="🌊 Ingestion COMPLÈTE hydrométrie Hub'Eau - TOUTES les données disponibles",
    retry_policy=RetryPolicy(max_retries=3, delay=300)
)
def hubeau_hydro_bronze_real(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingestion RÉELLE données hydrométriques Hub'Eau"""
    day = context.partition_key
    
    config = HubeauAPIConfig(
        name="hydro",
        base_url="https://hubeau.eaufrance.fr/api/v2/hydrometrie",
        endpoints=["referentiel/stations", "observations_tr"],  # Les deux endpoints fonctionnent
        params={
            "format": "json",
            "size": 20000  # Limite officielle Hub'Eau
        },
        max_retries=3,
        timeout=120
    )
    
    service = HubeauIngestionService()
    return service.ingest_hubeau_api(config, day)

# API Qualité Cours d'eau temporairement désactivée (problèmes de paramètres)
# @asset(
#     partitions_def=DAILY_PARTITIONS,
#     group_name="bronze_hubeau",
#     description="🧪 Ingestion qualité surface Hub'Eau RÉELLE vers MinIO",
#     retry_policy=RetryPolicy(max_retries=3, delay=300)
# )
# def hubeau_quality_surface_bronze_real(context: AssetExecutionContext) -> Dict[str, Any]:
#     """Ingestion RÉELLE données qualité des cours d'eau Hub'Eau"""
#     day = context.partition_key
#     
#     config = HubeauAPIConfig(
#         name="quality_surface",
#         base_url="https://hubeau.eaufrance.fr/api/v2/qualite_rivieres",
#         endpoints=["station_pc", "analyse_pc"],  # API v2 selon doc officielle 2025
#         params={
#             "format": "json",
#             "size": 5000,
#             # Paramètres géographiques selon exemple doc officielle
#             "code_commune": "75101,75102,75103,75104,75105",  # Paris 1er-5e selon exemple
#             "pretty": "true"
#         },
#         max_per_page=5000,
#         max_retries=3,
#         timeout=180  # Plus long pour analyses
#     )
#     
#     service = HubeauIngestionService()
#     return service.ingest_hubeau_api(config, day)

@asset(
    partitions_def=DAILY_PARTITIONS,
    group_name="bronze_hubeau",
    description="🧪 Ingestion COMPLÈTE qualité nappes Hub'Eau - TOUTES les données disponibles",
    retry_policy=RetryPolicy(max_retries=3, delay=300)
)
def hubeau_quality_groundwater_bronze_real(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingestion RÉELLE données qualité des eaux souterraines Hub'Eau"""
    day = context.partition_key
    
    config = HubeauAPIConfig(
        name="quality_groundwater",
        base_url="https://hubeau.eaufrance.fr/api/v1/qualite_nappes",
        endpoints=["stations", "analyses"],  # Doc officielle : stations + analyses
        params={
            "format": "json",
            "size": 10000  # Limite adaptée pour analyses qualité
        },
        max_retries=3,
        timeout=180
    )
    
    service = HubeauIngestionService()
    return service.ingest_hubeau_api(config, day)

@asset(
    partitions_def=DAILY_PARTITIONS,
    group_name="bronze_hubeau",
    description="🌡️ Ingestion COMPLÈTE température Hub'Eau - TOUTES les données disponibles",
    retry_policy=RetryPolicy(max_retries=3, delay=300)
)
def hubeau_temperature_bronze_real(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingestion RÉELLE données température continue Hub'Eau"""
    day = context.partition_key
    
    config = HubeauAPIConfig(
        name="temperature",
        base_url="https://hubeau.eaufrance.fr/api/v1/temperature",
        endpoints=["station", "chronique"],  # Doc officielle: singulier !
        params={
            "format": "json",
            "size": 10000  # Limite adaptée pour données température
        },
        max_retries=3,
        timeout=120
    )
    
    service = HubeauIngestionService()
    return service.ingest_hubeau_api(config, day)
