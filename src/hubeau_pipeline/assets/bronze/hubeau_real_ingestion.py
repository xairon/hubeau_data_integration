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
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field
import hashlib
import os

# Configuration des partitions journalières  
# HYDRO: Limitation 1 mois historique → démarrage récent
DAILY_PARTITIONS = DailyPartitionsDefinition(start_date="2024-09-01")

@dataclass
class DeduplicationConfig:
    """Paramètres de déduplication pour un endpoint Hub'Eau."""

    date_field: str
    group_keys: List[str]
    truncate_to_day: bool = True


@dataclass
class EndpointConfig:
    """Configuration spécifique à un endpoint Hub'Eau."""

    path: str
    params: Dict[str, Any] = field(default_factory=dict)
    apply_temporal_filter: bool = True
    temporal_param_keys: Optional[Tuple[str, str]] = None  # (start_key, end_key)
    lookback_days: Optional[int] = None
    temporal_format: str = "%Y-%m-%d"
    page_size: Optional[int] = None
    deduplication: Optional[DeduplicationConfig] = None


@dataclass
class HubeauAPIConfig:
    """Configuration pour une API Hub'Eau"""

    name: str
    base_url: str
    endpoints: Dict[str, EndpointConfig]
    base_params: Dict[str, Any] = field(default_factory=dict)
    max_retries: int = 3
    backoff_factor: float = 2.0
    timeout: int = 60
    rate_limit_delay: float = 0.5  # 2 req/sec max Hub'Eau
    default_lookback_days: int = 365

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
        endpoint_key = endpoint.split('/')[-1]
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
        if endpoint_key in required_fields_by_endpoint:
            required_fields.extend(required_fields_by_endpoint[endpoint_key])

        # Champs spécifiques par API
        if api_name in api_specific_fields and endpoint_key in api_specific_fields[api_name]:
            required_fields.extend(api_specific_fields[api_name][endpoint_key])
        
        # Validation
        for field in required_fields:
            if field not in sample:
                raise ValueError(f"Missing required field '{field}' in {api_name}/{endpoint} data")
    
    def paginate_api_call(
        self,
        config: HubeauAPIConfig,
        endpoint_name: str,
        endpoint_config: EndpointConfig,
        base_params: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
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
                'size': endpoint_config.page_size or base_params.get('size') or config.base_params.get('size', 200),
                'page': page
            })

            url = f"{config.base_url}/{endpoint_config.path}"
            response_data = self.call_api_with_retry(url, params, config, endpoint_config.path)
            
            if not response_data:
                print(f"❌ Échec récupération page {page} pour {endpoint_name}")
                break

            page_data = response_data.get('data', [])
            if not page_data:
                print(f"✅ Fin pagination {endpoint_name} - Page vide")
                break
                
            print(f"📄 Page {page}: {len(page_data)} records récupérés (total: {total_fetched + len(page_data)})")

            all_data.extend(page_data)
            total_fetched += len(page_data)
            page += 1

            # Fin de pagination quand la page est incomplète
            page_size = endpoint_config.page_size or base_params.get('size') or config.base_params.get('size', 200)
            if len(page_data) < page_size:
                print(f"✅ Fin pagination {endpoint_name} - Dernière page ({len(page_data)} < {page_size})")
                break

        if page > max_pages:
            print(f"⚠️ Arrêt pagination {endpoint_name} - Limite sécurité atteinte ({max_pages} pages)")

        print(f"🎯 TOTAL {endpoint_name}: {total_fetched} records récupérés")

        if endpoint_config.deduplication:
            all_data = self._deduplicate_records(all_data, endpoint_config.deduplication)
            print(f"🔧 Après déduplication {endpoint_name}: {len(all_data)} records uniques")

        return all_data

    def _deduplicate_records(self, data: List[Dict[str, Any]], config: DeduplicationConfig) -> List[Dict[str, Any]]:
        """Déduplication des observations : 1 observation par jour maximum par station"""
        if not data:
            return data

        date_field = config.date_field

        grouped = {}
        for record in data:
            if date_field in record and all(key in record for key in config.group_keys):
                try:
                    # Extraire la date (jour seulement)
                    date_str = record[date_field]
                    if isinstance(date_str, str):
                        date_value = date_str
                    else:
                        date_value = str(date_str)

                    if config.truncate_to_day:
                        date_value = date_value.split('T')[0]

                    key_parts = [record[key] for key in config.group_keys]
                    key_parts.append(date_value)
                    key = "::".join(map(str, key_parts))

                    if key not in grouped:
                        grouped[key] = record
                    else:
                        # Garder la première observation du jour (déjà triée par 'sort': 'asc')
                        pass
                        
                except Exception as e:
                    print(f"⚠️ Erreur déduplication record: {e}")
                    continue
        
        deduplicated = list(grouped.values())
        print(f"🔧 Déduplication {config.group_keys + [config.date_field]}: {len(data)} → {len(deduplicated)} records")
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
        results = {}
        total_records = 0

        for endpoint_name, endpoint_config in config.endpoints.items():
            try:
                logger.info(f"📡 Appel {config.name}/{endpoint_name}")

                endpoint_params = config.base_params.copy()
                endpoint_params.update(endpoint_config.params)

                if endpoint_config.apply_temporal_filter:
                    lookback = endpoint_config.lookback_days or config.default_lookback_days
                    date_debut = date_obj - timedelta(days=lookback)
                    date_fin = date_obj

                    if not endpoint_config.temporal_param_keys or len(endpoint_config.temporal_param_keys) != 2:
                        raise ValueError(
                            f"Endpoint {endpoint_name} doit définir temporal_param_keys pour appliquer un filtre temporel"
                        )

                    start_key, end_key = endpoint_config.temporal_param_keys
                    endpoint_params[start_key] = date_debut.strftime(endpoint_config.temporal_format)
                    endpoint_params[end_key] = date_fin.strftime(endpoint_config.temporal_format)

                else:
                    # S'assurer qu'aucun résidu de filtre temporel ne subsiste
                    if endpoint_config.temporal_param_keys:
                        start_key, end_key = endpoint_config.temporal_param_keys
                        endpoint_params.pop(start_key, None)
                        endpoint_params.pop(end_key, None)

                if endpoint_config.deduplication:
                    endpoint_params.setdefault('sort', 'asc')

                endpoint_data = self.paginate_api_call(
                    config,
                    endpoint_name,
                    endpoint_config,
                    endpoint_params,
                )

                if endpoint_data:
                    # Stockage MinIO par endpoint
                    object_key = f"{config.name}/{date_partition}/{endpoint_config.path}.json"
                    storage_success = self.store_to_minio(
                        endpoint_data,
                        self.minio_bucket,
                        object_key
                    )

                    results[endpoint_name] = {
                        'records_count': len(endpoint_data),
                        'minio_path': f"s3://{self.minio_bucket}/{object_key}",
                        'storage_success': storage_success,
                        'sample_record': endpoint_data[0] if endpoint_data else None
                    }

                    total_records += len(endpoint_data)
                    logger.info(f"✅ {endpoint_name}: {len(endpoint_data)} records stockés")
                else:
                    results[endpoint_name] = {
                        'records_count': 0,
                        'error': 'Aucune donnée récupérée',
                        'storage_success': False
                    }
                    logger.warning(f"⚠️ {endpoint_name}: Aucune donnée")
                    
            except Exception as e:
                logger.error(f"❌ Erreur {endpoint_name}: {e}")
                results[endpoint_name] = {
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
        endpoints={
            "stations": EndpointConfig(
                path="stations",
                apply_temporal_filter=False,
                page_size=5000,
            ),
            "chroniques_tr": EndpointConfig(
                path="chroniques_tr",
                temporal_param_keys=("date_debut_mesure", "date_fin_mesure"),
                lookback_days=30,
                deduplication=DeduplicationConfig(
                    date_field="date_mesure",
                    group_keys=["code_bss"],
                ),
            ),
            "chroniques": EndpointConfig(
                path="chroniques",
                temporal_param_keys=("date_debut_mesure", "date_fin_mesure"),
                lookback_days=365,
                deduplication=DeduplicationConfig(
                    date_field="date_mesure",
                    group_keys=["code_bss"],
                ),
            ),
        },
        base_params={
            "format": "json",
        },
        max_retries=3,
        backoff_factor=2.0,
        timeout=120,
        rate_limit_delay=0.5,
        default_lookback_days=365,
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
        endpoints={
            "referentiel/stations": EndpointConfig(
                path="referentiel/stations",
                apply_temporal_filter=False,
                page_size=5000,
            ),
            "observations_tr": EndpointConfig(
                path="observations_tr",
                temporal_param_keys=("date_debut_obs", "date_fin_obs"),
                lookback_days=7,
                deduplication=DeduplicationConfig(
                    date_field="date_obs",
                    group_keys=["code_station"],
                ),
            ),
            "observations": EndpointConfig(
                path="observations",
                temporal_param_keys=("date_debut_obs", "date_fin_obs"),
                lookback_days=365,
                deduplication=DeduplicationConfig(
                    date_field="date_obs",
                    group_keys=["code_station"],
                ),
            ),
        },
        base_params={
            "format": "json",
        },
        max_retries=3,
        timeout=120,
        default_lookback_days=30,
    )
    
    service = HubeauIngestionService()
    return service.ingest_hubeau_api(config, day)

@asset(
    partitions_def=DAILY_PARTITIONS,
    group_name="bronze_hubeau",
    description="🧪 Ingestion qualité surface Hub'Eau RÉELLE vers MinIO",
    retry_policy=RetryPolicy(max_retries=3, delay=300)
)
def hubeau_quality_surface_bronze_real(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingestion RÉELLE données qualité des cours d'eau Hub'Eau"""
    day = context.partition_key

    config = HubeauAPIConfig(
        name="quality_surface",
        base_url="https://hubeau.eaufrance.fr/api/v2/qualite_rivieres",
        endpoints={
            "station_pc": EndpointConfig(
                path="station_pc",
                apply_temporal_filter=False,
                page_size=500,
            ),
            "analyse_pc": EndpointConfig(
                path="analyse_pc",
                temporal_param_keys=("date_debut_prelevement", "date_fin_prelevement"),
                lookback_days=180,
                page_size=500,
                deduplication=DeduplicationConfig(
                    date_field="date_prelevement",
                    group_keys=["code_station", "code_parametre"],
                ),
            ),
        },
        base_params={
            "format": "json",
        },
        max_retries=3,
        timeout=180,
        default_lookback_days=180,
    )

    service = HubeauIngestionService()
    return service.ingest_hubeau_api(config, day)

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
        endpoints={
            "stations": EndpointConfig(
                path="stations",
                apply_temporal_filter=False,
                page_size=2000,
            ),
            "analyses": EndpointConfig(
                path="analyses",
                temporal_param_keys=("date_debut_prelevement", "date_fin_prelevement"),
                lookback_days=365,
                deduplication=DeduplicationConfig(
                    date_field="date_debut_prelevement",
                    group_keys=["code_bss", "code_parametre"],
                ),
            ),
        },
        base_params={
            "format": "json",
        },
        max_retries=3,
        timeout=180,
        default_lookback_days=365,
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
        endpoints={
            "station": EndpointConfig(
                path="station",
                apply_temporal_filter=False,
                page_size=2000,
            ),
            "chronique": EndpointConfig(
                path="chronique",
                temporal_param_keys=("date_debut_mesure_temp", "date_fin_mesure_temp"),
                lookback_days=365,
                deduplication=DeduplicationConfig(
                    date_field="date_mesure_temp",
                    group_keys=["code_station"],
                ),
            ),
        },
        base_params={
            "format": "json",
        },
        max_retries=3,
        timeout=120,
        default_lookback_days=365,
    )
    
    service = HubeauIngestionService()
    return service.ingest_hubeau_api(config, day)
