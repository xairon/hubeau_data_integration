"""
Assets Bronze Sandre - Implémentation réelle APIs avec gestion d'erreurs
Connexion APIs Sandre, validation, stockage MinIO
"""

from dagster import asset, AssetExecutionContext, get_dagster_logger, RetryPolicy
from datetime import datetime
import requests
import boto3
from botocore.exceptions import ClientError
from typing import Dict, List, Any, Optional
import json
import os
import time

class SandreIngestionService:
    """Service d'ingestion professionnelle APIs Sandre"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'BRGM-Sandre-Pipeline/1.0',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        })
        
        # Configuration MinIO
        self.minio_client = self._init_minio_client()
        self.minio_bucket = os.getenv("MINIO_BRONZE_BUCKET", "bronze")  # Bucket unifié Bronze
        
        # Configuration APIs Sandre
        self.sandre_base_url = "https://api.sandre.eaufrance.fr"
        
    def _init_minio_client(self):
        """Initialisation client MinIO"""
        try:
            client = boto3.client(
                's3',
                endpoint_url='http://minio:9000',  # Direct container name
                aws_access_key_id=os.getenv('MINIO_USER', 'minioadmin'),
                aws_secret_access_key=os.getenv('MINIO_PASS', 'minioadmin'),
                region_name='us-east-1'
            )
            
            # Test connexion simple
            client.list_buckets()
            return client
        except Exception as e:
            print(f"❌ MinIO connection FAILED: {e}")
            raise Exception(f"MinIO required for Sandre Bronze: {e}")
    
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
                except ClientError:
                    return False
            return False
    
    def call_sandre_api_with_retry(self, endpoint: str, params: Dict[str, Any], max_retries: int = 3) -> Optional[Dict[str, Any]]:
        """Appel API Sandre avec retry et gestion d'erreurs"""
        url = f"{self.sandre_base_url}{endpoint}"
        
        for attempt in range(max_retries):
            try:
                # Rate limiting très respectueux pour Sandre
                time.sleep(1.5)
                
                response = self.session.get(
                    url,
                    params=params,
                    timeout=120
                )
                
                if response.status_code in [200, 206]:  # 206 pour pagination Sandre
                    try:
                        data = response.json()
                        self._validate_sandre_response(data, endpoint)
                        return data
                    except json.JSONDecodeError as e:
                        print(f"❌ Invalid JSON from Sandre: {e}")
                        return None
                        
                elif response.status_code == 429:  # Rate limit
                    wait_time = 2 ** attempt * 60  # Minutes
                    print(f"⚠️ Rate limit Sandre, attente {wait_time:.1f}s")
                    time.sleep(wait_time)
                    continue
                    
                elif response.status_code in [500, 502, 503, 504]:
                    print(f"⚠️ Erreur serveur Sandre {response.status_code}, retry {attempt + 1}/{max_retries}")
                    time.sleep(2 ** attempt * 30)  # Backoff plus long pour Sandre
                    continue
                    
                elif response.status_code == 404:
                    print(f"❌ Endpoint Sandre non trouvé: {url}")
                    return None
                    
                else:
                    response.raise_for_status()
                    
            except requests.exceptions.Timeout:
                print(f"⚠️ Timeout Sandre API, retry {attempt + 1}/{max_retries}")
                time.sleep(2 ** attempt * 30)
                
            except requests.exceptions.ConnectionError as e:
                print(f"⚠️ Erreur connexion Sandre: {e}, retry {attempt + 1}/{max_retries}")
                time.sleep(2 ** attempt * 30)
                
            except Exception as e:
                print(f"❌ Erreur Sandre non-récupérable: {e}")
                return None
                
        print(f"❌ Échec Sandre après {max_retries} tentatives")
        return None
    
    def _validate_sandre_response(self, data: Dict[str, Any], endpoint: str) -> None:
        """Validation de la réponse API Sandre"""
        if not isinstance(data, dict):
            raise ValueError(f"Sandre response is not a dict for {endpoint}")
        
        # Vérification structure Sandre (peut varier selon l'endpoint)
        if 'data' in data:
            # Format standard avec 'data'
            data_array = data['data']
            if not isinstance(data_array, list):
                raise ValueError(f"Sandre 'data' field is not a list for {endpoint}")
        elif isinstance(data, list):
            # Format direct liste
            data_array = data
        else:
            # Format objet direct, on accepte
            data_array = [data]
        
        # Validation échantillon si données présentes
        if data_array and len(data_array) > 0:
            self._validate_sandre_item(data_array[0], endpoint)
    
    def _validate_sandre_item(self, item: Dict[str, Any], endpoint: str) -> None:
        """Validation d'un item Sandre selon l'endpoint"""
        # Champs attendus selon l'endpoint Sandre
        expected_fields = {
            '/parametres/': ['code', 'libelle'],
            '/unites/': ['code', 'symbole', 'libelle'],
            '/methodes/': ['code', 'libelle'],
            '/supports/': ['code', 'libelle'],
            '/fractions/': ['code', 'libelle']
        }
        
        for endpoint_pattern, fields in expected_fields.items():
            if endpoint_pattern in endpoint:
                for field in fields:
                    if field not in item:
                        print(f"⚠️ Missing field '{field}' in Sandre {endpoint} data")
                        # Warning seulement, pas d'exception (APIs Sandre peuvent varier)
                break
    
    def store_to_minio(self, data: Any, bucket: str, object_key: str) -> bool:
        """Stockage JSON Sandre vers MinIO"""
        if not self.minio_client:
            raise Exception("MinIO client not initialized - cannot store Sandre data")
            
        try:
            if not self._ensure_bucket_exists(bucket):
                return False
            
            json_data = json.dumps(data, ensure_ascii=False, indent=2)
            
            self.minio_client.put_object(
                Bucket=bucket,
                Key=object_key,
                Body=json_data.encode('utf-8'),
                ContentType='application/json'
            )
            
            print(f"✅ JSON Sandre stocké: s3://{bucket}/{object_key}")
            return True
            
        except Exception as e:
            print(f"❌ Erreur stockage Sandre: {e}")
            return False
    
    def ingest_sandre_nomenclature(self, nomenclature_config: Dict[str, str]) -> Dict[str, Any]:
        """Ingestion d'une nomenclature Sandre"""
        logger = get_dagster_logger()
        logger.info(f"📚 Ingestion Sandre: {nomenclature_config['description']}")
        
        # Paramètres API Sandre
        params = {
            'format': 'json',
            'size': 10000,  # Limite pour éviter surcharge
            'fields': 'code,libelle,definition,unite,theme,famille'
        }
        
        # Appel API avec retry
        response_data = self.call_sandre_api_with_retry(nomenclature_config['endpoint'], params)
        
        if not response_data:
            return {
                'nomenclature_name': nomenclature_config['name'],
                'success': False,
                'error': 'Failed to retrieve Sandre API data',
                'codes_count': 0
            }
        
        # Extraction des codes selon le format de réponse
        if isinstance(response_data, dict) and 'data' in response_data:
            codes_data = response_data['data']
            total_available = response_data.get('count', len(codes_data))
        elif isinstance(response_data, list):
            codes_data = response_data
            total_available = len(codes_data)
        else:
            codes_data = [response_data]
            total_available = 1
        
        # Stockage MinIO
        object_key = f"nomenclatures/{nomenclature_config['name']}.json"
        storage_success = self.store_to_minio(codes_data, self.minio_bucket, object_key)
        
        return {
            'nomenclature_name': nomenclature_config['name'],
            'endpoint': nomenclature_config['endpoint'],
            'description': nomenclature_config['description'],
            'success': True,
            'codes_count': len(codes_data),
            'total_available': total_available,
            'minio_path': f"s3://{self.minio_bucket}/{object_key}",
            'storage_success': storage_success,
            'sample_code': codes_data[0] if codes_data else None
        }

# ====================================
# ASSET SANDRE BRONZE RÉEL
# ====================================

@asset(
    group_name="bronze_external_real",
    description="📚 Ingestion Sandre APIs RÉELLES vers MinIO",
    retry_policy=RetryPolicy(max_retries=2, delay=900)  # 15min delay pour APIs Sandre
)
def sandre_thesaurus_bronze_real(context: AssetExecutionContext) -> Dict[str, Any]:
    """
    Ingestion RÉELLE thésaurus Sandre via APIs officielles
    - Source: https://api.sandre.eaufrance.fr/
    - Nomenclatures: paramètres, unités, méthodes, supports, fractions
    - Gestion rate limits et validation données
    """
    logger = get_dagster_logger()
    logger.info("📚 Démarrage ingestion Sandre APIs réelles")
    
    # Configuration nomenclatures Sandre essentielles
    nomenclatures = [
        {
            "name": "parametres",
            "endpoint": "/parametres/v1/parametres",
            "description": "Paramètres physicochimiques"
        },
        {
            "name": "unites",
            "endpoint": "/unites/v1/unites",
            "description": "Unités de mesure"
        },
        {
            "name": "methodes",
            "endpoint": "/methodes/v1/methodes",
            "description": "Méthodes d'analyse"
        },
        {
            "name": "supports",
            "endpoint": "/supports/v1/supports",
            "description": "Supports d'observation"
        },
        {
            "name": "fractions",
            "endpoint": "/fractions/v1/fractions",
            "description": "Fractions analysées"
        }
    ]
    
    service = SandreIngestionService()
    results = {}
    total_codes = 0
    success_count = 0
    
    for nomenclature in nomenclatures:
        try:
            result = service.ingest_sandre_nomenclature(nomenclature)
            results[nomenclature['name']] = result
            
            if result['success']:
                total_codes += result['codes_count']
                success_count += 1
                logger.info(f"✅ {nomenclature['name']}: {result['codes_count']} codes")
            else:
                logger.error(f"❌ {nomenclature['name']}: {result.get('error', 'Unknown error')}")
                
        except Exception as e:
            logger.error(f"❌ Exception {nomenclature['name']}: {e}")
            results[nomenclature['name']] = {
                'nomenclature_name': nomenclature['name'],
                'success': False,
                'error': str(e),
                'codes_count': 0
            }
    
    return {
        'execution_date': datetime.now().isoformat(),
        'source': 'Sandre APIs Real',
        'api_base_url': service.sandre_base_url,
        'nomenclatures_processed': [n['name'] for n in nomenclatures],
        'total_codes_ingested': total_codes,
        'successful_nomenclatures': success_count,
        'failed_nomenclatures': len(nomenclatures) - success_count,
        'minio_bucket': service.minio_bucket,
        'results_by_nomenclature': results,
        'status': 'success' if success_count > 0 else 'failed'
    }
