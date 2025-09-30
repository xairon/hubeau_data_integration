"""
Assets Bronze BDLISA - Implémentation réelle WFS avec gestion d'erreurs
Connexion WFS, parsing GML, validation, stockage MinIO
"""

from dagster import asset, AssetExecutionContext, get_dagster_logger, RetryPolicy
from datetime import datetime
import requests
import boto3
from botocore.exceptions import ClientError
from typing import Dict, List, Any, Optional
import xml.etree.ElementTree as ET
import json
import os
import time

class BDLISAIngestionService:
    """Service d'ingestion professionnelle BDLISA WFS"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'BRGM-BDLISA-Pipeline/1.0',
            'Accept': 'application/gml+xml,application/xml,text/xml'
        })
        
        # Configuration MinIO
        self.minio_client = self._init_minio_client()
        self.minio_bucket = "bdlisa-bronze"
        
        # Configuration WFS BDLISA
        self.wfs_base_url = "https://services.sandre.eaufrance.fr/geo/bdlisa"
        self.wfs_version = "2.0.0"
        
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
            raise Exception(f"MinIO required for BDLISA Bronze: {e}")
    
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
    
    def call_wfs_with_retry(self, params: Dict[str, Any], max_retries: int = 3) -> Optional[str]:
        """Appel WFS avec retry et gestion d'erreurs"""
        for attempt in range(max_retries):
            try:
                # Rate limiting respectueux pour serveurs Sandre
                time.sleep(1.0)
                
                response = self.session.get(
                    self.wfs_base_url,
                    params=params,
                    timeout=300  # WFS peut être lent
                )
                
                if response.status_code in [200, 206]:  # 206 pour pagination WFS
                    # Validation basique de la réponse XML/GML
                    content = response.text
                    if self._validate_wfs_response(content):
                        return content
                    else:
                        raise ValueError("Invalid WFS response format")
                        
                elif response.status_code in [500, 502, 503, 504]:
                    print(f"⚠️ Erreur serveur WFS {response.status_code}, retry {attempt + 1}/{max_retries}")
                    time.sleep(2 ** attempt)  # Backoff exponentiel
                    continue
                    
                else:
                    response.raise_for_status()
                    
            except requests.exceptions.Timeout:
                print(f"⚠️ Timeout WFS, retry {attempt + 1}/{max_retries}")
                time.sleep(2 ** attempt)
                
            except requests.exceptions.ConnectionError as e:
                print(f"⚠️ Erreur connexion WFS: {e}, retry {attempt + 1}/{max_retries}")
                time.sleep(2 ** attempt)
                
            except Exception as e:
                print(f"❌ Erreur WFS non-récupérable: {e}")
                return None
                
        print(f"❌ Échec WFS après {max_retries} tentatives")
        return None
    
    def _validate_wfs_response(self, content: str) -> bool:
        """Validation basique de la réponse WFS/GML"""
        try:
            # Vérification que c'est du XML valide
            root = ET.fromstring(content)
            
            # Vérification de la structure GML/WFS
            if root.tag.endswith('FeatureCollection') or 'FeatureCollection' in root.tag:
                return True
            elif root.tag.endswith('ExceptionReport') or 'Exception' in content:
                print(f"❌ WFS Exception: {content[:500]}...")
                return False
            else:
                # Accepter d'autres structures XML valides de BDLISA
                return True
                
        except ET.ParseError as e:
            print(f"❌ Invalid XML in WFS response: {e}")
            return False
        except Exception as e:
            print(f"❌ Error validating WFS response: {e}")
            return False
    
    def parse_gml_features(self, gml_content: str) -> Dict[str, Any]:
        """Parsing basique GML pour extraction métadonnées"""
        try:
            root = ET.fromstring(gml_content)
            
            # Comptage des features
            feature_count = 0
            feature_types = set()
            
            # Recherche de features dans différents namespaces
            for elem in root.iter():
                tag_name = elem.tag.split('}')[-1] if '}' in elem.tag else elem.tag
                
                if 'member' in tag_name.lower() or 'feature' in tag_name.lower():
                    feature_count += 1
                    
                if elem.attrib:
                    for attr_name in elem.attrib:
                        if 'type' in attr_name.lower():
                            feature_types.add(elem.attrib[attr_name])
            
            # Estimation taille de contenu
            content_size = len(gml_content)
            
            return {
                'feature_count_estimated': feature_count,
                'feature_types': list(feature_types),
                'content_size_bytes': content_size,
                'xml_valid': True,
                'parsed_successfully': True
            }
            
        except Exception as e:
            print(f"⚠️ Erreur parsing GML: {e}")
            return {
                'feature_count_estimated': 0,
                'feature_types': [],
                'content_size_bytes': len(gml_content),
                'xml_valid': False,
                'parsed_successfully': False,
                'error': str(e)
            }
    
    def store_to_minio(self, content: str, bucket: str, object_key: str) -> bool:
        """Stockage GML vers MinIO"""
        if not self.minio_client:
            raise Exception("MinIO client not initialized - cannot store BDLISA data")
            
        try:
            if not self._ensure_bucket_exists(bucket):
                return False
            
            self.minio_client.put_object(
                Bucket=bucket,
                Key=object_key,
                Body=content.encode('utf-8'),
                ContentType='application/gml+xml'
            )
            
            print(f"✅ GML stocké: s3://{bucket}/{object_key}")
            return True
            
        except Exception as e:
            print(f"❌ Erreur stockage GML: {e}")
            return False
    
    def ingest_bdlisa_dataset(self, dataset_config: Dict[str, str]) -> Dict[str, Any]:
        """Ingestion d'un dataset BDLISA via WFS"""
        logger = get_dagster_logger()
        logger.info(f"🗺️ Ingestion BDLISA: {dataset_config['description']}")
        
        # Paramètres WFS standardisés
        params = {
            'service': 'WFS',
            'version': self.wfs_version,
            'request': 'GetFeature',
            'typename': dataset_config['typename'],
            'outputFormat': 'application/gml+xml;version=3.2',
            'srsName': 'EPSG:4326',  # WGS84 pour compatibilité
            'maxFeatures': 10000  # Limite sécurité
        }
        
        # Appel WFS avec retry
        gml_content = self.call_wfs_with_retry(params)
        
        if not gml_content:
            return {
                'dataset_name': dataset_config['name'],
                'success': False,
                'error': 'Failed to retrieve WFS data',
                'features_count': 0
            }
        
        # Parsing GML
        parsing_result = self.parse_gml_features(gml_content)
        
        # Stockage MinIO
        object_key = f"wfs/{dataset_config['name']}.gml"
        storage_success = self.store_to_minio(gml_content, self.minio_bucket, object_key)
        
        return {
            'dataset_name': dataset_config['name'],
            'typename': dataset_config['typename'],
            'description': dataset_config['description'],
            'success': True,
            'features_count': parsing_result['feature_count_estimated'],
            'feature_types': parsing_result['feature_types'],
            'content_size_bytes': parsing_result['content_size_bytes'],
            'minio_path': f"s3://{self.minio_bucket}/{object_key}",
            'storage_success': storage_success,
            'parsing_success': parsing_result['parsed_successfully']
        }

# ====================================
# ASSET BDLISA BRONZE RÉEL
# ====================================

@asset(
    group_name="bronze_external_real",
    description="🗺️ Ingestion BDLISA WFS RÉELLE vers MinIO",
    retry_policy=RetryPolicy(max_retries=2, delay=600)  # 10min delay pour WFS
)
def bdlisa_geographic_bronze_real(context: AssetExecutionContext) -> Dict[str, Any]:
    """
    Ingestion RÉELLE données géographiques BDLISA via WFS
    - Source: https://services.sandre.eaufrance.fr/geo/bdlisa
    - Format: WFS 2.0 → GML → MinIO
    - Gestion erreurs réseau et validation XML
    """
    logger = get_dagster_logger()
    logger.info("🗺️ Démarrage ingestion BDLISA WFS réelle")
    
    # Configuration datasets BDLISA prioritaires
    datasets = [
        {
            "name": "masses_eau_souterraine",
            "typename": "BDLISA_MASSE_EAU_SOUTERRAINE",
            "description": "Masses d'eau souterraine"
        },
        {
            "name": "formations_aquiferes",
            "typename": "BDLISA_FORMATION_AQUIFERE", 
            "description": "Formations aquifères"
        },
        {
            "name": "formations_impermeables",
            "typename": "BDLISA_FORMATION_IMPERMEABLE",
            "description": "Formations imperméables"
        }
    ]
    
    service = BDLISAIngestionService()
    results = {}
    total_features = 0
    success_count = 0
    
    for dataset in datasets:
        try:
            result = service.ingest_bdlisa_dataset(dataset)
            results[dataset['name']] = result
            
            if result['success']:
                total_features += result['features_count']
                success_count += 1
                logger.info(f"✅ {dataset['name']}: {result['features_count']} features")
            else:
                logger.error(f"❌ {dataset['name']}: {result.get('error', 'Unknown error')}")
                
        except Exception as e:
            logger.error(f"❌ Exception {dataset['name']}: {e}")
            results[dataset['name']] = {
                'dataset_name': dataset['name'],
                'success': False,
                'error': str(e),
                'features_count': 0
            }
    
    return {
        'execution_date': datetime.now().isoformat(),
        'source': 'BDLISA WFS Real',
        'wfs_base_url': service.wfs_base_url,
        'datasets_processed': [d['name'] for d in datasets],
        'total_features_ingested': total_features,
        'successful_datasets': success_count,
        'failed_datasets': len(datasets) - success_count,
        'minio_bucket': service.minio_bucket,
        'results_by_dataset': results,
        'status': 'success' if success_count > 0 else 'failed'
    }
