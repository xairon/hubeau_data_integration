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
from concurrent.futures import ThreadPoolExecutor, as_completed

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
    max_page_size: Optional[int] = None  # Limite max selon l'API
    depth_limit: Optional[int] = None  # Profondeur max pour éviter troncature
    deduplication: Optional[DeduplicationConfig] = None
    spatial_filter_required: bool = False  # Certaines APIs v2 nécessitent un filtre spatial
    spatial_params: Dict[str, Any] = field(default_factory=dict)  # Filtres spatiaux par défaut
    spatial_dept_param: Optional[str] = None  # nom de la clé département pour cet endpoint
    pagination_mode: str = "page"  # "page" ou "cursor" pour v2
    supports_sort: bool = False  # True si l'endpoint supporte le paramètre 'sort'


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
    version: str = "v1"  # Version de l'API (v1, v2)
    requires_spatial_filter: bool = False  # API v2 qualité nécessite filtrage spatial
    default_spatial_params: Dict[str, Any] = field(default_factory=dict)  # Filtres spatiaux globaux

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
                    logger = get_dagster_logger()
                    logger.error(f"❌ Erreur HTTP {response.status_code} pour {url} avec params {params}")
                    logger.error(f"❌ Réponse: {response.text[:500]}")
                    response.raise_for_status()
                    
            except requests.exceptions.Timeout:
                logger = get_dagster_logger()
                logger.warning(f"⚠️ Timeout {config.timeout}s pour {url}, retry {attempt + 1}/{config.max_retries}")
                time.sleep(config.backoff_factor ** attempt)
                
            except requests.exceptions.ConnectionError as e:
                logger = get_dagster_logger()
                logger.warning(f"⚠️ Erreur connexion: {e}, retry {attempt + 1}/{config.max_retries}")
                time.sleep(config.backoff_factor ** attempt)
                
            except requests.exceptions.RequestException as e:
                logger = get_dagster_logger()
                logger.error(f"❌ Erreur requête non-récupérable pour {url}: {e}")
                return None
                
        logger = get_dagster_logger()
        logger.error(f"❌ Échec définitif après {config.max_retries} tentatives pour {url}")
        logger.error(f"❌ Derniers paramètres utilisés: {params}")
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
        """Validation d'un échantillon de données selon l'API (assouplie)"""
        logger = get_dagster_logger()
        endpoint_key = endpoint.split('/')[-1]

        # Validation spécifique par API et endpoint (plus précis et souple)
        api_specific_fields = {
            'piezo': {
                'stations': ['code_bss'],
                'chroniques_tr': ['code_bss', 'date_mesure'],
                'chroniques': ['code_bss', 'date_mesure'],
            },
            'hydro': {
                'stations': ['code_station'],
                'observations_tr': ['code_station', 'date_obs'],
            },
            'onde': {
                'stations': ['code_station'],
                'campagnes': [],  # Pas de clé obligatoire
                'observations': [], # Pas de clé obligatoire
            },
            'quality_surface': {
                'station_pc': ['code_station'],
                'analyse_pc': ['code_station', 'date_prelevement'],
            },
            'quality_groundwater': {
                'stations': [],  # hétérogène : ne pas bloquer
                'analyses': ['code_bss', 'date_debut_prelevement'],
            },
            'temperature': {
                'station': ['code_station'],
                'chronique': ['code_station', 'date_mesure'],
            },
        }
        
        required_fields = []
        if api_name in api_specific_fields and endpoint_key in api_specific_fields[api_name]:
            required_fields = api_specific_fields[api_name][endpoint_key]
        
        # Validation assouplie : log un warning au lieu de lever une exception
        for field in required_fields:
            if field not in sample:
                logger.warning(
                    f"Champ attendu manquant '{field}' dans {api_name}/{endpoint}; validation assouplie"
                )
                # On ne bloque pas l'exécution pour un champ manquant
                return
    
    def _fetch_cursor_paged(self, url: str, base_params: dict, config: HubeauAPIConfig, endpoint_name: str) -> list:
        """Pagination par cursor pour Hub'Eau v2 (ex: hydrométrie observations_tr)"""
        logger = get_dagster_logger()
        params = dict(base_params)
        all_data = []
        next_url = None
        iteration = 0
        max_iterations = 10000  # Sécurité contre boucle infinie
        
        while iteration < max_iterations:
            # Utiliser l'URL complète fournie par links.next ou l'URL initiale
            if next_url:
                response_data = self.call_api_with_retry(next_url, {}, config, endpoint_name)
            else:
                response_data = self.call_api_with_retry(url, params, config, endpoint_name)
                
            if not response_data:
                logger.warning(f"❌ Échec récupération cursor pour {endpoint_name}")
                break
            
            # v2 renvoie un objet avec clés 'data' et liens de pagination
            data = response_data.get("data", []) if isinstance(response_data, dict) else []
            if not data:
                logger.info(f"✅ Fin pagination cursor {endpoint_name} - Données vides")
                break
                
            all_data.extend(data)
            logger.debug(f"🔄 Cursor iteration {iteration + 1}: {len(data)} records (total: {len(all_data)})")
            
            # Récupérer l'URL complète du prochain cursor via links.next
            links = response_data.get("links", {}) if isinstance(response_data, dict) else {}
            next_url = links.get("next")  # URL complète fournie par l'API
            
            if not next_url:
                logger.info(f"✅ Fin pagination cursor {endpoint_name} - Pas de next URL")
                break
                
            iteration += 1
            # Rate limiting respectueux
            time.sleep(config.rate_limit_delay)
            
        if iteration >= max_iterations:
            logger.warning(f"⚠️ Arrêt pagination cursor {endpoint_name} - Limite sécurité atteinte")
            
        logger.info(f"🎯 TOTAL cursor {endpoint_name}: {len(all_data)} records récupérés")
        return all_data

    def paginate_page_call(
        self,
        config: HubeauAPIConfig,
        endpoint_name: str,
        endpoint_config: EndpointConfig,
        base_params: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """Pagination par page avec respect des limites par endpoint (pas de cap global)"""
        logger = get_dagster_logger()
        all_data = []
        page = 1
        total_fetched = 0
        max_pages = 10000  # Limite sécurité contre pagination infinie
        
        # Utiliser la taille de page recommandée par endpoint (SANS cap global)
        page_size = endpoint_config.page_size or endpoint_config.max_page_size or 1000
        max_page_size = endpoint_config.max_page_size or 20000
        
        # S'assurer que la taille ne dépasse pas le maximum autorisé par l'API
        effective_page_size = min(page_size, max_page_size)
        
        # Profondeur limite par endpoint (pas de cap global)
        depth_limit = getattr(endpoint_config, 'depth_limit', None)
        
        logger.info(f"📄 Pagination page pour {endpoint_name}: page_size={effective_page_size} (demandé: {page_size}, max: {max_page_size}), depth_limit={depth_limit}")

        # Log de contrôle : 1ère URL complète appelée
        first_url = f"{config.base_url}/{endpoint_config.path}"
        first_params = base_params.copy()
        first_params.update({'size': effective_page_size, 'page': 1})
        logger.info(f"🔍 URL complète première page: {first_url} avec params {first_params}")

        while page <= max_pages:
            # Paramètres pagination
            params = base_params.copy()
            params.update({
                'size': effective_page_size,
                'page': page
            })

            url = f"{config.base_url}/{endpoint_config.path}"
            response_data = self.call_api_with_retry(url, params, config, endpoint_config.path)
            
            if not response_data:
                logger.warning(f"❌ Échec récupération page {page} pour {endpoint_name}")
                break

            page_data = response_data.get('data', [])
            if not page_data:
                logger.info(f"✅ Fin pagination {endpoint_name} - Page vide")
                break
                
            # Log de contrôle : count de la page 1 et estimation profondeur
            if page == 1:
                page_count = response_data.get('count', 0)
                estimated_depth = page_count // effective_page_size if page_count > 0 else 0
                logger.info(f"🔍 Page 1 - count={page_count}, estimation profondeur={estimated_depth} pages")
                
            # Ajouter toutes les données (pas de limite globale)
            all_data.extend(page_data)
            total_fetched += len(page_data)
            
            logger.debug(f"📄 Page {page}: {len(page_data)} records récupérés (total: {total_fetched})")

            # Vérifier la profondeur limite par endpoint
            if depth_limit is not None and total_fetched >= depth_limit:
                logger.warning(f"⚠️ Profondeur atteinte ({depth_limit}) pour {endpoint_name}")
                break

            page += 1

            # Fin de pagination quand la page est incomplète
            if len(page_data) < effective_page_size:
                logger.info(f"✅ Fin pagination {endpoint_name} - Dernière page ({len(page_data)} < {effective_page_size})")
                break

        if page > max_pages:
            logger.warning(f"⚠️ Arrêt pagination {endpoint_name} - Limite sécurité atteinte ({max_pages} pages)")

        logger.info(f"🎯 TOTAL {endpoint_name}: {total_fetched} records récupérés")
        return all_data

    def paginate_api_call(
        self,
        config: HubeauAPIConfig,
        endpoint_name: str,
        endpoint_config: EndpointConfig,
        base_params: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """Pagination avec support cursor et page selon la configuration"""
        logger = get_dagster_logger()
        
        # Switch entre pagination cursor et page selon la configuration
        if getattr(endpoint_config, 'pagination_mode', 'page') == 'cursor':
            logger.info(f"🔄 Pagination cursor pour {endpoint_name}")
            url = f"{config.base_url}/{endpoint_config.path}"
            all_data = self._fetch_cursor_paged(url, base_params, config, endpoint_name)
        else:
            logger.info(f"📄 Pagination page pour {endpoint_name}")
            all_data = self.paginate_page_call(config, endpoint_name, endpoint_config, base_params)
        
        # Déduplication si configurée
        if endpoint_config.deduplication:
            all_data = self._deduplicate_records(all_data, endpoint_config.deduplication)
            logger.info(f"🔧 Après déduplication {endpoint_name}: {len(all_data)} records uniques")
        
        return all_data

    def _fetch_points_prelevement_with_health_check(
        self, 
        config: HubeauAPIConfig, 
        endpoint_name: str, 
        endpoint_config: EndpointConfig, 
        base_params: Dict[str, Any], 
        departments: List[str]
    ) -> List[Dict[str, Any]]:
        """Récupération spécialisée pour points_prelevement avec test de santé par département"""
        logger = get_dagster_logger()
        all_data = []
        
        logger.info(f"🚰 Récupération points_prelevement avec test de santé: {len(departments)} départements")
        
        for i, dept in enumerate(departments):
            logger.info(f"🌍 Département {i+1}/{len(departments)}: {dept}")
            
            # Test de santé avec size=1 pour vérifier que le département est pris en compte
            health_params = base_params.copy()
            health_params["code_departement"] = dept
            health_params["size"] = 1
            health_params["format"] = "json"
            
            url = f"{config.base_url}/{endpoint_config.path}"
            
            # Appel de contrôle
            health_response = self.call_api_with_retry(url, health_params, config, f"{endpoint_name}_health")
            
            if not health_response:
                logger.warning(f"⚠️ Échec test de santé pour département {dept}")
                continue
                
            health_count = health_response.get('count', 0)
            logger.info(f"🔍 Test de santé département {dept}: count={health_count}")
            
            if health_count == 0:
                logger.warning(f"⚠️ points_prelevement: 0 record pour département {dept}; passage au suivant")
                continue
            
            # Récupération complète avec pagination normale
            full_params = base_params.copy()
            full_params["code_departement"] = dept
            # Retirer size=1 pour utiliser la page_size configurée
            
            dept_data = self.paginate_api_call(config, endpoint_name, endpoint_config, full_params)
            all_data.extend(dept_data)
            
            logger.info(f"✅ Département {dept}: {len(dept_data)} records (total: {len(all_data)})")
        
        logger.info(f"🎯 TOTAL points_prelevement: {len(all_data)} records récupérés")
        return all_data

    def _fetch_chroniques_with_bss_codes(
        self, 
        config: HubeauAPIConfig, 
        endpoint_name: str, 
        endpoint_config: EndpointConfig, 
        base_params: Dict[str, Any], 
        departments: List[str]
    ) -> List[Dict[str, Any]]:
        """Récupération spécialisée pour chroniques avec codes BSS depuis stations"""
        logger = get_dagster_logger()
        all_data = []
        
        logger.info(f"🔍 Récupération chroniques avec codes BSS/ouvrage depuis références: {len(departments)} départements")
        
        # 1. Récupérer les codes BSS/ouvrage depuis l'endpoint approprié
        reference_endpoint = None
        for name, endpoint in config.endpoints.items():
            # Pour prélèvements, utiliser 'ouvrages' ; pour autres APIs, utiliser 'stations'
            if config.name == 'prelevements':
                if 'ouvrage' in name.lower() and not endpoint.apply_temporal_filter:
                    reference_endpoint = endpoint
                    break
            else:
                if 'station' in name.lower() and not endpoint.apply_temporal_filter:
                    reference_endpoint = endpoint
                    break
        
        if not reference_endpoint:
            logger.error(f"❌ Pas d'endpoint de référence trouvé pour récupérer les codes BSS/ouvrage")
            return []
        
        # 2. Récupérer les références par département pour extraire les codes BSS/ouvrage
        all_bss_codes = set()
        
        for dept in departments:
            logger.info(f"🌍 Récupération références département {dept}")
            
            # Récupérer les références du département
            reference_params = base_params.copy()
            reference_params["code_departement"] = dept
            reference_params["size"] = 1000  # Taille raisonnable pour références
            
            reference_url = f"{config.base_url}/{reference_endpoint.path}"
            reference_data = self.call_api_with_retry(reference_url, reference_params, config, f"{endpoint_name}_reference")
            
            if reference_data:
                references = reference_data.get('data', [])
                for reference in references:
                    # Extraire le code BSS/ouvrage selon l'API
                    bss_code = self._extract_bss_code(reference, config.name)
                    if bss_code:
                        all_bss_codes.add(bss_code)
                
                logger.info(f"✅ Département {dept}: {len(references)} références, {len([r for r in references if self._extract_bss_code(r, config.name)])} codes BSS/ouvrage")
        
        logger.info(f"🎯 Total codes BSS récupérés: {len(all_bss_codes)}")
        
        # 3. Récupérer les chroniques pour chaque code BSS
        if not all_bss_codes:
            logger.warning(f"⚠️ Aucun code BSS trouvé, impossible de récupérer les chroniques")
            return []
        
        # Grouper les codes BSS par lots pour éviter les URLs trop longues
        bss_chunks = list(all_bss_codes)
        chunk_size = 50  # Limite raisonnable pour éviter URLs trop longues
        bss_chunks = [bss_chunks[i:i + chunk_size] for i in range(0, len(bss_chunks), chunk_size)]
        
        logger.info(f"📊 Traitement {len(all_bss_codes)} codes BSS en {len(bss_chunks)} lots")
        
        for i, bss_chunk in enumerate(bss_chunks):
            logger.info(f"🔄 Lot {i+1}/{len(bss_chunks)}: {len(bss_chunk)} codes BSS")
            
            # Paramètres pour les chroniques avec codes BSS
            chroniques_params = base_params.copy()
            chroniques_params["code_bss"] = ",".join(bss_chunk)
            
            # Récupérer les chroniques pour ce lot de codes BSS
            chroniques_data = self.paginate_api_call(config, endpoint_name, endpoint_config, chroniques_params)
            all_data.extend(chroniques_data)
            
            logger.info(f"✅ Lot {i+1}: {len(chroniques_data)} chroniques récupérées (total: {len(all_data)})")
        
        logger.info(f"🎯 TOTAL chroniques avec codes BSS/ouvrage: {len(all_data)} records récupérés")
        return all_data

    def _extract_bss_code(self, station: Dict[str, Any], api_name: str) -> Optional[str]:
        """Extrait le code BSS d'une station selon l'API"""
        # Mapping des champs BSS par API
        bss_fields = {
            'piezo': ['code_bss'],
            'quality_groundwater': ['code_bss'],
            'prelevements': ['code_ouvrage'],  # Pour prélèvements, c'est code_ouvrage
        }
        
        fields_to_try = bss_fields.get(api_name, ['code_bss'])
        
        for field in fields_to_try:
            if field in station and station[field]:
                return str(station[field]).strip()
        
        return None

    def _fetch_with_spatial_chunking(
        self, 
        config: HubeauAPIConfig, 
        endpoint_name: str, 
        endpoint_config: EndpointConfig, 
        base_params: Dict[str, Any], 
        departments: List[str]
    ) -> List[Dict[str, Any]]:
        """Récupération avec découpage spatial pour éviter la limite 20k"""
        logger = get_dagster_logger()
        all_data = []
        HUB_EAU_MAX_RESULTS = 20000
        
        # Grouper les départements par petits lots pour éviter la limite
        # Ajuster la taille selon la profondeur limite
        depth_limit = endpoint_config.depth_limit or 20000
        if depth_limit <= 10000:
            chunk_size = 1  # 1 département pour les APIs avec limite 10k (Hydrobiologie)
        else:
            chunk_size = 5  # 5 départements pour les autres APIs
        dept_chunks = [departments[i:i + chunk_size] for i in range(0, len(departments), chunk_size)]
        
        logger.info(f"📊 Découpage spatial: {len(departments)} départements en {len(dept_chunks)} groupes")
        
        for i, dept_chunk in enumerate(dept_chunks):
            params = base_params.copy()
            departments_str = ",".join(dept_chunk)
            
            # Utiliser la clé département paramétrée par endpoint
            dept_key = (
                endpoint_config.spatial_dept_param
                or endpoint_config.spatial_params.get("dept_param")
                or config.default_spatial_params.get("dept_param")
                or "code_departement"
            )
            params[dept_key] = departments_str
            
            logger.info(f"🌍 Groupe {i+1}/{len(dept_chunks)}: départements {dept_chunk}")
            
            chunk_data = self.paginate_api_call(config, endpoint_name, endpoint_config, params)
            all_data.extend(chunk_data)
            
            logger.info(f"✅ Groupe {i+1}: {len(chunk_data)} records (total: {len(all_data)})")
        
        logger.info(f"🎯 TOTAL spatial: {len(all_data)} records récupérés")
        return all_data

    def _fetch_with_temporal_chunking(
        self, 
        config: HubeauAPIConfig, 
        endpoint_name: str, 
        endpoint_config: EndpointConfig, 
        base_params: Dict[str, Any], 
        date_obj: datetime
    ) -> List[Dict[str, Any]]:
        """Récupération avec découpage temporel adaptatif (bisection récursive) pour éviter la limite 20k"""
        logger = get_dagster_logger()
        all_data = []
        
        if not endpoint_config.apply_temporal_filter:
            # Pas de filtre temporel, récupération directe
            return self.paginate_api_call(config, endpoint_name, endpoint_config, base_params)
        
        # ✅ CORRIGÉ: Utiliser UNIQUEMENT la date de partition (pas de fenêtre glissante)
        # Pour les partitions quotidiennes, récupérer uniquement les données du jour spécifique
        date_debut = date_obj
        date_fin = date_obj
        
        logger.info(f"📅 Filtre temporel strict: partition {date_obj.strftime('%Y-%m-%d')} uniquement")
        
        # Utiliser la bisection récursive pour éviter les troncatures
        # Sauf pour les formats année où la subdivision temporelle n'a pas de sens
        if endpoint_config.temporal_format == "%Y":
            # Pour les données annuelles, pas de subdivision temporelle
            all_data = self.paginate_api_call(config, endpoint_name, endpoint_config, base_params)
        else:
            all_data = self._fetch_temporal_window_recursive(
                config, endpoint_name, endpoint_config, base_params,
                date_debut, date_fin, depth=0
            )
        
        logger.info(f"🎯 TOTAL temporel avec bisection: {len(all_data)} records récupérés")
        return all_data

    def _fetch_temporal_window_recursive(
        self,
        config: HubeauAPIConfig,
        endpoint_name: str,
        endpoint_config: EndpointConfig,
        base_params: Dict[str, Any],
        date_start: datetime,
        date_end: datetime,
        depth: int = 0
    ) -> List[Dict[str, Any]]:
        """Bisection récursive pour éviter les troncatures de profondeur"""
        logger = get_dagster_logger()
        max_depth = 10  # Limite pour éviter récursion infinie
        min_window_days = 1  # Granularité minimale : 1 jour
        
        # Préparer les paramètres temporels
        params = base_params.copy()
        start_key, end_key = endpoint_config.temporal_param_keys
        date_format = endpoint_config.temporal_format or "%Y-%m-%d"
        params[start_key] = date_start.strftime(date_format)
        params[end_key] = date_end.strftime(date_format)
        
        window_days = (date_end - date_start).days
        indent = "  " * depth
        logger.debug(f"{indent}🔍 Fenêtre temporelle [{params[start_key]} -> {params[end_key]}] ({window_days} jours, profondeur {depth})")
        
        # Récupérer les données pour cette fenêtre
        window_data = self.paginate_api_call(config, endpoint_name, endpoint_config, params)
        data_count = len(window_data)
        depth_limit = endpoint_config.depth_limit
        
        logger.info(f"{indent}📊 Fenêtre [{params[start_key]} -> {params[end_key]}]: {data_count} records")
        
        # Vérifier si on atteint la limite de profondeur ET si on peut encore subdiviser
        if (depth_limit is not None and 
            data_count >= depth_limit and 
            window_days > min_window_days and 
            depth < max_depth):
            
            logger.warning(f"{indent}⚠️ Limite de profondeur atteinte ({data_count} >= {depth_limit}), subdivision de la fenêtre")
            
            # Diviser la fenêtre en deux
            mid_date = date_start + timedelta(days=window_days // 2)
            
            logger.info(f"{indent}✂️ Division: [{date_start.strftime('%Y-%m-%d')} -> {mid_date.strftime('%Y-%m-%d')}] + [{mid_date.strftime('%Y-%m-%d')} -> {date_end.strftime('%Y-%m-%d')}]")
            
            # Récupération récursive des deux sous-fenêtres
            first_half = self._fetch_temporal_window_recursive(
                config, endpoint_name, endpoint_config, base_params,
                date_start, mid_date, depth + 1
            )
            
            second_half = self._fetch_temporal_window_recursive(
                config, endpoint_name, endpoint_config, base_params,
                mid_date, date_end, depth + 1
            )
            
            combined_data = first_half + second_half
            logger.info(f"{indent}🔄 Fusion: {len(first_half)} + {len(second_half)} = {len(combined_data)} records")
            return combined_data
        
        elif (depth_limit is not None and 
              data_count >= depth_limit and 
              window_days <= min_window_days):
            # Limite atteinte mais fenêtre trop petite pour subdivision
            logger.error(f"{indent}❌ ALERTE: Limite de profondeur atteinte ({data_count} >= {depth_limit}) sur fenêtre minimale de {window_days} jour(s)")
            logger.error(f"{indent}   Données possiblement tronquées pour [{params[start_key]} -> {params[end_key]}]")
        
        elif depth >= max_depth:
            logger.error(f"{indent}❌ ALERTE: Profondeur maximale de récursion atteinte ({depth})")
        
        return window_data

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
                        # Politique de remplacement : garder le plus récent
                        existing_date = grouped[key].get(date_field, '')
                        current_date = record.get(date_field, '')
                        
                        # Garder la plus récente des deux observations
                        if current_date > existing_date:
                            grouped[key] = record
                        
                except Exception as e:
                    print(f"⚠️ Erreur déduplication record: {e}")
                    continue
        
        deduplicated = list(grouped.values())
        print(f"🔧 Déduplication {config.group_keys + [config.date_field]}: {len(data)} → {len(deduplicated)} records")
        return deduplicated
    
    def _fetch_temperature_chronique_by_station(
        self,
        config: HubeauAPIConfig,
        endpoint_name: str,
        endpoint_config: EndpointConfig,
        base_params: Dict[str, Any],
        date_obj: datetime
    ) -> List[Dict[str, Any]]:
        """
        Récupération spécialisée température/chronique par station avec parallélisation
        et fallback en cas de données manquantes
        """
        logger = get_dagster_logger()
        
        # 1. Charger les stations depuis le cache référentiel
        station_data = self._load_cached_referentiel(config.name, "station")
        if not station_data:
            logger.warning("⚠️ Aucune station température trouvée dans le cache")
            return []
        
        station_codes = [s.get("code_station") for s in station_data if s.get("code_station")]
        logger.info(f"🌡️ Récupération température par station: {len(station_codes)} stations")
        
        # 2. Configuration des paramètres communs
        start_date = (date_obj - timedelta(days=1)).strftime("%Y-%m-%d")
        end_date = date_obj.strftime("%Y-%m-%d")
        
        common_params = {
            **base_params,
            "date_debut_mesure": start_date,
            "date_fin_mesure": end_date,
            "size": endpoint_config.page_size or 1000,
        }
        
        # 3. Fonction pour récupérer une station
        def fetch_station_data(station_code: str) -> List[Dict[str, Any]]:
            try:
                params = {**common_params, "code_station": station_code}
                url = f"{config.base_url}/{endpoint_config.path}"
                
                # Rate limiting respectueux
                time.sleep(0.25)  # 4 req/sec max
                
                response_data = self.call_api_with_retry(url, params, config, endpoint_name)
                if response_data and "data" in response_data:
                    return response_data["data"]
                return []
                
            except Exception as e:
                logger.debug(f"⚠️ Erreur station {station_code}: {e}")
                return []
        
        # 4. Récupération parallèle par paquets de stations
        MAX_WORKERS = 8
        RATE_LIMIT_PER_SEC = 4
        all_records = []
        
        # Découper en chunks pour éviter la surcharge
        chunk_size = 200
        station_chunks = [station_codes[i:i + chunk_size] for i in range(0, len(station_codes), chunk_size)]
        
        for chunk_idx, chunk in enumerate(station_chunks):
            logger.info(f"📡 Traitement chunk {chunk_idx + 1}/{len(station_chunks)}: {len(chunk)} stations")
            
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = {executor.submit(fetch_station_data, code): code for code in chunk}
                
                for future in as_completed(futures):
                    station_code = futures[future]
                    try:
                        records = future.result()
                        if records:
                            all_records.extend(records)
                            logger.debug(f"✅ Station {station_code}: {len(records)} records")
                    except Exception as e:
                        logger.debug(f"❌ Erreur station {station_code}: {e}")
            
            # Pause entre chunks pour respecter le rate limiting
            if chunk_idx < len(station_chunks) - 1:
                time.sleep(1)
        
        logger.info(f"🎯 TOTAL température chronique: {len(all_records)} records récupérés")
        
        # 5. Fallback si aucune donnée sur 24h
        if not all_records:
            logger.warning("⚠️ Aucune donnée température sur 24h, tentative fallback 7 jours")
            return self._fetch_temperature_fallback(config, endpoint_config, base_params, date_obj, station_codes)
        
        return all_records
    
    def _fetch_temperature_fallback(
        self,
        config: HubeauAPIConfig,
        endpoint_config: EndpointConfig,
        base_params: Dict[str, Any],
        date_obj: datetime,
        station_codes: List[str]
    ) -> List[Dict[str, Any]]:
        """
        Fallback température : récupération sur 7 jours si aucune donnée sur 24h
        """
        logger = get_dagster_logger()
        
        start_date = (date_obj - timedelta(days=7)).strftime("%Y-%m-%d")
        end_date = date_obj.strftime("%Y-%m-%d")
        
        logger.info(f"🔄 Fallback température: {start_date} → {end_date}")
        
        common_params = {
            **base_params,
            "date_debut_mesure": start_date,
            "date_fin_mesure": end_date,
            "size": endpoint_config.page_size or 1000,
        }
        
        # Récupération simplifiée (échantillon de stations pour éviter la surcharge)
        sample_stations = station_codes[:50]  # Limiter à 50 stations pour le fallback
        all_records = []
        
        for station_code in sample_stations:
            try:
                params = {**common_params, "code_station": station_code}
                url = f"{config.base_url}/{endpoint_config.path}"
                
                time.sleep(0.3)  # Rate limiting plus conservateur
                
                response_data = self.call_api_with_retry(url, params, config, "chronique")
                if response_data and "data" in response_data:
                    records = response_data["data"]
                    if records:
                        all_records.extend(records)
                        logger.debug(f"✅ Fallback station {station_code}: {len(records)} records")
                
            except Exception as e:
                logger.debug(f"⚠️ Erreur fallback station {station_code}: {e}")
                continue
        
        logger.info(f"🔄 Fallback température: {len(all_records)} records récupérés")
        return all_records
    
    def _load_cached_referentiel(self, api_name: str, endpoint_name: str) -> List[Dict[str, Any]]:
        """
        Charge les données référentiel depuis le cache MinIO
        """
        try:
            # Construire la clé MinIO pour le référentiel (pas de partition temporelle)
            object_key = f"{api_name}_{endpoint_name}_referentiel"
            
            cached_data = self.load_from_minio(self.minio_bucket, object_key)
            if cached_data and "data" in cached_data:
                return cached_data["data"]
            
            return []
            
        except Exception as e:
            logger = get_dagster_logger()
            logger.warning(f"⚠️ Erreur chargement cache référentiel {api_name}/{endpoint_name}: {e}")
            return []
    
    def _perform_health_checks(
        self, 
        config: HubeauAPIConfig, 
        results: Dict[str, Any], 
        total_records: int, 
        logger
    ) -> None:
        """
        Tests de santé et assertions pour validation de l'ingestion
        """
        # 1. Vérification des champs obligatoires pour température
        if config.name == "temperature":
            self._validate_temperature_data_quality(results, logger)
        
        # 2. Vérification de la densité minimale attendue
        self._validate_data_density(config, total_records, logger)
        
        # 3. Vérification de la cohérence des résultats par endpoint
        self._validate_endpoint_consistency(results, logger)
    
    def _validate_temperature_data_quality(self, results: Dict[str, Any], logger) -> None:
        """
        Validation spécifique qualité des données température
        """
        chronique_result = results.get("chronique", {})
        chronique_data = chronique_result.get("data", [])
        
        if chronique_data:
            # Vérifier les champs obligatoires
            required_fields = {"code_station", "date_mesure_temp", "heure_mesure_temp", "resultat"}
            
            sample_record = chronique_data[0] if chronique_data else {}
            missing_fields = required_fields - set(sample_record.keys())
            
            if missing_fields:
                logger.warning(f"⚠️ [TEMPERATURE] Champs manquants: {missing_fields}")
            else:
                logger.info(f"✅ [TEMPERATURE] Champs obligatoires présents: {required_fields}")
            
            # Vérifier la granularité horaire
            hourly_records = [r for r in chronique_data if r.get("heure_mesure_temp")]
            if hourly_records:
                logger.info(f"✅ [TEMPERATURE] Granularité horaire: {len(hourly_records)}/{len(chronique_data)} records")
            else:
                logger.warning("⚠️ [TEMPERATURE] Aucune donnée avec granularité horaire")
    
    def _validate_data_density(self, config: HubeauAPIConfig, total_records: int, logger) -> None:
        """
        Validation de la densité minimale attendue selon l'API
        """
        # Seuils minimaux par API (heuristiques)
        min_thresholds = {
            "temperature": 10,    # Au moins 10 points sur 24h au niveau national
            "piezo": 50,          # Piézométrie plus dense
            "hydro": 100,         # Hydrométrie très dense
            "quality_surface": 5, # Qualité moins fréquente
            "quality_groundwater": 5,
            "onde": 20,           # Écoulements
            "prelevements": 5,    # Prélèvements ponctuels
        }
        
        min_threshold = min_thresholds.get(config.name, 1)
        
        if total_records == 0:
            logger.warning(f"⚠️ [{config.name.upper()}] Aucune donnée récupérée - vérifier fallback et filtres")
        elif total_records < min_threshold:
            logger.warning(f"⚠️ [{config.name.upper()}] Densité faible: {total_records} < {min_threshold} (seuil minimal)")
        else:
            logger.info(f"✅ [{config.name.upper()}] Densité OK: {total_records} records")
    
    def _validate_endpoint_consistency(self, results: Dict[str, Any], logger) -> None:
        """
        Validation de la cohérence des résultats par endpoint
        """
        for endpoint_name, result in results.items():
            records_count = result.get("records_count", 0)
            storage_success = result.get("storage_success", False)
            error = result.get("error")
            
            if error:
                logger.warning(f"⚠️ [{endpoint_name.upper()}] Erreur: {error}")
            elif records_count == 0:
                logger.info(f"ℹ️ [{endpoint_name.upper()}] Aucune donnée (normal pour certains endpoints)")
            elif not storage_success:
                logger.warning(f"⚠️ [{endpoint_name.upper()}] Stockage échoué malgré {records_count} records")
            else:
                logger.info(f"✅ [{endpoint_name.upper()}] {records_count} records stockés avec succès")
    
    def check_minio_exists(self, bucket: str, object_key: str) -> bool:
        """Vérifie si un objet existe déjà dans MinIO"""
        if not self.minio_client:
            return False
            
        try:
            self.minio_client.head_object(Bucket=bucket, Key=object_key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            else:
                print(f"⚠️ Erreur vérification MinIO: {e}")
                return False
        except Exception as e:
            print(f"⚠️ Erreur inattendue vérification: {e}")
            return False
    
    def load_from_minio(self, bucket: str, object_key: str) -> Optional[Dict[str, Any]]:
        """Charge des données depuis MinIO"""
        if not self.minio_client:
            return None
            
        try:
            response = self.minio_client.get_object(Bucket=bucket, Key=object_key)
            content = response['Body'].read().decode('utf-8')
            return json.loads(content)
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return None
            else:
                print(f"❌ Erreur chargement MinIO: {e}")
                return None
        except Exception as e:
            print(f"❌ Erreur inattendue chargement: {e}")
            return None

    def _store_endpoint_data(self, config: HubeauAPIConfig, endpoint_name: str, endpoint_config: EndpointConfig, 
                           all_endpoint_data: List[Dict[str, Any]], date_partition: str, 
                           available_departments: List[str], results: Dict[str, Any], total_records: int) -> None:
        """Stocke les données d'un endpoint dans MinIO"""
        logger = get_dagster_logger()
        
        storage_metadata = {
            'api_name': config.name,
            'endpoint': endpoint_name,
            'date_partition': date_partition,
            'records_count': len(all_endpoint_data),
            'version': getattr(config, 'version', 'v1'),
            'deduplication_applied': endpoint_config.deduplication is not None,
            'spatial_filters_used': (endpoint_config.spatial_filter_required or config.requires_spatial_filter),
            'departments_queried_count': len(available_departments) if (endpoint_config.spatial_filter_required or config.requires_spatial_filter) else 0
        }
        
        storage_data = {
            'metadata': storage_metadata,
            'data': all_endpoint_data
        }
        
        object_key = f"{config.name}/{date_partition}/{endpoint_config.path}.json"
        storage_success = self.store_to_minio(
            storage_data,
            self.minio_bucket,
            object_key
        )

        results[endpoint_name] = {
            'records_count': len(all_endpoint_data),
            'minio_path': f"s3://{self.minio_bucket}/{object_key}",
            'storage_success': storage_success,
            'from_cache': False,
            'sample_record': all_endpoint_data[0] if all_endpoint_data else None
        }

        total_records += len(all_endpoint_data)
        logger.info(f"✅ {endpoint_name}: {len(all_endpoint_data)} records stockés")

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
    
    def get_available_departments(self, config: HubeauAPIConfig) -> List[str]:
        """Récupère les départements disponibles pour une API avec fallback robuste"""
        logger = get_dagster_logger()
        
        # Liste complète des départements français (métropole + Corse + DROM)
        FRENCH_DEPARTMENTS = [
            # Métropole (01-95)
            '01', '02', '03', '04', '05', '06', '07', '08', '09', '10',
            '11', '12', '13', '14', '15', '16', '17', '18', '19', '2A', '2B',
            '21', '22', '23', '24', '25', '26', '27', '28', '29', '30',
            '31', '32', '33', '34', '35', '36', '37', '38', '39', '40',
            '41', '42', '43', '44', '45', '46', '47', '48', '49', '50',
            '51', '52', '53', '54', '55', '56', '57', '58', '59', '60',
            '61', '62', '63', '64', '65', '66', '67', '68', '69', '70',
            '71', '72', '73', '74', '75', '76', '77', '78', '79', '80',
            '81', '82', '83', '84', '85', '86', '87', '88', '89', '90',
            '91', '92', '93', '94', '95',
            # DROM (971-976)
            '971', '972', '973', '974', '976'
        ]
        
        # Endpoint pour récupérer les stations sans filtre géographique
        stations_endpoint = None
        for name, endpoint in config.endpoints.items():
            name_has = ('station' in name.lower() or 'referentiel' in name.lower())
            path_has = ('station' in endpoint.path.lower() or 'referentiel' in endpoint.path.lower())
            if (name_has or path_has) and not endpoint.apply_temporal_filter:
                stations_endpoint = endpoint
                break
        
        if not stations_endpoint:
            logger.warning(f"Pas d'endpoint stations trouvé pour {config.name}, utilisation de la liste complète FR")
            return FRENCH_DEPARTMENTS
        
        try:
            # Récupérer un échantillon de stations pour extraire les départements
            url = f"{config.base_url}/{stations_endpoint.path}"
            params = {
                'format': 'json',
                'size': min(stations_endpoint.page_size or 1000, 1000),  # Échantillon suffisant
                'page': 1
            }
            
            response_data = self.call_api_with_retry(url, params, config, stations_endpoint.path)
            if not response_data:
                logger.warning(f"Pas de réponse pour {config.name}, utilisation de la liste complète FR")
                return FRENCH_DEPARTMENTS
            
            stations = response_data.get('data', [])
            departments = set()
            
            for station in stations:
                # Extraire le département selon l'API
                dept_code = self._extract_department_code(station, config.name)
                if dept_code:
                    departments.add(dept_code)
            
            dept_list = sorted(list(departments))
            logger.info(f"Départements détectés pour {config.name}: {dept_list[:10]}... (total: {len(dept_list)})")
            
            # Si moins de 50 départements détectés, utiliser la liste complète
            if len(dept_list) < 50:
                logger.warning(f"Peu de départements détectés ({len(dept_list)}), utilisation de la liste complète FR")
                return FRENCH_DEPARTMENTS
            
            return dept_list
            
        except Exception as e:
            logger.warning(f"Erreur récupération départements pour {config.name}: {e}, utilisation de la liste complète FR")
            return FRENCH_DEPARTMENTS
    
    def _extract_department_code(self, station: Dict[str, Any], api_name: str) -> Optional[str]:
        """Extrait le code département d'une station selon l'API avec fallback INSEE"""
        # Mapping des champs département par API
        dept_fields = {
            'piezo': ['code_departement', 'departement', 'num_departement'],
            'hydro': ['code_departement', 'departement', 'num_departement'],
            'quality_surface': ['code_departement', 'departement', 'num_departement'],
            'quality_groundwater': ['num_departement', 'code_departement', 'departement'],
            'temperature': ['code_departement', 'departement', 'num_departement'],
            'onde': ['code_departement', 'departement', 'num_departement'],
            'hydrobiologie': ['code_departement', 'departement', 'num_departement'],
            'prelevements': ['code_departement', 'departement', 'num_departement'],
        }
        
        fields_to_try = dept_fields.get(api_name, ['code_departement', 'departement', 'num_departement'])
        
        for field in fields_to_try:
            if field in station and station[field]:
                dept_code = str(station[field]).strip().upper()
                
                # Regex pour codes département français (métropole + Corse + DROM)
                import re
                dept_regex = re.compile(r"^(?:0[1-9]|[1-8]\d|9[0-5]|2A|2B|97[1-6])$")
                
                if dept_regex.match(dept_code):
                    return dept_code

        # Fallback INSEE -> 2 premiers caractères
        for insee_key in ['code_commune', 'code_insee_commune', 'code_commune_insee']:
            if insee_key in station and station[insee_key]:
                val = str(station[insee_key]).strip()
                if len(val) >= 2:
                    dept = val[:2].upper()
                    # Gérer la Corse (20 -> 2A/2B, on prend 2A par défaut)
                    if dept == '20':
                        return '2A' 
                    return dept
        
        return None
        
    def ingest_hubeau_api(self, config: HubeauAPIConfig, date_partition: str) -> Dict[str, Any]:
        """Ingestion complète d'une API Hub'Eau pour une date donnée avec filtres intelligents"""
        logger = get_dagster_logger()
        logger.info(f"🌊 Ingestion {config.name} pour {date_partition}")
        
        # Récupérer les départements disponibles intelligemment
        available_departments = self.get_available_departments(config)
        logger.info(f"📊 Départements disponibles: {available_departments[:5]}... (total: {len(available_departments)})")
        
        # Paramètres de base avec filtre temporel
        date_obj = datetime.fromisoformat(date_partition)
        results = {}
        total_records = 0

        for endpoint_name, endpoint_config in config.endpoints.items():
            try:
                logger.info(f"📡 Appel {config.name}/{endpoint_name}")

                # Garde-fou : vérifier que chroniques est appelé avec filtre spatial obligatoire
                if endpoint_name == "chroniques" and endpoint_config.spatial_filter_required:
                    if not available_departments:
                        logger.error(f"❌ ALERTE: chroniques nécessite un filtre spatial mais aucun département disponible")
                        results[endpoint_name] = {
                            'records_count': 0,
                            'error': 'Filtre spatial obligatoire manquant pour chroniques',
                            'storage_success': False
                        }
                        continue

                base_endpoint_params = config.base_params.copy()
                base_endpoint_params.update(endpoint_config.params)

                # ✅ Paramètres spécifiques obs_elab v2
                if endpoint_name == "obs_elab" and config.name == "hydro":
                    # Préciser au moins une grandeur (débit moyen journalier)
                    base_endpoint_params["grandeur_hydro_elab"] = "QmnJ"
                    logger.info(f"✅ Paramètre grandeur_hydro_elab=QmnJ ajouté pour obs_elab")

                # Application des filtres temporels selon l'endpoint
                if endpoint_config.apply_temporal_filter:
                    if not endpoint_config.temporal_param_keys or len(endpoint_config.temporal_param_keys) != 2:
                        raise ValueError(
                            f"Endpoint {endpoint_name} doit définir temporal_param_keys pour appliquer un filtre temporel"
                        )

                    start_key, end_key = endpoint_config.temporal_param_keys
                    date_format = endpoint_config.temporal_format
                    
                    # Gestion spéciale pour les formats année (Prélèvements)
                    if date_format == "%Y":
                        # Pour les chroniques annuelles - calcul plus robuste
                        current_year = date_obj.year
                        lookback_years = max(1, (endpoint_config.lookback_days or 365) // 365)
                        annee_debut = current_year - lookback_years
                        annee_fin = current_year
                        base_endpoint_params[start_key] = str(annee_debut)
                        base_endpoint_params[end_key] = str(annee_fin)
                        logger.info(f"Format annuel pour {endpoint_name}: {annee_debut} -> {annee_fin}")
                    else:
                        # ✅ CORRIGÉ: Filtres temporels stricts basés sur la partition uniquement
                        # Pour les partitions quotidiennes, utiliser uniquement la date de partition
                        date_debut = date_obj
                        date_fin = date_obj
                        base_endpoint_params[start_key] = date_debut.strftime(date_format)
                        base_endpoint_params[end_key] = date_fin.strftime(date_format)
                    
                    logger.info(f"Filtre temporel strict {endpoint_name}: {base_endpoint_params[start_key]} -> {base_endpoint_params[end_key]} (partition uniquement)")

                else:
                    if endpoint_config.temporal_param_keys:
                        start_key, end_key = endpoint_config.temporal_param_keys
                        base_endpoint_params.pop(start_key, None)
                        base_endpoint_params.pop(end_key, None)
                    logger.info(f"Pas de filtre temporel pour {endpoint_name} (référentiel)")

                # Configuration du tri pour optimiser la déduplication (liste blanche)
                if endpoint_config.deduplication and endpoint_config.supports_sort:
                    # Pour Hub'Eau, sort = "asc" ou "desc", pas le nom du champ
                    base_endpoint_params.setdefault('sort', 'desc')  # Tri décroissant (plus récent d'abord)
                    logger.info(f"Tri activé : sort=desc pour optimiser déduplication")
                elif endpoint_config.deduplication and not endpoint_config.supports_sort:
                    logger.debug(f"Tri non supporté par {endpoint_name}, déduplication post-traitement uniquement")
                
                all_endpoint_data = []
                # Récupération de TOUTES les données avec découpage intelligent
                if endpoint_config.spatial_filter_required or config.requires_spatial_filter:
                    logger.info(f"Filtre spatial requis pour {endpoint_name}.")
                    
                    if available_departments:
                        # Cas spécial pour chroniques : récupération avec codes BSS depuis stations
                        if endpoint_name == "chroniques":
                            all_endpoint_data = self._fetch_chroniques_with_bss_codes(
                                config, endpoint_name, endpoint_config, base_endpoint_params, available_departments
                            )
                        # Cas spécial pour points_prelevement : test de santé par département
                        elif endpoint_name == "points_prelevement":
                            all_endpoint_data = self._fetch_points_prelevement_with_health_check(
                                config, endpoint_name, endpoint_config, base_endpoint_params, available_departments
                            )
                        else:
                            # Découpage intelligent par groupes de départements pour éviter la limite 20k
                            all_endpoint_data = self._fetch_with_spatial_chunking(
                                config, endpoint_name, endpoint_config, base_endpoint_params, available_departments
                            )
                    else:
                        logger.warning(f"Filtre spatial requis mais aucun département trouvé pour {config.name}.")
                        all_endpoint_data = []
                else:
                    # Cas spécial pour température chronique : découpage par station
                    if config.name == "temperature" and endpoint_name == "chronique":
                        all_endpoint_data = self._fetch_temperature_chronique_by_station(
                            config, endpoint_name, endpoint_config, base_endpoint_params, date_obj
                        )
                    else:
                        # Découpage temporel pour les APIs sans filtre spatial
                        all_endpoint_data = self._fetch_with_temporal_chunking(
                            config, endpoint_name, endpoint_config, base_endpoint_params, date_obj
                        )
                
                # Déduplication finale sur l'ensemble des données collectées
                if all_endpoint_data and endpoint_config.deduplication:
                    all_endpoint_data = self._deduplicate_records(all_endpoint_data, endpoint_config.deduplication)

                if all_endpoint_data:
                    # Vérifier si les données existent déjà dans MinIO
                    object_key = f"{config.name}/{date_partition}/{endpoint_config.path}.json"
                    
                    if self.check_minio_exists(self.minio_bucket, object_key):
                        logger.info(f"📦 {endpoint_name}: Données déjà présentes dans MinIO, chargement depuis le cache")
                        
                        # Charger les données existantes
                        existing_data = self.load_from_minio(self.minio_bucket, object_key)
                        if existing_data:
                            cached_records = len(existing_data.get('data', []))
                            results[endpoint_name] = {
                                'records_count': cached_records,
                                'minio_path': f"s3://{self.minio_bucket}/{object_key}",
                                'storage_success': True,
                                'from_cache': True,
                                'sample_record': existing_data.get('data', [{}])[0] if existing_data.get('data') else None
                            }
                            total_records += cached_records
                            logger.info(f"✅ {endpoint_name}: {cached_records} records chargés depuis le cache")
                        else:
                            # Erreur de chargement, stocker les nouvelles données
                            logger.warning(f"⚠️ {endpoint_name}: Erreur chargement cache, stockage des nouvelles données")
                            self._store_endpoint_data(config, endpoint_name, endpoint_config, all_endpoint_data, date_partition, available_departments, results, total_records)
                    else:
                        # Données absentes, stocker les nouvelles données
                        logger.info(f"📦 {endpoint_name}: Données absentes du cache, stockage des nouvelles données")
                        self._store_endpoint_data(config, endpoint_name, endpoint_config, all_endpoint_data, date_partition, available_departments, results, total_records)
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

        # Tests de santé et assertions
        self._perform_health_checks(config, results, total_records, logger)

        return {
            'execution_date': datetime.now().isoformat(),
            'partition_date': date_partition,
            'api_name': config.name,
            'total_records_ingested': total_records,
            'endpoints_processed': list(config.endpoints),
            'minio_bucket': self.minio_bucket,
            'results_by_endpoint': results,
            'available_departments': available_departments,
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
    
    from .hubeau_configs import get_hubeau_piezo_config
    config = get_hubeau_piezo_config()
    
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
    
    from .hubeau_configs import get_hubeau_hydro_config
    config = get_hubeau_hydro_config()
    
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

    from .hubeau_configs import get_hubeau_quality_surface_config
    config = get_hubeau_quality_surface_config()

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
    
    from .hubeau_configs import get_hubeau_quality_groundwater_config
    config = get_hubeau_quality_groundwater_config()
    
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
    
    from .hubeau_configs import get_hubeau_temperature_config
    config = get_hubeau_temperature_config()
    
    service = HubeauIngestionService()
    return service.ingest_hubeau_api(config, day)

# ====================================
# ASSET ÉCOULEMENT ONDE
# ====================================

@asset(
    group_name="bronze_hubeau",
    partitions_def=DAILY_PARTITIONS, 
    description="🌊 Hub'Eau ONDE (Écoulements) - Ingestion RÉELLE",
    retry_policy=RetryPolicy(max_retries=2, delay=300)
)
def hubeau_onde_bronze_real(context: AssetExecutionContext) -> Dict[str, Any]:
    """
    Ingestion RÉELLE API ONDE (Observatoire National Des Étiages)
    - URL: /api/v1/ecoulement/
    - Endpoints: stations, campagnes, observations
    - Fréquence: Campagnes saisonnières (été principalement)
    """
    logger = get_dagster_logger()
    day = context.partition_key
    logger.info(f"🌊 Démarrage ingestion ONDE COMPLÈTE {day}")
    
    from .hubeau_configs import get_hubeau_onde_config
    config = get_hubeau_onde_config()
    
    service = HubeauIngestionService()
    return service.ingest_hubeau_api(config, day)

# ====================================
# ASSET HYDROBIOLOGIE  
# ====================================

@asset(
    group_name="bronze_hubeau",
    partitions_def=DAILY_PARTITIONS,
    description="🐟 Hub'Eau Hydrobiologie - Ingestion RÉELLE", 
    retry_policy=RetryPolicy(max_retries=2, delay=300)
)
def hubeau_hydrobiologie_bronze_real(context: AssetExecutionContext) -> Dict[str, Any]:
    """
    Ingestion RÉELLE API Hydrobiologie Hub'Eau
    - URL: /api/v1/hydrobio/
    - Endpoints: stations_hydrobio, indices, taxons
    - Fréquence: Campagnes annuelles/saisonnières
    """
    logger = get_dagster_logger()
    day = context.partition_key
    logger.info(f"🐟 Démarrage ingestion Hydrobiologie COMPLÈTE {day}")
    
    from .hubeau_configs import get_hubeau_hydrobiologie_config
    config = get_hubeau_hydrobiologie_config()
    
    service = HubeauIngestionService()
    return service.ingest_hubeau_api(config, day)

# ====================================
# ASSET PRÉLÈVEMENTS
# ====================================

@asset(
    group_name="bronze_hubeau",
    partitions_def=DAILY_PARTITIONS,
    description="🚰 Hub'Eau Prélèvements - Ingestion RÉELLE",
    retry_policy=RetryPolicy(max_retries=2, delay=300)
)
def hubeau_prelevements_bronze_real(context: AssetExecutionContext) -> Dict[str, Any]:
    """
    Ingestion RÉELLE API Prélèvements Hub'Eau
    - URL: /api/v1/prelevements/
    - Endpoints: points_prelevement, chroniques
    - Données: Volumes prélevés déclarés
    """
    logger = get_dagster_logger()
    day = context.partition_key
    logger.info(f"🚰 Démarrage ingestion Prélèvements COMPLÈTE {day}")
    
    from .hubeau_configs import get_hubeau_prelevements_config
    config = get_hubeau_prelevements_config()
    
    service = HubeauIngestionService()
    return service.ingest_hubeau_api(config, day)