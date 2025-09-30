"""
Client Hub'Eau moderne avec httpx + tenacity + pydantic
Architecture robuste et performante pour l'ingestion des données Hub'Eau
"""

import asyncio
import json
import os
import random
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import httpx
from tenacity import (
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
    AsyncRetrying  # ✅ CORRECTIF C: Pour retries dynamiques
)
from pydantic import BaseModel, Field, validator
from dagster import get_dagster_logger
import boto3
from botocore.client import BaseClient
from botocore.exceptions import ClientError

logger = get_dagster_logger()

# ====================================
# MODÈLES PYDANTIC POUR VALIDATION
# ====================================

class HubeauApiResponse(BaseModel):
    """Modèle de base pour toutes les réponses Hub'Eau"""
    data: List[Dict[str, Any]] = Field(default_factory=list)
    count: Optional[int] = None
    next: Optional[str] = None
    previous: Optional[str] = None
    
    @validator('data')
    def validate_data_not_empty(cls, v):
        # Accepter les listes vides pour les réponses sans données
        return v

class HubeauStation(BaseModel):
    """Station Hub'Eau générique"""
    code_station: str
    libelle_station: Optional[str] = None
    code_departement: Optional[str] = None
    code_commune: Optional[str] = None
    longitude: Optional[float] = None
    latitude: Optional[float] = None

class HubeauObservation(BaseModel):
    """Observation Hub'Eau générique"""
    code_station: str
    date_obs: str
    resultat: Optional[float] = None
    code_qualification: Optional[str] = None

# ====================================
# CONFIGURATION DES ENDPOINTS
# ====================================

class HubeauEndpointConfig(BaseModel):
    """Configuration d'un endpoint Hub'Eau"""
    path: str
    temporal_params: Optional[Dict[str, str]] = None
    spatial_params: Optional[Dict[str, str]] = None
    page_size: int = 1000
    max_pages: int = 100
    supports_cursor: bool = False
    requires_spatial_filter: bool = False
    cache_duration: int = 30  # Jours de cache
    realtime_cache_duration: int = 15  # Minutes pour données temps réel
    depth_limit: Optional[int] = None  # Limite de profondeur pour éviter troncature
    end_offset_days: int = 0  # Offset pour borne fin exclusive (ex. Hydrobiologie: +1 jour)

class HubeauApiConfig(BaseModel):
    """Configuration complète d'une API Hub'Eau"""
    name: str
    base_url: str
    version: str = "v1"
    endpoints: Dict[str, HubeauEndpointConfig]
    rate_limit_delay: float = 0.5
    timeout: int = 60
    max_retries: int = 3
    max_results_limit: int = 100000  # Limite Hub'Eau augmentée

# ====================================
# CLIENT HUB'EAU MODERNE
# ====================================

class HubeauClient:
    """Client Hub'Eau moderne avec httpx + tenacity + pydantic"""
    
    def __init__(self, config: HubeauApiConfig):
        self.config = config
        self.logger = logger
        self.metrics = IngestionMetrics()  # ✅ CORRECTIF D: Métriques d'observabilité
        
        # Configuration httpx
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(config.timeout),
            headers={
                'User-Agent': 'BRGM-HubEau-Pipeline/2.0',
                'Accept': 'application/json'
            }
        )
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.client.aclose()
    
    async def _make_request(self, endpoint: str, params: Dict[str, Any]) -> HubeauApiResponse:
        """Fait une requête HTTP avec retry automatique et jitter (CORRECTIF C)"""
        url = f"{self.config.base_url}/{endpoint}"
        
        # Rate limiting respectueux
        await asyncio.sleep(self.config.rate_limit_delay)
        
        self.logger.debug(f"Requête Hub'Eau: {url} avec params {params}")
        
        # ✅ CORRECTIF C: Retries dynamiques avec jitter
        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(self.config.max_retries),
            wait=wait_exponential(multiplier=1, min=2, max=10),
            retry=retry_if_exception_type((httpx.HTTPError, httpx.TimeoutException)),
            before_sleep=before_sleep_log(logger, 30),
            reraise=True
        ):
            with attempt:
                # Jitter pour éviter rafales synchrones
                await asyncio.sleep(random.random() * 0.5)
                
                response = await self.client.get(url, params=params)
                response.raise_for_status()
                
                data = response.json()
                return HubeauApiResponse(**data)
    
    async def get_stations(self, endpoint_name: str = "stations") -> List[Dict[str, Any]]:
        """Récupère toutes les stations de TOUT LE TERRITOIRE FRANÇAIS"""
        endpoint_config = self.config.endpoints[endpoint_name]
        all_data = []
        
        # Si pas de filtre spatial requis, récupérer toutes les données
        if not endpoint_config.requires_spatial_filter:
            return await self._fetch_all_pages(endpoint_config, {"format": "json", "size": endpoint_config.page_size})
        
        # Traiter TOUT LE TERRITOIRE FRANÇAIS par chunks adaptatifs (inspiré du legacy)
        all_departments = self._get_french_departments()
        
        # Chunking adaptatif selon la profondeur limite (legacy strategy)
        depth_limit = getattr(endpoint_config, 'depth_limit', None) or self.config.max_results_limit
        if depth_limit <= 10000:
            chunk_size = 1  # 1 département pour les APIs avec limite 10k (Hydrobiologie)
        else:
            chunk_size = 5  # 5 départements pour les autres APIs
        
        dept_chunks = [all_departments[i:i + chunk_size] for i in range(0, len(all_departments), chunk_size)]
        
        self.logger.info(f"🌍 Récupération stations {endpoint_name} pour TOUT LE TERRITOIRE FRANÇAIS")
        self.logger.info(f"📊 Découpage spatial: {len(all_departments)} départements en {len(dept_chunks)} groupes (chunk_size={chunk_size})")
        
        spatial_param_key = self._resolve_spatial_param(endpoint_config)

        for i, dept_chunk in enumerate(dept_chunks):
            self.logger.info(f"🌍 Groupe {i+1}/{len(dept_chunks)}: départements {dept_chunk}")

            # Paramètres pour ce chunk
            params = {
                "format": "json",
                "size": endpoint_config.page_size,
                spatial_param_key: ",".join(dept_chunk)
            }
            
            # Récupérer toutes les pages pour ce chunk
            chunk_data = await self._fetch_all_pages(endpoint_config, params)
            all_data.extend(chunk_data)
            
            # ✅ CORRECTIF D: Mise à jour métriques
            self.metrics.departements_traites += len(dept_chunk)
            self.metrics.stations_total = len(all_data)
            
            self.logger.info(f"✅ Groupe {i+1}: {len(chunk_data)} stations (total: {len(all_data)})")
            
            # Vérifier la profondeur limite par endpoint (pas de cap global)
            if depth_limit is not None and len(all_data) >= depth_limit:
                self.logger.warning(f"⚠️ Profondeur atteinte ({depth_limit}) pour {endpoint_name}")
                break
        
        self.logger.info(f"🎯 TOTAL stations récupérées ({endpoint_name}): {len(all_data)} sur TOUT LE TERRITOIRE")
        return all_data
    
    async def _fetch_all_pages(
        self, 
        endpoint_config, 
        params: Dict[str, Any], 
        bubble_exceptions: bool = False  # ✅ CORRECTIF B: paramètre pour propager exceptions
    ) -> List[Dict[str, Any]]:
        """Helper pour récupérer toutes les pages d'un endpoint"""
        all_data = []
        page = 1
        
        while page <= endpoint_config.max_pages:
            page_params = params.copy()
            page_params["page"] = page
            
            try:
                response_data = await self._make_request(endpoint_config.path, page_params)
                
                if not response_data.data:
                    break
                
                all_data.extend(response_data.data)
                
                # Vérifier si c'est la dernière page
                if len(response_data.data) < params["size"]:
                    break
                
                page += 1
                
            except Exception as e:
                # ✅ CORRECTIF B: Propager exception si demandé (pour split binaire)
                if bubble_exceptions:
                    raise
                self.logger.error(f"Erreur page {page}: {e}")
                break

        expected_records_cap = endpoint_config.max_pages * params.get("size", endpoint_config.page_size)
        if page > endpoint_config.max_pages and len(all_data) >= expected_records_cap:
            self.logger.warning(
                "⚠️ TRONCATURE: max_pages=%s atteint pour %s. Récupéré %s records, mais il pourrait y en avoir plus !",
                endpoint_config.max_pages,
                endpoint_config.path,
                len(all_data),
            )

        return all_data
    
    async def get_observations(
        self, 
        endpoint_name: str, 
        entity_codes: List[str],
        date_partition: str,
        api_name: str = None,
        realtime: bool = False
    ) -> List[Dict[str, Any]]:
        """Récupère les observations pour une date donnée"""
        # ✅ CORRECTIF: Vérifier que l'endpoint existe
        if endpoint_name not in self.config.endpoints:
            self.logger.error(f"❌ Endpoint {endpoint_name} non trouvé dans la configuration")
            return []
        
        endpoint_config = self.config.endpoints[endpoint_name]
        all_data = []
        
        # Paramètres de base
        params = {"format": "json", "size": endpoint_config.page_size}
        
        # Filtres temporels stricts (partition uniquement)
        if endpoint_config.temporal_params:
            # Parser la date correctement
            try:
                date_obj = datetime.strptime(date_partition, "%Y-%m-%d")
            except ValueError:
                # Fallback si format différent
                date_obj = datetime.fromisoformat(date_partition)
            
            start_key = endpoint_config.temporal_params["start"]
            end_key = endpoint_config.temporal_params["end"]
            
            # Gestion spéciale pour les prélèvements (années)
            if "annee" in start_key:
                year = date_obj.year
                params[start_key] = year
                params[end_key] = year
            else:
                # Format standard pour les autres APIs avec gestion de borne fin exclusive
                end_offset = getattr(endpoint_config, 'end_offset_days', 0)
                
                # Hydrobiologie : données saisonnières → fenêtre large de 30 jours
                if self.config.name == "hydrobiology":
                    start_dt = date_obj - timedelta(days=30)
                    end_dt = date_obj + timedelta(days=1)
                    self.logger.info(f"📅 Hydrobiologie (campagnes saisonnières): fenêtre de 30 jours [{start_dt} → {end_dt}[")
                else:
                    start_dt = date_obj
                    end_dt = date_obj + timedelta(days=end_offset)
                    if end_offset > 0:
                        self.logger.info(f"📅 Fenêtre temporelle: [{start_dt} → {end_dt}[ (borne fin exclusive)")
                
                params[start_key] = start_dt.strftime("%Y-%m-%d")
                params[end_key] = end_dt.strftime("%Y-%m-%d")
        
        # APPROCHE ADAPTÉE : Vérifier d'abord la configuration de l'endpoint
        # Priorité 1 : Si l'endpoint demande un filtre spatial → utiliser approche spatiale
        # Priorité 2 : Si codes d'entités disponibles → utiliser approche par codes
        # Priorité 3 : Récupération directe sans filtre
        
        if endpoint_config.requires_spatial_filter and endpoint_config.spatial_params:
            # APPROCHE SPATIALE (départements)
            all_departments = self._get_french_departments()

            # Chunking adaptatif selon la profondeur limite et l'API
            depth_limit = getattr(endpoint_config, 'depth_limit', None)
            if depth_limit is None:
                depth_limit = 20000
            
            # Hydrobiologie : chunk_size = 1 pour éviter les 500
            if self.config.name == "hydrobiology":
                chunk_size = 1
            elif depth_limit <= 10000:
                chunk_size = 1  # 1 département pour les APIs avec limite 10k
            else:
                chunk_size = 5  # 5 départements pour les autres APIs
            
            dept_chunks = [all_departments[i:i + chunk_size] for i in range(0, len(all_departments), chunk_size)]
            
            self.logger.info(f"🌍 Récupération observations {endpoint_name} par départements (parallélisée)")
            self.logger.info(f"📊 Découpage spatial: {len(all_departments)} départements en {len(dept_chunks)} groupes (chunk_size={chunk_size})")
            
            # Parallélisation des requêtes par département (réduite pour hydrobiologie)
            MAX_CONCURRENT_SPATIAL = 4 if self.config.name == "hydrobiology" else 10
            semaphore = asyncio.Semaphore(MAX_CONCURRENT_SPATIAL)
            self.logger.info(f"⚡ Parallélisme: {MAX_CONCURRENT_SPATIAL} requêtes simultanées")

            spatial_param_key = self._resolve_spatial_param(endpoint_config)

            async def fetch_dept_chunk(i, dept_chunk):
                async with semaphore:
                    try:
                        chunk_params = params.copy()
                        chunk_params[spatial_param_key] = ",".join(dept_chunk)

                        chunk_data = await self._fetch_all_pages(endpoint_config, chunk_params)
                        self.logger.info(f"✅ Groupe {i+1}/{len(dept_chunks)}: {len(chunk_data)} observations")
                        return chunk_data
                    except Exception as e:
                        # Fallback : si le chunk multi-départements échoue, réessayer département par département
                        if len(dept_chunk) > 1:
                            self.logger.warning(f"⚠️ Groupe {i+1} erreur ({e}), retry par département individuel...")
                            merged = []
                            for dept in dept_chunk:
                                try:
                                    one_params = params.copy()
                                    one_params[spatial_param_key] = dept
                                    one_data = await self._fetch_all_pages(endpoint_config, one_params)
                                    merged.extend(one_data)
                                except Exception as ee:
                                    self.logger.warning(f"⚠️ Département {dept} ignoré: {str(ee)}")
                            return merged
                        else:
                            self.logger.warning(f"⚠️ Groupe {i+1} ignoré: {str(e)}")
                            return []
            
            # Lancer toutes les requêtes en parallèle
            tasks = [fetch_dept_chunk(i, chunk) for i, chunk in enumerate(dept_chunks)]
            results = await asyncio.gather(*tasks)
            
            # Agréger les résultats
            all_data = []
            for result in results:
                all_data.extend(result)
                # Vérifier la profondeur limite
                if endpoint_config.depth_limit and len(all_data) >= endpoint_config.depth_limit:
                    self.logger.warning(f"⚠️ Profondeur atteinte ({endpoint_config.depth_limit})")
                    break
            
            self.logger.info(f"✅ Total: {len(all_data)} observations (parallélisé avec {MAX_CONCURRENT_SPATIAL} requêtes simultanées)")
            return all_data
        
        elif entity_codes:
            # APPROCHE PAR CODES D'ENTITÉS
            api_name_actual = api_name or self.config.name
            if api_name_actual in [
                "hydrometry",
                "piezometry",
                "ground_water_quality",
                "prelevements",
                "temperature",
                "onde",
                "hydrobiology",
            ]:
                entity_key = self._get_entity_key_for_api(api_name_actual)
                self.logger.info(f"📊 Approche avec {entity_key} pour observations {endpoint_name}")
                
                # Approche cl-hubeau : chunking systématique pour éviter URL trop longue
                entity_name = entity_key.replace("code_", "").replace("_", " ")
                self.logger.info(f"🌍 Récupération observations {endpoint_name} par {entity_key}")
                self.logger.info(f"📊 {len(entity_codes)} {entity_name} disponibles")
                
                # Chunking systématique si trop de codes (évite URL too long)
                # Hydrobiologie : limite URL ≈2083 caractères → chunks plus petits
                if api_name_actual == "hydrobiology":
                    MAX_CODES_PER_REQUEST = 25
                elif api_name_actual == "hydrometry" and endpoint_name == "observations_tr":
                    MAX_CODES_PER_REQUEST = 25
                else:
                    MAX_CODES_PER_REQUEST = 50
                
                if len(entity_codes) > MAX_CODES_PER_REQUEST:
                    # Découpage systématique en chunks avec parallélisation
                    entity_chunks = [entity_codes[i:i + MAX_CODES_PER_REQUEST] for i in range(0, len(entity_codes), MAX_CODES_PER_REQUEST)]
                    self.logger.info(f"📦 Découpage en {len(entity_chunks)} chunks de {MAX_CODES_PER_REQUEST} (évite URL trop longue)")
                    
                    # Parallélisation avec asyncio.gather() pour accélérer l'ingestion
                    # Hydrobiologie : API sensible → parallélisme réduit
                    if api_name_actual == "hydrobiology":
                        MAX_CONCURRENT = 4
                    elif api_name_actual == "hydrometry" and endpoint_name == "observations_tr":
                        MAX_CONCURRENT = 8  # ✅ Parallélisme modéré pour limiter la pression API
                    else:
                        MAX_CONCURRENT = 15
                    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
                    self.logger.info(f"⚡ Parallélisme: {MAX_CONCURRENT} requêtes simultanées")
                    
                    async def fetch_chunk(i, chunk):
                        async with semaphore:
                            try:
                                chunk_params = params.copy()
                                chunk_params[entity_key] = ",".join(chunk)
                                # ✅ CORRECTIF B: Activer bubble_exceptions pour déclencher split binaire
                                chunk_data = await self._fetch_all_pages(
                                    endpoint_config, 
                                    chunk_params, 
                                    bubble_exceptions=True
                                )
                                
                                # ✅ CORRECTIF D: Mise à jour métriques
                                self.metrics.chunks_total += 1
                                if len(chunk_data) == 0:
                                    self.metrics.chunks_vides += 1
                                    self.metrics.stations_sans_donnees.extend(chunk)
                                else:
                                    self.metrics.chunks_ok += 1
                                
                                # Log tous les 50 chunks
                                if i % 50 == 0 or i == len(entity_chunks) - 1:
                                    self.logger.info(f"✅ Traité {i+1}/{len(entity_chunks)} chunks")
                                
                                return chunk_data
                            except Exception as e:
                                # ✅ CORRECTIF D: Tracking erreurs par type
                                if "500" in str(e):
                                    self.metrics.erreurs_http_500 += 1
                                elif "timeout" in str(e).lower():
                                    self.metrics.erreurs_timeout += 1
                                
                                # Fallback : si un chunk échoue, on le coupe en 2 jusqu'à taille 1
                                if len(chunk) > 1:
                                    self.logger.warning(f"⚠️ Chunk {i+1} erreur ({str(e)[:50]}...), découpage en 2...")
                                    mid = len(chunk) // 2
                                    left = await fetch_chunk(i, chunk[:mid])
                                    right = await fetch_chunk(i, chunk[mid:])
                                    return (left or []) + (right or [])
                                else:
                                    # ✅ CORRECTIF D: Logger les codes fautifs pour observabilité
                                    self.metrics.chunks_echoues += 1
                                    self.metrics.codes_echoues.append(chunk[0])
                                    self.logger.error(f"❌ Chunk {i+1} échoué définitivement (code: {chunk[0]}): {str(e)}")
                                    return []
                    
                    # Lancer toutes les requêtes en parallèle
                    tasks = [fetch_chunk(i, chunk) for i, chunk in enumerate(entity_chunks)]
                    results = await asyncio.gather(*tasks)
                    
                    # Agréger tous les résultats
                    all_data = []
                    for result in results:
                        all_data.extend(result)
                    
                    self.logger.info(f"✅ Total: {len(all_data)} observations récupérées (parallélisé avec {MAX_CONCURRENT} requêtes simultanées)")
                    return all_data
                    
                else:
                    # Peu de codes : requête unique
                    params[entity_key] = ",".join(entity_codes)
                    all_data = await self._fetch_all_pages(endpoint_config, params)
                    self.logger.info(f"✅ {len(all_data)} observations récupérées avec {entity_key}")
                    return all_data
        
        # Pas d'approche spécifique : récupération directe
        else:
            self.logger.info(f"📊 Récupération observations {endpoint_name} sans filtrage spécifique")
            return await self._fetch_all_pages(endpoint_config, params)
    
    def _get_french_departments(self) -> List[str]:
        """Retourne la liste complète des départements français (métropole + Corse + DROM)"""
        return [
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
    
    def _get_entity_key(self, endpoint_name: str) -> str:
        """Retourne la clé d'entité appropriée selon l'endpoint"""
        entity_keys = {
            "observations_tr": "code_entite",
            "chroniques_tr": "code_bss",
            "chroniques": "code_bss",
            "obs_elab": "code_entite",
            "analyses": "bss_id",
            "operations": "code_point_prelevement_aspe"
        }
        return entity_keys.get(endpoint_name, "code_station")
    
    def _get_entity_key_for_api(self, api_name: str) -> str:
        """Retourne la clé d'entité appropriée pour chaque API"""
        entity_keys = {
            "hydrometry": "code_entite",
            "piezometry": "code_bss",  # Piézométrie utilise code_bss
            "superficial_waterbodies_quality": "code_station",
            "ground_water_quality": "code_bss",
            "temperature": "code_station",
            "onde": "code_station",
            "hydrobiology": "code_station_hydrobio",  # Champ dédié hydrobiologie
            "prelevements": "code_ouvrage"
        }
        return entity_keys.get(api_name, "code_station")

    def _resolve_spatial_param(self, endpoint_config: HubeauEndpointConfig) -> str:
        """Retourne la clé de paramètre spatial attendue par l'endpoint."""
        spatial_params = endpoint_config.spatial_params or {}
        if "dept" in spatial_params:
            return spatial_params["dept"]

        if spatial_params:
            # Prendre le premier paramètre disponible (ex: bbox, code_commune, etc.)
            return next(iter(spatial_params.values()))

        raise ValueError(
            f"Endpoint {endpoint_config.path} requiert un filtre spatial mais aucun paramètre n'est défini"
        )

# ====================================
# MÉTRIQUES D'OBSERVABILITÉ (CORRECTIF D)
# ====================================

class IngestionMetrics(BaseModel):
    """Métriques d'observabilité pour l'ingestion"""
    departements_traites: int = 0
    departements_total: int = 101
    stations_total: int = 0
    stations_nouvelles: int = 0
    stations_mises_a_jour: int = 0
    chunks_total: int = 0
    chunks_ok: int = 0
    chunks_vides: int = 0
    chunks_echoues: int = 0
    stations_sans_donnees: List[str] = Field(default_factory=list)
    codes_echoues: List[str] = Field(default_factory=list)
    erreurs_http_500: int = 0
    erreurs_timeout: int = 0
    
    def to_summary(self) -> str:
        """Génère un résumé textuel"""
        return f"""
ℹ️ Hydrobiologie — Synthèse
- Départements traités : {self.departements_traites}/{self.departements_total}
- Stations : {self.stations_total} (nouveaux: {self.stations_nouvelles}, MAJ: {self.stations_mises_a_jour})
- Chunks indices : {self.chunks_total} (ok: {self.chunks_ok}, vides: {self.chunks_vides}, échoués: {self.chunks_echoues})
- Stations sans indices: {len(self.stations_sans_donnees)} {self.stations_sans_donnees[:10] if self.stations_sans_donnees else ''}
- Erreurs HTTP 500: {self.erreurs_http_500}, Timeouts: {self.erreurs_timeout}
        """

# ====================================
# SERVICE D'INGESTION BRONZE
# ====================================

class HubeauIngestionService:
    """Service d'ingestion Hub'Eau pour la couche Bronze"""

    def __init__(
        self,
        minio_resource: Optional[Dict[str, Any]] = None,
        bucket: Optional[str] = None,
    ):
        self.logger = logger
        self.minio_client, detected_bucket = self._resolve_minio_client(minio_resource)
        default_bucket = os.getenv("MINIO_BRONZE_BUCKET") or detected_bucket or "bronze"
        self.minio_bucket = bucket or default_bucket

        if not self.minio_bucket:
            raise ValueError("MinIO bucket must be provided through resources or environment")

    def _resolve_minio_client(
        self, minio_resource: Optional[Dict[str, Any]]
    ) -> tuple[BaseClient, Optional[str]]:
        """Initialise le client MinIO via ressource Dagster ou variables d'environnement."""
        if minio_resource is not None:
            client = minio_resource.get("client")
            bucket = minio_resource.get("bucket")
            if client is None or bucket is None:
                raise ValueError("The provided MinIO resource must expose 'client' and 'bucket' keys")
            return client, bucket

        return self._init_minio_client(), None

    def _init_minio_client(self):
        """Initialisation client MinIO"""
        try:
            client = boto3.client(
                's3',
                endpoint_url=os.getenv('MINIO_ENDPOINT', 'http://minio:9000'),
                aws_access_key_id=os.getenv('MINIO_USER', 'admin'),
                aws_secret_access_key=os.getenv('MINIO_PASS', 'BrgmMinio2024!'),
                region_name=os.getenv('MINIO_REGION', 'us-east-1')
            )
            client.list_buckets()
            return client
        except Exception as e:
            self.logger.error(f"MinIO connection FAILED: {e}")
            raise Exception(f"MinIO required for Bronze layer: {e}")
    
    async def ingest_api_data(self, config: HubeauApiConfig, date_partition: str) -> Dict[str, Any]:
        """Ingestion complète d'une API Hub'Eau"""
        self.logger.info(f"Ingestion {config.name} pour {date_partition}")
        
        results = {}
        total_records = 0
        
        async with HubeauClient(config) as client:
            stations = []  # Initialiser stations
            
            # Récupérer les stations
            stations_endpoint = self._get_stations_endpoint(config)
            if stations_endpoint:
                try:
                    stations = await client.get_stations(stations_endpoint)
                    results[stations_endpoint] = {
                        'records_count': len(stations),
                        'type': 'stations',
                        'data': stations  # Ajouter les données brutes
                    }
                    total_records += len(stations)
                except Exception as e:
                    self.logger.error(f"Erreur stations {stations_endpoint}: {e}")
            
            # Récupérer les observations
            observations_endpoints = self._get_observations_endpoints(config)
            for endpoint_name in observations_endpoints:
                try:
                    # Extraire les codes de stations avec le bon champ selon l'API
                    if stations:
                        # Déterminer le champ à utiliser selon l'API
                        if config.name == "hydrobiology":
                            station_codes = [s.get("code_station_hydrobio", "") for s in stations]
                        elif config.name in ["piezometry", "ground_water_quality"]:
                            station_codes = [s.get("code_bss", "") for s in stations]
                        elif config.name == "prelevements":
                            station_codes = [s.get("code_ouvrage", "") for s in stations]
                        else:
                            station_codes = [s.get("code_station", s.get("code_entite", "")) for s in stations]
                        
                        # Filtrer les valeurs vides/None
                        station_codes = [code for code in station_codes if code and code.strip()]
                        
                        if not station_codes:
                            self.logger.warning(f"⚠️ Aucun code station valide trouvé pour {endpoint_name}")
                    else:
                        station_codes = []
                    
                    observations = await client.get_observations(
                        endpoint_name, 
                        station_codes,
                        date_partition,
                        config.name
                    )
                    
                    # ✅ CORRECTIF: Vérifier que observations n'est pas None
                    if observations is None:
                        self.logger.warning(f"⚠️ observations {endpoint_name} est None, utilisation liste vide")
                        observations = []
                    
                    results[endpoint_name] = {
                        'records_count': len(observations),
                        'type': 'observations',
                        'stations_used': len(station_codes),
                        'data': observations  # Ajouter les données brutes
                    }
                    total_records += len(observations)
                    
                except Exception as e:
                    self.logger.error(f"Erreur observations {endpoint_name}: {e}")
        
        # Sauvegarder les données dans MinIO si disponibles
        if total_records > 0:
            try:
                self._save_to_minio(config.name, date_partition, results, client.metrics)
            except Exception as e:
                self.logger.warning(f"Erreur sauvegarde MinIO: {e}")
        
        # ✅ CORRECTIF D: Afficher synthèse métriques
        if config.name == "hydrobiology":
            self.logger.info(client.metrics.to_summary())
        
        return {
            'execution_date': datetime.now().isoformat(),
            'partition_date': date_partition,
            'api_name': config.name,
            'total_records_ingested': total_records,
            'results_by_endpoint': results,
            'status': 'success' if total_records > 0 else 'no_data',
            'metrics': client.metrics.dict() if hasattr(client, 'metrics') else {}  # ✅ CORRECTIF D
        }
    
    def _get_stations_endpoint(self, config: HubeauApiConfig) -> Optional[str]:
        """Retourne l'endpoint des stations selon l'API"""
        station_endpoints = {
            "hydrometry": "referentiel_stations",
            "piezometry": "stations",
            "superficial_waterbodies_quality": "station_pc",
            "ground_water_quality": "stations",
            "temperature": "station",
            "onde": "stations",
            "hydrobiology": "stations_hydrobio",
            "prelevements": "points_prelevement"
        }
        return station_endpoints.get(config.name)
    
    def _get_observations_endpoints(self, config: HubeauApiConfig) -> List[str]:
        """Retourne les endpoints d'observations selon l'API"""
        observation_endpoints = {
            "hydrometry": ["observations_tr", "obs_elab"],
            "piezometry": ["chroniques_tr", "chroniques"],
            "superficial_waterbodies_quality": ["operation_pc", "analyse_pc"],
            "ground_water_quality": ["analyses"],
            "temperature": ["chronique"],
            "onde": ["campagnes", "observations"],
            "hydrobiology": ["indices", "taxons"],
            "prelevements": ["chroniques"]
        }
        return observation_endpoints.get(config.name, [])
    
    def _get_entity_key_for_api(self, api_name: str) -> str:
        """Retourne la clé d'entité appropriée pour chaque API"""
        entity_keys = {
            "hydrometry": "code_entite",
            "piezometry": "code_bss",  # Piézométrie utilise code_bss
            "superficial_waterbodies_quality": "code_station",
            "ground_water_quality": "code_bss",
            "temperature": "code_station",
            "onde": "code_station",
            "hydrobiology": "code_station",
            "prelevements": "code_ouvrage"
        }
        return entity_keys.get(api_name, "code_station")
    
    def _save_to_minio(
        self, 
        api_name: str, 
        date_partition: str, 
        results: Dict[str, Any],
        metrics: Optional[IngestionMetrics] = None  # ✅ CORRECTIF D
    ):
        """Sauvegarde les données dans MinIO"""
        if self.minio_client is None or self.minio_bucket is None:
            raise RuntimeError("MinIO client is not configured")

        try:
            # Créer le bucket s'il n'existe pas
            try:
                self.minio_client.head_bucket(Bucket=self.minio_bucket)
            except ClientError:
                self.minio_client.create_bucket(Bucket=self.minio_bucket)

            # Sauvegarder les métadonnées d'ingestion
            key = f"{api_name}/{date_partition}/ingestion_metadata.json"
            metadata = {
                "api_name": api_name,
                "partition_date": date_partition,
                "execution_date": datetime.now().isoformat(),
                "results_by_endpoint": results,
                "total_records": sum(r.get('records_count', 0) for r in results.values()),
                "metrics": metrics.dict() if metrics else {}  # ✅ CORRECTIF D
            }

            self.minio_client.put_object(
                Bucket=self.minio_bucket,
                Key=key,
                Body=json.dumps(metadata, indent=2).encode("utf-8"),
                ContentType='application/json'
            )

            # Sauvegarder les données brutes pour chaque endpoint
            for endpoint_name, endpoint_result in results.items():
                if endpoint_result.get('records_count', 0) > 0:
                    # Données brutes
                    data_key = f"{api_name}/{date_partition}/{endpoint_name}_data.json"
                    self.minio_client.put_object(
                        Bucket=self.minio_bucket,
                        Key=data_key,
                        Body=json.dumps(endpoint_result.get('data', []), indent=2).encode("utf-8"),
                        ContentType='application/json'
                    )

                    # Métadonnées de l'endpoint
                    endpoint_metadata_key = f"{api_name}/{date_partition}/{endpoint_name}_metadata.json"
                    endpoint_metadata = {
                        "endpoint_name": endpoint_name,
                        "records_count": endpoint_result.get('records_count', 0),
                        "api_name": api_name,
                        "partition_date": date_partition,
                        "execution_date": datetime.now().isoformat()
                    }

                    self.minio_client.put_object(
                        Bucket=self.minio_bucket,
                        Key=endpoint_metadata_key,
                        Body=json.dumps(endpoint_metadata, indent=2).encode("utf-8"),
                        ContentType='application/json'
                    )

            self.logger.info(f"💾 Données sauvegardées dans MinIO: {key}")
            self.logger.info(f"📊 {len(results)} endpoints avec données brutes sauvegardés")

        except Exception as e:
            self.logger.error(f"Erreur sauvegarde MinIO: {e}")
            raise
