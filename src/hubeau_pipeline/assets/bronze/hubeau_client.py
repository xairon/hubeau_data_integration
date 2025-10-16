"""
Client Hub'Eau moderne avec httpx + tenacity + pydantic
Architecture robuste et performante pour l'ingestion des données Hub'Eau
"""

import asyncio
import json
import os
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlparse

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
# SÉMAPHORE GLOBAL POUR TOUTES LES APIS HUB'EAU
# ====================================
# ✅ CORRECTIF: Limite globale de requêtes simultanées vers Hub'Eau
# pour éviter la surcharge quand plusieurs APIs tournent en parallèle
GLOBAL_HUBEAU_SEMAPHORE = asyncio.Semaphore(10)  # Max 10 requêtes simultanées TOUS CLIENTS CONFONDUS

# ====================================
# EXCEPTIONS SPÉCIFIQUES
# ====================================


class HubeauPageFetchError(RuntimeError):
    """Erreur levée lorsqu'une page Hub'Eau ne peut pas être récupérée."""

    def __init__(
        self,
        endpoint: str,
        page: int,
        params: Dict[str, Any],
        original: Exception,
    ):
        redacted_params = {k: v for k, v in params.items() if k not in {"page", "cursor"}}
        message = (
            f"Echec de récupération de la page {page} pour '{endpoint}' "
            f"avec paramètres {redacted_params}: {original}"
        )
        super().__init__(message)
        self.endpoint = endpoint
        self.page = page
        self.params = redacted_params
        self.original = original

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
    """
    Configuration d'un endpoint Hub'Eau

    ✅ EXPLOITATION BUG API: Pagination illimitée sans paramètre 'size'
    L'API Hub'Eau permet de paginer indéfiniment avec page=N si on omet 'size'.
    Cela contourne la limite officielle de 20k records.

    WARNING: Cette approche repose sur un bug non documenté qui pourrait être corrigé.
    """
    path: str
    temporal_params: Optional[Dict[str, str]] = None
    supports_cursor: bool = False
    cache_duration: int = 30  # Jours de cache
    realtime_cache_duration: int = 15  # Minutes pour données temps réel
    end_offset_days: int = 0  # Offset pour borne fin exclusive (ex. Hydrobiologie: +1 jour)

    # ❌ REMOVED: page_size, max_pages, depth_limit (no longer needed with unlimited pagination)
    # ❌ REMOVED: spatial_params, requires_spatial_filter (no more dept/spatial chunking)

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
        self._last_truncation_info = {}  # ✅ CORRECTIF: Info de troncature pour température
        
        # Configuration httpx
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(config.timeout),
            headers={
                'User-Agent': 'BRGM-HubEau-Pipeline/2.0',
                'Accept': 'application/json'
            }
        )
        
        # ✅ CORRECTIF: Log de la limite globale pour observabilité
        self.logger.info(
            f"🌐 Client Hub'Eau {config.name} initialisé "
            f"(limite globale: {GLOBAL_HUBEAU_SEMAPHORE._value} requêtes simultanées pour TOUS les clients)"
        )
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.client.aclose()
    
    async def _make_request(self, endpoint: str, params: Dict[str, Any]) -> HubeauApiResponse:
        """Fait une requête HTTP avec retry automatique et jitter (CORRECTIF C)"""
        url = f"{self.config.base_url}/{endpoint}"
        
        # ✅ CORRECTIF: Sémaphore global pour limiter le nombre total de requêtes simultanées
        # Cela évite la surcharge quand 6 APIs tournent en parallèle
        async with GLOBAL_HUBEAU_SEMAPHORE:
            # Rate limiting respectueux
            await asyncio.sleep(self.config.rate_limit_delay)
            
            self.logger.debug(f"Requête Hub'Eau: {url} avec params {params}")
            
            # ✅ CORRECTIF C: Retries dynamiques avec jitter et gestion spéciale erreurs 500
            async for attempt in AsyncRetrying(
                stop=stop_after_attempt(self.config.max_retries),
                wait=wait_exponential(multiplier=2, min=3, max=30),  # ✅ CORRECTIF: Backoff plus long pour erreurs 500
                retry=retry_if_exception_type((httpx.HTTPError, httpx.TimeoutException)),
                before_sleep=before_sleep_log(logger, 30),
                reraise=True
            ):
                with attempt:
                    # Jitter pour éviter rafales synchrones
                    await asyncio.sleep(random.random() * 0.5)
                    
                    self.logger.debug(f"🌐 Requête Hub'Eau: {url} avec params {params}")
                    response = await self.client.get(url, params=params)
                    self.logger.debug(f"✅ Réponse reçue: {response.status_code}, taille: {len(response.content) if response.content else 0} bytes")
                    
                    # ✅ CORRECTIF: Ne pas retry sur erreurs 400 (Bad Request)
                    # Les erreurs 400 indiquent un problème avec la requête elle-même
                    if response.status_code == 400:
                        self.logger.warning(f"⚠️ Erreur 400 Bad Request pour {url} avec params {params}")
                        raise httpx.HTTPStatusError(
                            f"Bad Request: {response.status_code}", 
                            request=response.request, 
                            response=response
                        )
                    
                    # ✅ CORRECTIF: Logging spécial pour erreurs 500 avec détails
                    if response.status_code == 500:
                        self.logger.warning(
                            f"⚠️ Erreur 500 Internal Server Error pour {url} "
                            f"(page: {params.get('page', 'N/A')}, "
                            f"dept: {params.get('code_departement', 'N/A')}) - "
                            f"Retry dans {2 ** attempt.retry_state.attempt_number} secondes"
                        )
                    
                    response.raise_for_status()
                    
                    data = response.json()
                    return HubeauApiResponse(**data)
    
    async def get_stations(self, endpoint_name: str = "stations") -> List[Dict[str, Any]]:
        """
        ✅ Récupère TOUTES les stations via pagination simple

        Plus besoin de chunking départemental grâce au bug API Hub'Eau
        qui permet une pagination illimitée sans paramètre 'size'.

        Args:
            endpoint_name: Nom de l'endpoint stations dans la config

        Returns:
            Liste complète de toutes les stations
        """
        endpoint_config = self.config.endpoints[endpoint_name]

        self.logger.info(f"🌍 Récupération stations {endpoint_name} (pagination illimitée)")

        # ✅ Simple: pas de 'size', pas de filtrage spatial
        stations = await self._fetch_all_pages(
            endpoint_config,
            {"format": "json"}
        )

        self.logger.info(f"✅ TOTAL stations récupérées: {len(stations)}")

        # ✅ Update metrics
        self.metrics.stations_total = len(stations)

        return stations
    
    async def _fetch_all_pages(
        self,
        endpoint_config: HubeauEndpointConfig,
        params: Dict[str, Any],
        bubble_exceptions: bool = False
    ) -> List[Dict[str, Any]]:
        """
        ✅ HUB'EAU API PAGINATION EXPLOIT

        When 'size' parameter is OMITTED, the API allows unlimited pagination
        via 'page' parameter only. This bypasses the official 20k depth limit.

        Example: https://hubeau.eaufrance.fr/api/v1/niveaux_nappes/stations?format=json&page=5
        Returns 3205 records (last page), which should be impossible with official limits.

        WARNING: This relies on an undocumented API bug that may be fixed in the future.

        Args:
            endpoint_config: Configuration de l'endpoint
            params: Paramètres de base (format, filtres temporels, etc.)
            bubble_exceptions: Si True, propage les exceptions

        Returns:
            Liste complète de tous les records paginés
        """
        all_data: List[Dict[str, Any]] = []
        page_num = 1
        cursor: Optional[str] = None

        # ✅ CRITICAL: Remove 'size' parameter to exploit API bug
        params = {k: v for k, v in params.items() if k not in ["size", "page_size"]}

        self.logger.info(f"🔄 Pagination illimitée pour {endpoint_config.path} (bug API exploité)")

        while True:
            page_params = params.copy()

            # Cursor-based pagination (for some endpoints)
            if endpoint_config.supports_cursor:
                if cursor:
                    page_params["cursor"] = cursor
            # Page-based pagination (most endpoints)
            else:
                page_params["page"] = page_num

            try:
                self.logger.debug(f"📄 Page {page_num}: requête en cours...")
                response_data = await self._make_request(endpoint_config.path, page_params)

                # ✅ Stop condition 1: No data returned
                if not response_data.data:
                    self.logger.debug(f"📄 Page {page_num}: aucune donnée, arrêt pagination")
                    break

                all_data.extend(response_data.data)
                self.logger.debug(
                    f"📄 Page {page_num}: {len(response_data.data)} records "
                    f"(total: {len(all_data)})"
                )

                page_num += 1

                # ✅ Stop condition 2 (cursor): No next cursor
                if endpoint_config.supports_cursor:
                    next_cursor = self._extract_cursor(response_data.next)
                    if not next_cursor or next_cursor == cursor:
                        self.logger.debug("📄 Curseur épuisé, arrêt pagination")
                        break
                    cursor = next_cursor

                # ✅ Stop condition 3 (page): No 'next' field
                elif not response_data.next:
                    self.logger.debug("📄 Pas de page suivante, arrêt pagination")
                    break

            except Exception as exc:
                self.logger.exception(
                    f"❌ Erreur page {page_num} pour {endpoint_config.path}"
                )

                error = HubeauPageFetchError(
                    endpoint_config.path,
                    page_num,
                    page_params,
                    exc,
                )

                if bubble_exceptions:
                    raise error from exc

                # Stop pagination on error
                break

        self.logger.info(
            f"✅ Pagination terminée: {len(all_data)} records "
            f"sur {page_num - 1} pages"
        )

        return all_data

    @staticmethod
    def _extract_cursor(next_url: Optional[str]) -> Optional[str]:
        """Extrait le curseur Hub'Eau depuis l'URL `next` fournie par l'API."""

        if not next_url:
            return None

        try:
            parsed = urlparse(next_url)
        except ValueError:
            return None

        query = parse_qs(parsed.query)
        cursor_values = query.get("cursor")
        if not cursor_values:
            return None

        return cursor_values[0]
    
    async def get_observations(
        self,
        endpoint_name: str,
        entity_codes: List[str],
        date_partition: str,
        api_name: str = None,
        realtime: bool = False,
        partition_key: str = None
    ) -> List[Dict[str, Any]]:
        """
        ✅ Récupère observations avec pagination simple

        Plus besoin de chunking départemental ou par station grâce au bug API.
        Seuls les filtres temporels et les codes entités sont conservés (pas de chunking).

        Args:
            endpoint_name: Nom de l'endpoint observations
            entity_codes: Codes des entités (stations/BSS/ouvrages) - SANS CHUNKING
            date_partition: Date de partition (YYYY-MM-DD)
            api_name: Nom de l'API (pour entity_key)
            realtime: Mode temps réel (non utilisé maintenant)
            partition_key: Clé de partition originale (pour détecter annuel/mensuel)

        Returns:
            Liste complète des observations
        """
        if endpoint_name not in self.config.endpoints:
            self.logger.error(f"❌ Endpoint {endpoint_name} non trouvé")
            return []

        endpoint_config = self.config.endpoints[endpoint_name]

        # ✅ Paramètres de base (NO SIZE)
        params = {"format": "json"}

        # ✅ KEEP: Filtres temporels (dates needed for observations)
        if endpoint_config.temporal_params:
            try:
                date_obj = datetime.strptime(date_partition, "%Y-%m-%d")
            except ValueError:
                date_obj = datetime.fromisoformat(date_partition)

            start_key = endpoint_config.temporal_params["start"]
            end_key = endpoint_config.temporal_params["end"]

            # Handle year-based params (prelevements API uses 'annee')
            if "annee" in start_key:
                year = date_obj.year
                params[start_key] = year
                params[end_key] = year + 1
                self.logger.info(f"📅 Filtrage annuel: année {year}")
            else:
                # Standard date params
                original_key = partition_key if partition_key else date_partition
                is_yearly_partition = len(original_key) == 4  # Ex: "2024"

                # Check if yearly partition for specific APIs
                yearly_apis = [
                    "hydrobiology",
                    "superficial_waterbodies_quality",
                    "ground_water_quality",
                    "ecoulement",
                    "temperature"
                ]

                if is_yearly_partition and self.config.name in yearly_apis:
                    # Annual partition → full year
                    year = date_obj.year
                    start_dt = datetime(year, 1, 1)
                    end_dt = datetime(year + 1, 1, 1)
                    self.logger.info(
                        f"📅 Partition annuelle {original_key}: "
                        f"[{start_dt.date()} → {end_dt.date()}["
                    )
                elif self.config.name == "hydrobiology":
                    # Daily/monthly partition for hydrobio → 30-day window
                    start_dt = date_obj - timedelta(days=30)
                    end_dt = date_obj + timedelta(days=1)
                    self.logger.info(
                        f"📅 Hydrobiologie (fenêtre mobile): "
                        f"[{start_dt.date()} → {end_dt.date()}["
                    )
                else:
                    # Standard daily partition
                    start_dt = date_obj
                    effective_offset = max(endpoint_config.end_offset_days, 1)
                    end_dt = start_dt + timedelta(days=effective_offset)

                    if effective_offset > 1:
                        self.logger.info(
                            f"📅 Fenêtre temporelle: "
                            f"[{start_dt.date()} → {end_dt.date()}[ "
                            f"(offset: {effective_offset} jours)"
                        )

                params[start_key] = start_dt.strftime("%Y-%m-%d")
                params[end_key] = end_dt.strftime("%Y-%m-%d")

        # ✅ Add entity codes filter (NO CHUNKING - API handles all codes)
        if entity_codes:
            api_name_actual = api_name or self.config.name
            entity_key = self._get_entity_key_for_api(api_name_actual)

            self.logger.info(
                f"📊 Filtrage par {len(entity_codes)} {entity_key} "
                f"(sans chunking - API gère tout)"
            )

            # ✅ SIMPLE: Pass ALL codes in one param (no chunking needed)
            # The API will paginate automatically
            params[entity_key] = ",".join(entity_codes)

        # ✅ Simple fetch with unlimited pagination
        observations = await self._fetch_all_pages(endpoint_config, params)

        self.logger.info(f"✅ Total observations: {len(observations)}")

        # ✅ Update metrics
        self.metrics.records_total = len(observations)

        return observations



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



# ====================================
# MÉTRIQUES D'OBSERVABILITÉ (CORRECTIF D)
# ====================================

class IngestionMetrics(BaseModel):
    """
    Métriques d'observabilité pour l'ingestion

    ✅ Simplifiées: plus de chunking départemental/station
    """
    # Stations
    stations_total: int = 0

    # Pagination
    pages_fetched: int = 0
    records_total: int = 0

    # Erreurs
    erreurs_http_500: int = 0
    erreurs_timeout: int = 0


    def to_summary(self) -> str:
        """Génère un résumé textuel"""
        return f"""
ℹ️ Hub'Eau — Synthèse d'ingestion
- Stations: {self.stations_total}
- Pages récupérées: {self.pages_fetched}
- Records totaux: {self.records_total}
- Erreurs HTTP 500: {self.erreurs_http_500}
- Timeouts: {self.erreurs_timeout}
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
        (
            self.minio_client,
            detected_bucket,
            fallback_dir,
        ) = self._resolve_minio_client(minio_resource)
        self.local_fallback_dir = fallback_dir

        default_bucket = os.getenv("MINIO_BRONZE_BUCKET") or detected_bucket or "bronze"
        self.minio_bucket = bucket or default_bucket

        if not self.minio_bucket:
            raise ValueError("MinIO bucket must be provided through resources or environment")

    def _resolve_minio_client(
        self, minio_resource: Optional[Dict[str, Any]]
    ) -> tuple[Optional[BaseClient], Optional[str], Optional[Path]]:
        """Initialise le client MinIO via ressource Dagster ou variables d'environnement."""
        if minio_resource is not None:
            client = minio_resource.get("client")
            bucket = minio_resource.get("bucket")
            if client is None or bucket is None:
                raise ValueError("The provided MinIO resource must expose 'client' and 'bucket' keys")
            return client, bucket, None

        try:
            return self._init_minio_client(), None, None
        except Exception as exc:  # pragma: no cover - fallback tested via behaviour
            fallback_dir = self._prepare_local_fallback()
            self.logger.warning(
                "MinIO connection unavailable (%s). Falling back to local storage at %s.",
                exc,
                fallback_dir,
            )
            return None, None, fallback_dir

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
            raise

    def _prepare_local_fallback(self) -> Path:
        """Prepare a local directory used when MinIO is unavailable."""
        base_path = Path(os.getenv("HUBEAU_LOCAL_CACHE", "./data/hubeau_bronze"))
        base_path.mkdir(parents=True, exist_ok=True)
        return base_path
    




    async def ingest_api_data(
        self, 
        config: HubeauApiConfig, 
        date_partition: str,
        partition_key: str = None  # ✅ CORRECTIF: Partition originale pour détecter type
    ) -> Dict[str, Any]:
        """Ingestion complète d'une API Hub'Eau"""
        self.logger.info(f"Ingestion {config.name} pour {partition_key or date_partition}")
        
        # Pas de logique spéciale pour l'API écoulement - utilise la logique normale
        # Les campagnes sont récupérées comme endpoint normal
        
        results: Dict[str, Any] = {}
        total_records = 0
        errors: List[str] = []
        metrics_snapshot: Optional[IngestionMetrics] = None

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
                    message = f"Erreur stations {stations_endpoint}: {e}"
                    self.logger.error(message)
                    errors.append(message)
            
            # Récupérer les observations
            observations_endpoints = self._get_observations_endpoints(config)
            for endpoint_name in observations_endpoints:
                try:
                    # Extraire les codes de stations avec le bon champ selon l'API
                    if stations:
                        entity_field = self._get_entity_key_for_api(config.name)
                        station_codes = []
                        for station in stations:
                            code = station.get(entity_field, "")

                            # Champs de secours pour certaines APIs
                            if not code:
                                if config.name == "hydrometry":
                                    code = station.get("code_entite") or station.get("code_station")
                                elif config.name in {"piezometry", "ground_water_quality"}:
                                    code = station.get("code_bss") or station.get("bss_id")
                                elif config.name == "hydrobiology":
                                    code = station.get("code_station_hydrobio")
                                elif config.name == "prelevements":
                                    code = station.get("code_ouvrage")
                                else:
                                    code = station.get("code_station")

                            if code and isinstance(code, str) and code.strip():
                                station_codes.append(code.strip())

                        if not station_codes:
                            self.logger.warning(f"⚠️ Aucun code station valide trouvé pour {endpoint_name}")
                    else:
                        station_codes = []

                    observations = await client.get_observations(
                        endpoint_name,
                        station_codes,
                        date_partition,
                        config.name,
                        realtime=False,
                        partition_key=partition_key  # ✅ CORRECTIF: Passer partition originale
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
                    message = f"Erreur observations {endpoint_name}: {e}"
                    self.logger.error(message)
                    errors.append(message)

            metrics_snapshot = client.metrics

        # Sauvegarder les données dans MinIO/local même lorsqu'il n'y a pas de données
        try:
            self._save_to_minio(config.name, date_partition, results, metrics_snapshot)
        except Exception as e:
            warning_message = f"Erreur sauvegarde MinIO: {e}"
            self.logger.warning(warning_message)
            errors.append(warning_message)

        # ✅ CORRECTIF D: Afficher synthèse métriques
        if config.name == "hydrobiology" and metrics_snapshot is not None:
            self.logger.info(metrics_snapshot.to_summary())

        if errors and total_records == 0:
            status = 'error'
        elif errors:
            status = 'partial_success'
        elif total_records > 0:
            status = 'success'
        else:
            status = 'no_data'

        metrics_dict = metrics_snapshot.model_dump() if metrics_snapshot is not None else {}

        return {
            'execution_date': datetime.now().isoformat(),
            'partition_date': date_partition,
            'api_name': config.name,
            'total_records_ingested': total_records,
            'results_by_endpoint': results,
            'status': status,
            'metrics': metrics_dict,  # ✅ CORRECTIF D
            'errors': errors,
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
            "hydrometry": ["observations_tr", "obs_elab", "referentiel_sites", "referentiel_stations"],
            "piezometry": ["chroniques_tr", "chroniques"],
            "superficial_waterbodies_quality": ["analyse_pc"],  # ✅ operation_pc utilisé pour filtrage seulement
            "ground_water_quality": ["analyses"],
            "temperature": ["chronique"],
            "ecoulement": ["campagnes", "observations", "stations"],  # ✅ Campagnes en premier pour filtrage temporel
            "hydrobiology": ["indices", "taxons"],
            "prelevements": ["chroniques", "points_prelevement", "ouvrages"]
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
            "ecoulement": "code_station",
            "hydrobiology": "code_station_hydrobio",
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
        if self.minio_client is None:
            self._save_to_local(api_name, date_partition, results, metrics)
            return

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
                "endpoints_summary": {
                    endpoint_name: {
                        "records_count": endpoint_result.get('records_count', 0),
                        "has_data": endpoint_result.get('records_count', 0) > 0
                    }
                    for endpoint_name, endpoint_result in results.items()
                },
                "total_records": sum(r.get('records_count', 0) for r in results.values()),
                "metrics": metrics.model_dump() if metrics else {}  # ✅ CORRECTIF D
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

    def _save_to_local(
        self,
        api_name: str,
        date_partition: str,
        results: Dict[str, Any],
        metrics: Optional[IngestionMetrics] = None,
    ) -> None:
        """Persist data to the filesystem when MinIO is unavailable."""
        if self.local_fallback_dir is None:
            self.logger.warning("Aucun backend de stockage disponible pour %s/%s", api_name, date_partition)
            return

        base_dir = self.local_fallback_dir / api_name / date_partition
        base_dir.mkdir(parents=True, exist_ok=True)

        metadata = {
            "api_name": api_name,
            "partition_date": date_partition,
            "execution_date": datetime.now().isoformat(),
            "results_by_endpoint": results,
            "total_records": sum(r.get('records_count', 0) for r in results.values()),
            "metrics": metrics.model_dump() if metrics else {},
        }

        metadata_path = base_dir / "ingestion_metadata.json"
        metadata_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")

        for endpoint_name, endpoint_result in results.items():
            if endpoint_result.get('records_count', 0) == 0:
                continue

            data_path = base_dir / f"{endpoint_name}_data.json"
            data_path.write_text(
                json.dumps(endpoint_result.get('data', []), indent=2),
                encoding="utf-8",
            )

            endpoint_metadata = {
                "endpoint_name": endpoint_name,
                "records_count": endpoint_result.get('records_count', 0),
                "api_name": api_name,
                "partition_date": date_partition,
                "execution_date": datetime.now().isoformat(),
            }
            metadata_endpoint_path = base_dir / f"{endpoint_name}_metadata.json"
            metadata_endpoint_path.write_text(
                json.dumps(endpoint_metadata, indent=2),
                encoding="utf-8",
            )

        self.logger.info("💾 Données sauvegardées en local: %s", base_dir)
