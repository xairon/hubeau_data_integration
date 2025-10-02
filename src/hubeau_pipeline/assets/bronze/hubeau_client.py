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
    """Configuration d'un endpoint Hub'Eau"""
    path: str
    temporal_params: Optional[Dict[str, str]] = None
    spatial_params: Optional[Dict[str, str]] = None
    page_size: int = 1000
    max_pages: Optional[int] = 100  # ✅ Optional[int] pour permettre None (sans limite)
    supports_cursor: bool = False
    requires_spatial_filter: bool = False
    cache_duration: int = 30  # Jours de cache
    realtime_cache_duration: int = 15  # Minutes pour données temps réel
    depth_limit: Optional[int] = None  # Limite de profondeur pour éviter troncature (None = sans limite)
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
        """Récupère toutes les stations de TOUT LE TERRITOIRE FRANÇAIS"""
        endpoint_config = self.config.endpoints[endpoint_name]
        all_data = []
        
        # Si pas de filtre spatial requis, récupérer toutes les données
        if not endpoint_config.requires_spatial_filter:
            return await self._fetch_all_pages(endpoint_config, {"format": "json", "size": endpoint_config.page_size})
        
        # Traiter TOUT LE TERRITOIRE FRANÇAIS par chunks adaptatifs (inspiré du legacy)
        all_departments = self._get_french_departments()
        
        # Chunking adaptatif selon la profondeur limite (legacy strategy)
        depth_limit = getattr(endpoint_config, 'depth_limit', None)
        
        # ✅ CORRECTIF: Gestion spéciale pour prélèvements (sans limite)
        if endpoint_config.path == "referentiel/points_prelevement":
            chunk_size = 1  # 1 département (API sensible)
        elif self.config.name == "prelevements":
            chunk_size = 1  # ✅ Prélèvements: 1 département pour éviter la limite de 20k
        elif depth_limit is not None and depth_limit <= 10000:
            chunk_size = 1  # 1 département pour les APIs avec limite 10k (Hydrobiologie)
        else:
            chunk_size = 5  # 5 départements pour les autres APIs (ou sans limite)
        
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
        all_data: List[Dict[str, Any]] = []
        pages_fetched = 0
        cursor: Optional[str] = None
        max_pages = endpoint_config.max_pages
        page_size = params.get("size", endpoint_config.page_size)

        while True:
            if max_pages is not None and pages_fetched >= max_pages:
                break

            page_params = params.copy()
            if endpoint_config.supports_cursor:
                if cursor:
                    page_params["cursor"] = cursor
            else:
                page_params["page"] = pages_fetched + 1

            try:
                self.logger.debug(f"📄 Page {pages_fetched + 1}: requête en cours...")
                response_data = await self._make_request(endpoint_config.path, page_params)

                if not response_data.data:
                    self.logger.debug(f"📄 Page {pages_fetched + 1}: aucune donnée, arrêt de la pagination")
                    break

                all_data.extend(response_data.data)
                self.logger.debug(f"📄 Page {pages_fetched + 1}: {len(response_data.data)} records ajoutés (total: {len(all_data)})")

                pages_fetched += 1

                if endpoint_config.supports_cursor:
                    next_cursor = self._extract_cursor(response_data.next)
                    if not next_cursor or next_cursor == cursor:
                        break
                    cursor = next_cursor
                else:
                    if len(response_data.data) < page_params.get("size", page_size):
                        break

            except Exception as exc:
                self.logger.exception(
                    "Erreur lors de la récupération de la page %s pour %s",
                    pages_fetched + 1,
                    endpoint_config.path,
                )

                error = HubeauPageFetchError(
                    endpoint_config.path,
                    pages_fetched + 1,
                    page_params,
                    exc,
                )

                if bubble_exceptions:
                    raise error from exc

        # Warning de troncature seulement si max_pages défini
        if endpoint_config.max_pages is not None:
            expected_records_cap = endpoint_config.max_pages * page_size
            if pages_fetched >= endpoint_config.max_pages and len(all_data) >= expected_records_cap:
                self.logger.warning(
                    "⚠️ TRONCATURE: max_pages=%s atteint pour %s. Récupéré %s records, mais il pourrait y en avoir plus !",
                    endpoint_config.max_pages,
                    endpoint_config.path,
                    len(all_data),
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
    
    async def get_temperature_observations_yearly(
        self,
        station_code: str,
        date_partition: str,
    ) -> List[Dict[str, Any]]:
        """Récupère les observations de température mois par mois pour éviter la limite de 20k enregistrements.

        L'API Hub'Eau limite la profondeur d'accès (page * size) à 20 000 enregistrements. Certaines stations
        de température disposent d'une mesure toutes les 5 minutes, ce qui dépasse cette limite sur une année
        complète (≈100k enregistrements). Pour garantir une ingestion exhaustive, on découpe donc l'année en
        12 fenêtres mensuelles et on interroge l'endpoint `chronique` pour chacune d'elles.
        """

        endpoint_name = "chronique"
        if endpoint_name not in self.config.endpoints:
            self.logger.error("❌ Endpoint 'chronique' introuvable pour l'API température")
            return []

        endpoint_config = self.config.endpoints[endpoint_name]
        temporal_params = endpoint_config.temporal_params or {}
        start_key = temporal_params.get("start")
        end_key = temporal_params.get("end")

        if not start_key or not end_key:
            self.logger.error("❌ Configuration temporelle manquante pour l'endpoint chronique (température)")
            return []

        try:
            partition_dt = datetime.strptime(date_partition, "%Y-%m-%d")
        except ValueError:
            # Support des partitions ISO complètes (ex: "2024")
            partition_dt = datetime(int(date_partition[:4]), 1, 1)

        year = partition_dt.year
        all_data: List[Dict[str, Any]] = []

        self.logger.info(
            "🌡️ Température: récupération mensuelle pour la station %s (année %s)",
            station_code,
            year,
        )

        base_params = {"format": "json", "size": endpoint_config.page_size, "code_station": station_code}

        for month in range(1, 13):
            start_dt = datetime(year, month, 1)
            if month == 12:
                end_dt = datetime(year + 1, 1, 1)
            else:
                end_dt = datetime(year, month + 1, 1)

            month_params = base_params.copy()
            month_params[start_key] = start_dt.strftime("%Y-%m-%d")
            month_params[end_key] = end_dt.strftime("%Y-%m-%d")

            self.logger.debug(
                "🌡️ Température: station %s, mois %02d → fenêtre [%s → %s[",
                station_code,
                month,
                month_params[start_key],
                month_params[end_key],
            )

            month_data = await self._fetch_all_pages(endpoint_config, month_params)
            all_data.extend(month_data)

            if month_data:
                self.logger.info(
                    "✅ Température: %s observations pour %s-%02d",
                    len(month_data),
                    year,
                    month,
                )

        self.logger.info(
            "🎯 Température: %s observations totales récupérées pour %s (%s)",
            len(all_data),
            station_code,
            year,
        )

        return all_data

    async def get_observations(
        self,
        endpoint_name: str,
        entity_codes: List[str],
        date_partition: str,
        api_name: str = None,
        realtime: bool = False,
        partition_key: str = None  # ✅ CORRECTIF: Partition originale pour détecter annuelles/mensuelles
    ) -> List[Dict[str, Any]]:
        """Récupère les observations pour une date donnée"""
        # ✅ CORRECTIF: Vérifier que l'endpoint existe
        if endpoint_name not in self.config.endpoints:
            self.logger.error(f"❌ Endpoint {endpoint_name} non trouvé dans la configuration")
            return []
        
        endpoint_config = self.config.endpoints[endpoint_name]
        all_data = []
        
        # ✅ OPTIMISATION GÉNÉRIQUE: Filtrage intelligent par endpoint léger
        # Permet de ne requêter que les stations/opérations ayant des données à la date
        # Note: Désactivé pour partitions annuelles (on récupère toute l'année d'un coup)
        original_key = partition_key if partition_key else date_partition
        is_yearly_partition = len(original_key) == 4
        
        if not is_yearly_partition:
            entity_codes = await self._smart_filter_entities(
                endpoint_name, 
                entity_codes, 
                date_partition,
                api_name or self.config.name,
                partition_key=partition_key
            )
            if not entity_codes:
                self.logger.info("ℹ️ Aucune entité active pour cette date après filtrage intelligent")
                return []
        else:
            self.logger.info(
                f"ℹ️ Partition annuelle détectée ({original_key}): "
                f"filtrage intelligent désactivé (récupération annuelle complète)"
            )
        
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
                # Les API Hub'Eau attendent une fenêtre inclusive-exclusive
                params[end_key] = year + 1
            else:
                # Format standard pour les autres APIs avec gestion de borne fin exclusive
                end_offset = getattr(endpoint_config, 'end_offset_days', 0)

                # ✅ CORRECTIF: Détecter les partitions annuelles via partition_key originale
                # Pour qualité eau et hydrobiologie avec partitions annuelles
                original_key = partition_key if partition_key else date_partition
                is_yearly_partition = len(original_key) == 4  # Ex: "2024"
                
                if is_yearly_partition and self.config.name in ["hydrobiology", "superficial_waterbodies_quality", "ground_water_quality", "ecoulement", "temperature"]:
                    # Partition annuelle → récupérer toute l'année
                    year = date_obj.year
                    start_dt = datetime(year, 1, 1)
                    end_dt = datetime(year + 1, 1, 1)  # Fin exclusive = début année suivante
                    self.logger.info(
                        f"📅 {self.config.name.upper()} (partition annuelle {original_key}): "
                        f"fenêtre annuelle [{start_dt.date()} → {end_dt.date()}["
                    )
                elif self.config.name == "hydrobiology":
                    # Partition quotidienne/mensuelle (legacy) → fenêtre de 30 jours
                    start_dt = date_obj - timedelta(days=30)
                    end_dt = date_obj + timedelta(days=1)
                    self.logger.info(f"📅 Hydrobiologie (campagnes saisonnières): fenêtre de 30 jours [{start_dt} → {end_dt}[")
                else:
                    start_dt = date_obj

                    # Plusieurs endpoints Hub'Eau attendent une borne fin strictement supérieure
                    # à la borne début. Lorsque aucun décalage n'est configuré (offset=0),
                    # on traite la partition journalière comme [J, J+1[ afin d'éviter
                    # des fenêtres vides provoquant des erreurs HTTP 500 côté Hub'Eau.
                    effective_offset = end_offset if end_offset and end_offset > 0 else 1
                    end_dt = start_dt + timedelta(days=effective_offset)

                    if effective_offset != end_offset:
                        self.logger.debug(
                            "🔁 Offset temporel ajusté pour %s: fin=%s (offset=%s)",
                            endpoint_name,
                            end_dt.strftime("%Y-%m-%d"),
                            effective_offset,
                        )
                    elif end_offset > 0:
                        self.logger.info(
                            f"📅 Fenêtre temporelle: [{start_dt} → {end_dt}[ (borne fin exclusive)"
                        )

                params[start_key] = start_dt.strftime("%Y-%m-%d")
                params[end_key] = end_dt.strftime("%Y-%m-%d")
        
        # APPROCHE ADAPTÉE : Vérifier d'abord la configuration de l'endpoint
        # Priorité 1 : Si l'endpoint demande un filtre spatial → utiliser approche spatiale
        # Priorité 2 : Si codes d'entités disponibles → utiliser approche par codes
        # Priorité 3 : Récupération directe sans filtre
        
        if endpoint_config.requires_spatial_filter and endpoint_config.spatial_params:
            # APPROCHE SPATIALE (départements)
            all_departments = self._get_french_departments()

            # ✅ Stratégie de chunking adaptative selon l'API
            depth_limit = getattr(endpoint_config, 'depth_limit', None)
            
            # Déterminer chunk_size selon l'API et la configuration
            if self.config.name == "prelevements":
                chunk_size = 1  # ✅ Prélèvements: 1 département pour éviter la limite de 20k
            elif self.config.name == "temperature":
                chunk_size = 1  # ✅ Température: 1 département pour respecter limite 20k (selon doc officielle)
            elif self.config.name == "hydrobiology":
                chunk_size = 1  # Hydrobiologie: 1 département pour éviter les 500
            elif self.config.name == "superficial_waterbodies_quality":
                chunk_size = 1  # ✅ Qualité cours d'eau: 1 département (API très sensible aux erreurs 400)
            elif self.config.name == "ground_water_quality":
                chunk_size = 1  # ✅ Qualité nappes: 1 département (API très sensible aux erreurs 400)
            elif self.config.name == "onde":
                chunk_size = 5  # ✅ ONDE: 5 départements (approche départementale comme cl-hubeau)
            elif depth_limit is not None and depth_limit <= 10000:
                chunk_size = 1  # 1 département pour les APIs avec limite 10k
            else:
                chunk_size = 5  # 5 départements pour les autres APIs
            
            dept_chunks = [all_departments[i:i + chunk_size] for i in range(0, len(all_departments), chunk_size)]
            
            self.logger.info(f"🌍 Récupération observations {endpoint_name} par départements (parallélisée)")
            self.logger.info(f"📊 Découpage spatial: {len(all_departments)} départements en {len(dept_chunks)} groupes (chunk_size={chunk_size})")
            
            # Parallélisation adaptative selon l'API
            if self.config.name == "prelevements" and chunk_size == 1:
                MAX_CONCURRENT_SPATIAL = 15  # ✅ Prélèvements: parallélisme élevé (1 dept/requête)
            elif self.config.name == "temperature":
                MAX_CONCURRENT_SPATIAL = 8   # ✅ Température: parallélisme modéré (1 dept/requête, API sensible)
            elif self.config.name == "hydrobiology":
                MAX_CONCURRENT_SPATIAL = 4   # Hydrobiologie: conservateur (API sensible)
            elif self.config.name == "superficial_waterbodies_quality":
                MAX_CONCURRENT_SPATIAL = 3   # ✅ Qualité cours d'eau: parallélisme très conservateur (API très sensible)
            elif self.config.name == "ground_water_quality":
                MAX_CONCURRENT_SPATIAL = 3   # ✅ Qualité nappes: parallélisme très conservateur (API très sensible)
            elif self.config.name == "onde":
                MAX_CONCURRENT_SPATIAL = 6   # ✅ ONDE: parallélisme modéré (5 depts/requête)
            else:
                MAX_CONCURRENT_SPATIAL = 10  # Défaut
            
            semaphore = asyncio.Semaphore(MAX_CONCURRENT_SPATIAL)
            self.logger.info(f"⚡ Parallélisme: {MAX_CONCURRENT_SPATIAL} requêtes simultanées (optimisé pour chunk_size={chunk_size})")

            spatial_param_key = self._resolve_spatial_param(endpoint_config)

            async def fetch_dept_chunk(i, dept_chunk):
                async with semaphore:
                    try:
                        chunk_params = params.copy()
                        chunk_params[spatial_param_key] = ",".join(dept_chunk)

                        chunk_data = await self._fetch_all_pages(endpoint_config, chunk_params)
                        
                        # Log adaptatif selon chunk_size
                        if len(dept_chunk) == 1:
                            self.logger.info(f"✅ Département {dept_chunk[0]} ({i+1}/{len(dept_chunks)}): {len(chunk_data)} observations")
                        else:
                            self.logger.info(f"✅ Groupe {i+1}/{len(dept_chunks)}: {len(chunk_data)} observations")
                        return chunk_data
                    except Exception as e:
                        # Fallback : si le chunk multi-départements échoue, réessayer département par département
                        # Note: Si chunk_size=1 déjà, pas de fallback possible
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
                            # chunk_size=1 : pas de fallback, juste logger et continuer
                            self.logger.warning(f"⚠️ Département {dept_chunk[0]} ignoré: {str(e)}")
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
                # Hydrométrie observations_tr : beaucoup de données par station → chunks réduits
                # Température : API sensible aux URL longues → chunks réduits
                # ONDE : API EXTRÊMEMENT sensible aux erreurs 500 → chunks minimalistes
                if api_name_actual == "hydrobiology":
                    MAX_CODES_PER_REQUEST = 25
                elif api_name_actual == "hydrometry" and endpoint_name == "observations_tr":
                    MAX_CODES_PER_REQUEST = 25  # ✅ CORRECTIF: Réduire pour éviter surcharge
                elif api_name_actual == "temperature":
                    MAX_CODES_PER_REQUEST = 1  # ✅ CORRECTIF: 1 station à la fois pour respecter limite 20k
                elif api_name_actual == "onde":
                    MAX_CODES_PER_REQUEST = 3  # ✅ CORRECTIF: Réduction drastique - API très instable
                else:
                    MAX_CODES_PER_REQUEST = 50
                
                if len(entity_codes) > MAX_CODES_PER_REQUEST:
                    # Découpage systématique en chunks avec parallélisation
                    entity_chunks = [entity_codes[i:i + MAX_CODES_PER_REQUEST] for i in range(0, len(entity_codes), MAX_CODES_PER_REQUEST)]
                    self.logger.info(f"📦 Découpage en {len(entity_chunks)} chunks de {MAX_CODES_PER_REQUEST} (évite URL trop longue)")
                    
                    # Parallélisation avec asyncio.gather() pour accélérer l'ingestion
                    # Hydrobiologie : API sensible → parallélisme réduit
                    # Hydrométrie observations_tr : beaucoup de chunks → parallélisme modéré
                    # ONDE : API EXTRÊMEMENT sensible → parallélisme minimal
                    if api_name_actual == "hydrobiology":
                        MAX_CONCURRENT = 4
                    elif api_name_actual == "hydrometry" and endpoint_name == "observations_tr":
                        MAX_CONCURRENT = 8  # ✅ CORRECTIF: Parallélisme modéré pour ~245 chunks
                    elif api_name_actual == "onde":
                        MAX_CONCURRENT = 2  # ✅ CORRECTIF: Parallélisme minimal - API très instable
                    else:
                        MAX_CONCURRENT = 15
                    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
                    self.logger.info(f"⚡ Parallélisme: {MAX_CONCURRENT} requêtes simultanées")
                    
                    async def _execute_chunk(chunk: List[str]) -> List[Dict[str, Any]]:
                        async with semaphore:
                            chunk_params = params.copy()
                            chunk_params[entity_key] = ",".join(chunk)
                            return await self._fetch_all_pages(
                                endpoint_config,
                                chunk_params,
                                bubble_exceptions=True,
                            )

                    async def fetch_chunk(i, chunk: List[str], depth: int = 0) -> List[Dict[str, Any]]:
                        # Chaque tentative (y compris les splits) est comptabilisée
                        self.metrics.chunks_total += 1

                        try:
                            chunk_data = await _execute_chunk(chunk)
                        except Exception as e:
                            # ✅ CORRECTIF D: Tracking erreurs par type avec plus de détails
                            message = str(e)
                            if "500" in message:
                                self.metrics.erreurs_http_500 += 1
                                self.logger.warning(
                                    f"⚠️ Erreur HTTP 500 détectée dans chunk {i + 1} "
                                    f"(département: {chunk[0] if chunk else 'N/A'}): {message[:100]}..."
                                )
                            elif "timeout" in message.lower():
                                self.metrics.erreurs_timeout += 1
                                self.logger.warning(
                                    f"⚠️ Timeout détecté dans chunk {i + 1} "
                                    f"(département: {chunk[0] if chunk else 'N/A'}): {message[:100]}..."
                                )

                            if len(chunk) > 1:
                                self.logger.warning(
                                    "⚠️ Chunk %s erreur (%s...), découpage en 2...",
                                    i + 1,
                                    message[:50],
                                )
                                mid = len(chunk) // 2
                                left = await fetch_chunk(i, chunk[:mid], depth + 1)
                                right = await fetch_chunk(i, chunk[mid:], depth + 1)
                                return (left or []) + (right or [])

                            # ✅ CORRECTIF D: Logger les codes fautifs pour observabilité avec plus de contexte
                            self.metrics.chunks_echoues += 1
                            if chunk:
                                self.metrics.codes_echoues.append(chunk[0])
                                self.logger.error(
                                    "❌ Chunk %s échoué définitivement (département: %s, erreur: %s)",
                                    i + 1,
                                    chunk[0],
                                    message[:200],  # Plus de détails sur l'erreur
                                )
                            else:
                                self.logger.error(
                                    "❌ Chunk %s échoué définitivement: %s",
                                    i + 1,
                                    message[:200],
                                )
                            return []

                        if len(chunk_data) == 0:
                            self.metrics.chunks_vides += 1
                            self.metrics.stations_sans_donnees.extend(chunk)
                        else:
                            self.metrics.chunks_ok += 1

                        if depth == 0 and (i % 50 == 0 or i == len(entity_chunks) - 1):
                            self.logger.info(f"✅ Traité {i+1}/{len(entity_chunks)} chunks")

                        return chunk_data
                    
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
    
    async def _smart_filter_entities(
        self,
        target_endpoint: str,
        entity_codes: List[str],
        date_partition: str,
        api_name: str,
        partition_key: str = None
    ) -> List[str]:
        """
        ✅ OPTIMISATION GÉNÉRIQUE: Filtre intelligent des entités (stations/opérations)
        en interrogeant d'abord un endpoint léger pour identifier celles avec données
        
        Configuration par API:
        - ONDE: campagnes → observations (filtrage par stations actives)
        - Qualité Cours d'Eau: operation_pc → analyse_pc (filtrage par opérations actives)
        - Autres APIs: pas de filtrage (retourne toutes les entités)
        """
        
        # Configuration des règles de filtrage par API
        # Note: Filtrage intelligent désactivé pour TOUTES les APIs maintenant
        # car toutes les APIs de campagnes utilisent des partitions annuelles
        filter_rules = {
            # ONDE : Partitions annuelles maintenant, pas besoin de filtrage
            # "superficial_waterbodies_quality": Partitions annuelles, pas besoin de filtrage
        }
        
        # Vérifier si cette API/endpoint a une règle de filtrage
        rule = filter_rules.get(api_name)
        if not rule or rule["target_endpoint"] != target_endpoint:
            # Pas de filtrage configuré → retourner toutes les entités
            return entity_codes
        
        try:
            # Parser la date
            date_obj = datetime.strptime(date_partition, "%Y-%m-%d")
            
            # ✅ CORRECTIF: Pour partitions mensuelles, utiliser tout le mois (pas ±jours)
            original_key = partition_key if partition_key else date_partition
            is_monthly_partition = len(original_key) == 7  # Ex: "2024-09"
            
            if is_monthly_partition:
                # Partition mensuelle → fenêtre = mois complet
                year = date_obj.year
                month = date_obj.month
                start_date = datetime(year, month, 1).strftime("%Y-%m-%d")
                # Dernier jour du mois
                if month == 12:
                    end_date = datetime(year + 1, 1, 1).strftime("%Y-%m-%d")
                else:
                    end_date = datetime(year, month + 1, 1).strftime("%Y-%m-%d")
                self.logger.info(
                    f"🔍 Partition mensuelle {original_key}: fenêtre mois complet [{start_date} → {end_date}["
                )
            else:
                # Partition quotidienne → fenêtre ±jours
                time_window = rule["time_window_days"]
                start_date = (date_obj - timedelta(days=time_window)).strftime("%Y-%m-%d")
                end_date = (date_obj + timedelta(days=time_window)).strftime("%Y-%m-%d")
            
            # Récupérer l'endpoint de filtrage
            filter_endpoint_name = rule["filter_endpoint"]
            filter_endpoint = self.config.endpoints.get(filter_endpoint_name)
            
            if not filter_endpoint:
                self.logger.warning(
                    f"⚠️ Endpoint '{filter_endpoint_name}' non trouvé pour filtrage, "
                    f"requête de toutes les entités"
                )
                return entity_codes
            
            # Construire les paramètres temporels SANS filtrer par entités
            # (on veut TOUTES les campagnes/opérations de la période, pas juste celles de nos stations)
            temporal_params = rule["temporal_params"]
            params = {
                "format": "json",
                "size": filter_endpoint.page_size,
                temporal_params["start"]: start_date,
                temporal_params["end"]: end_date
            }
            
            self.logger.info(
                f"🔍 {api_name.upper()}: Filtrage intelligent via {rule['description']} "
                f"entre {start_date} et {end_date} (récupération TOUTES les {rule['description']})"
            )
            
            # Récupérer TOUTES les données de filtrage de la période
            # Note: On ne filtre PAS par code_station ici, on veut tout !
            filter_data = await self._fetch_all_pages(filter_endpoint, params)
            
            if not filter_data:
                self.logger.warning(
                    f"⚠️ Aucune {rule['description']} trouvée pour {start_date} → {end_date}"
                )
                return []
            
            # Extraire les codes d'entités uniques
            entity_field = rule["entity_field"]
            active_entities = set()
            for item in filter_data:
                entity_code = item.get(entity_field)
                if entity_code:
                    active_entities.add(entity_code)
            
            # Filtrer les entités disponibles
            filtered_codes = [code for code in entity_codes if code in active_entities]
            
            reduction_pct = (1 - len(filtered_codes) / len(entity_codes)) * 100 if entity_codes else 0
            
            self.logger.info(
                f"📊 Filtrage {rule['description']}: {len(filter_data)} {rule['description']} "
                f"→ {len(active_entities)} entités actives "
                f"→ {len(filtered_codes)}/{len(entity_codes)} à requêter "
                f"(-{reduction_pct:.1f}% de requêtes)"
            )
            
            return filtered_codes
            
        except Exception as e:
            self.logger.warning(
                f"⚠️ Erreur lors du filtrage intelligent ({api_name}): {e}, "
                f"requête de toutes les entités"
            )
            return entity_codes

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
    
    async def _ingest_ecoulement_with_campagnes(
        self, 
        config: HubeauApiConfig, 
        date_partition: str,
        partition_key: str = None
    ) -> Dict[str, Any]:
        """Ingestion spéciale pour l'API écoulement utilisant les campagnes pour filtrer les observations"""
        self.logger.info(f"🌊 Ingestion écoulement avec campagnes pour {partition_key or date_partition}")
        
        results: Dict[str, Any] = {}
        total_records = 0
        errors: List[str] = []
        
        async with HubeauClient(config) as client:
            # 1. Récupérer les stations
            try:
                stations = await client.get_stations("stations")
                results["stations"] = {
                    'records_count': len(stations),
                    'type': 'stations',
                    'data': stations
                }
                total_records += len(stations)
                self.logger.info(f"✅ Stations récupérées: {len(stations)}")
            except Exception as e:
                message = f"Erreur stations: {e}"
                self.logger.error(message)
                errors.append(message)
                stations = []
            
            # 2. Récupérer les campagnes de l'année
            try:
                # Utiliser l'année complète pour récupérer toutes les campagnes
                year = partition_key if partition_key else date_partition[:4]
                campagnes = await client.get_observations(
                    "campagnes", 
                    [],  # Pas besoin de codes stations pour les campagnes
                    f"{year}-01-01",  # Début d'année
                    config.name,
                    realtime=False,
                    partition_key=partition_key
                )
                
                results["campagnes"] = {
                    'records_count': len(campagnes),
                    'type': 'campagnes',
                    'data': campagnes
                }
                total_records += len(campagnes)
                self.logger.info(f"✅ Campagnes récupérées: {len(campagnes)}")
                
                # 3. Utiliser les dates des campagnes pour récupérer les observations
                if campagnes:
                    # Extraire les dates des campagnes (API écoulement utilise 'date_campagne')
                    campagne_dates = []
                    for campagne in campagnes:
                        date_campagne = campagne.get('date_campagne')
                        if date_campagne:
                            # Pour l'API écoulement, chaque campagne a une seule date
                            campagne_dates.append(date_campagne)
                    
                    self.logger.info(f"📅 Dates de campagnes trouvées: {len(campagne_dates)} dates")
                    
                    # STRATÉGIE ALTERNATIVE: L'API écoulement ignore les paramètres temporels
                    # Récupérer les observations par dates de campagnes individuelles
                    self.logger.info(f"📊 Récupération des observations par dates de campagnes ({len(campagne_dates)} dates)")
                    
                    observations = []
                    campagne_dates_set = set(campagne_dates)
                    
                    # Traiter par chunks de dates pour éviter trop de requêtes
                    chunk_size = 50  # 50 dates par chunk
                    for i in range(0, len(campagne_dates), chunk_size):
                        chunk_dates = campagne_dates[i:i + chunk_size]
                        self.logger.info(f"🔍 Chunk {i//chunk_size + 1}: traitement de {len(chunk_dates)} dates")
                        
                        for date_campagne in chunk_dates:
                            try:
                                # Récupérer les observations pour cette date spécifique
                                date_obs = await client.get_observations(
                                    "observations",
                                    [],  # Pas de filtrage par station
                                    date_campagne,  # Date spécifique
                                    config.name,
                                    realtime=False,
                                    partition_key=partition_key
                                )
                                
                                if date_obs:
                                    observations.extend(date_obs)
                                    
                            except Exception as e:
                                self.logger.warning(f"⚠️ Erreur pour la date {date_campagne}: {e}")
                        
                        self.logger.info(f"✅ Chunk {i//chunk_size + 1} terminé: {len(observations)} observations total")
                    
                    self.logger.info(f"🔍 Observations récupérées: {len(observations)} au total")
                    
                    results["observations"] = {
                        'records_count': len(observations),
                        'type': 'observations',
                        'data': observations
                    }
                    total_records += len(observations)
                    self.logger.info(f"✅ Observations récupérées: {len(observations)}")
                else:
                    self.logger.warning("⚠️ Aucune campagne trouvée, pas d'observations récupérées")
                    results["observations"] = {
                        'records_count': 0,
                        'type': 'observations',
                        'data': []
                    }
                    
            except Exception as e:
                message = f"Erreur campagnes/observations: {e}"
                self.logger.error(message)
                errors.append(message)
        
        # Sauvegarder dans MinIO
        try:
            self._save_to_minio(config.name, partition_key or date_partition, results)
            self.logger.info(f"💾 Données écoulement sauvegardées dans MinIO")
        except Exception as e:
            message = f"Erreur sauvegarde MinIO: {e}"
            self.logger.error(message)
            errors.append(message)
        
        return {
            "execution_date": datetime.now().isoformat(),
            "partition_date": partition_key or date_partition,
            "api_name": config.name,
            "status": "success" if not errors else "error",
            "total_records_ingested": total_records,
            "results_by_endpoint": results,
            "errors": errors
        }
    
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
                    
                    # ✅ LOGIQUE SPÉCIALE TEMPÉRATURE: Traitement par station pour respecter limite 20k
                    if config.name == "temperature" and station_codes:
                        self.logger.info(f"🌡️ Température: Traitement de {len(station_codes)} stations une par une")
                        all_observations = []

                        for i, station_code in enumerate(station_codes):
                            try:
                                self.logger.debug(f"🌡️ Station {i+1}/{len(station_codes)}: {station_code}")

                                original_key = partition_key if partition_key else date_partition
                                is_yearly_partition = len(original_key) == 4

                                if is_yearly_partition:
                                    station_observations = await client.get_temperature_observations_yearly(
                                        station_code,
                                        date_partition,
                                    )
                                else:
                                    station_observations = await client.get_observations(
                                        endpoint_name,
                                        [station_code],
                                        date_partition,
                                        config.name,
                                        partition_key=partition_key
                                    )

                                all_observations.extend(station_observations)
                                self.logger.debug(
                                    "🌡️ Station %s: %s observations cumulées",
                                    station_code,
                                    len(station_observations),
                                )
                            except Exception as e:
                                self.logger.warning(f"⚠️ Erreur station {station_code}: {e}")
                                continue

                        results[endpoint_name] = {
                            'records_count': len(all_observations),
                            'type': 'observations',
                            'data': all_observations
                        }
                        total_records += len(all_observations)
                        self.logger.info(f"✅ Température: {len(all_observations)} observations récupérées")
                        continue  # Passer au prochain endpoint
                    
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
