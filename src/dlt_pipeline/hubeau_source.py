"""
Source DLT Hybride pour Hub'Eau API - Version 2.0

Architecture:
- Utilise les primitives natives DLT (RESTClient, @dlt.source, @dlt.resource)
- Ajoute la logique métier Hub'Eau (slicing, chunking, filtrage stations)
- Format de configuration standard et extensible

Usage:
    from dlt_pipeline.hubeau_source import hubeau_rest_source

    source = hubeau_rest_source(
        config_path="configs/hubeau/piezometry_stations.yml",
        stations_data=None,
        partition_date=None
    )

    pipeline.run(source)
"""

from typing import Iterator, Optional, Dict, Any, List
from datetime import datetime, date, timedelta
from pathlib import Path
import yaml
import logging
import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator
from dlt.sources.helpers.requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configure logger pour Hub'Eau source
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# ============================================
# DATE UTILITIES
# ============================================

def get_month_end_date(month_str: str) -> str:
    """
    Calcule le dernier jour d'un mois donné.

    Args:
        month_str: Format "YYYY-MM" (ex: "2024-02")

    Returns:
        Date fin de mois format "YYYY-MM-DD" (ex: "2024-02-29")

    Examples:
        >>> get_month_end_date("2024-02")
        "2024-02-29"
        >>> get_month_end_date("2024-04")
        "2024-04-30"
        >>> get_month_end_date("2024-12")
        "2024-12-31"
    """
    import calendar
    year, month = map(int, month_str.split('-'))
    last_day = calendar.monthrange(year, month)[1]
    return f"{year:04d}-{month:02d}-{last_day:02d}"


# ============================================
# CHARGEMENT ET VALIDATION CONFIG
# ============================================

def load_hubeau_config(config_path: str) -> Dict[str, Any]:
    """
    Charge et valide la configuration Hub'Eau depuis YAML.

    Gère les chemins relatifs et absolus, avec support Docker.

    Args:
        config_path: Chemin vers le fichier YAML de configuration

    Returns:
        Dict contenant la configuration complète

    Raises:
        FileNotFoundError: Si le fichier n'existe pas
        yaml.YAMLError: Si le fichier n'est pas un YAML valide
    """
    config_file = Path(config_path)

    if not config_file.is_absolute():
        # Try /app for Docker, fallback to current directory
        docker_path = Path("/app") / config_path
        if docker_path.exists():
            full_path = docker_path
        else:
            full_path = Path.cwd() / config_path
    else:
        full_path = config_file

    with open(full_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)

    # Validation basique
    if 'resource' not in config:
        raise ValueError(f"Config {config_path} must contain 'resource' section")

    if 'endpoint' not in config['resource']:
        raise ValueError(f"Config {config_path} must contain 'resource.endpoint'")

    return config


# ============================================
# SESSION HTTP CONFIGURÉE
# ============================================

def create_hubeau_session(performance_config: Dict[str, Any]) -> Session:
    """
    Crée une session HTTP avec retry strategy et rate limiting.

    Args:
        performance_config: Configuration de performance depuis YAML

    Returns:
        Session configurée avec retry et timeout
    """
    session = Session()

    # Configuration retry avec backoff exponentiel
    retry_times = performance_config.get('retry_times', 3)
    retry_strategy = Retry(
        total=retry_times,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"]
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    # Timeout par défaut
    timeout = performance_config.get('timeout', 30)
    session.timeout = timeout

    return session


# ============================================
# PAGINATOR HUBEAU
# ============================================

def create_hubeau_paginator(pagination_config: Dict[str, Any]) -> PageNumberPaginator:
    """
    Crée un paginator configuré pour Hub'Eau API.

    Hub'Eau utilise la pagination par numéro de page avec:
    - Paramètre 'page' pour le numéro de page (commence à 1 par défaut)
    - Champ 'last_page' dans la réponse pour le nombre total de pages

    Args:
        pagination_config: Configuration de pagination depuis YAML

    Returns:
        PageNumberPaginator configuré pour Hub'Eau
    
    Note:
        Selon la documentation DLT, PageNumberPaginator ne supporte que:
        - total_path: chemin JSON vers le nombre total de pages
        - maximum_page: nombre maximum de pages (optionnel)
        La page initiale est toujours 1 par défaut dans DLT.
    """
    total_path = pagination_config.get('total_path', 'last_page')

    return PageNumberPaginator(
        total_path=total_path
    )


# ============================================
# REST CLIENT HUBEAU
# ============================================

def create_hubeau_client(config: Dict[str, Any]) -> RESTClient:
    """
    Crée un RESTClient DLT configuré pour Hub'Eau.

    Note: On n'utilise PAS de paginator automatique car Hub'Eau commence
    les pages à 1 (pas à 0), ce qui nécessite une pagination manuelle.

    Args:
        config: Configuration complète depuis YAML

    Returns:
        RESTClient configuré avec session et base_url (sans paginator)
    """
    resource_config = config['resource']
    performance_config = config.get('performance', {})

    base_url = resource_config.get('base_url', 'https://hubeau.eaufrance.fr/api')

    # Créer session avec retry
    session = create_hubeau_session(performance_config)

    # Créer REST client SANS paginator (pagination manuelle)
    return RESTClient(
        base_url=base_url,
        paginator=None,  # Pas de paginator automatique
        session=session
    )


# ============================================
# EXTRACTION DE RECORDS
# ============================================

def extract_records(
    page_data: Any,
    extraction_config: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """
    Extrait les records depuis la réponse API selon records_path.

    Supporte différents formats de réponse:
    - {"data": [...]} (format Hub'Eau standard)
    - [...] (array direct)
    - {...} (objet unique)

    Args:
        page_data: Données de la page depuis l'API
        extraction_config: Configuration d'extraction avec records_path

    Returns:
        Liste de records extraits
    """
    records_path = extraction_config.get('records_path', '$.data')

    if records_path == '$.data' and isinstance(page_data, dict):
        return page_data.get('data', [])
    elif isinstance(page_data, list):
        return page_data
    elif isinstance(page_data, dict):
        # Essayer d'extraire selon le path
        if '.' in records_path:
            # Navigation dans un path imbriqué
            parts = records_path.replace('$', '').strip('.').split('.')
            result = page_data
            for part in parts:
                if isinstance(result, dict):
                    result = result.get(part, [])
            return result if isinstance(result, list) else [result]
        return [page_data]
    else:
        return []


def get_primary_keys(config: Dict[str, Any]) -> List[str]:
    """
    Extrait les clés primaires depuis la configuration.

    Args:
        config: Configuration complète depuis YAML

    Returns:
        Liste des champs clés primaires (peut être vide)
    """
    resource_config = config['resource']
    primary_keys = resource_config.get('primary_key', [])

    # Normaliser en liste
    if isinstance(primary_keys, str):
        return [primary_keys]

    return primary_keys if primary_keys else []


# ============================================
# STRATÉGIES DE SLICING
# ============================================

def slice_global(
    client: RESTClient,
    endpoint: str,
    extraction_config: Dict[str, Any]
) -> Iterator[Dict[str, Any]]:
    """
    Stratégie global: une seule requête paginée sans découpage.

    Utilisé pour les endpoints avec peu de données ou sans besoin de filtrage.
    Implémente la pagination manuelle car Hub'Eau commence à page=1 (pas 0).

    Args:
        client: RESTClient DLT
        endpoint: Chemin de l'endpoint (ex: /stations)
        extraction_config: Configuration d'extraction

    Yields:
        Records extraits avec métadonnée _slice_mode
    """
    logger.info(f"🌍 Starting GLOBAL slicing for endpoint: {endpoint}")

    pagination_config = extraction_config.get('pagination', {})
    # No page_size - exploit API bug

    # Construire les paramètres de base
    params = {
        'format': 'json',
        # No size parameter
    }

    # Ajouter les paramètres par défaut
    default_params = extraction_config.get('default_params', {})
    params.update(default_params)

    logger.info(f"📄 Pagination config: page_size={page_size}, params={params}")

    # Pagination manuelle (Hub'Eau commence à page=1)
    page = 1
    total_records = 0
    api_total_count = None

    while True:
        # Ajouter le numéro de page
        page_params = {**params, 'page': page}

        # Faire la requête
        logger.info(f"📡 HTTP GET {client.base_url}{endpoint} page={page}/{page_params}")
        response = client.get(endpoint, params=page_params)
        logger.info(f"✅ Response: status={response.status_code}, content-length={len(response.content)} bytes")

        page_data = response.json()

        # Récupérer le count total annoncé par l'API (première page seulement)
        if page == 1 and 'count' in page_data:
            api_total_count = page_data.get('count')
            logger.info(f"📊 API announced total: {api_total_count} records")

        # Extraire les records
        records = extract_records(page_data, extraction_config)
        record_count = len(records)
        total_records += record_count

        # Si pas de records, on arrête
        if not records:
            logger.info(f"⚠️ No records on page {page}, stopping pagination")
            break

        logger.info(f"📊 Extracted {record_count} records from page {page}")

        # Yield les records
        for record in records:
            record['_slice_mode'] = 'global'
            yield record

        # Vérifier si on a atteint la dernière page
        last_page = page_data.get('last_page', page_data.get('count', 1))
        logger.info(f"📖 Page {page}/{last_page} completed ({record_count} records)")

        if page >= last_page:
            logger.info(f"✅ Reached last page ({last_page})")
            break

        page += 1

    # ⚠️ Vérification de truncation : comparer avec le total annoncé
    if api_total_count and total_records < api_total_count:
        logger.warning(f"⚠️ WARNING: Possible data truncation detected!")
        logger.warning(f"   Expected: {api_total_count} records (from API count)")
        logger.warning(f"   Received: {total_records} records")
        logger.warning(f"   Missing: {api_total_count - total_records} records ({100*(api_total_count-total_records)/api_total_count:.1f}%)")
        logger.warning(f"   Solution: Use 'dept' or 'datetime' slicing mode instead of 'global'")
    
    # ⚠️ Détection si on atteint 20k (limite Hub'Eau)
    if total_records >= 20000:
        logger.warning(f"⚠️ WARNING: Reached Hub'Eau 20k records limit!")
        logger.warning(f"   This endpoint may have more data. Consider using finer slicing.")
    
    logger.info(f"🏁 GLOBAL slicing completed: {total_records} total records extracted")




def slice_by_datetime(
    client: RESTClient,
    endpoint: str,
    extraction_config: Dict[str, Any],
    temporal_config: Dict[str, Any],
    incremental_state: Optional[Any] = None
) -> Iterator[Dict[str, Any]]:
    """
    Stratégie datetime: découpage temporel par période.

    Traite les slices temporelles en découpant la période en chunks configurables.

    Args:
        client: RESTClient DLT
        endpoint: Chemin de l'endpoint
        extraction_config: Configuration d'extraction
        temporal_config: Configuration temporelle
        incremental_state: State DLT pour incremental

    Yields:
        Records extraits avec métadonnées de slicing temporel
    """
    # Récupérer la configuration de slicing
    start_date = datetime.fromisoformat(
        temporal_config.get('start_date', '2020-01-01')
    ).date()

    end_date = datetime.now().date() - timedelta(days=1)

    # Générer les slices par période
    period_days = extraction_config.get('period_days', 30)
    pagination_config = extraction_config.get('pagination', {})
    # No page_size - exploit API bug
    start_param = temporal_config.get('start_param', 'date_debut')
    end_param = temporal_config.get('end_param', 'date_fin')

    logger.info(f"📅 Starting DATETIME slicing for endpoint: {endpoint}")
    logger.info(f"🗓️ Date range: {start_date} to {end_date} ({(end_date - start_date).days} days)")
    logger.info(f"⏱️ Period chunk size: {period_days} days, page_size: {page_size}")
    logger.info(f"🔑 Date params: {start_param}/{end_param}")

    # Calculer le nombre total de périodes
    total_days = (end_date - start_date).days
    total_periods = (total_days + period_days - 1) // period_days
    logger.info(f"📊 Total periods to process: {total_periods}")

    current = start_date
    period_index = 0
    total_records_all = 0

    while current < end_date:
        period_index += 1
        next_date = min(current + timedelta(days=period_days), end_date)
        period_days_actual = (next_date - current).days

        logger.info(f"📆 Period {period_index}/{total_periods}: {current} to {next_date} ({period_days_actual} days)")

        params = {
            'format': 'json',
            # No size parameter,
            start_param: current.isoformat(),
            end_param: next_date.isoformat()
        }

        # Ajouter les paramètres par défaut
        default_params = extraction_config.get('default_params', {})
        params.update(default_params)

        # Pagination manuelle pour cette période
        page = 1
        period_total_records = 0

        while True:
            page_params = {**params, 'page': page}

            logger.info(f"📡 HTTP GET {client.base_url}{endpoint} period={period_index} page={page} dates={current}...{next_date}")
            response = client.get(endpoint, params=page_params)
            logger.info(f"✅ Response: status={response.status_code}, content-length={len(response.content)} bytes")

            page_data = response.json()

            records = extract_records(page_data, extraction_config)
            record_count = len(records)
            period_total_records += record_count

            if not records:
                logger.info(f"⚠️ No records on page {page} for period {current}...{next_date}, moving to next period")
                break

            logger.info(f"📊 Extracted {record_count} records from page {page} for period {current}...{next_date}")

            for record in records:
                record['_slice_start'] = current.isoformat()
                record['_slice_end'] = next_date.isoformat()
                yield record

            # Vérifier dernière page
            last_page = page_data.get('last_page', page_data.get('count', 1))
            logger.info(f"📖 Page {page}/{last_page} completed for period {current}...{next_date} ({record_count} records)")

            if page >= last_page:
                logger.info(f"✅ Reached last page ({last_page}) for period {current}...{next_date}")
                break

            page += 1

        total_records_all += period_total_records
        logger.info(f"✅ Period {period_index}/{total_periods} completed: {period_total_records} records extracted")

        current = next_date + timedelta(days=1)

    logger.info(f"🏁 DATETIME slicing completed: {total_records_all} total records extracted across {period_index} periods")



@dlt.source(name="hubeau")
def hubeau_rest_source(
    config_path: str,
    stations_data: Optional[Dict[str, List[str]]] = None,
    partition_date: Optional[str] = None
) -> Iterator[Any]:
    """
    Source DLT hybride pour Hub'Eau API.

    Utilise les primitives natives DLT + logique métier Hub'Eau.

    Supporte:
    - Référentiels (stations, ouvrages, sites, points) avec slicing global/dept
    - Chroniques (séries temporelles) avec incremental loading et slicing station_month_chunked/datetime
    - Observations/Analyses avec slicing avancé
    - Fallback générique pour endpoints non reconnus

    Args:
        config_path: Chemin vers fichier YAML de configuration
        stations_data: Dict {station_code: [months]} pour filtrage temporel (optionnel)
        partition_date: Date de partition pour traitement batch (optionnel)

    Yields:
        Resources DLT configurées selon le type d'endpoint
    """
    # Charger configuration
    config = load_hubeau_config(config_path)

    resource_config = config['resource']
    extraction_config = config.get('extraction', {})

    # Créer REST client
    client = create_hubeau_client(config)

    # Endpoint complet
    endpoint = resource_config['endpoint']

    # Déterminer le nom de la resource
    resource_name = resource_config['name']

    # Primary keys et write disposition
    primary_keys = get_primary_keys(config)
    write_disposition = resource_config.get('write_disposition', 'merge')

    # Détection intelligente du type d'endpoint
    endpoint_lower = endpoint.lower()

    # RÉFÉRENTIELS (stations, ouvrages, sites, points)
    if any(keyword in endpoint_lower for keyword in ["stations", "ouvrages", "points", "sites"]):
        yield create_reference_resource(
            client, endpoint, resource_name,
            primary_keys, write_disposition, extraction_config
        )

    # CHRONIQUES (séries temporelles avec incremental)
    elif any(keyword in endpoint_lower for keyword in ["chroniques"]):
        temporal_config = config.get('temporal_filter', {})
        yield create_chroniques_resource(
            client, endpoint, resource_name,
            primary_keys, write_disposition,
            extraction_config, temporal_config,
            stations_data, partition_date
        )

    # OBSERVATIONS/ANALYSES/OPÉRATIONS
    elif any(keyword in endpoint_lower for keyword in [
        "observations", "analyse", "obs_elab",
        "taxons", "indices", "operations", "conditions", "campagnes"
    ]):
        temporal_config = config.get('temporal_filter', {})
        yield create_observations_resource(
            client, endpoint, resource_name,
            primary_keys, write_disposition,
            extraction_config, temporal_config,
            stations_data, partition_date
        )

    # GÉNÉRIQUE (fallback)
    else:
        yield create_generic_resource(
            client, endpoint, resource_name,
            primary_keys, write_disposition, extraction_config
        )
