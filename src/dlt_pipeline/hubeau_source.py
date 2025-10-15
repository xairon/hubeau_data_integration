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
    page_size = pagination_config.get('page_size', 5000)

    # Construire les paramètres de base
    params = {
        'format': 'json',
        'size': page_size
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


def slice_by_department(
    client: RESTClient,
    endpoint: str,
    extraction_config: Dict[str, Any]
) -> Iterator[Dict[str, Any]]:
    """
    Stratégie dept: découpage par département français.

    Itère sur une liste de codes départements (01-95 + DOM-TOM).
    Utilisé pour contourner les limites API ou paralléliser l'extraction.
    Implémente la pagination manuelle car Hub'Eau commence à page=1 (pas 0).

    Args:
        client: RESTClient DLT
        endpoint: Chemin de l'endpoint
        extraction_config: Configuration avec values (liste départements) et param

    Yields:
        Records extraits avec métadonnée _slice_dept
    """
    departments = extraction_config.get('values', [])
    param_name = extraction_config.get('param', 'code_departement')
    pagination_config = extraction_config.get('pagination', {})
    page_size = pagination_config.get('page_size', 5000)

    logger.info(f"🗺️ Starting DEPARTMENT slicing for endpoint: {endpoint}")
    logger.info(f"📍 Processing {len(departments)} departments with param '{param_name}'")
    logger.info(f"📄 Pagination config: page_size={page_size}")

    total_records_all = 0
    dept_index = 0

    # Pour chaque département
    for dept in departments:
        dept_index += 1
        logger.info(f"🏛️ Department {dept_index}/{len(departments)}: {dept}")

        # Construire les paramètres de base
        params = {
            'format': 'json',
            'size': page_size,
            param_name: dept
        }

        # Ajouter les paramètres par défaut
        default_params = extraction_config.get('default_params', {})
        params.update(default_params)

        # ✅ AJOUT: Support temporal_filter pour découpage temporel par département
        temporal_filter = extraction_config.get('temporal_filter')
        if temporal_filter:
            start_param_temp = temporal_filter.get('start_param')
            end_param_temp = temporal_filter.get('end_param')
            start_date_str = temporal_filter.get('start_date')
            end_offset_days = temporal_filter.get('end_offset_days', 0)

            if start_param_temp and end_param_temp and start_date_str:
                # Calculer date de fin
                from datetime import datetime, timedelta
                end_date = datetime.now().date() - timedelta(days=end_offset_days)

                # Ajouter paramètres temporels
                params[start_param_temp] = start_date_str
                params[end_param_temp] = end_date.isoformat()

                logger.info(f"📅 Temporal filter applied: {start_date_str} to {end_date.isoformat()}")

        # Pagination manuelle pour ce département
        page = 1
        dept_total_records = 0

        while True:
            # Ajouter le numéro de page
            page_params = {**params, 'page': page}

            # Faire la requête
            logger.info(f"📡 HTTP GET {client.base_url}{endpoint} dept={dept} page={page}/{page_params}")
            response = client.get(endpoint, params=page_params)
            logger.info(f"✅ Response: status={response.status_code}, content-length={len(response.content)} bytes")

            page_data = response.json()

            # Extraire les records
            records = extract_records(page_data, extraction_config)
            record_count = len(records)
            dept_total_records += record_count

            # Si pas de records, passer au département suivant
            if not records:
                logger.info(f"⚠️ No records on page {page} for dept {dept}, moving to next department")
                break

            logger.info(f"📊 Extracted {record_count} records from page {page} for dept {dept}")

            # Yield les records
            for record in records:
                record['_slice_dept'] = dept
                yield record

            # Vérifier si on a atteint la dernière page
            last_page = page_data.get('last_page', page_data.get('count', 1))
            logger.info(f"📖 Page {page}/{last_page} completed for dept {dept} ({record_count} records)")

            if page >= last_page:
                logger.info(f"✅ Reached last page ({last_page}) for dept {dept}")
                break

            page += 1

        total_records_all += dept_total_records
        
        # ⚠️ Détection de truncation : Hub'Eau limite à 20k records par requête
        if dept_total_records >= 20000:
            logger.warning(f"⚠️ WARNING: Department {dept} has reached 20k records limit!")
            logger.warning(f"   This may indicate data truncation. Consider using finer slicing.")
            logger.warning(f"   Possible solutions:")
            logger.warning(f"   1. Add additional slicing dimension (e.g. first letter of station code)")
            logger.warning(f"   2. Use date-based slicing if available")
            logger.warning(f"   3. Check API documentation for pagination limits")
        
        logger.info(f"✅ Department {dept} completed: {dept_total_records} records extracted")

    logger.info(f"🏁 DEPARTMENT slicing completed: {total_records_all} total records across {len(departments)} departments")


def slice_by_station_month(
    client: RESTClient,
    endpoint: str,
    extraction_config: Dict[str, Any],
    stations_data: Dict[str, List[str]],
    incremental_state: Optional[Any] = None
) -> Iterator[Dict[str, Any]]:
    """
    Stratégie station_month_chunked: découpage par station + mois avec chunks.

    Traite les chunks station/mois pour contourner les limites d'URL.
    Supporte l'incremental loading : les mois déjà chargés sont automatiquement
    skippés en utilisant le state DLT (incremental_state.last_value).

    Args:
        client: RESTClient DLT pour les requêtes HTTP
        endpoint: Chemin de l'endpoint API
        extraction_config: Configuration d'extraction
        stations_data: Dict {station_code: [months]} des stations et mois actifs
        incremental_state: State DLT pour tracking incremental

    Yields:
        Records extraits de l'API avec métadonnées de chunking
    """
    chunk_size = extraction_config.get('station_chunk_size', 80)
    station_param = extraction_config.get('station_param', 'code_bss')
    start_param = extraction_config.get('start_param', 'date_debut_mesure')
    end_param = extraction_config.get('end_param', 'date_fin_mesure')
    pagination_config = extraction_config.get('pagination', {})
    page_size = pagination_config.get('page_size', 5000)

    station_codes = list(stations_data.keys())

    logger.info(f"🏢 Starting STATION_MONTH_CHUNKED slicing for endpoint: {endpoint}")
    logger.info(f"📊 Total stations: {len(station_codes)}, chunk_size: {chunk_size}, page_size: {page_size}")
    logger.info(f"🔑 Station param: '{station_param}', date params: {start_param}/{end_param}")

    # Récupérer la dernière date chargée depuis le state DLT
    last_loaded_month = None
    if incremental_state and hasattr(incremental_state, 'last_value'):
        last_value = incremental_state.last_value
        if last_value:
            # last_value est une date string comme "2024-03-15" ou un objet date
            # On extrait le mois : "2024-03"
            try:
                if isinstance(last_value, str):
                    # Format attendu : "YYYY-MM-DD" -> extraire "YYYY-MM"
                    last_loaded_month = last_value[:7]  # "2024-03-15" -> "2024-03"
                elif hasattr(last_value, 'strftime'):
                    # Si c'est un objet date
                    last_loaded_month = last_value.strftime("%Y-%m")
                logger.info(f"🔄 Incremental loading: last_loaded_month = {last_loaded_month}")
            except Exception as e:
                # Si erreur, on recharge tout (mode safe)
                logger.warning(f"⚠️ Error parsing incremental state: {e}, reloading all data")
                last_loaded_month = None
    else:
        logger.info(f"📥 No incremental state, loading all data")

    total_records_all = 0
    total_chunks = (len(station_codes) + chunk_size - 1) // chunk_size
    chunk_index = 0
    skipped_months = 0

    # Chunker les stations
    for i in range(0, len(station_codes), chunk_size):
        chunk = station_codes[i:i + chunk_size]
        chunk_index += 1

        logger.info(f"📦 Processing chunk {chunk_index}/{total_chunks} ({len(chunk)} stations)")

        # Pour chaque mois actif dans ce chunk
        months_in_chunk = set()
        for code in chunk:
            months_in_chunk.update(stations_data[code])

        logger.info(f"📅 Chunk has {len(months_in_chunk)} unique months to process")

        for month_idx, month in enumerate(sorted(months_in_chunk), 1):
            # Skip les mois déjà chargés
            if last_loaded_month:
                # Comparer les mois : "2024-01" vs "2024-03"
                if month < last_loaded_month:
                    # Ce mois est avant le dernier mois chargé -> skip
                    skipped_months += 1
                    logger.debug(f"⏭️ Skipping month {month} (before last_loaded {last_loaded_month})")
                    continue
                elif month == last_loaded_month:
                    # Même mois -> skip aussi (déjà chargé)
                    # Note : on pourrait charger quand même pour updates
                    skipped_months += 1
                    logger.debug(f"⏭️ Skipping month {month} (equals last_loaded)")
                    continue

            # Filtrer les stations actives pour ce mois
            active_stations = [
                code for code in chunk
                if month in stations_data[code]
            ]

            if not active_stations:
                logger.debug(f"⚠️ No active stations for month {month} in chunk {chunk_index}, skipping")
                continue

            logger.info(f"📆 Month {month_idx}/{len(months_in_chunk)}: {month} with {len(active_stations)} active stations")

            # Construire les paramètres
            params = {
                'format': 'json',
                'size': page_size,
                station_param: ','.join(active_stations),
                start_param: f"{month}-01",
                end_param: get_month_end_date(month)  # ✅ CORRECTION: Utilise le dernier jour réel du mois
            }

            # Pagination manuelle pour ce chunk
            page = 1
            month_total_records = 0

            while True:
                page_params = {**params, 'page': page}

                logger.info(f"📡 HTTP GET {client.base_url}{endpoint} chunk={chunk_index} month={month} page={page} stations={len(active_stations)}")
                response = client.get(endpoint, params=page_params)
                logger.info(f"✅ Response: status={response.status_code}, content-length={len(response.content)} bytes")

                page_data = response.json()

                records = extract_records(page_data, extraction_config)
                record_count = len(records)
                month_total_records += record_count

                if not records:
                    logger.info(f"⚠️ No records on page {page} for chunk {chunk_index} month {month}, moving to next month")
                    break

                logger.info(f"📊 Extracted {record_count} records from page {page} for month {month}")

                for record in records:
                    record['_chunk_month'] = month
                    record['_chunk_stations'] = len(active_stations)
                    yield record

                # Vérifier dernière page
                last_page = page_data.get('last_page', page_data.get('count', 1))
                logger.info(f"📖 Page {page}/{last_page} completed for month {month} ({record_count} records)")

                if page >= last_page:
                    logger.info(f"✅ Reached last page ({last_page}) for month {month}")
                    break

                page += 1

            total_records_all += month_total_records
            logger.info(f"✅ Month {month} completed: {month_total_records} records extracted")

        logger.info(f"✅ Chunk {chunk_index}/{total_chunks} completed")

    logger.info(f"🏁 STATION_MONTH_CHUNKED slicing completed: {total_records_all} total records extracted")
    if skipped_months > 0:
        logger.info(f"⏭️ Skipped {skipped_months} months due to incremental loading")


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
    page_size = pagination_config.get('page_size', 5000)
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
            'size': page_size,
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


def slice_by_dept_datetime(
    client: RESTClient,
    endpoint: str,
    extraction_config: Dict[str, Any]
) -> Iterator[Dict[str, Any]]:
    """
    Stratégie dept_datetime: chunking par département + fenêtres temporelles.

    Combine le découpage départemental (en chunks) avec des fenêtres temporelles mensuelles.
    Optimisé pour éviter la truncation tout en minimisant le nombre de requêtes.

    Args:
        client: RESTClient DLT
        endpoint: Chemin de l'endpoint
        extraction_config: Configuration avec dept_list, dept_chunk_size, dates, etc.

    Yields:
        Records extraits avec métadonnées _slice_dept_chunk et _slice_period
    """
    import calendar
    from datetime import datetime, timedelta

    # Configuration départements
    dept_list = extraction_config.get('dept_list', [])
    dept_chunk_size = extraction_config.get('dept_chunk_size', 8)
    dept_param = extraction_config.get('dept_param', 'code_departement')

    # Configuration temporelle
    start_date_str = extraction_config.get('start_date', '2020-01-01')
    end_offset_days = extraction_config.get('end_offset_days', 0)
    window_days = extraction_config.get('window_days', 30)
    start_param = extraction_config.get('start_param', 'date_debut')
    end_param = extraction_config.get('end_param', 'date_fin')

    # Pagination
    pagination_config = extraction_config.get('pagination', {})
    page_size = pagination_config.get('page_size', 5000)

    # Calculer dates de début et fin
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
    end_date = date.today() - timedelta(days=end_offset_days)

    # Si start_date est une partition annuelle (ex: 2024-01-01), limiter à la fin de l'année
    if start_date.month == 1 and start_date.day == 1:
        end_of_year = date(start_date.year, 12, 31)
        end_date = min(end_date, end_of_year)

    logger.info(f"🗺️📅 Starting DEPT_DATETIME slicing for endpoint: {endpoint}")
    logger.info(f"📍 {len(dept_list)} departments in {(len(dept_list) + dept_chunk_size - 1) // dept_chunk_size} chunks of {dept_chunk_size}")
    logger.info(f"🗓️ Date range: {start_date} to {end_date} ({(end_date - start_date).days} days)")
    logger.info(f"⏱️ Window size: {window_days} days, page_size: {page_size}")

    # Générer les fenêtres mensuelles
    def month_windows(start: date, end: date) -> Iterator[tuple[date, date]]:
        """Génère des fenêtres mensuelles exactes"""
        cursor = date(start.year, start.month, 1)
        final = date(end.year, end.month, calendar.monthrange(end.year, end.month)[1])
        while cursor <= final:
            last_day = calendar.monthrange(cursor.year, cursor.month)[1]
            period_end = date(cursor.year, cursor.month, last_day)
            yield cursor, min(period_end, final)
            if cursor.month == 12:
                cursor = date(cursor.year + 1, 1, 1)
            else:
                cursor = date(cursor.year, cursor.month + 1, 1)

    # Calculer nombre total de chunks et périodes
    num_dept_chunks = (len(dept_list) + dept_chunk_size - 1) // dept_chunk_size
    periods = list(month_windows(start_date, end_date))
    num_periods = len(periods)
    total_slices = num_dept_chunks * num_periods

    logger.info(f"📊 Total slices: {num_dept_chunks} dept chunks × {num_periods} periods = {total_slices} slices")

    total_records_all = 0
    slice_index = 0

    # Chunker les départements
    for dept_chunk_idx in range(0, len(dept_list), dept_chunk_size):
        dept_chunk = dept_list[dept_chunk_idx:dept_chunk_idx + dept_chunk_size]
        dept_chunk_str = ",".join(dept_chunk)

        logger.info(f"🏛️ Processing dept chunk {dept_chunk_idx // dept_chunk_size + 1}/{num_dept_chunks}: {dept_chunk}")

        # Pour chaque fenêtre mensuelle
        for period_idx, (period_start, period_end) in enumerate(periods, 1):
            slice_index += 1

            logger.info(f"📆 Slice {slice_index}/{total_slices}: Depts [{dept_chunk[0]}...{dept_chunk[-1]}] × Period {period_start} to {period_end}")

            # Construire les paramètres
            params = {
                'format': 'json',
                'size': page_size,
                dept_param: dept_chunk_str,
                start_param: period_start.isoformat(),
                end_param: period_end.isoformat()
            }

            # Ajouter paramètres par défaut
            default_params = extraction_config.get('default_params', {})
            params.update(default_params)

            # Pagination manuelle pour cette slice
            page = 1
            slice_total_records = 0

            while True:
                page_params = {**params, 'page': page}

                logger.info(f"📡 HTTP GET {client.base_url}{endpoint} slice={slice_index} page={page} params={page_params}")
                response = client.get(endpoint, params=page_params)
                logger.info(f"✅ Response: status={response.status_code}, content-length={len(response.content)} bytes")

                page_data = response.json()

                # Extraire records
                records = extract_records(page_data, extraction_config)
                record_count = len(records)
                slice_total_records += record_count

                if not records:
                    logger.info(f"⚠️ No records on page {page}, moving to next slice")
                    break

                logger.info(f"📊 Extracted {record_count} records from page {page}")

                # Yield records avec métadonnées
                for record in records:
                    record['_slice_dept_chunk'] = dept_chunk_str
                    record['_slice_period_start'] = period_start.isoformat()
                    record['_slice_period_end'] = period_end.isoformat()
                    yield record

                # Vérifier dernière page
                last_page = page_data.get('last_page', page_data.get('count', 1))
                logger.info(f"📖 Page {page}/{last_page} completed ({record_count} records)")

                if page >= last_page:
                    logger.info(f"✅ Reached last page ({last_page})")
                    break

                # ⚠️ Détection de truncation
                if slice_total_records >= 20000:
                    logger.warning(f"⚠️ WARNING: Slice reached 20k records limit! Consider reducing chunk size or window.")
                    break

                page += 1

            total_records_all += slice_total_records
            logger.info(f"✅ Slice {slice_index}/{total_slices} completed: {slice_total_records} records")

    logger.info(f"🏁 DEPT_DATETIME slicing completed: {total_records_all} total records across {slice_index} slices")


# ============================================
# FACTORIES DE RESOURCES DLT
# ============================================

def create_reference_resource(
    client: RESTClient,
    endpoint: str,
    resource_name: str,
    primary_keys: List[str],
    write_disposition: str,
    extraction_config: Dict[str, Any]
):
    """
    Crée une resource DLT pour les référentiels (stations, ouvrages, sites, etc.).

    Supporte les stratégies de slicing:
    - global: requête unique
    - dept: découpage par département

    Args:
        client: RESTClient DLT
        endpoint: Chemin de l'endpoint
        resource_name: Nom de la resource DLT
        primary_keys: Liste des clés primaires
        write_disposition: Mode d'écriture (replace/merge/append)
        extraction_config: Configuration d'extraction

    Returns:
        Resource DLT configurée et instanciée
    """

    @dlt.resource(
        name=resource_name,
        primary_key=primary_keys if primary_keys else None,
        write_disposition=write_disposition
    )
    def reference_resource() -> Iterator[Dict[str, Any]]:
        """Resource pour référentiel avec slicing natif"""

        slicing_mode = extraction_config.get('slicing_mode', 'global')

        if slicing_mode == 'dept':
            yield from slice_by_department(client, endpoint, extraction_config)
        else:
            yield from slice_global(client, endpoint, extraction_config)

    return reference_resource()


def create_chroniques_resource(
    client: RESTClient,
    endpoint: str,
    resource_name: str,
    primary_keys: List[str],
    write_disposition: str,
    extraction_config: Dict[str, Any],
    temporal_config: Dict[str, Any],
    stations_data: Optional[Dict[str, List[str]]] = None,
    partition_date: Optional[str] = None
):
    """
    Crée une resource DLT pour les chroniques avec incremental loading.

    Supporte les stratégies de slicing:
    - dept_datetime: chunking par département + fenêtres temporelles (recommandé)
    - station_month_chunked: découpage par stations + mois (avec chunks)
    - datetime: découpage temporel par période
    - global: requête unique (fallback)

    Args:
        client: RESTClient DLT
        endpoint: Chemin de l'endpoint
        resource_name: Nom de la resource DLT
        primary_keys: Liste des clés primaires
        write_disposition: Mode d'écriture (replace/merge/append)
        extraction_config: Configuration d'extraction
        temporal_config: Configuration temporelle pour incremental
        stations_data: Dict {station_code: [months]} pour filtrage
        partition_date: Date de partition pour batch processing

    Returns:
        Resource DLT configurée et instanciée avec incremental loading
    """
    # Déterminer le champ de date pour incremental
    date_field = temporal_config.get('date_field', 'date_mesure')
    start_date_str = temporal_config.get('start_date', '2020-01-01')

    # Convert initial value based on the field type
    # timestamp_mesure is in milliseconds Unix timestamp format
    if 'timestamp' in date_field.lower():
        # Convert date string to Unix timestamp in milliseconds
        start_dt = datetime.strptime(start_date_str, '%Y-%m-%d')
        # Convert to Unix timestamp in milliseconds (like the API returns)
        initial_value = int(start_dt.timestamp() * 1000)
        logger.info(f"📅 Using timestamp incremental: field={date_field}, initial_value={initial_value} (from {start_date_str})")
    else:
        # For date fields, keep as string
        initial_value = start_date_str
        logger.info(f"📅 Using date incremental: field={date_field}, initial_value={initial_value}")

    @dlt.resource(
        name=resource_name,
        primary_key=primary_keys if primary_keys else None,
        write_disposition=write_disposition
    )
    def chroniques_resource(
        last_value=dlt.sources.incremental(date_field, initial_value=initial_value)
    ) -> Iterator[Dict[str, Any]]:
        """Resource pour chroniques avec incremental loading natif"""

        slicing_mode = extraction_config.get('slicing_mode', 'global')

        if slicing_mode == 'dept_datetime':
            yield from slice_by_dept_datetime(
                client, endpoint, extraction_config
            )
        elif slicing_mode == 'station_month_chunked' and stations_data:
            yield from slice_by_station_month(
                client, endpoint, extraction_config,
                stations_data, last_value
            )
        elif slicing_mode == 'datetime':
            yield from slice_by_datetime(
                client, endpoint, extraction_config,
                temporal_config, last_value
            )
        else:
            yield from slice_global(client, endpoint, extraction_config)

    return chroniques_resource()


def create_observations_resource(
    client: RESTClient,
    endpoint: str,
    resource_name: str,
    primary_keys: List[str],
    write_disposition: str,
    extraction_config: Dict[str, Any],
    temporal_config: Dict[str, Any],
    stations_data: Optional[Dict[str, List[str]]] = None,
    partition_date: Optional[str] = None
):
    """
    Crée une resource DLT pour les observations/analyses.

    Supporte les stratégies de slicing:
    - station_month_chunked: découpage par stations + mois
    - datetime: découpage temporel par période
    - global: requête unique (fallback)

    Args:
        client: RESTClient DLT
        endpoint: Chemin de l'endpoint
        resource_name: Nom de la resource DLT
        primary_keys: Liste des clés primaires
        write_disposition: Mode d'écriture (replace/merge/append)
        extraction_config: Configuration d'extraction
        temporal_config: Configuration temporelle
        stations_data: Dict {station_code: [months]} pour filtrage (optionnel)
        partition_date: Date de partition pour batch processing

    Returns:
        Resource DLT configurée et instanciée
    """

    @dlt.resource(
        name=resource_name,
        primary_key=primary_keys if primary_keys else None,
        write_disposition=write_disposition
    )
    def observations_resource() -> Iterator[Dict[str, Any]]:
        """Resource pour observations"""

        slicing_mode = extraction_config.get('slicing_mode', 'global')

        if slicing_mode == 'station_month_chunked' and stations_data:
            yield from slice_by_station_month(
                client, endpoint, extraction_config,
                stations_data, None
            )
        elif slicing_mode == 'dept':
            yield from slice_by_department(client, endpoint, extraction_config)
        elif slicing_mode == 'datetime':
            yield from slice_by_datetime(
                client, endpoint, extraction_config,
                temporal_config, None
            )
        else:
            yield from slice_global(client, endpoint, extraction_config)

    return observations_resource()


def create_generic_resource(
    client: RESTClient,
    endpoint: str,
    resource_name: str,
    primary_keys: List[str],
    write_disposition: str,
    extraction_config: Dict[str, Any]
):
    """
    Crée une resource DLT générique pour tout endpoint.

    Utilise la stratégie global par défaut.

    Args:
        client: RESTClient DLT
        endpoint: Chemin de l'endpoint
        resource_name: Nom de la resource DLT
        primary_keys: Liste des clés primaires
        write_disposition: Mode d'écriture (replace/merge/append)
        extraction_config: Configuration d'extraction

    Returns:
        Resource DLT configurée et instanciée
    """

    @dlt.resource(
        name=resource_name,
        primary_key=primary_keys if primary_keys else None,
        write_disposition=write_disposition
    )
    def generic_resource() -> Iterator[Dict[str, Any]]:
        """Resource générique"""
        yield from slice_global(client, endpoint, extraction_config)

    return generic_resource()


# ============================================
# SOURCE DLT PRINCIPALE
# ============================================

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
