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
import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator
from dlt.sources.helpers.requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


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
    - Paramètre 'page' pour le numéro de page (commence à 1)
    - Champ 'last_page' dans la réponse pour le nombre total de pages

    Args:
        pagination_config: Configuration de pagination depuis YAML

    Returns:
        PageNumberPaginator configuré pour Hub'Eau
    """
    total_path = pagination_config.get('total_path', 'last_page')

    return PageNumberPaginator(
        total_path=total_path,
        base_page=1
    )


# ============================================
# REST CLIENT HUBEAU
# ============================================

def create_hubeau_client(config: Dict[str, Any]) -> RESTClient:
    """
    Crée un RESTClient DLT configuré pour Hub'Eau.

    Args:
        config: Configuration complète depuis YAML

    Returns:
        RESTClient configuré avec paginator, session et base_url
    """
    resource_config = config['resource']
    extraction_config = config.get('extraction', {})
    performance_config = config.get('performance', {})

    base_url = resource_config.get('base_url', 'https://hubeau.eaufrance.fr/api')

    # Créer paginator Hub'Eau
    pagination_config = extraction_config.get('pagination', {})
    paginator = create_hubeau_paginator(pagination_config)

    # Créer session avec retry
    session = create_hubeau_session(performance_config)

    # Créer REST client
    return RESTClient(
        base_url=base_url,
        paginator=paginator,
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

    Args:
        client: RESTClient DLT
        endpoint: Chemin de l'endpoint (ex: /stations)
        extraction_config: Configuration d'extraction

    Yields:
        Records extraits avec métadonnée _slice_mode
    """
    pagination_config = extraction_config.get('pagination', {})
    page_size = pagination_config.get('page_size', 5000)

    # Construire les paramètres
    params = {
        'format': 'json',
        'size': page_size
    }

    # Ajouter les paramètres par défaut
    default_params = extraction_config.get('default_params', {})
    params.update(default_params)

    # Paginer automatiquement avec DLT
    for page_data in client.paginate(endpoint, params=params):
        records = extract_records(page_data, extraction_config)
        for record in records:
            record['_slice_mode'] = 'global'
            yield record


def slice_by_department(
    client: RESTClient,
    endpoint: str,
    extraction_config: Dict[str, Any]
) -> Iterator[Dict[str, Any]]:
    """
    Stratégie dept: découpage par département français.

    Itère sur une liste de codes départements (01-95 + DOM-TOM).
    Utilisé pour contourner les limites API ou paralléliser l'extraction.

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

    # Pour chaque département
    for dept in departments:
        # Construire les paramètres
        params = {
            'format': 'json',
            'size': page_size,
            param_name: dept
        }

        # Ajouter les paramètres par défaut
        default_params = extraction_config.get('default_params', {})
        params.update(default_params)

        # Paginer pour ce département
        for page_data in client.paginate(endpoint, params=params):
            records = extract_records(page_data, extraction_config)
            for record in records:
                record['_slice_dept'] = dept
                yield record


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
    chunk_size = extraction_config.get('chunk_size', 80)
    station_param = extraction_config.get('station_param', 'code_bss')
    start_param = extraction_config.get('start_param', 'date_debut_mesure')
    end_param = extraction_config.get('end_param', 'date_fin_mesure')
    pagination_config = extraction_config.get('pagination', {})
    page_size = pagination_config.get('page_size', 5000)

    station_codes = list(stations_data.keys())

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
            except:
                # Si erreur, on recharge tout (mode safe)
                last_loaded_month = None

    # Chunker les stations
    for i in range(0, len(station_codes), chunk_size):
        chunk = station_codes[i:i + chunk_size]

        # Pour chaque mois actif dans ce chunk
        months_in_chunk = set()
        for code in chunk:
            months_in_chunk.update(stations_data[code])

        for month in sorted(months_in_chunk):
            # Skip les mois déjà chargés
            if last_loaded_month:
                # Comparer les mois : "2024-01" vs "2024-03"
                if month < last_loaded_month:
                    # Ce mois est avant le dernier mois chargé -> skip
                    continue
                elif month == last_loaded_month:
                    # Même mois -> skip aussi (déjà chargé)
                    # Note : on pourrait charger quand même pour updates
                    continue

            # Filtrer les stations actives pour ce mois
            active_stations = [
                code for code in chunk
                if month in stations_data[code]
            ]

            if not active_stations:
                continue

            # Construire les paramètres
            params = {
                'format': 'json',
                'size': page_size,
                station_param: ','.join(active_stations),
                start_param: f"{month}-01",
                end_param: f"{month}-31"
            }

            # Paginate
            for page_data in client.paginate(endpoint, params=params):
                records = extract_records(page_data, extraction_config)
                for record in records:
                    record['_chunk_month'] = month
                    record['_chunk_stations'] = len(active_stations)
                    yield record


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

    current = start_date

    while current < end_date:
        next_date = min(current + timedelta(days=period_days), end_date)

        params = {
            'format': 'json',
            'size': page_size,
            temporal_config.get('start_param', 'date_debut'): current.isoformat(),
            temporal_config.get('end_param', 'date_fin'): next_date.isoformat()
        }

        # Ajouter les paramètres par défaut
        default_params = extraction_config.get('default_params', {})
        params.update(default_params)

        # Paginate pour cette période
        for page_data in client.paginate(endpoint, params=params):
            records = extract_records(page_data, extraction_config)
            for record in records:
                record['_slice_start'] = current.isoformat()
                record['_slice_end'] = next_date.isoformat()
                yield record

        current = next_date + timedelta(days=1)


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
    start_date = temporal_config.get('start_date', '2020-01-01')

    @dlt.resource(
        name=resource_name,
        primary_key=primary_keys if primary_keys else None,
        write_disposition=write_disposition
    )
    def chroniques_resource(
        last_value=dlt.sources.incremental(date_field, initial_value=start_date)
    ) -> Iterator[Dict[str, Any]]:
        """Resource pour chroniques avec incremental loading natif"""

        slicing_mode = extraction_config.get('slicing_mode', 'global')

        if slicing_mode == 'station_month_chunked' and stations_data:
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
