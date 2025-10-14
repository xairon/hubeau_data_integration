"""
Sources DLT natives pour Hub'Eau - Version 2.0
Utilise 100% les capacités natives de DLT
"""

from typing import Iterator, Optional, Dict, Any, List, Generator
from datetime import datetime, date, timedelta
from pathlib import Path
import yaml
import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator
from dlt.sources.helpers.requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def load_config(config_path: str) -> Dict[str, Any]:
    """Charge la configuration depuis un fichier YAML"""
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
        return yaml.safe_load(f)


def configure_session() -> Session:
    """Configure session avec retry et rate limiting DLT natifs"""
    session = Session()

    # Configuration retry avec backoff exponentiel
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"]
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    # Timeout par défaut
    session.timeout = 30

    return session


@dlt.source(name="hubeau")
def hubeau_source(
    config_path: str,
    api_key: Optional[str] = None,
    base_url: str = "https://hubeau.eaufrance.fr/api",
    stations_data: Optional[Dict[str, List[str]]] = None,
    partition_date: Optional[str] = None
):
    """
    Source DLT native pour Hub'Eau

    Args:
        config_path: Chemin vers le fichier de configuration YAML
        api_key: Clé API optionnelle
        base_url: URL de base de l'API
        stations_data: Données de stations pour filtrage temporel
        partition_date: Date de partition pour traitement batch
    """

    # Charger configuration
    config = load_config(config_path)

    # Extraire les paramètres de l'API
    api_config = config.get('api', {})
    api_name = api_config.get('name')
    endpoint_name = api_config.get('endpoint')

    # Configuration extraction
    extraction_config = config.get('extraction', {})
    slicing_mode = extraction_config.get('slicing_mode', 'global')

    # Créer RESTClient avec configuration appropriée
    client = create_rest_client(base_url, api_key, extraction_config)

    # Déterminer le type de resource selon l'endpoint
    if "stations" in endpoint_name:
        yield create_stations_resource(client, config, api_name, endpoint_name)
    elif "chroniques" in endpoint_name:
        yield create_chroniques_resource(client, config, api_name, endpoint_name, stations_data, partition_date)
    elif "observations" in endpoint_name or "obs_elab" in endpoint_name:
        yield create_observations_resource(client, config, api_name, endpoint_name)
    else:
        # Resource générique pour autres endpoints
        yield create_generic_resource(client, config, api_name, endpoint_name)


def create_rest_client(
    base_url: str,
    api_key: Optional[str],
    extraction_config: Dict[str, Any]
) -> RESTClient:
    """Crée un RESTClient configuré pour Hub'Eau"""

    # Configuration de la pagination
    pagination_config = extraction_config.get('pagination', {})
    page_size = pagination_config.get('page_size', 5000)

    # Créer le paginator approprié
    paginator = PageNumberPaginator(
        page_param="page",
        size_param="size",
        total_pages_param="last_page",
        page_size=page_size,
        page_start=1  # Hub'Eau commence à page 1
    )

    # Headers avec API key si fournie
    headers = {}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    # Créer le client avec session configurée
    client = RESTClient(
        base_url=base_url,
        headers=headers,
        paginator=paginator,
        session=configure_session()
    )

    return client


def create_stations_resource(
    client: RESTClient,
    config: Dict[str, Any],
    api_name: str,
    endpoint_name: str
):
    """Crée une resource DLT pour les stations"""

    extraction_config = config.get('extraction', {})
    slicing_mode = extraction_config.get('slicing_mode', 'global')

    # Déterminer les clés primaires depuis la config
    primary_keys = get_primary_keys(config)

    @dlt.resource(
        name=f"{api_name}_{endpoint_name}",
        primary_key=primary_keys,
        write_disposition="merge"
    )
    def stations_resource() -> Iterator[Dict[str, Any]]:
        """Resource pour les stations avec slicing natif"""

        endpoint_path = f"/v1/{api_name}/{endpoint_name}"

        if slicing_mode == 'dept':
            # Slicing par département
            departments = extraction_config.get('values', [])
            param_name = extraction_config.get('param', 'code_departement')

            for dept in departments:
                params = {
                    param_name: dept,
                    "format": "json"
                }

                # Ajouter paramètres par défaut
                default_params = extraction_config.get('default_params', {})
                params.update(default_params)

                # Paginate automatiquement avec DLT
                for page_data in client.paginate(endpoint_path, params=params):
                    # Extraire les données selon le records_path
                    records_path = extraction_config.get('records_path', '$.data')

                    if records_path == '$.data' and isinstance(page_data, dict):
                        records = page_data.get('data', [])
                    else:
                        records = page_data if isinstance(page_data, list) else [page_data]

                    for record in records:
                        # Ajouter métadonnées de slicing
                        record['_slice_dept'] = dept
                        yield record
        else:
            # Mode global - une seule requête paginée
            params = {"format": "json"}
            default_params = extraction_config.get('default_params', {})
            params.update(default_params)

            for page_data in client.paginate(endpoint_path, params=params):
                # Extraire les données
                records_path = extraction_config.get('records_path', '$.data')

                if records_path == '$.data' and isinstance(page_data, dict):
                    records = page_data.get('data', [])
                else:
                    records = page_data if isinstance(page_data, list) else [page_data]

                for record in records:
                    record['_slice_mode'] = 'global'
                    yield record

    return stations_resource()


def create_chroniques_resource(
    client: RESTClient,
    config: Dict[str, Any],
    api_name: str,
    endpoint_name: str,
    stations_data: Optional[Dict[str, List[str]]] = None,
    partition_date: Optional[str] = None
):
    """Crée une resource DLT pour les chroniques avec incremental loading"""

    extraction_config = config.get('extraction', {})
    slicing_mode = extraction_config.get('slicing_mode', 'global')
    primary_keys = get_primary_keys(config)

    # Déterminer le champ de date pour incremental
    temporal_config = config.get('temporal_filter', {})
    date_field = temporal_config.get('date_field', 'date_mesure')

    @dlt.resource(
        name=f"{api_name}_{endpoint_name}",
        primary_key=primary_keys,
        write_disposition="merge"
    )
    def chroniques_resource(
        last_value=dlt.sources.incremental(
            date_field,
            initial_value="2020-01-01"
        )
    ) -> Iterator[Dict[str, Any]]:
        """Resource pour chroniques avec incremental loading natif"""

        endpoint_path = f"/v1/{api_name}/{endpoint_name}"

        if slicing_mode == 'station_month_chunked' and stations_data:
            # Slicing par station et mois
            yield from process_station_month_chunks(
                client, endpoint_path, extraction_config,
                stations_data, last_value
            )
        elif slicing_mode == 'datetime':
            # Slicing par période temporelle
            yield from process_datetime_slices(
                client, endpoint_path, extraction_config,
                temporal_config, last_value
            )
        else:
            # Mode standard avec incremental
            params = {
                "format": "json",
                temporal_config.get('start_param', 'date_debut'): last_value.start_value
            }

            # Ajouter date de fin si configurée
            if temporal_config.get('end_param'):
                end_date = datetime.now().date() - timedelta(days=1)
                params[temporal_config['end_param']] = end_date.isoformat()

            # Paginate avec incremental
            for page_data in client.paginate(endpoint_path, params=params):
                records = extract_records(page_data, extraction_config)
                for record in records:
                    yield record

    return chroniques_resource()


def create_observations_resource(
    client: RESTClient,
    config: Dict[str, Any],
    api_name: str,
    endpoint_name: str
):
    """Crée une resource DLT pour les observations"""

    extraction_config = config.get('extraction', {})
    primary_keys = get_primary_keys(config)

    @dlt.resource(
        name=f"{api_name}_{endpoint_name}",
        primary_key=primary_keys,
        write_disposition="append"
    )
    def observations_resource() -> Iterator[Dict[str, Any]]:
        """Resource pour observations"""

        endpoint_path = f"/v1/{api_name}/{endpoint_name}"
        params = {"format": "json"}

        # Ajouter filtres temporels si configurés
        temporal_config = config.get('temporal_filter', {})
        if temporal_config:
            if temporal_config.get('start_date'):
                params[temporal_config.get('start_param', 'date_debut')] = temporal_config['start_date']
            if temporal_config.get('end_date'):
                params[temporal_config.get('end_param', 'date_fin')] = temporal_config['end_date']

        # Paginate
        for page_data in client.paginate(endpoint_path, params=params):
            records = extract_records(page_data, extraction_config)
            for record in records:
                yield record

    return observations_resource()


def create_generic_resource(
    client: RESTClient,
    config: Dict[str, Any],
    api_name: str,
    endpoint_name: str
):
    """Crée une resource DLT générique pour tout endpoint"""

    extraction_config = config.get('extraction', {})
    primary_keys = get_primary_keys(config)

    @dlt.resource(
        name=f"{api_name}_{endpoint_name}",
        primary_key=primary_keys,
        write_disposition="merge"
    )
    def generic_resource() -> Iterator[Dict[str, Any]]:
        """Resource générique"""

        endpoint_path = f"/v1/{api_name}/{endpoint_name}"
        params = extraction_config.get('default_params', {"format": "json"})

        for page_data in client.paginate(endpoint_path, params=params):
            records = extract_records(page_data, extraction_config)
            for record in records:
                yield record

    return generic_resource()


def process_station_month_chunks(
    client: RESTClient,
    endpoint_path: str,
    extraction_config: Dict[str, Any],
    stations_data: Dict[str, List[str]],
    incremental_state
) -> Iterator[Dict[str, Any]]:
    """Traite les chunks station/mois pour contourner les limites d'URL"""

    chunk_size = extraction_config.get('chunk_size', 80)
    station_codes = list(stations_data.keys())

    # Chunker les stations
    for i in range(0, len(station_codes), chunk_size):
        chunk = station_codes[i:i + chunk_size]

        # Pour chaque mois actif dans ce chunk
        months_in_chunk = set()
        for code in chunk:
            months_in_chunk.update(stations_data[code])

        for month in sorted(months_in_chunk):
            # Filtrer les stations actives pour ce mois
            active_stations = [
                code for code in chunk
                if month in stations_data[code]
            ]

            if not active_stations:
                continue

            # Construire les paramètres
            params = {
                "code_bss": ",".join(active_stations),
                "date_debut_mesure": f"{month}-01",
                "date_fin_mesure": f"{month}-31",
                "format": "json"
            }

            # Paginate
            for page_data in client.paginate(endpoint_path, params=params):
                records = extract_records(page_data, extraction_config)
                for record in records:
                    record['_chunk_month'] = month
                    record['_chunk_stations'] = len(active_stations)
                    yield record


def process_datetime_slices(
    client: RESTClient,
    endpoint_path: str,
    extraction_config: Dict[str, Any],
    temporal_config: Dict[str, Any],
    incremental_state
) -> Iterator[Dict[str, Any]]:
    """Traite les slices temporelles"""

    # Récupérer la configuration de slicing
    start_date = datetime.fromisoformat(
        temporal_config.get('start_date', '2020-01-01')
    ).date()

    end_date = datetime.now().date() - timedelta(days=1)

    # Générer les slices par période
    period_days = extraction_config.get('period_days', 30)
    current = start_date

    while current < end_date:
        next_date = min(current + timedelta(days=period_days), end_date)

        params = {
            temporal_config.get('start_param', 'date_debut'): current.isoformat(),
            temporal_config.get('end_param', 'date_fin'): next_date.isoformat(),
            "format": "json"
        }

        # Paginate pour cette période
        for page_data in client.paginate(endpoint_path, params=params):
            records = extract_records(page_data, extraction_config)
            for record in records:
                record['_slice_start'] = current.isoformat()
                record['_slice_end'] = next_date.isoformat()
                yield record

        current = next_date + timedelta(days=1)


def extract_records(
    page_data: Any,
    extraction_config: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """Extrait les records depuis la réponse API selon la config"""

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
    """Extrait les clés primaires depuis la configuration"""

    schema = config.get('schema', {})
    primary_keys = []

    for field_name, field_config in schema.items():
        if field_config.get('primary_key'):
            primary_keys.append(field_name)

    # Fallback sur des clés par défaut si aucune trouvée
    if not primary_keys:
        # Chercher des patterns communs
        for field in ['code_station', 'code_bss', 'code', 'id']:
            if field in schema:
                primary_keys.append(field)
                break

    return primary_keys if primary_keys else ['id']

