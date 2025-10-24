"""
Source DLT CSV Hub'Eau avec support multi-mode et slicing

Modes d'ingestion :
- FULL : Tout l'historique (pas de filtre date)
- YEAR : Une annee specifique
- INCREMENTAL : Derniers N jours

Slicing :
- Par station pour piezometry_chroniques (API impose code_bss)
"""

import dlt
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import io
from typing import Iterator, List, Dict, Optional
import logging
import time
from enum import Enum
from datetime import datetime, timedelta

# Configure logger pour qu'il soit capture par Dagster
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Ajouter un handler console si pas deja present pour que Dagster capture les logs
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(levelname)s - %(name)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


class IngestionMode(Enum):
    """Modes d'ingestion disponibles"""
    FULL = "full"
    YEAR = "year"
    INCREMENTAL = "incremental"


class HubeauAPIClient:
    """
    Client HTTP optimise avec :
    - Connection pooling
    - Retry automatique
    - Rate limiting
    """

    def __init__(self, base_url: str, rate_limit: float = 2.0):
        self.base_url = base_url
        self.rate_limit = rate_limit
        self.last_request_time = 0

        # Session HTTP avec connection pooling
        self.session = requests.Session()

        # Retry strategy
        retry_strategy = Retry(
            total=5,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )

        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=10,
            pool_maxsize=10
        )

        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def _rate_limit(self):
        """Rate limiting intelligent"""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.rate_limit:
            time.sleep(self.rate_limit - elapsed)
        self.last_request_time = time.time()

    def get(self, endpoint: str, params: Dict = None, timeout: int = 180) -> requests.Response:
        """GET avec rate limiting et retry (timeout 180s pour grandes pages CSV)"""
        self._rate_limit()

        url = f"{self.base_url}{endpoint}"

        try:
            response = self.session.get(url, params=params, timeout=timeout)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Erreur requete {url}: {e}")
            raise


def detect_date_field(resource_name: str) -> Optional[str]:
    """
    Detecte le nom du champ date selon le type d'endpoint
    """
    if 'chronique' in resource_name or 'observation' in resource_name or 'obs_elab' in resource_name:
        return 'mesure'
    elif 'analyse' in resource_name:
        return 'prelevement'
    elif 'condition' in resource_name or 'operation' in resource_name:
        return 'prelevement'
    elif 'campagne' in resource_name:
        return 'campagne'
    else:
        return None


def build_params_for_mode(
    mode: IngestionMode,
    year: Optional[int],
    incremental_days: int,
    default_params: Dict,
    resource_name: str
) -> Dict:
    """
    Construit les parametres API selon le mode d'ingestion
    """
    params = {**(default_params or {})}

    date_field = detect_date_field(resource_name)

    if not date_field:
        # Pas de filtre date disponible (stations, referentiels)
        return params

    if mode == IngestionMode.FULL:
        # Pas de filtre date
        logger.info(f"{resource_name}: Mode FULL - pas de filtre date")

    elif mode == IngestionMode.YEAR:
        if not year:
            raise ValueError("Mode YEAR necessite le parametre 'year'")

        params[f'date_debut_{date_field}'] = f"{year}-01-01"
        params[f'date_fin_{date_field}'] = f"{year}-12-31"

        logger.info(f"{resource_name}: Mode YEAR - annee {year}")

    elif mode == IngestionMode.INCREMENTAL:
        today = datetime.now()
        start_date = today - timedelta(days=incremental_days)

        params[f'date_debut_{date_field}'] = start_date.strftime("%Y-%m-%d")
        params[f'date_fin_{date_field}'] = today.strftime("%Y-%m-%d")

        logger.info(
            f"{resource_name}: Mode INCREMENTAL - "
            f"{start_date.strftime('%Y-%m-%d')} a {today.strftime('%Y-%m-%d')}"
        )

    return params


def get_total_pages_from_json(
    client: HubeauAPIClient,
    endpoint: str,
    params: Dict = None
) -> tuple:
    """
    Requete JSON pour obtenir count + estimation pages

    Retour : (total_pages, total_count)
    """
    endpoint_json = endpoint.replace('.csv', '')

    try:
        # Requete 1 : count total
        response = client.get(endpoint_json, params={**(params or {}), 'page': 1, 'size': 1})
        data = response.json()
        total_count = data.get('count', 0)

        if total_count == 0:
            return 1, 0

        # Requete 2 : taille de page reelle
        response = client.get(endpoint_json, params={**(params or {}), 'page': 1})
        data = response.json()
        actual_page_size = len(data.get('data', []))

        if actual_page_size == 0:
            actual_page_size = 20000  # Default Hub'Eau

        total_pages = (total_count // actual_page_size) + (1 if total_count % actual_page_size > 0 else 0)

        logger.info(f"📊 {endpoint}: Total={total_count:,} records, page_size={actual_page_size}, pages={total_pages}")

        return total_pages, total_count

    except Exception as e:
        logger.error(f"Impossible de recuperer count: {e}")
        raise


def fetch_csv_page(
    client: HubeauAPIClient,
    endpoint: str,
    page: int,
    params: Dict = None,
    chunk_size: int = 5000,
    max_retries: int = 3
) -> Iterator[pd.DataFrame]:
    """
    Telecharge et parse une page CSV en chunks avec retry sur timeout

    Args:
        client: Client HTTP
        endpoint: Endpoint API
        page: Numéro de page
        params: Paramètres de requête
        chunk_size: Taille des chunks pour réduire mémoire (défaut: 5000 lignes)
        max_retries: Nombre de tentatives en cas de timeout (défaut: 3)

    Yields:
        DataFrames par chunks pour économiser la mémoire
    """
    retry_count = 0

    while retry_count <= max_retries:
        try:
            response = client.get(endpoint, params={**(params or {}), 'page': page}, timeout=180)

            # Hub'Eau utilise separateur point-virgule
            # Utiliser chunksize pour lire par morceaux et économiser mémoire
            chunks = pd.read_csv(
                io.StringIO(response.text),
                sep=';',
                quotechar='"',
                low_memory=False,
                on_bad_lines='skip',
                chunksize=chunk_size
            )

            for chunk in chunks:
                yield chunk

            # Succès, sortir de la boucle retry
            break

        except pd.errors.EmptyDataError:
            logger.warning(f"⚠️  Page {page}: CSV vide")
            yield pd.DataFrame()
            break

        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError, requests.exceptions.ReadTimeout) as e:
            retry_count += 1
            if retry_count > max_retries:
                logger.error(f"❌ Page {page}: Max retries ({max_retries}) atteint après timeout: {e}")
                raise

            # Backoff exponentiel: 5s, 10s, 20s
            wait_time = 5 * (2 ** (retry_count - 1))
            logger.warning(
                f"⚠️  Page {page}: Timeout (tentative {retry_count}/{max_retries}), "
                f"retry dans {wait_time}s - Error: {type(e).__name__}"
            )
            time.sleep(wait_time)

        except Exception as e:
            logger.error(f"❌ Erreur parsing CSV page {page}: {type(e).__name__}: {e}")
            raise


def _paginate_csv(
    client: HubeauAPIClient,
    endpoint: str,
    params: Dict,
    resource_name: str
) -> Iterator[List[Dict]]:
    """
    Pagination CSV avec requete JSON prealable
    """
    try:
        total_pages, total_count = get_total_pages_from_json(client, endpoint, params)
    except Exception as e:
        logger.error(f"{resource_name}: Impossible de recuperer count, abandon: {e}")
        raise

    if total_count == 0:
        logger.warning(f"⚠️  {resource_name}: Aucune donnee trouvee pour ces parametres")
        return

    records_total = 0
    errors_count = 0
    max_consecutive_errors = 3

    for page in range(1, total_pages + 1):
        try:
            chunk_count = 0
            page_records = 0

            # Itérer sur les chunks de la page
            for df_chunk in fetch_csv_page(client, endpoint, page, params):
                if df_chunk.empty:
                    continue

                chunk_count += 1
                records = df_chunk.to_dict('records')
                page_records += len(records)
                records_total += len(records)

                # Log avec progression
                progress_pct = (records_total / total_count * 100) if total_count > 0 else 0
                logger.info(
                    f"📥 {resource_name}: Page {page}/{total_pages} chunk {chunk_count} -> "
                    f"{len(records)} records (page: {page_records:,}, total: {records_total:,}/{total_count:,} = {progress_pct:.1f}%)"
                )

                # Yield immédiatement pour libérer mémoire
                yield records

            # Log fin de page
            if page_records == 0:
                logger.warning(f"⚠️  {resource_name}: Page {page}/{total_pages} vide (0 records)")
            else:
                logger.info(f"✅ {resource_name}: Page {page}/{total_pages} terminée ({page_records:,} records)")

            errors_count = 0

        except Exception as e:
            errors_count += 1
            logger.error(f"❌ {resource_name}: Erreur page {page}/{total_pages} (erreur #{errors_count}): {type(e).__name__}: {e}")

            if errors_count >= max_consecutive_errors:
                logger.error(f"❌ {resource_name}: ABANDON après {errors_count} erreurs consécutives")
                raise

            continue

    logger.info(f"✅ {resource_name}: TERMINE - {records_total:,} records ingeres")


def _paginate_with_station_slicing(
    client: HubeauAPIClient,
    endpoint: str,
    params: Dict,
    resource_name: str,
    station_field: str = 'code_bss'
) -> Iterator[List[Dict]]:
    """
    Pagination avec slicing par station (pour piezometry_chroniques)

    L'API Hub'Eau piezometry/chroniques IMPOSE le parametre code_bss
    """
    logger.info(f"{resource_name}: Mode SLICING par station (champ: {station_field})")

    # Etape 1 : Recuperer la liste des stations
    # Endpoint stations correspondant
    if 'piezometry' in resource_name:
        stations_endpoint = '/stations.csv'
        stations_base_url = client.base_url
    else:
        raise ValueError(f"Slicing par station non supporte pour {resource_name}")

    logger.info(f"{resource_name}: Recuperation liste des stations...")

    try:
        stations_response = client.get(stations_endpoint, params={'page': 1, 'size': 10000})
        stations_df = pd.read_csv(
            io.StringIO(stations_response.text),
            sep=';',
            quotechar='"'
        )

        stations = stations_df[station_field].unique().tolist()
        logger.info(f"{resource_name}: {len(stations)} stations a traiter")

    except Exception as e:
        logger.error(f"{resource_name}: Impossible de recuperer stations: {e}")
        raise

    # Etape 2 : Pour chaque station, recuperer ses chroniques
    total_records = 0

    for idx, station_code in enumerate(stations, 1):
        logger.info(f"{resource_name}: Station {idx}/{len(stations)}: {station_code}")

        # Parametres avec code station
        station_params = {**params, station_field: station_code}

        try:
            station_records = 0
            # Pagination pour cette station
            for records in _paginate_csv(client, endpoint, station_params, f"{resource_name}[{station_code}]"):
                station_records += len(records)
                total_records += len(records)
                yield records

            logger.info(f"✅ {resource_name}: Station {idx}/{len(stations)} ({station_code}) terminée: {station_records:,} records")

        except Exception as e:
            logger.error(f"❌ {resource_name}: Erreur station {station_code}: {type(e).__name__}: {e}")
            # Continuer avec les autres stations
            continue

    logger.info(f"{resource_name}: TERMINE slicing - {total_records:,} records ingeres")


@dlt.source
def hubeau_csv_source(
    resource_name: str,
    endpoint: str,
    base_url: str,
    primary_key: List[str],
    performance_config: Dict,
    default_params: Dict = None,
    mode: IngestionMode = IngestionMode.FULL,
    year: Optional[int] = None,
    incremental_days: int = 2,
    use_station_slicing: bool = False
):
    """
    Source DLT pour ingestion CSV Hub'Eau

    Args:
        resource_name: Nom de la ressource
        endpoint: Endpoint API (ex: '/chroniques.csv')
        base_url: URL de base
        primary_key: Cles primaires
        performance_config: Config performance
        default_params: Parametres par defaut
        mode: Mode d'ingestion (FULL, YEAR, INCREMENTAL)
        year: Annee (mode YEAR)
        incremental_days: Nombre de jours (mode INCREMENTAL)
        use_station_slicing: Activer slicing par station
    """

    # Compteur global pour tracking
    total_records_yielded = {'count': 0}

    @dlt.resource(
        name=resource_name,
        primary_key=primary_key,
        write_disposition="merge"
    )
    def csv_resource() -> Iterator[List[Dict]]:

        # Client HTTP
        client = HubeauAPIClient(
            base_url=base_url,
            rate_limit=performance_config.get('rate_limit', 2.0)
        )

        # Construire parametres selon mode
        params = build_params_for_mode(
            mode=mode,
            year=year,
            incremental_days=incremental_days,
            default_params=default_params,
            resource_name=resource_name
        )

        logger.info(f"🔧 {resource_name}: Mode={mode.value}, Params={params}")

        # Choisir strategie de pagination
        if use_station_slicing:
            # Slicing par station (piezometry_chroniques)
            for batch in _paginate_with_station_slicing(
                client=client,
                endpoint=endpoint,
                params=params,
                resource_name=resource_name
            ):
                total_records_yielded['count'] += len(batch)
                yield batch
        else:
            # Pagination standard
            for batch in _paginate_csv(
                client=client,
                endpoint=endpoint,
                params=params,
                resource_name=resource_name
            ):
                total_records_yielded['count'] += len(batch)
                yield batch

        logger.info(f"✅ {resource_name}: Total yielded = {total_records_yielded['count']:,} records")

    # Attacher le compteur a la resource pour y acceder depuis l'asset
    csv_resource._total_records = total_records_yielded

    return csv_resource
