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
        # Requete 1 : count total SANS specifier 'size' pour exploiter le bug Hub'Eau
        # Bug Hub'Eau : si 'size' n'est PAS specifie dans les params, on peut acceder a TOUTES les pages
        # sans la limite habituelle de 20,000 records max (avec le page_size par defaut ~5k)
        # ⚠️ NE PAS AJOUTER 'size' sinon Hub'Eau impose une limite de 20k records total !
        response = client.get(endpoint_json, params={**(params or {}), 'page': 1})
        data = response.json()
        total_count = data.get('count', 0)

        if total_count == 0:
            return 1, 0

        # Taille de page reelle (varie selon endpoint, generalement ~5000 records)
        actual_page_size = len(data.get('data', []))

        # ⚠️ DETECTION HYDROBIO : Si page_size == count, l'API n'a PAS de pagination !
        if actual_page_size == total_count:
            logger.warning(
                f"⚠️ {endpoint}: API sans pagination détectée ! "
                f"{total_count:,} records sur UNE SEULE page (risque RAM élevé)"
            )
            # Forcer 1 seule page (tout est déjà là)
            return 1, total_count

        if actual_page_size == 0:
            actual_page_size = 5000  # Default Hub'Eau (estimation conservatrice)

        total_pages = (total_count // actual_page_size) + (1 if total_count % actual_page_size > 0 else 0)

        logger.info(f"📊 {endpoint}: Total={total_count:,} records ({actual_page_size} rec/page, {total_pages} pages)")

        return total_pages, total_count

    except Exception as e:
        logger.error(f"Impossible de recuperer count: {e}")
        raise


def fetch_csv_page(
    client: HubeauAPIClient,
    endpoint: str,
    page: int,
    params: Dict = None,
    max_retries: int = 3
) -> pd.DataFrame:
    """
    Télécharge une page CSV complète (~5000 records par défaut selon l'API)

    SIMPLIFIÉ: Pas de chunking - l'API Hub'Eau retourne des pages de ~5k records
    On charge la page complète en mémoire (avec 32GB RAM, pas de problème)

    Args:
        client: Client HTTP
        endpoint: Endpoint API
        page: Numéro de page
        params: Paramètres de requête
        max_retries: Nombre de tentatives en cas de timeout (défaut: 3)

    Returns:
        DataFrame complet de la page (~5000 lignes)
    """
    retry_count = 0

    while retry_count <= max_retries:
        try:
            # Simple GET request
            response = client.get(
                endpoint,
                params={**(params or {}), 'page': page},
                timeout=180
            )

            # Parse CSV complet (pas de chunking)
            df = pd.read_csv(
                io.StringIO(response.text),
                sep=';',
                quotechar='"',
                low_memory=False,
                on_bad_lines='skip'
            )

            return df

        except pd.errors.EmptyDataError:
            logger.warning(f"⚠️  Page {page}: CSV vide")
            return pd.DataFrame()

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
    Pagination simple: une page = ~5000 records

    SIMPLIFIÉ: Pas de chunking, pas de micro-batching
    L'API Hub'Eau retourne des pages de ~5k records, on les charge complètes
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

    logger.info(f"🚀 {resource_name}: Pagination de {total_pages} pages (~5k records/page)")

    for page in range(1, total_pages + 1):
        try:
            # ✅ Charger page complète (DataFrame complet)
            df = fetch_csv_page(client, endpoint, page, params)

            if df.empty:
                logger.warning(f"⚠️  {resource_name}: Page {page}/{total_pages} vide")
                continue

            # Convertir toute la page en liste de dicts
            records = df.to_dict('records')
            records_count = len(records)
            records_total += records_count

            # Log progression
            progress_pct = (records_total / total_count * 100) if total_count > 0 else 0
            logger.info(
                f"📥 {resource_name}: Page {page}/{total_pages} -> "
                f"{records_count:,} records (total: {records_total:,}/{total_count:,} = {progress_pct:.1f}%)"
            )

            # Yield toute la page d'un coup
            yield records

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
        # ⚠️ NE PAS specifier 'size' pour exploiter le bug Hub'Eau (pas de limite 20k records)
        # Paginer les stations + chunking pour économiser RAM (certains endpoints ont 50k+ stations)
        all_stations = []
        page = 1
        max_pages = 100  # Sécurité pour éviter boucle infinie

        logger.info(f"{resource_name}: Récupération liste des stations (avec pagination + chunking)...")

        while page <= max_pages:
            try:
                stations_response = client.get(stations_endpoint, params={'page': page})

                # Vérifier si réponse vide
                if not stations_response.text or len(stations_response.text) < 10:
                    logger.info(f"  Page {page}: vide, fin de pagination")
                    break

                # ✅ Charger page complète (pas de chunking avec 32GB RAM)
                df_stations = pd.read_csv(
                    io.StringIO(stations_response.text),
                    sep=';',
                    quotechar='"',
                    low_memory=False,
                    on_bad_lines='skip'
                )

                if station_field not in df_stations.columns:
                    raise ValueError(f"Colonne {station_field} introuvable dans les stations")

                # Extraire codes stations uniques de la page
                page_stations = df_stations[station_field].dropna().unique().tolist()

                if not page_stations:
                    logger.info(f"  Page {page}: aucune station, fin de pagination")
                    break

                all_stations.extend(page_stations)
                logger.info(f"  Page {page}: +{len(page_stations)} stations (total cumulé: {len(all_stations)})")

                # Si < 1000 stations sur cette page, probablement la dernière
                if len(page_stations) < 1000:
                    logger.info(f"  Page {page}: moins de 1000 stations, considéré comme dernière page")
                    break

                page += 1

            except Exception as page_error:
                logger.warning(f"  Page {page}: erreur {type(page_error).__name__}: {page_error}")
                # Si erreur à la page > 1, on continue avec ce qu'on a
                if page > 1:
                    logger.info(f"  Erreur pagination page {page}, mais {len(all_stations)} stations déjà récupérées")
                    break
                else:
                    # Erreur à la page 1 = problème critique
                    raise

        # Dédupliquer les stations (au cas où même station sur plusieurs pages)
        stations = list(set(all_stations))
        logger.info(f"{resource_name}: {len(stations)} stations uniques à traiter (dédupliquées depuis {len(all_stations)} entrées)")

        # Log RAM après chargement stations (monitoring)
        try:
            import psutil
            import os
            process = psutil.Process(os.getpid())
            mem_mb = process.memory_info().rss / 1024 / 1024
            logger.info(f"[MEM] Après chargement {len(stations)} stations: {mem_mb:.1f} MB")
        except ImportError:
            pass  # psutil pas dispo, skip
        except Exception as mem_error:
            logger.debug(f"Impossible de logger RAM: {mem_error}")

    except Exception as e:
        logger.error(f"{resource_name}: Impossible de recuperer stations: {e}")
        raise

    # Etape 2 : Pour chaque batch de stations, recuperer leurs chroniques
    # ✅ BATCH PAR 80 STATIONS (API Hub'Eau supporte jusqu'à 200 code_bss en query param)
    STATIONS_PER_BATCH = 80
    total_records = 0

    # Diviser stations en batches de 80
    station_batches = [stations[i:i + STATIONS_PER_BATCH] for i in range(0, len(stations), STATIONS_PER_BATCH)]
    num_batches = len(station_batches)

    logger.info(f"{resource_name}: {len(stations)} stations → {num_batches} batches de {STATIONS_PER_BATCH} stations")

    for batch_idx, station_batch in enumerate(station_batches, 1):
        # Joindre les codes stations avec virgule pour query param
        stations_param = ','.join(station_batch)

        logger.info(
            f"{resource_name}: Batch {batch_idx}/{num_batches} "
            f"({len(station_batch)} stations: {station_batch[0]}...{station_batch[-1]})"
        )

        # Parametres avec batch de stations
        batch_params = {**params, station_field: stations_param}

        try:
            batch_records = 0
            # Pagination pour ce batch de stations
            for records in _paginate_csv(client, endpoint, batch_params, f"{resource_name}[batch_{batch_idx}]"):
                batch_records += len(records)
                total_records += len(records)
                yield records

            logger.info(
                f"✅ {resource_name}: Batch {batch_idx}/{num_batches} terminé: "
                f"{batch_records:,} records ({total_records:,} cumulés)"
            )

        except Exception as e:
            logger.error(
                f"❌ {resource_name}: Erreur batch {batch_idx}/{num_batches} "
                f"(stations {station_batch[0]}...{station_batch[-1]}): {type(e).__name__}: {e}"
            )
            # Continuer avec les autres batches
            continue

    logger.info(f"{resource_name}: TERMINE slicing - {total_records:,} records ingeres depuis {num_batches} batches")


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
        # NOTE: primary_key non utilisé car on utilise custom destination PostgreSQL
        # qui fait le merge manuellement dans postgres_optimized_v2.py
        # Garder None ici économise de la RAM (DLT ne track pas les PK inutilement)
        primary_key=None,
        write_disposition="append"  # append car on gère le merge nous-même
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


def get_raw_data_iterator(config_dict: Dict) -> Iterator[List[Dict]]:
    """
    Get raw data iterator without DLT resource wrapper.
    Returns actual lists of records (pages), not individual records.

    This bypasses DLT's automatic list unpacking behavior which converts
    yielded lists into individual dict records.

    Args:
        config_dict: Configuration dictionary containing:
            - resource: name, endpoint, base_url, primary_key
            - extraction: default_params
            - performance: rate_limit
            - pagination: station_field, stations_endpoint (optional)

    Returns:
        Iterator yielding lists of ~5000 dict records (complete pages)
    """
    # Extract configuration
    resource_config = config_dict.get('resource', {})
    extraction_config = config_dict.get('extraction', {})
    performance_config = config_dict.get('performance', {})
    pagination_config = config_dict.get('pagination', {})

    resource_name = resource_config.get('name', 'unknown')
    endpoint = resource_config.get('endpoint')
    base_url = resource_config.get('base_url', 'https://hubeau.eaufrance.fr/api/v1')

    # Create API client
    client = HubeauAPIClient(
        base_url=base_url,
        rate_limit=performance_config.get('rate_limit', 2.0)
    )

    # Build parameters (default to FULL mode for simplicity)
    params = extraction_config.get('default_params', {})

    logger.info(f"🔧 {resource_name}: Direct iterator (bypass DLT), params={params}")

    # Choose pagination method based on config
    if 'station_field' in pagination_config:
        # Station-based pagination (e.g., piezometry_chroniques)
        station_field = pagination_config['station_field']

        logger.info(f"📍 {resource_name}: Using station slicing (field: {station_field})")

        return _paginate_with_station_slicing(
            client=client,
            endpoint=endpoint,
            params=params,
            resource_name=resource_name,
            station_field=station_field
        )
    else:
        # Standard pagination
        logger.info(f"📄 {resource_name}: Using standard pagination")

        return _paginate_csv(
            client=client,
            endpoint=endpoint,
            params=params,
            resource_name=resource_name
        )
