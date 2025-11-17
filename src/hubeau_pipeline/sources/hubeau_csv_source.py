"""
Source DLT pour Hub'Eau API - Bronze Layer (raw tables)

Architecture simplifiée:
- DLT native parallelization (parallelized=True)
- Custom pagination (Hub'Eau specific)
- Date filters (Hub'Eau specific)
- No FK filtering (Bronze layer loads all data)

Modes:
- FULL: Load all data (stations)
- YEAR: Load specific year (chroniques - partition mode)
- INCREMENTAL: Load since last date (chroniques - incremental mode)
"""

import dlt
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import io
from typing import Iterator, Dict, Any, Optional, List
import logging
import time
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


# ==============================================================================
# HTTP CLIENT
# ==============================================================================

import threading

# ==============================================================================
# GLOBAL THREAD-SAFE RATE LIMITER
# ==============================================================================

class GlobalRateLimiter:
    """
    Thread-safe rate limiter shared across all parallel workers.
    Prevents API bombardment when using DLT parallelization.
    """
    _lock = threading.Lock()
    _last_request_time = 0

    @classmethod
    def wait(cls, rate_limit: float):
        """Thread-safe rate limiting"""
        with cls._lock:
            elapsed = time.time() - cls._last_request_time
            if elapsed < rate_limit:
                sleep_time = rate_limit - elapsed
                time.sleep(sleep_time)
            cls._last_request_time = time.time()


class HubeauAPIClient:
    """
    HTTP client with connection pooling and retry logic.
    Uses GLOBAL rate limiter for thread-safe parallel requests.
    """
    def __init__(self, base_url: str, rate_limit: float = 0.3):
        self.base_url = base_url
        self.rate_limit = rate_limit

        # Session with connection pooling
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

    def get(self, endpoint: str, params: Dict = None, timeout: int = 180) -> requests.Response:
        """GET with GLOBAL thread-safe rate limiting and retry"""
        # Use global rate limiter (shared across all workers)
        GlobalRateLimiter.wait(self.rate_limit)

        url = f"{self.base_url}{endpoint}"

        try:
            response = self.session.get(url, params=params, timeout=timeout)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error {url}: {e}")
            raise


# ==============================================================================
# PAGINATION HELPERS
# ==============================================================================

def get_total_pages(
    client: HubeauAPIClient,
    endpoint: str,
    params: Dict = None
) -> tuple:
    """
    Get total pages and count from JSON endpoint

    Returns:
        (total_pages, total_count)
    """
    endpoint_json = endpoint.replace('.csv', '')

    try:
        logger.info(f"🔍 Fetching pagination info from {endpoint_json} (this may take 30-60 seconds)...")
        logger.info(f"   Params: {params or {}}")
        response = client.get(endpoint_json, params={**(params or {}), 'page': 1})
        logger.info(f"✅ Got response from API")
        data = response.json()
        total_count = data.get('count', 0)

        if total_count == 0:
            return 1, 0

        # Actual page size from first page
        actual_page_size = len(data.get('data', []))

        # Single page API
        if actual_page_size == total_count:
            logger.info(f"{endpoint} - Single page API ({total_count:,} records)")
            return 1, total_count

        if actual_page_size == 0:
            actual_page_size = 1000  # Default Hub'Eau

        total_pages = (total_count // actual_page_size) + (1 if total_count % actual_page_size > 0 else 0)
        logger.info(f"{endpoint} - {total_count:,} records, {total_pages} pages")
        return total_pages, total_count

    except Exception as e:
        logger.error(f"Cannot get count: {e}")
        raise


def detect_delimiter_from_sample(sample: str) -> str:
    """
    Detect CSV delimiter from sample text.
    Hub'Eau typically uses ';' but we auto-detect for robustness.
    """
    import csv
    delimiters = [';', ',', '\t', '|']
    
    try:
        sniffer = csv.Sniffer()
        detected = sniffer.sniff(sample, delimiters=''.join(delimiters))
        return detected.delimiter
    except (csv.Error, AttributeError):
        # Fallback: count occurrences
        delimiter_counts = {}
        for delim in delimiters:
            lines = sample.split('\n')[:5]
            counts = [line.count(delim) for line in lines if line.strip()]
            if counts:
                delimiter_counts[delim] = sum(counts) / len(counts)
        
        if delimiter_counts:
            return max(delimiter_counts, key=delimiter_counts.get)
    
    # Default for Hub'Eau
    return ';'


def fetch_page(
    client: HubeauAPIClient,
    endpoint: str,
    page: int,
    params: Dict = None
) -> List[Dict]:
    """
    Fetch single page from Hub'Eau API with robust CSV parsing.
    
    DLT FEATURES:
    - Auto-detects delimiter (typically ';' for Hub'Eau)
    - Handles encoding issues gracefully
    - Skips bad lines instead of failing
    - Normalizes column names
    
    Returns list of records (dicts)
    """
    final_params = {**(params or {}), 'page': page}

    try:
        response = client.get(endpoint, params=final_params, timeout=180)

        if not response.text or len(response.text.strip()) == 0:
            logger.warning(f"Page {page}: Empty response")
            return []

        # Auto-detect delimiter from first few lines
        sample = response.text[:1024] if len(response.text) > 1024 else response.text
        delimiter = detect_delimiter_from_sample(sample)
        
        # Note: response.text is already decoded by requests library
        # No need to specify encoding for StringIO

        # Parse CSV with robust error handling
        try:
            df = pd.read_csv(
                io.StringIO(response.text),
                sep=delimiter,
                quotechar='"',
                low_memory=False,
                on_bad_lines='skip'  # Skip bad lines instead of failing
            )
        except Exception as parse_error:
            # Fallback: try with default Hub'Eau delimiter
            logger.warning(f"Page {page}: Parse error with delimiter '{delimiter}', trying ';': {parse_error}")
            df = pd.read_csv(
                io.StringIO(response.text),
                sep=';',
                quotechar='"',
                low_memory=False,
                on_bad_lines='skip'
            )

        if df.empty:
            return []

        # Normalize column names (DLT also does this, but pre-processing ensures consistency)
        # Convert to lowercase and replace spaces/special chars
        df.columns = df.columns.str.lower().str.replace(' ', '_').str.replace('[^\w]', '_', regex=True)
        
        # Replace NaN with None for DLT
        df = df.where(pd.notnull(df), None)

        return df.to_dict('records')

    except Exception as e:
        logger.warning(f"Page {page} error: {e}")
        return []


# ==============================================================================
# STATION SLICING HELPERS
# ==============================================================================

def get_station_codes(
    base_url: str,
    stations_endpoint: str,
    station_param: str,
    rate_limit: float = 0.3
) -> List[str]:
    """
    Fetch list of station codes from stations endpoint.

    Args:
        base_url: Base API URL
        stations_endpoint: Endpoint to fetch stations (e.g., /stations.csv)
        station_param: Name of station code field (e.g., code_bss)
        rate_limit: Rate limit for requests

    Returns:
        List of station codes
    """
    client = HubeauAPIClient(base_url, rate_limit=rate_limit)

    logger.info(f"🔍 Fetching station list from {stations_endpoint}...")

    # Get total pages for stations
    total_pages, total_count = get_total_pages(client, stations_endpoint)

    if total_count == 0:
        logger.warning(f"No stations found at {stations_endpoint}")
        return []

    logger.info(f"Found {total_count:,} stations across {total_pages} pages")

    # Fetch all stations
    all_stations = []
    for page_num in range(1, total_pages + 1):
        records = fetch_page(client, stations_endpoint, page_num)
        if records:
            all_stations.extend(records)

    # Extract station codes
    station_codes = []
    for station in all_stations:
        # Try normalized key (lowercase with underscores)
        code = station.get(station_param.lower().replace('-', '_'))
        if not code:
            # Try original key
            code = station.get(station_param)
        if code:
            station_codes.append(str(code))

    logger.info(f"✅ Extracted {len(station_codes):,} station codes")
    return station_codes


# ==============================================================================
# DLT RESOURCES - Bronze Layer
# ==============================================================================

@dlt.resource(
    parallelized=False,  # Disabled - Dagster multiprocess_executor handles parallelism
    write_disposition="replace"
)
def hubeau_stations(
    config: Dict[str, Any]
) -> Iterator[List[Dict]]:
    """
    DLT resource for FULL load (stations)
    - write_disposition="replace" → TRUNCATE + INSERT
    - parallelized=False → Sequential fetching (Dagster handles asset-level parallelism)

    Args:
        config: Resource configuration from YAML

    Yields:
        List of records per page
    """
    base_url = config["resource"]["base_url"]
    endpoint = config["resource"]["endpoint"]
    rate_limit = config.get("performance", {}).get("rate_limit", 0.3)

    client = HubeauAPIClient(base_url, rate_limit=rate_limit)

    # Get pagination info
    total_pages, total_count = get_total_pages(client, endpoint)

    if total_count == 0:
        logger.warning(f"No data found for {endpoint}")
        return

    logger.info(f"=== STARTING FULL LOAD ===")
    logger.info(f"Total records: {total_count:,}")
    logger.info(f"Total pages: {total_pages}")
    logger.info(f"Mode: Sequential fetching (parallelism handled by Dagster)")
    logger.info(f"Rate limit: {rate_limit}s per request")
    logger.info(f"Batch writes: every 10,000 records")

    # Sequential fetching - Dagster handles asset-level parallelism (3 assets concurrently)
    # Each page fetched sequentially with rate limiting
    records_yielded = 0
    for page_num in range(1, total_pages + 1):
        records = fetch_page(client, endpoint, page_num)
        if records:
            records_yielded += len(records)
            yield records

            # Log progress every 10 pages
            if page_num % 10 == 0:
                progress_pct = (page_num / total_pages) * 100
                logger.info(f"Progress: {page_num}/{total_pages} pages ({progress_pct:.1f}%) - {records_yielded:,} records fetched")

    logger.info(f"=== FETCH COMPLETE === Total: {records_yielded:,} records")


@dlt.resource(
    parallelized=False,  # Disabled - Dagster multiprocess_executor handles parallelism
    write_disposition="append"
)
def hubeau_chroniques_year(
    config: Dict[str, Any],
    year: str
) -> Iterator[List[Dict]]:
    """
    DLT resource for MODE partition (chroniques)
    - write_disposition="append" → INSERT only
    - parallelized=False → Sequential fetching (Dagster handles asset-level parallelism)

    Args:
        config: Resource configuration from YAML
        year: Partition mode - "full" for ALL data, or specific year (e.g., "2024")

    Yields:
        List of records per page
    """
    base_url = config["resource"]["base_url"]
    endpoint = config["resource"]["endpoint"]
    rate_limit = config.get("performance", {}).get("rate_limit", 0.3)

    # Get date filter parameter names from config (with fallback to defaults)
    date_filter_params = config.get("extraction", {}).get("date_filter_params", {})
    param_date_debut = date_filter_params.get("date_debut", "date_debut_mesure")
    param_date_fin = date_filter_params.get("date_fin", "date_fin_mesure")

    client = HubeauAPIClient(base_url, rate_limit=rate_limit)

    # Date filters based on partition mode
    if year == "full":
        # FULL MODE: Load ALL historical data (no date filter)
        params = {}
        mode_label = "FULL LOAD MODE (ALL HISTORICAL DATA)"
    else:
        # YEAR MODE: Load specific year
        mode_label = f"YEAR PARTITION LOAD: {year}"

        # Check if this is a single-param API (like prelevements with 'annee')
        if param_date_fin is None or param_date_fin == "null":
            # Single parameter: just the year as integer (e.g., annee=2020)
            params = {
                param_date_debut: year
            }
            logger.info(f"Using single year parameter: {param_date_debut}={year}")
        else:
            # Date range parameters (e.g., date_debut_mesure=2020-01-01, date_fin_mesure=2020-12-31)
            date_debut = f"{year}-01-01"
            date_fin = f"{year}-12-31"
            params = {
                param_date_debut: date_debut,
                param_date_fin: date_fin
            }
            logger.info(f"Using date range parameters: {param_date_debut}={date_debut}, {param_date_fin}={date_fin}")

    # Check if station slicing is required (e.g., piezometry API)
    station_slicing_config = config.get("extraction", {}).get("station_slicing", {})
    if station_slicing_config.get("enabled", False):
        logger.info("⚡ STATION BATCHING MODE ENABLED")

        # Get configuration
        station_param = station_slicing_config.get("station_param")
        stations_endpoint = station_slicing_config.get("stations_endpoint")
        batch_size = station_slicing_config.get("batch_size", 50)

        # Fetch list of station codes
        station_codes = get_station_codes(base_url, stations_endpoint, station_param, rate_limit)

        if len(station_codes) == 0:
            logger.warning("No stations found - cannot proceed")
            return

        total_stations = len(station_codes)
        total_chunks = (total_stations + batch_size - 1) // batch_size

        logger.info(f"=== {mode_label} (STATION BATCHING) ===")
        logger.info(f"Total stations: {total_stations:,}")
        logger.info(f"Batch size: {batch_size} stations per chunk")
        logger.info(f"Total chunks: {total_chunks}")
        logger.info(f"Mode: Batching stations with comma-separated codes")
        logger.info(f"Rate limit: {rate_limit}s per request")

        # Process stations in batches
        total_records_yielded = 0
        for chunk_index in range(0, total_stations, batch_size):
            chunk_stations = station_codes[chunk_index:chunk_index + batch_size]
            chunk_num = (chunk_index // batch_size) + 1

            logger.info(f"📦 Chunk {chunk_num}/{total_chunks}: {len(chunk_stations)} stations")
            logger.info(f"   Stations: {', '.join(chunk_stations[:5])}{'...' if len(chunk_stations) > 5 else ''}")

            # Build params with comma-separated station codes
            chunk_params = {
                **params,
                station_param: ','.join(chunk_stations)
            }

            try:
                # Get pagination info for this chunk
                total_pages, total_count = get_total_pages(client, endpoint, chunk_params)

                if total_count == 0:
                    logger.info(f"  No data for chunk {chunk_num}")
                    continue

                logger.info(f"  Found {total_count:,} records across {total_pages} pages")

                # Fetch all pages for this chunk
                for page_num in range(1, total_pages + 1):
                    records = fetch_page(client, endpoint, page_num, chunk_params)
                    if records:
                        total_records_yielded += len(records)
                        yield records

                logger.info(f"✅ Chunk {chunk_num}/{total_chunks} complete: {total_count:,} records")

            except Exception as e:
                logger.error(f"  Error processing chunk {chunk_num}: {e}")
                continue

        logger.info(f"=== {mode_label} COMPLETE === Total: {total_records_yielded:,} records from {total_stations} stations in {total_chunks} chunks")

    else:
        # Standard mode (no station slicing)
        # Get pagination info
        total_pages, total_count = get_total_pages(client, endpoint, params)

        if total_count == 0:
            logger.warning(f"No data found for partition: {year}")
            return

        logger.info(f"=== {mode_label} ===")
        logger.info(f"Total records: {total_count:,}")
        logger.info(f"Total pages: {total_pages}")
        logger.info(f"Mode: Sequential fetching (parallelism handled by Dagster)")
        logger.info(f"Rate limit: {rate_limit}s per request")
        logger.info(f"Batch writes: every 10,000 records")

        # Sequential fetching - Dagster handles asset-level parallelism (3 assets concurrently)
        records_yielded = 0
        for page_num in range(1, total_pages + 1):
            records = fetch_page(client, endpoint, page_num, params)
            if records:
                records_yielded += len(records)
                yield records

            # Log progress every 10 pages
            if page_num % 10 == 0:
                progress_pct = (page_num / total_pages) * 100
                logger.info(f"Progress: {page_num}/{total_pages} pages ({progress_pct:.1f}%) - {records_yielded:,} records")

        logger.info(f"=== {mode_label} COMPLETE === Total: {records_yielded:,} records")


@dlt.resource(
    parallelized=False,  # Disabled - Dagster multiprocess_executor handles parallelism
    write_disposition="append"
)
def hubeau_chroniques_incremental(
    config: Dict[str, Any],
    last_date: Optional[dlt.sources.incremental] = None
) -> Iterator[List[Dict]]:
    """
    DLT resource for INCREMENTAL loading (chroniques)
    - write_disposition="append" → INSERT only
    - parallelized=False → Sequential fetching (Dagster handles asset-level parallelism)
    - DLT tracks last_date automatically

    Args:
        config: Resource configuration from YAML
        last_date: DLT incremental tracker (automatic)

    Yields:
        List of records per page
    """
    base_url = config["resource"]["base_url"]
    endpoint = config["resource"]["endpoint"]
    rate_limit = config.get("performance", {}).get("rate_limit", 0.3)

    # Get date filter parameter names from config (with fallback to defaults)
    date_filter_params = config.get("extraction", {}).get("date_filter_params", {})
    param_date_debut = date_filter_params.get("date_debut", "date_debut_mesure")
    param_date_fin = date_filter_params.get("date_fin", "date_fin_mesure")

    client = HubeauAPIClient(base_url, rate_limit=rate_limit)

    # Determine date range
    if last_date and last_date.last_value:
        # Continue from last loaded date
        date_debut = last_date.last_value
        logger.info(f"Incremental: loading since {date_debut}")
    else:
        # First load: start from last year
        date_debut = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
        logger.info(f"First load: loading from {date_debut}")

    date_fin = datetime.now().strftime("%Y-%m-%d")

    # Use API-specific parameter names from config
    # Check if this is a single-param API (like prelevements with 'annee')
    if param_date_fin is None or param_date_fin == "null":
        # Single parameter: extract year from date_debut (e.g., annee=2024)
        year = date_debut[:4]  # Extract YYYY from YYYY-MM-DD
        params = {
            param_date_debut: year
        }
        logger.info(f"Using single year parameter: {param_date_debut}={year}")
    else:
        # Date range parameters (standard case)
        params = {
            param_date_debut: date_debut,
            param_date_fin: date_fin
        }
        logger.info(f"Using date range parameters: {param_date_debut}={date_debut}, {param_date_fin}={date_fin}")

    # Get pagination info
    total_pages, total_count = get_total_pages(client, endpoint, params)

    if total_count == 0:
        logger.info(f"No new data since {date_debut}")
        return

    logger.info(f"=== INCREMENTAL LOAD: {date_debut} → {date_fin} ===")
    logger.info(f"Total records: {total_count:,}")
    logger.info(f"Total pages: {total_pages}")
    logger.info(f"Mode: Sequential fetching (parallelism handled by Dagster)")
    logger.info(f"Rate limit: {rate_limit}s per request")
    logger.info(f"Batch writes: every 10,000 records")

    # Sequential fetching - Dagster handles asset-level parallelism (3 assets concurrently)
    records_yielded = 0
    for page_num in range(1, total_pages + 1):
        records = fetch_page(client, endpoint, page_num, params)
        if records:
            records_yielded += len(records)
            yield records

        # Log progress every 10 pages
        if page_num % 10 == 0:
            progress_pct = (page_num / total_pages) * 100
            logger.info(f"Progress: {page_num}/{total_pages} pages ({progress_pct:.1f}%) - {records_yielded:,} records")

    logger.info(f"=== INCREMENTAL COMPLETE === Total: {records_yielded:,} records")
