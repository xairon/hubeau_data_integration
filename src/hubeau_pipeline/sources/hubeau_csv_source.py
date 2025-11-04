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


def fetch_page(
    client: HubeauAPIClient,
    endpoint: str,
    page: int,
    params: Dict = None
) -> List[Dict]:
    """
    Fetch single page from Hub'Eau API
    Returns list of records (dicts)
    """
    final_params = {**(params or {}), 'page': page}

    try:
        response = client.get(endpoint, params=final_params, timeout=180)

        if not response.text or len(response.text.strip()) == 0:
            logger.warning(f"Page {page}: Empty response")
            return []

        # Parse CSV
        df = pd.read_csv(
            io.StringIO(response.text),
            sep=';',
            quotechar='"',
            low_memory=False,
            on_bad_lines='skip'
        )

        if df.empty:
            return []

        return df.to_dict('records')

    except Exception as e:
        logger.warning(f"Page {page} error: {e}")
        return []


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

    client = HubeauAPIClient(base_url, rate_limit=rate_limit)

    # Date filters based on partition mode
    if year == "full":
        # FULL MODE: Load ALL historical data (no date filter)
        params = {}
        mode_label = "FULL LOAD MODE (ALL HISTORICAL DATA)"
    else:
        # YEAR MODE: Load specific year
        date_debut = f"{year}-01-01"
        date_fin = f"{year}-12-31"
        params = {
            "date_debut_mesure": date_debut,
            "date_fin_mesure": date_fin
        }
        mode_label = f"YEAR PARTITION LOAD: {year}"

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

    params = {
        "date_debut_mesure": date_debut,
        "date_fin_mesure": date_fin
    }

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
