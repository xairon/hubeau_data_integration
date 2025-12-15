"""
Simplified Hub'Eau API fetcher - NO DLT dependency.
Pure Python generator that fetches data from Hub'Eau API.
"""
import logging
import time
from typing import Any, Dict, Iterator, List, Optional

import httpx

logger = logging.getLogger(__name__)

# Constants
DEFAULT_PAGE_SIZE = 10000
DEFAULT_RATE_LIMIT = 0.3  # seconds between requests


class SimpleHubeauClient:
    """Simple HTTP client for Hub'Eau API with rate limiting."""
    
    def __init__(self, base_url: str, rate_limit: float = DEFAULT_RATE_LIMIT):
        self.base_url = base_url.rstrip('/')
        self.rate_limit = rate_limit
        self.last_request_time = 0
        self.client = httpx.Client(timeout=120.0)
    
    def _wait_for_rate_limit(self):
        elapsed = time.time() - self.last_request_time
        if elapsed < self.rate_limit:
            time.sleep(self.rate_limit - elapsed)
    
    def get(self, endpoint: str, params: Dict[str, Any]) -> httpx.Response:
        self._wait_for_rate_limit()
        url = f"{self.base_url}{endpoint}"
        response = self.client.get(url, params=params)
        self.last_request_time = time.time()
        return response
    
    def close(self):
        self.client.close()


def parse_csv_response(text: str) -> List[Dict[str, str]]:
    """Parse CSV response into list of dicts."""
    import csv
    import io
    
    if not text.strip():
        return []
    
    # Auto-detect delimiter
    first_line = text.split('\n')[0]
    delimiter = ';' if ';' in first_line else ','
    
    reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)
    return list(reader)


def fetch_chroniques_year(
    config: Dict[str, Any],
    year: str,
    dagster_context=None
) -> Iterator[List[Dict]]:
    """
    Fetch piezometry chroniques for a specific year.
    
    This is a SIMPLE generator with NO DLT dependency.
    Yields batches of records for direct PostgreSQL insertion.
    
    Args:
        config: Configuration from YAML
        year: Year to fetch (e.g., "2004")
        dagster_context: Optional Dagster context for logging
    
    Yields:
        List[Dict] batches of records
    """
    def log_info(msg: str):
        logger.info(msg)
        if dagster_context:
            dagster_context.log.info(msg)
    
    def log_warning(msg: str):
        logger.warning(msg)
        if dagster_context:
            dagster_context.log.warning(msg)
    
    # Extract config
    base_url = config["resource"]["base_url"]
    endpoint = config["resource"]["endpoint"]
    stations_endpoint = config["resource"]["stations_endpoint"]
    station_param = config["extraction"]["station_slicing"]["station_param"]
    batch_size = config["extraction"]["station_slicing"].get("batch_size", 20)
    rate_limit = config.get("performance", {}).get("rate_limit", DEFAULT_RATE_LIMIT)
    
    # Date filter params
    date_filter_params = config.get("extraction", {}).get("date_filter_params", {})
    param_date_debut = date_filter_params.get("date_debut", "date_debut_mesure")
    param_date_fin = date_filter_params.get("date_fin", "date_fin_mesure")
    
    client = SimpleHubeauClient(base_url, rate_limit)
    
    try:
        # Step 1: Fetch all station codes
        log_info(f"🔍 Fetching station list from {stations_endpoint}...")
        station_codes = _fetch_all_stations(client, stations_endpoint, station_param)
        log_info(f"✅ Found {len(station_codes):,} stations")
        
        if not station_codes:
            log_warning("No stations found!")
            return
        
        # Step 2: Process stations in batches
        total_records = 0
        num_batches = (len(station_codes) + batch_size - 1) // batch_size
        
        for batch_idx in range(num_batches):
            start_idx = batch_idx * batch_size
            end_idx = min(start_idx + batch_size, len(station_codes))
            batch_stations = station_codes[start_idx:end_idx]
            
            log_info(f"📦 [{batch_idx + 1}/{num_batches}] Processing {len(batch_stations)} stations...")
            
            # Build query params
            params = {
                station_param: ",".join(batch_stations),
                param_date_debut: f"{year}-01-01",
                param_date_fin: f"{year}-12-31",
                "size": DEFAULT_PAGE_SIZE
            }
            
            # Fetch all pages for this batch
            batch_records = _fetch_all_pages(client, endpoint, params)
            
            if batch_records:
                total_records += len(batch_records)
                log_info(f"✅ [{batch_idx + 1}/{num_batches}] Got {len(batch_records):,} records (Total: {total_records:,})")
                yield batch_records
        
        log_info(f"🎉 COMPLETE: {total_records:,} total records from {len(station_codes):,} stations")
        
    finally:
        client.close()


def _fetch_all_stations(client: SimpleHubeauClient, endpoint: str, station_param: str) -> List[str]:
    """Fetch all station codes from stations endpoint."""
    all_stations = []
    page = 1
    
    while True:
        params = {"page": page, "size": DEFAULT_PAGE_SIZE}
        response = client.get(endpoint, params)
        
        if response.status_code != 200:
            logger.warning(f"Station fetch failed: {response.status_code}")
            break
        
        records = parse_csv_response(response.text)
        if not records:
            break
        
        for record in records:
            # Try normalized key (lowercase with underscores)
            code = record.get(station_param.lower().replace('-', '_'))
            if not code:
                code = record.get(station_param)
            if code:
                all_stations.append(str(code))
        
        if len(records) < DEFAULT_PAGE_SIZE:
            break
        
        page += 1
    
    return all_stations


def _fetch_all_pages(client: SimpleHubeauClient, endpoint: str, params: Dict[str, Any]) -> List[Dict]:
    """Fetch all pages for given params."""
    all_records = []
    page = 1
    
    while True:
        params["page"] = page
        response = client.get(endpoint, params)
        
        if response.status_code != 200:
            logger.warning(f"Fetch failed: {response.status_code}")
            break
        
        records = parse_csv_response(response.text)
        if not records:
            break
        
        all_records.extend(records)
        
        if len(records) < DEFAULT_PAGE_SIZE:
            break
        
        page += 1
    
    return all_records
