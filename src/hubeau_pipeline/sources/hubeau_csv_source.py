"""
Source DLT pour Hub'Eau API - Bronze Layer
"""
import dlt
import csv
import logging
import time
from io import StringIO
from typing import Iterator, Dict, Any, List
from dlt.sources.helpers.rest_client import RESTClient

logger = logging.getLogger(__name__)

# ==============================================================================
# CSV STREAMING PARSER
# ==============================================================================

def parse_csv_stream(text: str, delimiter: str = ';') -> Iterator[Dict[str, Any]]:
    if not text or not text.strip():
        return
    first_line = text.split('\n')[0]
    if ';' in first_line and ',' not in first_line:
        delimiter = ';'
    elif ',' in first_line:
        delimiter = ','
    reader = csv.DictReader(StringIO(text), delimiter=delimiter)
    for row in reader:
        normalized = {k.lower().strip(): (v if v != '' else None) for k, v in row.items()}
        yield normalized

# ==============================================================================
# PAGINATION & HELPERS
# ==============================================================================

def fetch_all_pages(client: RESTClient, endpoint: str, params: Dict[str, Any] = None, context=None) -> Iterator[Dict[str, Any]]:
    """
    Fetch data from Hub'Eau API with cursor-based pagination.
    Hub'Eau API uses Link header with rel="next" for pagination.
    Each page returns up to 1000 records.
    Includes retry logic for 503 errors.
    """
    import re
    import requests
    import time as time_module
    
    log = context.log.info if context else logger.info
    
    MAX_RETRIES = 5
    BASE_DELAY = 2  # seconds
    
    def request_with_retry(url, params=None):
        """Make request with exponential backoff retry for 5xx errors."""
        for attempt in range(MAX_RETRIES):
            try:
                response = requests.get(url, params=params)
                response.raise_for_status()
                return response
            except requests.exceptions.HTTPError as e:
                if e.response is not None and e.response.status_code in [500, 502, 503, 504]:
                    if attempt < MAX_RETRIES - 1:
                        delay = BASE_DELAY * (2 ** attempt)
                        log(f"⚠️ API returned {e.response.status_code}, retry {attempt + 1}/{MAX_RETRIES} in {delay}s...")
                        time_module.sleep(delay)
                        continue
                raise
            except Exception as e:
                raise
        return None  # Should not reach here
    
    # Build initial URL
    base_url = client.base_url + endpoint
    
    # Fix: Remove 'size' parameter for API v1 (doesn't support it)
    # API v1: https://hubeau.eaufrance.fr/api/v1/*
    # API v2: https://hubeau.eaufrance.fr/api/v2/*
    cleaned_params = params.copy() if params else {}
    if '/api/v1/' in base_url and 'size' in cleaned_params:
        del cleaned_params['size']
    
    # First request
    try:
        response = request_with_retry(base_url, params=cleaned_params)
    except Exception as e:
        log(f"❌ HTTP Request failed: {e}")
        raise e
    
    page = 1
    total_yielded = 0
    
    while True:
        records = list(parse_csv_stream(response.text))
        page_count = len(records)
        
        for record in records:
            total_yielded += 1
            yield record
        
        # Check for next page in Link header
        link_header = response.headers.get('Link', '')
        next_match = re.search(r'<([^>]+)>;\s*rel="next"', link_header)
        
        if next_match and page_count > 0:
            next_url = next_match.group(1)
            page += 1
            
            # Fix: Remove unsupported 'size' parameter from pagination URL
            # Hub'Eau API v1 doesn't support size parameter in pagination URLs
            from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
            parsed = urlparse(next_url)
            query_params = parse_qs(parsed.query)
            # Remove 'size' parameter if present
            if 'size' in query_params:
                del query_params['size']
                # Rebuild URL without size parameter
                new_query = urlencode(query_params, doseq=True)
                next_url = urlunparse((
                    parsed.scheme, parsed.netloc, parsed.path,
                    parsed.params, new_query, parsed.fragment
                ))
            
            # Small delay between pages to avoid overwhelming API
            time_module.sleep(0.2)
            
            try:
                response = request_with_retry(next_url)
            except Exception as e:
                log(f"❌ HTTP Request failed on page {page}: {e}")
                raise e
        else:
            # No more pages
            break
    
    if page > 1:
        log(f"  ↳ Fetched {total_yielded:,} records ({page} pages)")

def batch_stations(stations: List[str], batch_size: int = 50) -> Iterator[List[str]]:
    for i in range(0, len(stations), batch_size):
        yield stations[i:i + batch_size]

# ==============================================================================
# DLT RESOURCES
# ==============================================================================

def hubeau_stations(config: Dict[str, Any], dagster_context=None) -> Iterator[Dict[str, Any]]:
    base_url = config["resource"]["base_url"]
    endpoint = config["resource"]["endpoint"]
    client = RESTClient(base_url=base_url)
    log = dagster_context.log.info if dagster_context else logger.info
    log(f"Loading stations from {base_url}{endpoint}")
    yield from fetch_all_pages(client, endpoint, context=dagster_context)


def hubeau_chroniques_year(
    config: Dict[str, Any],
    year: str,
    station_codes: List[str],  # PURE LIST: No path, no complexity
    dagster_context=None
) -> Iterator[Dict[str, Any]]:
    """
    Generator that fetches chroniques for a given list of stations.
    Handles batching internally.
    """
    base_url = config["resource"]["base_url"]
    endpoint = config["resource"]["endpoint"]
    
    extraction = config.get("extraction", {})
    date_filter = extraction.get("date_filter_params", {})
    date_debut_param = date_filter.get("date_debut", "date_debut_mesure")
    date_fin_param = date_filter.get("date_fin", "date_fin_mesure")
    
    batching = extraction.get("station_slicing", {})
    station_field = batching.get("station_param", "code_bss")
    batch_size = batching.get("batch_size", 30)
    
    client = RESTClient(base_url=base_url)
    log = dagster_context.log.info if dagster_context else logger.info
    
    date_start = f"{year}-01-01"
    date_end = f"{year}-12-31"
    
    if not station_codes:
        log("⚠️ Aucun code de station fourni à hubeau_chroniques_year.")
        return

    total_stations = len(station_codes)
    total_batches = (total_stations + batch_size - 1) // batch_size
    log(f"📊 Traitement de {total_stations:,} stations en {total_batches} lots de {batch_size} pour l'année {year}")
    log(f"📅 Période: {date_start} → {date_end}")
    
    # Old log line removed - using new format above
    # log(f"� Traitement de {total_stations:,} stations en {total_batches} lots de {batch_size} pour l'année {year}")
    
    total_records = 0
    batch_start_time = time.time()
    
    # Simple, standard batching loop
    for batch_idx, station_batch in enumerate(batch_stations(station_codes, batch_size), 1):
        codes_str = ','.join(station_batch)
        progress_pct = (batch_idx / total_batches) * 100
        
        # Start with default_params from config (e.g. grandeur_hydro_elab: QmnJ)
        default_params = extraction.get("default_params", {})
        params = {
            **default_params,
            date_debut_param: date_start,
            date_fin_param: date_end,
            station_field: codes_str
        }
        
        # Debug: log params for first batch
        if batch_idx == 1:
            log(f"🔧 DEBUG params: {params}")
        
        batch_records = 0
        for record in fetch_all_pages(client, endpoint, params=params, context=dagster_context):
            total_records += 1
            batch_records += 1
            
            if total_records == 1:
                 log(f"🔍 SAMPLE RECORD: {record}")
            
            yield record
        
        # Log progress for every batch
        elapsed = time.time() - batch_start_time
        if batch_idx > 1:
            eta_seconds = (elapsed / batch_idx) * (total_batches - batch_idx)
            eta_str = f"ETA: {eta_seconds/60:.1f}min"
        else:
            eta_str = "ETA: calculating..."
        
        log(f"📊 Lot {batch_idx}/{total_batches} ({progress_pct:.1f}%) | {batch_records:,} records | Total: {total_records:,} | {eta_str}")

    elapsed_total = time.time() - batch_start_time
    log(f"✅ Année {year} terminée: {total_records:,} enregistrements au total en {elapsed_total/60:.1f} minutes")
