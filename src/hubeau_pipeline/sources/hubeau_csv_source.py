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
    log = context.log.info if context else logger.info
    page = 1
    total_yielded = 0
    while True:
        request_params = {**(params or {}), "page": page}
        try:
            response = client.get(endpoint, params=request_params)
        except Exception as e:
            log(f"❌ HTTP Request failed: {e}")
            raise e
        
        page_records = list(parse_csv_stream(response.text))
        page_count = len(page_records)
        
        if page_count == 0:
            break
            
        for record in page_records:
            total_yielded += 1
            yield record
        
        page += 1
        if page % 10 == 0:
            log(f"📊 Progress: {total_yielded:,} records...")
    
    log(f"✅ Fetched {total_yielded:,} records total.")

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
        
        # Log progress every 10 batches or at start/end
        if batch_idx == 1 or batch_idx % 10 == 0 or batch_idx == total_batches:
            elapsed = time.time() - batch_start_time
            progress_pct = (batch_idx / total_batches) * 100
            log(f"🔄 Lot {batch_idx}/{total_batches} ({progress_pct:.1f}%) - {len(station_batch)} stations - {total_records:,} enregistrements récupérés jusqu'à présent")
            
        params = {
            date_debut_param: date_start,
            date_fin_param: date_end,
            station_field: codes_str
        }
        
        batch_records = 0
        for record in fetch_all_pages(client, endpoint, params=params, context=dagster_context):
            total_records += 1
            batch_records += 1
            yield record
        
        # Log batch completion for first few batches or every 50th batch
        if batch_idx <= 3 or batch_idx % 50 == 0:
            log(f"  ✓ Lot {batch_idx} terminé: {batch_records:,} enregistrements récupérés")

    elapsed_total = time.time() - batch_start_time
    log(f"✅ Année {year} terminée: {total_records:,} enregistrements au total en {elapsed_total:.1f} secondes")
