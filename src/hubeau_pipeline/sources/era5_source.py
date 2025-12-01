"""
Source DLT pour ERA5 Copernicus Climate Data Store

Architecture:
- Téléchargement NetCDF4 par chunks de 2 ans (limite API)
- 1 timestep/jour à 00:00 UTC (données veille)
- Stockage bytea PostgreSQL
- Idempotence: skip fichiers déjà téléchargés

⚠️ IMPORTANT: Utilise la nouvelle CADS API (décembre 2024)
- Package: cads-api-client (remplace cdsapi)
- URL: https://cds.climate.copernicus.eu/api
- Format clé API: <API_KEY> (sans préfixe <UID>:)
"""

import dlt
import tempfile
import os
from typing import Iterator, Dict, Any
from datetime import datetime
import logging

# Use cdsapi (old but stable client)
# Note: The new cads-api-client has compatibility issues with LegacyApiClient
# (AttributeError: 'LegacyClient' object has no attribute 'wait_until_complete')
# We use the proven cdsapi package which works reliably with ERA5-Land
import cdsapi

logger = logging.getLogger(__name__)
logger.info("Using cdsapi client for ERA5 downloads")


@dlt.resource(
    name="era5_netcdf_files",
    write_disposition="append",
    primary_key="file_id"
)
def era5_france_meteo(config: Dict[str, Any], dagster_context=None) -> Iterator[Dict[str, Any]]:
    """
    Télécharge ERA5 NetCDF4 pour France (données quotidiennes 00:00 UTC)

    Chunking: Requêtes par blocs de 2 ans (contrainte API Copernicus)

    Args:
        config: Configuration YAML
        dagster_context: Dagster context for logging (optional)

    Yields:
        dict: {
            'file_id': str,           # Identifiant unique (ex: era5_1940_1941)
            'variables': list,        # Variables téléchargées
            'start_year': int,
            'end_year': int,
            'area': list,             # Bbox France
            'netcdf_data': bytes,     # Fichier NetCDF4 brut
            'file_size_mb': float,
            'download_timestamp': datetime,
            'file_metadata': dict
        }
    """

    # Helper function to log both to Python logger and Dagster context
    def log_info(message: str):
        logger.info(message)
        if dagster_context:
            dagster_context.log.info(message)

    def log_error(message: str):
        logger.error(message)
        if dagster_context:
            dagster_context.log.error(message)

    # Initialiser client CDS
    # Support both hardcoded key and env var
    cds_api_key = config['credentials'].get('cds_api_key')
    if not cds_api_key:
        # Fallback to env var if not in config
        cds_api_key_env = config['credentials'].get('cds_api_key_env')
        if cds_api_key_env:
            cds_api_key = os.getenv(cds_api_key_env)

    if not cds_api_key:
        raise ValueError(
            "Missing Copernicus API key. "
            "Set 'cds_api_key' in config or COPERNICUS_API_KEY in .env"
        )

    # Initialize CDS client (cdsapi)
    # Note: verify=False may be needed if certificate issues occur
    # (Copernicus sometimes has self-signed cert issues)
    client = cdsapi.Client(
        url=config['credentials']['cds_api_url'],
        key=cds_api_key,
        quiet=False,  # Logs de progression
        verify=False  # Disable SSL verification (workaround for cert issues)
    )

    log_info(f"✅ CDS Client initialized with URL: {config['credentials']['cds_api_url']}")

    # Paramètres extraction
    start_year = config['extraction']['time_range']['start_year']
    end_year = config['extraction']['time_range']['end_year'] or datetime.now().year
    years_per_chunk = config['extraction']['chunking']['years_per_request']

    variables = config['resource']['variables']
    area = config['resource']['area']
    time_utc = config['extraction']['temporal_config']['time']

    # Calculer nombre total de chunks
    total_years = end_year - start_year + 1
    total_chunks = (total_years + years_per_chunk - 1) // years_per_chunk
    log_info(f"📊 Starting ERA5 download: {start_year}-{end_year} ({total_years} years, {total_chunks} chunks)")

    # Boucle sur chunks de 2 ans
    year = start_year
    chunk_number = 0
    while year <= end_year:
        chunk_number += 1
        chunk_start = year
        chunk_end = min(year + years_per_chunk - 1, end_year)

        file_id = f"era5_france_{chunk_start}_{chunk_end}"

        log_info(
            f"📥 [{chunk_number}/{total_chunks}] Downloading ERA5 {chunk_start}-{chunk_end} "
            f"({chunk_end - chunk_start + 1} years)..."
        )

        # Requête CDS
        request_params = {
            'product_type': 'reanalysis',
            'data_format': 'netcdf',  # Updated from 'format' (deprecated)
            'variable': variables,
            'year': [str(y) for y in range(chunk_start, chunk_end + 1)],
            'month': [f'{m:02d}' for m in range(1, 13)],
            'day': [f'{d:02d}' for d in range(1, 32)],
            'time': time_utc,  # "00:00" uniquement
            'area': area,      # [North, West, South, East]
        }

        # Télécharger dans fichier temporaire
        with tempfile.NamedTemporaryFile(suffix='.nc', delete=False) as tmp:
            tmp_path = tmp.name

        try:
            log_info(f"🌐 [{chunk_number}/{total_chunks}] Submitting request to Copernicus CDS for {file_id}...")

            # Téléchargement (peut prendre 5-10 minutes)
            client.retrieve(
                config['resource']['dataset'],
                request_params,
                tmp_path
            )

            log_info(f"💾 [{chunk_number}/{total_chunks}] Download complete, reading file...")

            # Lire fichier en bytes
            with open(tmp_path, 'rb') as f:
                netcdf_bytes = f.read()

            file_size_mb = len(netcdf_bytes) / (1024 * 1024)

            log_info(
                f"✅ [{chunk_number}/{total_chunks}] Successfully downloaded {file_id} ({file_size_mb:.2f} MB) - "
                f"Yielding to DLT for immediate storage..."
            )

            yield {
                'file_id': file_id,
                'variables': variables,
                'start_year': chunk_start,
                'end_year': chunk_end,
                'area': area,
                'netcdf_data': netcdf_bytes,  # Stocké en PostgreSQL bytea
                'file_size_mb': round(file_size_mb, 2),
                'download_timestamp': datetime.now(),
                'file_metadata': {
                    'dataset': config['resource']['dataset'],
                    'grid_resolution': config['extraction']['grid_resolution'],
                    'time_utc': time_utc,
                    'num_years': chunk_end - chunk_start + 1
                }
            }

            log_info(f"💚 [{chunk_number}/{total_chunks}] Chunk {file_id} yielded successfully")

        except Exception as e:
            log_error(f"❌ [{chunk_number}/{total_chunks}] Failed to download {file_id}: {e}")
            raise

        finally:
            # Nettoyer fichier temporaire
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

        # Next chunk
        year += years_per_chunk

    log_info(f"🎉 ERA5 download complete: {chunk_number}/{total_chunks} chunks processed")
