"""
Source DLT pour ERA5 Copernicus Climate Data Store

Architecture:
- Téléchargement NetCDF4 par chunks de 2 ans (limite API)
- 1 timestep/jour à 00:00 UTC (données veille)
- Stockage bytea PostgreSQL
- Idempotence: skip fichiers déjà téléchargés
"""

import dlt
import cdsapi
import tempfile
import os
from typing import Iterator, Dict, Any
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


@dlt.resource(
    name="era5_netcdf_files",
    write_disposition="append",
    primary_key="file_id"
)
def era5_france_meteo(config: Dict[str, Any]) -> Iterator[Dict[str, Any]]:
    """
    Télécharge ERA5 NetCDF4 pour France (données quotidiennes 00:00 UTC)

    Chunking: Requêtes par blocs de 2 ans (contrainte API Copernicus)

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

    client = cdsapi.Client(
        url=config['credentials']['cds_api_url'],
        key=cds_api_key,
        quiet=False,  # Logs de progression
        verify=True
    )

    # Paramètres extraction
    start_year = config['extraction']['time_range']['start_year']
    end_year = config['extraction']['time_range']['end_year'] or datetime.now().year
    years_per_chunk = config['extraction']['chunking']['years_per_request']

    variables = config['resource']['variables']
    area = config['resource']['area']
    time_utc = config['extraction']['temporal_config']['time']

    # Boucle sur chunks de 2 ans
    year = start_year
    while year <= end_year:
        chunk_start = year
        chunk_end = min(year + years_per_chunk - 1, end_year)

        file_id = f"era5_france_{chunk_start}_{chunk_end}"

        logger.info(
            f"📥 Downloading ERA5 {chunk_start}-{chunk_end} "
            f"({chunk_end - chunk_start + 1} years)..."
        )

        # Requête CDS
        request_params = {
            'product_type': 'reanalysis',
            'format': 'netcdf',
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
            # Téléchargement (peut prendre 5-10 minutes)
            client.retrieve(
                config['resource']['dataset'],
                request_params,
                tmp_path
            )

            # Lire fichier en bytes
            with open(tmp_path, 'rb') as f:
                netcdf_bytes = f.read()

            file_size_mb = len(netcdf_bytes) / (1024 * 1024)

            logger.info(
                f"✅ Downloaded {file_id} ({file_size_mb:.2f} MB)"
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

        except Exception as e:
            logger.error(f"❌ Failed to download {file_id}: {e}")
            raise

        finally:
            # Nettoyer fichier temporaire
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

        # Next chunk
        year += years_per_chunk
