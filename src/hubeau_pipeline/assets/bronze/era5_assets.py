"""
ERA5 Bronze Layer Assets

Stockage NetCDF4 bruts en PostgreSQL
- 1 timestep/jour (00:00 UTC)
- Chunks de 2 ans (limite API)
- ~43 fichiers pour 1940-2025
"""

import yaml
import dlt
import gc
from typing import Dict, Any
from dagster import asset
from hubeau_pipeline.sources.era5_source import era5_france_meteo
from hubeau_pipeline.utils.dlt_batching import create_dlt_pipeline


@asset(
    compute_kind="era5",
    group_name="era5_meteo",
    io_manager_key="noop_io_manager"
)
def era5_france_meteo_raw(context):
    """
    ERA5 France Météo - NetCDF4 daily data (00:00 UTC)

    Variables:
    - 2m_temperature (K)
    - total_precipitation (m)
    - potential_evaporation (m)

    Coverage: France métropolitaine (0.25° grid)
    Period: 1950-present (ERA5-Land)
    Frequency: Daily at 00:00 UTC (previous day data)
    Storage: PostgreSQL bytea (~38 files × 50-100 MB)

    Note: Downloads in 2-year chunks (API limitation)
    Expected runtime: ~3-5 hours for full historical download

    ⚠️ Each chunk is stored IMMEDIATELY in PostgreSQL after download (true incremental loading)
    """
    config_path = "configs/era5/era5_france_meteo.yml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    context.log.info("🚀 Starting ERA5 download with TRUE incremental loading (chunk-by-chunk storage)...")

    total_files = 0
    total_size_mb = 0.0

    # Iterate through ERA5 generator and save EACH chunk individually
    for chunk_index, record in enumerate(era5_france_meteo(config, dagster_context=context), start=1):

        # ⚠️ MEMORY FIX: Recreate pipeline for each chunk to avoid memory accumulation
        context.log.info(f"🔧 [{chunk_index}] Creating fresh DLT pipeline to avoid memory leaks...")
        pipeline = create_dlt_pipeline("era5_france_meteo", context=context)

        # Create a single-record resource for this chunk
        @dlt.resource(
            name="era5_netcdf_files",
            write_disposition="append",
            primary_key="file_id"
        )
        def single_chunk():
            """Single chunk resource for immediate storage"""
            yield record

        # Store THIS chunk immediately in PostgreSQL
        context.log.info(
            f"💾 [{chunk_index}] Storing chunk {record['file_id']} in PostgreSQL NOW "
            f"({record['file_size_mb']:.2f} MB)..."
        )

        load_info = pipeline.run(single_chunk, table_name="era5_france_meteo_raw")

        total_files += 1
        total_size_mb += record.get('file_size_mb', 0)

        context.log.info(
            f"✅ [{chunk_index}] Chunk {record['file_id']} stored successfully in PostgreSQL! "
            f"Total: {total_files} files, {total_size_mb:.2f} MB"
        )

        # ⚠️ MEMORY FIX: Force garbage collection after each chunk
        del pipeline
        del load_info
        gc.collect()
        context.log.info(f"🧹 [{chunk_index}] Memory cleaned (garbage collection completed)")

    context.log.info(
        f"🎉 ERA5 download complete! Stored {total_files} NetCDF4 files in PostgreSQL ({total_size_mb:.2f} MB)"
    )

    return {
        "rows_loaded": total_files,
        "total_size_mb": total_size_mb,
        "status": "success",
        "table_name": "era5_france_meteo_raw"
    }
