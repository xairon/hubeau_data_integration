"""
ERA5 Bronze Layer Assets

Stockage NetCDF4 bruts en PostgreSQL
- 1 timestep/jour (00:00 UTC)
- Chunks de 2 ans (limite API)
- ~43 fichiers pour 1940-2025
"""

import yaml
from dagster import asset
from hubeau_pipeline.sources.era5_source import era5_france_meteo
from hubeau_pipeline.utils.dlt_batching import (
    create_dlt_pipeline,
    run_dlt_resource,
)


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
    Period: 1940-present
    Frequency: Daily at 00:00 UTC (previous day data)
    Storage: PostgreSQL bytea (~43 files × 50-100 MB)

    Note: Downloads in 2-year chunks (API limitation)
    Expected runtime: ~3-5 hours for full historical download
    """
    config_path = "configs/era5/era5_france_meteo.yml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    pipeline = create_dlt_pipeline("era5_france_meteo", context)

    metrics = run_dlt_resource(
        pipeline=pipeline,
        resource=era5_france_meteo(config),
        context=context,
        table_name="era5_france_meteo_raw",
    )

    context.log.info(
        f"✅ Stored {metrics.get('rows_loaded', 0)} NetCDF4 files "
        f"(Total: {metrics.get('total_size_mb', 0):.2f} MB)"
    )

    return metrics
