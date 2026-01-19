"""
Jobs ERA5 - Bronze Layer

Job pour téléchargement historique ERA5
- Partitionné par chunks de 2 ans (ex: "2024_2025")
- Durée estimée: 5-10 minutes par chunk
- Mode: APPEND (idempotent par file_id)
- Support de partitions: relance un chunk spécifique (ex: 2006)
"""

from dagster import define_asset_job, AssetSelection, AssetKey
from ..assets.bronze.era5_assets import ERA5_PARTITIONS_DEF


era5_meteo_job = define_asset_job(
    name="era5_meteo_bronze",
    description=(
        "Bronze: ERA5 France météo NetCDF4 files (daily 00:00 UTC). "
        "Partitionné par chunks de 2 ans (ex: 2024_2025). Runtime: ~5-10 min par chunk."
    ),
    selection=AssetSelection.keys(
        AssetKey("era5_france_meteo_raw")
    ),
    partitions_def=ERA5_PARTITIONS_DEF,
    tags={"dagster/concurrency_key": "era5_meteo_bronze"}
)


era5_timeseries_job = define_asset_job(
    name="era5_timeseries_extract",
    description=(
        "Extract ERA5 NetCDF bytea data to normalized time series table. "
        "Unpacks ZIP-compressed NetCDF, converts units (K→°C, m→mm), "
        "creates bronze.era5_france_timeseries (~300M rows). "
        "Runtime: ~30-60 minutes for full dataset, ~1-2 min per chunk."
    ),
    selection=AssetSelection.keys(
        AssetKey("era5_france_timeseries")
    ),
    tags={"dagster/concurrency_key": "era5_timeseries"}
)
