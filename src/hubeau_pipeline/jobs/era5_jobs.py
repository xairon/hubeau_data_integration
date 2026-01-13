"""
Jobs ERA5 - Bronze Layer

Job pour téléchargement historique ERA5
- Durée estimée: 3-5 heures (43 requêtes × 5-10 min/requête)
- Mode: APPEND (idempotent par file_id)
- Support de partitions: relance un chunk spécifique (ex: 2006)
"""

from dagster import define_asset_job, AssetSelection, AssetKey


era5_meteo_job = define_asset_job(
    name="era5_meteo_bronze",
    description=(
        "Bronze: ERA5 France météo NetCDF4 files (daily 00:00 UTC). "
        "Downloads 2-year chunks. Runtime: ~3-5 hours for full history. "
        "PARTITIONNÉ: Sélectionne une année (ex: 2006) pour re-télécharger uniquement ce chunk."
    ),
    selection=AssetSelection.keys(
        AssetKey("era5_france_meteo_raw")
    ),
    tags={"dagster/concurrency_key": "era5_meteo_bronze"},
    # Permet de lancer avec ou sans partition
    partitions_def=None  # Hérité de l'asset
)


era5_timeseries_job = define_asset_job(
    name="era5_timeseries_extract",
    description=(
        "Extract ERA5 NetCDF bytea data to normalized time series table. "
        "Unpacks ZIP-compressed NetCDF, converts units (K→°C, m→mm), "
        "creates staging.era5_france_timeseries (~277M rows). "
        "Runtime: ~30-60 minutes for full dataset. "
        "PARTITIONNÉ: Sélectionne une année (ex: 2006) pour extraire uniquement ce chunk."
    ),
    selection=AssetSelection.keys(
        AssetKey("era5_france_timeseries")
    ),
    tags={"dagster/concurrency_key": "era5_timeseries"},
    # Permet de lancer avec ou sans partition
    partitions_def=None  # Hérité de l'asset
)
