"""
Jobs ERA5 - Bronze Layer

Job pour téléchargement historique ERA5
- Durée estimée: 3-5 heures (43 requêtes × 5-10 min/requête)
- Mode: APPEND (idempotent par file_id)
"""

from dagster import define_asset_job, AssetSelection, AssetKey


era5_meteo_job = define_asset_job(
    name="era5_meteo_bronze",
    description=(
        "Bronze: ERA5 France météo NetCDF4 files (daily 00:00 UTC). "
        "Downloads 2-year chunks. Runtime: ~3-5 hours for full history."
    ),
    selection=AssetSelection.keys(
        AssetKey("era5_france_meteo_raw")
    ),
    tags={"dagster/concurrency_key": "era5_meteo_bronze"}
)
