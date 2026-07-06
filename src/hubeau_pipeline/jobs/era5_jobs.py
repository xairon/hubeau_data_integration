"""
Jobs ERA5 - Bronze Layer

Job pour téléchargement historique ERA5
- Partitionné par chunks de 1 an (ERA5_YEARS_PER_CHUNK, ex: "2024")
- Durée estimée: 5-10 minutes par chunk
- Mode: DELETE overlap + insert (idempotent par fenêtre)
- Support de partitions: relance un chunk spécifique (ex: 2006)
"""

from dagster import define_asset_job, AssetSelection, AssetKey
from ..assets.bronze.era5_assets import ERA5_PARTITIONS_DEF


era5_meteo_job = define_asset_job(
    name="era5_historical_load",
    description=(
        "Historique ERA5 (1950-Present) - Direct to Timeseries. "
        "Partitionné par chunks de 1 an. Télécharge & Insère directement."
    ),
    selection=AssetSelection.keys(
        AssetKey("era5_france_timeseries_historical")
    ),
    partitions_def=ERA5_PARTITIONS_DEF,
    tags={"dagster/concurrency_key": "era5_historical"},
    hooks=set(),
)


# Job Weekly (Direct update)
era5_weekly_job = define_asset_job(
    name="era5_weekly_update_job",  # Renommé pour éviter conflit avec l'asset
    description="Mise à jour hebdomadaire ERA5 (Last N days -> Timeseries)",
    selection=AssetSelection.keys(AssetKey("era5_weekly_update")),
    tags={"dagster/concurrency_key": "era5_weekly"},
    hooks=set(),
)


# L'ancien job d'extraction est obsolète car intégré dans l'ingestion directe.
