"""
Jobs ERA5 - Bronze Layer

Job pour téléchargement historique ERA5
- Partitionné par chunks de 1 an (ERA5_YEARS_PER_CHUNK, ex: "2024")
- Durée estimée: 5-10 minutes par chunk
- Mode: DELETE overlap + insert (idempotent par fenêtre)
- Support de partitions: relance un chunk spécifique (ex: 2006)
"""

from dagster import AssetKey, AssetSelection, define_asset_job

from ..assets.bronze.era5_assets import ERA5_PARTITIONS_DEF
from ..assets.bronze.era5_daily_temp_assets import ERA5_DAILY_TEMP_PARTITIONS_DEF

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


# Job Historique - Températures journalières (mean/min/max, backfill 1950-Present)
era5_daily_temp_historical_load = define_asset_job(
    name="era5_daily_temp_historical_load",
    description=(
        "Historique ERA5 daily temp stats (1950-Present) - Direct to Bronze. "
        "Partitionné par chunks de 1 an. Télécharge (mean/min/max) & Insère directement."
    ),
    selection=AssetSelection.keys(
        AssetKey("era5_daily_temp_stats_historical")
    ),
    partitions_def=ERA5_DAILY_TEMP_PARTITIONS_DEF,
    # Clé de concurrence DÉDIÉE à l'écriture de bronze.era5_daily_temp_stats (limit 1).
    # Partagée avec era5_daily_temp_update_job pour empêcher TOUTE exécution parallèle
    # des deux writers de cette table : la partition de l'année courante chevauche la
    # fenêtre du nightly, et l'écriture (DELETE overlap + INSERT non atomique, table sans
    # contrainte d'unicité) produirait des doublons en cas de concurrence. Voir incident
    # 2026-07-07 (docs/OPERATIONS.md). NB : le backfill temp se sérialise (1 partition à la fois),
    # tradeoff assumé — le backfill grille (era5_historical, limit 3) n'est pas affecté.
    tags={"dagster/concurrency_key": "era5_daily_temp_write"},
    hooks=set(),
)


# Job Daily Smart Update - Températures journalières
era5_daily_temp_update_job = define_asset_job(
    name="era5_daily_temp_update_job",
    description="Mise à jour quotidienne ERA5 daily temp stats (Last N days -> Bronze)",
    selection=AssetSelection.keys(AssetKey("era5_daily_temp_stats_update")),
    # Même clé dédiée que le backfill historique temp : les deux écrivent
    # bronze.era5_daily_temp_stats et ne doivent JAMAIS tourner en parallèle (limit 1).
    tags={"dagster/concurrency_key": "era5_daily_temp_write"},
    hooks=set(),
)


# L'ancien job d'extraction est obsolète car intégré dans l'ingestion directe.
