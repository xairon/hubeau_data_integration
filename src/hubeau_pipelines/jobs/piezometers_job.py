"""
Job Dagster pour charger les données de piézomètres depuis un fichier CSV.
"""

from dagster import (
    job,
    op,
    RunRequest,
    ScheduleDefinition,
    DefaultScheduleStatus,
    AssetSelection,
    define_asset_job
)


# Job pour charger les données de piézomètres
piezometers_csv_job = define_asset_job(
    name="load_piezometers_csv",
    selection=AssetSelection.keys("piezometers_csv_data"),
    description="Charge les données de piézomètres depuis le fichier CSV vers PostgreSQL",
    tags={
        "type": "csv_ingestion",
        "source": "local_csv",
        "destination": "postgres",
        "table": "piezometers"
    }
)


# Schedule optionnel - peut être activé si on veut recharger régulièrement
piezometers_csv_schedule = ScheduleDefinition(
    job=piezometers_csv_job,
    cron_schedule="0 2 * * 1",  # Tous les lundis à 2h du matin
    default_status=DefaultScheduleStatus.STOPPED,  # Désactivé par défaut
    description="Schedule pour recharger les données de piézomètres chaque semaine"
)