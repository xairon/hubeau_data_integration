"""
Jobs incrementaux et schedules pour ingestion CSV Hub'Eau
"""

from dagster import define_asset_job, schedule, RunRequest, AssetSelection


# Job incremental pour chroniques (donnees temporelles)
incremental_chroniques_job = define_asset_job(
    name="incremental_chroniques_csv",
    description="Ingestion incrementale chroniques (derniers 2 jours)",
    selection=AssetSelection.assets([
        "piezometry_chroniques_csv",
        "quality_groundwater_analyses_csv",
        "quality_rivers_analyses_csv",
        "quality_rivers_conditions_csv",
        "quality_rivers_operations_csv",
        "temperature_chroniques_csv",
        "hydrometry_obs_elab_csv",
        "hydrobio_indices_csv",
        "hydrobio_taxons_csv",
        "ecoulement_observations_csv",
        "prelevements_chroniques_csv"
    ]),
    config={
        "ops": {
            "**": {
                "config": {
                    "mode": "incremental",
                    "incremental_days": 2
                }
            }
        }
    }
)


# Job refresh referentiels
incremental_references_job = define_asset_job(
    name="incremental_references_csv",
    description="Refresh hebdomadaire referentiels (stations)",
    selection=AssetSelection.assets([
        "piezometry_stations_csv",
        "quality_groundwater_stations_csv",
        "quality_rivers_stations_csv",
        "temperature_stations_csv",
        "hydrometry_sites_csv",
        "hydrometry_stations_csv",
        "hydrobio_stations_csv",
        "ecoulement_stations_csv",
        "ecoulement_campagnes_csv",
        "prelevements_points_csv",
        "prelevements_ouvrages_csv"
    ]),
    config={
        "ops": {
            "**": {
                "config": {
                    "mode": "full"
                }
            }
        }
    }
)


# Schedule quotidien pour chroniques
@schedule(
    job=incremental_chroniques_job,
    cron_schedule="0 2 * * *",  # Tous les jours a 02h00
    name="daily_chroniques_refresh_csv"
)
def daily_chroniques_schedule(context):
    """
    Schedule quotidien pour chroniques
    Recupere les donnees des dernieres 48h
    """
    return RunRequest(
        run_key=f"daily_chroniques_{context.scheduled_execution_time.strftime('%Y%m%d')}",
        tags={
            "mode": "incremental",
            "incremental_days": "2",
            "scheduled": "true"
        }
    )


# Schedule hebdomadaire pour referentiels
@schedule(
    job=incremental_references_job,
    cron_schedule="0 3 * * 0",  # Tous les dimanches a 03h00
    name="weekly_references_refresh_csv"
)
def weekly_references_schedule(context):
    """
    Schedule hebdomadaire pour referentiels
    Full refresh (les referentiels changent rarement)
    """
    return RunRequest(
        run_key=f"weekly_references_{context.scheduled_execution_time.strftime('%Y%m%d')}",
        tags={
            "mode": "full",
            "scheduled": "true"
        }
    )
