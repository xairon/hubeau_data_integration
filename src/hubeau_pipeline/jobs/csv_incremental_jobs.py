"""
Jobs incrementaux et schedules pour ingestion CSV Hub'Eau
"""

from dagster import define_asset_job, schedule, RunRequest, AssetSelection, AssetKey


# Job incremental pour chroniques (donnees temporelles)
incremental_chroniques_job = define_asset_job(
    name="incremental_chroniques_csv",
    description="Ingestion incrementale chroniques (derniers 2 jours)",
    selection=AssetSelection.keys(
        AssetKey("piezometry_chroniques_csv"),
        AssetKey("quality_groundwater_analyses_csv"),
        AssetKey("quality_rivers_analyses_csv"),
        AssetKey("quality_rivers_conditions_csv"),
        AssetKey("quality_rivers_operations_csv"),
        AssetKey("temperature_chroniques_csv"),
        AssetKey("hydrometry_obs_elab_csv"),
        AssetKey("hydrobio_indices_csv"),
        AssetKey("hydrobio_taxons_csv"),
        AssetKey("ecoulement_observations_csv"),
        AssetKey("prelevements_chroniques_csv")
    )
    # Config will be provided by schedule or at runtime
)


# Job refresh referentiels
incremental_references_job = define_asset_job(
    name="incremental_references_csv",
    description="Refresh hebdomadaire referentiels (stations)",
    selection=AssetSelection.keys(
        AssetKey("piezometry_stations_csv"),
        AssetKey("quality_groundwater_stations_csv"),
        AssetKey("quality_rivers_stations_csv"),
        AssetKey("temperature_stations_csv"),
        AssetKey("hydrometry_sites_csv"),
        AssetKey("hydrometry_stations_csv"),
        AssetKey("hydrobio_stations_csv"),
        AssetKey("ecoulement_stations_csv"),
        AssetKey("ecoulement_campagnes_csv"),
        AssetKey("prelevements_points_csv"),
        AssetKey("prelevements_ouvrages_csv")
    )
    # Config will be provided by schedule or at runtime
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
    # Build config for all chroniques assets
    asset_names = [
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
    ]

    ops_config = {
        asset_name: {
            "config": {
                "mode": "incremental",
                "incremental_days": 2
            }
        }
        for asset_name in asset_names
    }

    return RunRequest(
        run_key=f"daily_chroniques_{context.scheduled_execution_time.strftime('%Y%m%d')}",
        run_config={"ops": ops_config},
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
    # Build config for all reference assets
    asset_names = [
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
    ]

    ops_config = {
        asset_name: {
            "config": {
                "mode": "full"
            }
        }
        for asset_name in asset_names
    }

    return RunRequest(
        run_key=f"weekly_references_{context.scheduled_execution_time.strftime('%Y%m%d')}",
        run_config={"ops": ops_config},
        tags={
            "mode": "full",
            "scheduled": "true"
        }
    )
