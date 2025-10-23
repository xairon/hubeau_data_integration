"""
Jobs CSV Hub'Eau - Un job par API

Chaque job contient les assets d'une API spécifique:
- Stations/référentiels
- Chroniques/analyses/observations
"""

from dagster import define_asset_job, AssetSelection, AssetKey, schedule, RunRequest


# ====================================
# JOBS PAR API
# ====================================

# PIEZOMETRY: Stations + Chroniques
piezometry_csv_job = define_asset_job(
    name="piezometry_csv",
    description="Piézométrie CSV: Stations → Chroniques (avec slicing)",
    selection=AssetSelection.keys(
        AssetKey("piezometry_stations_csv"),
        AssetKey("piezometry_chroniques_csv")
    )
)

# QUALITY RIVERS: Stations + Analyses + Conditions + Operations
quality_rivers_csv_job = define_asset_job(
    name="quality_rivers_csv",
    description="Qualité Cours d'Eau CSV: Stations → Analyses/Conditions/Operations",
    selection=AssetSelection.keys(
        AssetKey("quality_rivers_stations_csv"),
        AssetKey("quality_rivers_analyses_csv"),
        AssetKey("quality_rivers_conditions_csv"),
        AssetKey("quality_rivers_operations_csv")
    )
)

# QUALITY GROUNDWATER: Stations + Analyses
quality_groundwater_csv_job = define_asset_job(
    name="quality_groundwater_csv",
    description="Qualité Nappes CSV: Stations → Analyses",
    selection=AssetSelection.keys(
        AssetKey("quality_groundwater_stations_csv"),
        AssetKey("quality_groundwater_analyses_csv")
    )
)

# HYDROMETRY: Sites + Stations + Observations
hydrometry_csv_job = define_asset_job(
    name="hydrometry_csv",
    description="Hydrométrie CSV: Sites + Stations → Observations",
    selection=AssetSelection.keys(
        AssetKey("hydrometry_sites_csv"),
        AssetKey("hydrometry_stations_csv"),
        AssetKey("hydrometry_obs_elab_csv")
    )
)

# TEMPERATURE: Stations + Chroniques
temperature_csv_job = define_asset_job(
    name="temperature_csv",
    description="Température CSV: Stations → Chroniques",
    selection=AssetSelection.keys(
        AssetKey("temperature_stations_csv"),
        AssetKey("temperature_chroniques_csv")
    )
)

# HYDROBIO: Stations + Indices + Taxons
hydrobio_csv_job = define_asset_job(
    name="hydrobio_csv",
    description="Hydrobiologie CSV: Stations → Indices/Taxons",
    selection=AssetSelection.keys(
        AssetKey("hydrobio_stations_csv"),
        AssetKey("hydrobio_indices_csv"),
        AssetKey("hydrobio_taxons_csv")
    )
)

# ECOULEMENT: Stations + Campagnes + Observations
ecoulement_csv_job = define_asset_job(
    name="ecoulement_csv",
    description="Écoulement CSV: Stations + Campagnes → Observations",
    selection=AssetSelection.keys(
        AssetKey("ecoulement_stations_csv"),
        AssetKey("ecoulement_campagnes_csv"),
        AssetKey("ecoulement_observations_csv")
    )
)

# PRELEVEMENTS: Ouvrages + Points + Chroniques
prelevements_csv_job = define_asset_job(
    name="prelevements_csv",
    description="Prélèvements CSV: Ouvrages + Points → Chroniques",
    selection=AssetSelection.keys(
        AssetKey("prelevements_ouvrages_csv"),
        AssetKey("prelevements_points_csv"),
        AssetKey("prelevements_chroniques_csv")
    )
)


# ====================================
# JOBS GLOBAUX
# ====================================

# Job pour TOUTES les stations/référentiels
all_stations_csv_job = define_asset_job(
    name="all_stations_csv",
    description="Toutes les stations/référentiels CSV",
    selection=AssetSelection.keys(
        # Stations
        AssetKey("piezometry_stations_csv"),
        AssetKey("quality_rivers_stations_csv"),
        AssetKey("quality_groundwater_stations_csv"),
        AssetKey("hydrometry_sites_csv"),
        AssetKey("hydrometry_stations_csv"),
        AssetKey("temperature_stations_csv"),
        AssetKey("hydrobio_stations_csv"),
        AssetKey("ecoulement_stations_csv"),
        AssetKey("ecoulement_campagnes_csv"),
        AssetKey("prelevements_ouvrages_csv"),
        AssetKey("prelevements_points_csv")
    )
)

# Job pour TOUTES les chroniques/analyses/observations
all_chroniques_csv_job = define_asset_job(
    name="all_chroniques_csv",
    description="Toutes les chroniques/analyses/observations CSV",
    selection=AssetSelection.keys(
        # Chroniques/Observations
        AssetKey("piezometry_chroniques_csv"),
        AssetKey("quality_rivers_analyses_csv"),
        AssetKey("quality_rivers_conditions_csv"),
        AssetKey("quality_rivers_operations_csv"),
        AssetKey("quality_groundwater_analyses_csv"),
        AssetKey("hydrometry_obs_elab_csv"),
        AssetKey("temperature_chroniques_csv"),
        AssetKey("hydrobio_indices_csv"),
        AssetKey("hydrobio_taxons_csv"),
        AssetKey("ecoulement_observations_csv"),
        AssetKey("prelevements_chroniques_csv")
    )
)

# Job COMPLET (tout)
all_hubeau_csv_job = define_asset_job(
    name="all_hubeau_csv",
    description="Toutes les données Hub'Eau CSV (stations + chroniques)",
    selection=AssetSelection.all()
)


# ====================================
# SCHEDULES
# ====================================

# Schedule pour refresh hebdomadaire des stations
@schedule(
    job=all_stations_csv_job,
    cron_schedule="0 3 * * 0",  # Dimanche 03h00
    name="weekly_stations_refresh_csv"
)
def weekly_stations_schedule(context):
    """
    Refresh hebdomadaire des stations/référentiels
    Mode FULL car les référentiels changent rarement
    """
    # Config pour chaque station asset
    station_assets = [
        "piezometry_stations_csv",
        "quality_rivers_stations_csv",
        "quality_groundwater_stations_csv",
        "hydrometry_sites_csv",
        "hydrometry_stations_csv",
        "temperature_stations_csv",
        "hydrobio_stations_csv",
        "ecoulement_stations_csv",
        "ecoulement_campagnes_csv",
        "prelevements_ouvrages_csv",
        "prelevements_points_csv"
    ]

    ops_config = {
        asset: {"config": {"mode": "full"}}
        for asset in station_assets
    }

    return RunRequest(
        run_key=f"weekly_stations_{context.scheduled_execution_time.strftime('%Y%m%d')}",
        run_config={"ops": ops_config},
        tags={"mode": "full", "type": "stations", "scheduled": "true"}
    )


# Schedule pour refresh quotidien des chroniques
@schedule(
    job=all_chroniques_csv_job,
    cron_schedule="0 2 * * *",  # Tous les jours 02h00
    name="daily_chroniques_refresh_csv"
)
def daily_chroniques_schedule(context):
    """
    Refresh quotidien des chroniques/analyses
    Mode INCREMENTAL: derniers 2 jours avec overlap
    """
    # Config pour chaque chroniques asset
    chroniques_assets = [
        "piezometry_chroniques_csv",
        "quality_rivers_analyses_csv",
        "quality_rivers_conditions_csv",
        "quality_rivers_operations_csv",
        "quality_groundwater_analyses_csv",
        "hydrometry_obs_elab_csv",
        "temperature_chroniques_csv",
        "hydrobio_indices_csv",
        "hydrobio_taxons_csv",
        "ecoulement_observations_csv",
        "prelevements_chroniques_csv"
    ]

    ops_config = {
        asset: {
            "config": {
                "mode": "incremental",
                "incremental_days": 2
            }
        }
        for asset in chroniques_assets
    }

    return RunRequest(
        run_key=f"daily_chroniques_{context.scheduled_execution_time.strftime('%Y%m%d')}",
        run_config={"ops": ops_config},
        tags={"mode": "incremental", "type": "chroniques", "scheduled": "true"}
    )
