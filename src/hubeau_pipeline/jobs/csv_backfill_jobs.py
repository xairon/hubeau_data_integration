"""
Jobs de backfill pour ingestion CSV Hub'Eau
"""

from dagster import job
from hubeau_pipeline.assets.bronze.csv_assets import *


@job(name="backfill_temperature_csv")
def backfill_temperature_job():
    """Backfill temperature (stations + chroniques)"""
    temperature_stations_csv()
    temperature_chroniques_csv()


@job(name="backfill_piezometry_csv")
def backfill_piezometry_job():
    """Backfill piezometrie (stations + chroniques avec slicing)"""
    piezometry_stations_csv()
    piezometry_chroniques_csv()


@job(name="backfill_quality_rivers_csv")
def backfill_quality_rivers_job():
    """Backfill qualite rivieres"""
    quality_rivers_stations_csv()
    quality_rivers_analyses_csv()
    quality_rivers_conditions_csv()
    quality_rivers_operations_csv()


@job(name="backfill_hydrometry_csv")
def backfill_hydrometry_job():
    """Backfill hydrometrie"""
    hydrometry_sites_csv()
    hydrometry_stations_csv()
    hydrometry_obs_elab_csv()


@job(name="backfill_all_csv")
def backfill_all_job():
    """
    Backfill complet - Tous les endpoints
    ATTENTION : Peut prendre plusieurs heures
    """
    # Referentiels
    piezometry_stations_csv()
    quality_groundwater_stations_csv()
    quality_rivers_stations_csv()
    temperature_stations_csv()
    hydrometry_sites_csv()
    hydrometry_stations_csv()
    hydrobio_stations_csv()
    ecoulement_stations_csv()
    ecoulement_campagnes_csv()
    prelevements_points_csv()
    prelevements_ouvrages_csv()

    # Chroniques/Analyses
    piezometry_chroniques_csv()
    quality_groundwater_analyses_csv()
    quality_rivers_analyses_csv()
    quality_rivers_conditions_csv()
    quality_rivers_operations_csv()
    temperature_chroniques_csv()
    hydrometry_obs_elab_csv()
    hydrobio_indices_csv()
    hydrobio_taxons_csv()
    ecoulement_observations_csv()
    prelevements_chroniques_csv()
