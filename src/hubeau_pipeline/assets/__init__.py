"""
Hub'Eau Assets - Bronze Layer + CSV + dbt
"""

from .bronze import (
    piezometry_stations_raw,
    piezometry_chroniques_raw,
    piezometry_chroniques_daily_raw,  # Daily
    hydrometry_sites_raw,
    hydrometry_stations_raw,
    hydrometry_obs_elab_raw,
    hydrometry_obs_daily_raw,  # Daily
    era5_france_timeseries_historical,  # Updated ERA5
    era5_weekly_update,  # Weekly Smart Update
    bdlisa_entites_raw,  # BDLISA V3 référentiel (GeoPackage → PostGIS)
    tme_entites_hydrogeo,  # Attributs TME (TME.csv) — enrichit stg_tme_entites
    sandre_nomenclatures_eh,  # Nomenclatures Sandre (ref_*_eh)
    referentiel_regions,
    referentiel_departements,
    referentiel_zones_hydro,
)


from .dbt_assets import hubeau_dbt_assets

all_bronze_assets = [
    piezometry_stations_raw,
    piezometry_chroniques_raw,
    piezometry_chroniques_daily_raw,
    hydrometry_sites_raw,
    hydrometry_stations_raw,
    hydrometry_obs_elab_raw,
    hydrometry_obs_daily_raw,
    era5_france_timeseries_historical,
    era5_weekly_update,
    bdlisa_entites_raw,
    tme_entites_hydrogeo,
    sandre_nomenclatures_eh,
    referentiel_regions,
    referentiel_departements,
    referentiel_zones_hydro,
]

all_csv_assets = []

# dbt assets replace the manual aggregation assets
all_dbt_assets = [hubeau_dbt_assets]

all_assets = all_bronze_assets + all_dbt_assets

__all__ = ["all_assets", "all_bronze_assets", "all_csv_assets", "all_dbt_assets"]
