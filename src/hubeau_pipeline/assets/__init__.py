"""
Hub'Eau Assets - Bronze Layer + dbt
"""

from .bronze import (
    era5_france_timeseries_historical,  # Updated ERA5
    era5_weekly_update,  # Weekly Smart Update
    hydrometry_obs_daily_raw,  # Daily
    hydrometry_obs_elab_raw,
    hydrometry_sites_raw,
    hydrometry_stations_raw,
    piezometry_chroniques_daily_raw,  # Daily
    piezometry_chroniques_raw,
    piezometry_stations_raw,
    referentiel_departements,  # Geographic reference: departements
    referentiel_regions,  # Geographic reference: regions
    referentiel_zones_hydro,  # Geographic reference: zones hydro
    sandre_nomenclatures_eh,  # Sandre nomenclatures
    tme_entites_hydrogeo,  # Attributs TME (TME.csv) — enrichit stg_tme_entites
)
from .dbt_assets import hubeau_dbt_assets
from .current_index_assets import station_current_index
from .monthly_index_assets import fct_monthly_index
from .reference_stats_assets import station_reference_stats

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
    tme_entites_hydrogeo,
    sandre_nomenclatures_eh,
    referentiel_regions,
    referentiel_departements,
    referentiel_zones_hydro,
]

# dbt assets replace the manual aggregation assets
all_dbt_assets = [hubeau_dbt_assets]

all_indices_assets = [station_reference_stats, station_current_index, fct_monthly_index]

all_assets = all_bronze_assets + all_dbt_assets + all_indices_assets

__all__ = ["all_assets", "all_bronze_assets", "all_dbt_assets", "all_indices_assets"]
