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
    tme_entites_hydrogeo,  # Attributs TME (TME.csv) — enrichit stg_tme_entites
)
from .bronze.era5_daily_temp_assets import (
    era5_daily_temp_stats_historical,  # Historique 1950-Present (partitionné)
    era5_daily_temp_stats_update,  # Smart Update quotidien
)
from .current_index_assets import station_current_index
from .dbt_assets import hubeau_dbt_assets
from .era5_indices_assets import fct_era5_indices_grid
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
    era5_daily_temp_stats_historical,
    era5_daily_temp_stats_update,
    tme_entites_hydrogeo,
]

# dbt assets replace the manual aggregation assets
all_dbt_assets = [hubeau_dbt_assets]

all_indices_assets = [
    station_reference_stats,
    station_current_index,
    fct_monthly_index,
    fct_era5_indices_grid,
]

all_assets = all_bronze_assets + all_dbt_assets + all_indices_assets

__all__ = ["all_assets", "all_bronze_assets", "all_dbt_assets", "all_indices_assets"]
