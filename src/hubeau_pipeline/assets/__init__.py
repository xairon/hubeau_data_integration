"""
Hub'Eau Assets - Bronze Layer + dbt
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
    tme_entites_hydrogeo,  # Attributs TME (TME.csv) — enrichit stg_tme_entites
    sandre_nomenclatures_eh,  # Sandre nomenclatures
    referentiel_regions,  # Geographic reference: regions
    referentiel_departements,  # Geographic reference: departements
    referentiel_zones_hydro,  # Geographic reference: zones hydro
)


from .dbt_assets import hubeau_dbt_assets

from .ml_assets import (
    ml_piezo_model_train,
    ml_hydro_model_train,
    ml_piezo_embeddings_update,
    ml_hydro_embeddings_update,
    ml_piezo_clusters,
    ml_hydro_clusters,
)

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

# ML — SoftCLT Embeddings
all_ml_assets = [
    ml_piezo_model_train,
    ml_hydro_model_train,
    ml_piezo_embeddings_update,
    ml_hydro_embeddings_update,
    ml_piezo_clusters,
    ml_hydro_clusters,
]

all_assets = all_bronze_assets + all_dbt_assets + all_ml_assets

__all__ = ["all_assets", "all_bronze_assets", "all_dbt_assets", "all_ml_assets"]
