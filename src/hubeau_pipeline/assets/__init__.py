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
from .ml_assets import (
    ml_hydro_multi_clusters,
    ml_hydro_multi_embeddings_update,
    ml_hydro_multi_model_train,
    ml_hydro_uni_clusters,
    ml_hydro_uni_embeddings_update,
    ml_hydro_uni_model_train,
    ml_piezo_multi_clusters,
    ml_piezo_multi_embeddings_update,
    ml_piezo_multi_model_train,
    ml_piezo_uni_clusters,
    ml_piezo_uni_embeddings_update,
    ml_piezo_uni_model_train,
)
from .pastas_assets import ml_piezo_pastas_irf_features
from .pastas_refit_asset import ml_piezo_pastas_full_refit
from .pastas_sgi_asset import ml_piezo_sgi
from .pastas_signatures_asset import ml_piezo_groundwater_signatures
from .current_index_assets import station_current_index

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

# ML — SoftCLT Embeddings (uni + multi spaces)
all_ml_assets = [
    ml_piezo_multi_model_train,
    ml_piezo_uni_model_train,
    ml_hydro_multi_model_train,
    ml_hydro_uni_model_train,
    ml_piezo_multi_embeddings_update,
    ml_piezo_uni_embeddings_update,
    ml_hydro_multi_embeddings_update,
    ml_hydro_uni_embeddings_update,
    ml_piezo_multi_clusters,
    ml_piezo_uni_clusters,
    ml_hydro_multi_clusters,
    ml_hydro_uni_clusters,
    ml_piezo_pastas_irf_features,
    ml_piezo_groundwater_signatures,
    ml_piezo_sgi,
    ml_piezo_pastas_full_refit,
]

all_indices_assets = [station_current_index]

all_assets = all_bronze_assets + all_dbt_assets + all_ml_assets + all_indices_assets

__all__ = ["all_assets", "all_bronze_assets", "all_dbt_assets", "all_ml_assets", "all_indices_assets"]
