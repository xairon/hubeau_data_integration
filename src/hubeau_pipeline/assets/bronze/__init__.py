"""
Bronze Layer Assets - Hub'Eau Raw Data Ingestion
"""

from hubeau_pipeline.assets.bronze.dlt_assets import (
    # Piezometry
    piezometry_stations_raw,
    piezometry_chroniques_raw,
    piezometry_chroniques_daily_raw,
    # Hydrometry
    hydrometry_sites_raw,
    hydrometry_stations_raw,
    hydrometry_obs_elab_raw,
    hydrometry_obs_daily_raw,
    # Partitions
    MODE_PARTITIONS,
)

# ERA5 Meteorology
from hubeau_pipeline.assets.bronze.era5_assets import (
    era5_france_timeseries_historical,
    era5_weekly_update,
    ERA5_PARTITIONS_DEF,
)

# BDLISA (GeoPackage → PostGIS)
from hubeau_pipeline.assets.bronze.bdlisa_assets import bdlisa_entites_raw
# TME attributs (niveau, etat, nature, ...) depuis TME.csv — enrichit stg_tme_entites
from hubeau_pipeline.assets.bronze.tme_entites_assets import tme_entites_hydrogeo
# Sandre nomenclatures (ref_*_eh)
from hubeau_pipeline.assets.bronze.sandre_nomenclatures_assets import sandre_nomenclatures_eh
# Référentiels géographiques (calques Superset)
from hubeau_pipeline.assets.bronze.referentiel_geo_assets import (
    referentiel_regions,
    referentiel_departements,
    referentiel_zones_hydro,
)

__all__ = [
    # Hub'Eau Historical
    "piezometry_stations_raw",
    "piezometry_chroniques_raw",
    "hydrometry_sites_raw",
    "hydrometry_stations_raw",
    "hydrometry_obs_elab_raw",
    # Hub'Eau Daily
    "piezometry_chroniques_daily_raw",
    "hydrometry_obs_daily_raw",
    # ERA5
    "era5_france_timeseries_historical",
    "era5_weekly_update",
    # BDLISA
    "bdlisa_entites_raw",
    # TME attributs (TME.csv)
    "tme_entites_hydrogeo",
    # Sandre nomenclatures
    "sandre_nomenclatures_eh",
    # Référentiels géographiques
    "referentiel_regions",
    "referentiel_departements",
    "referentiel_zones_hydro",
    # Configs
    "MODE_PARTITIONS",
    "ERA5_PARTITIONS_DEF",
]
