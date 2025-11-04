"""
Jobs Hub'Eau - Bronze Layer

Jobs for Bronze layer assets:
- Stations: FULL load (no partitions)
- Chroniques: MODE partitioned load ("full" for all data, or specific year)

Bronze layer uses DLT standard with simple materialization.

CRITICAL FIX: All jobs have concurrency tags to prevent multiple runs
of the same job from executing simultaneously (prevents DLT file conflicts).

ARCHITECTURE: Jobs are separated by type:
- *_stations jobs: FULL load only (no partitions)
- *_chroniques jobs: Partitioned load (MODE_PARTITIONS)
This prevents mixing partitioned and non-partitioned assets in the same job.
"""

from dagster import define_asset_job, AssetSelection, AssetKey
from ..assets.bronze.dlt_assets import MODE_PARTITIONS


# ====================================
# JOBS PAR API - STATIONS ONLY (no partitions)
# ====================================

# PIEZOMETRY: Stations only
piezometry_stations_job = define_asset_job(
    name="piezometry_stations_bronze",
    description="Bronze: Piezometry stations (FULL load, no partition)",
    selection=AssetSelection.keys(
        AssetKey("piezometry_stations_raw")
    ),
    tags={"dagster/concurrency_key": "piezometry_stations_bronze"}
)

# QUALITY RIVERS: Stations only
quality_rivers_stations_job = define_asset_job(
    name="quality_rivers_stations_bronze",
    description="Bronze: Quality rivers stations (FULL load, no partition)",
    selection=AssetSelection.keys(
        AssetKey("quality_rivers_stations_raw")
    ),
    tags={"dagster/concurrency_key": "quality_rivers_stations_bronze"}
)

# QUALITY GROUNDWATER: Stations only
quality_groundwater_stations_job = define_asset_job(
    name="quality_groundwater_stations_bronze",
    description="Bronze: Quality groundwater stations (FULL load, no partition)",
    selection=AssetSelection.keys(
        AssetKey("quality_groundwater_stations_raw")
    ),
    tags={"dagster/concurrency_key": "quality_groundwater_stations_bronze"}
)

# HYDROMETRY: Sites + Stations only
hydrometry_stations_job = define_asset_job(
    name="hydrometry_stations_bronze",
    description="Bronze: Hydrometry sites + stations (FULL load, no partition)",
    selection=AssetSelection.keys(
        AssetKey("hydrometry_sites_raw"),
        AssetKey("hydrometry_stations_raw")
    ),
    tags={"dagster/concurrency_key": "hydrometry_stations_bronze"}
)

# TEMPERATURE: Stations only
temperature_stations_job = define_asset_job(
    name="temperature_stations_bronze",
    description="Bronze: Temperature stations (FULL load, no partition)",
    selection=AssetSelection.keys(
        AssetKey("temperature_stations_raw")
    ),
    tags={"dagster/concurrency_key": "temperature_stations_bronze"}
)

# HYDROBIO: Stations only
hydrobio_stations_job = define_asset_job(
    name="hydrobio_stations_bronze",
    description="Bronze: Hydrobiology stations (FULL load, no partition)",
    selection=AssetSelection.keys(
        AssetKey("hydrobio_stations_raw")
    ),
    tags={"dagster/concurrency_key": "hydrobio_stations_bronze"}
)

# ECOULEMENT: Stations + Campagnes only
ecoulement_stations_job = define_asset_job(
    name="ecoulement_stations_bronze",
    description="Bronze: Ecoulement stations + campagnes (FULL load, no partition)",
    selection=AssetSelection.keys(
        AssetKey("ecoulement_stations_raw"),
        AssetKey("ecoulement_campagnes_raw")
    ),
    tags={"dagster/concurrency_key": "ecoulement_stations_bronze"}
)

# PRELEVEMENTS: Ouvrages + Points only
prelevements_stations_job = define_asset_job(
    name="prelevements_stations_bronze",
    description="Bronze: Prelevements ouvrages + points (FULL load, no partition)",
    selection=AssetSelection.keys(
        AssetKey("prelevements_ouvrages_raw"),
        AssetKey("prelevements_points_raw")
    ),
    tags={"dagster/concurrency_key": "prelevements_stations_bronze"}
)


# ====================================
# JOBS PAR API - CHRONIQUES ONLY (partitioned)
# ====================================

# PIEZOMETRY: Chroniques only
piezometry_chroniques_job = define_asset_job(
    name="piezometry_chroniques_bronze",
    description="Bronze: Piezometry chroniques (partitioned: full or year)",
    selection=AssetSelection.keys(
        AssetKey("piezometry_chroniques_raw")
    ),
    partitions_def=MODE_PARTITIONS,
    tags={"dagster/concurrency_key": "piezometry_chroniques_bronze"}
)

# QUALITY RIVERS: Analyses + Conditions + Operations
quality_rivers_chroniques_job = define_asset_job(
    name="quality_rivers_chroniques_bronze",
    description="Bronze: Quality rivers analyses + conditions + operations (partitioned: full or year)",
    selection=AssetSelection.keys(
        AssetKey("quality_rivers_analyses_raw"),
        AssetKey("quality_rivers_conditions_raw"),
        AssetKey("quality_rivers_operations_raw")
    ),
    partitions_def=MODE_PARTITIONS,
    tags={"dagster/concurrency_key": "quality_rivers_chroniques_bronze"}
)

# QUALITY GROUNDWATER: Analyses only
quality_groundwater_chroniques_job = define_asset_job(
    name="quality_groundwater_chroniques_bronze",
    description="Bronze: Quality groundwater analyses (partitioned: full or year)",
    selection=AssetSelection.keys(
        AssetKey("quality_groundwater_analyses_raw")
    ),
    partitions_def=MODE_PARTITIONS,
    tags={"dagster/concurrency_key": "quality_groundwater_chroniques_bronze"}
)

# HYDROMETRY: Observations only
hydrometry_chroniques_job = define_asset_job(
    name="hydrometry_chroniques_bronze",
    description="Bronze: Hydrometry observations (partitioned: full or year)",
    selection=AssetSelection.keys(
        AssetKey("hydrometry_obs_elab_raw")
    ),
    partitions_def=MODE_PARTITIONS,
    tags={"dagster/concurrency_key": "hydrometry_chroniques_bronze"}
)

# TEMPERATURE: Chroniques only
temperature_chroniques_job = define_asset_job(
    name="temperature_chroniques_bronze",
    description="Bronze: Temperature chroniques (partitioned: full or year)",
    selection=AssetSelection.keys(
        AssetKey("temperature_chroniques_raw")
    ),
    partitions_def=MODE_PARTITIONS,
    tags={"dagster/concurrency_key": "temperature_chroniques_bronze"}
)

# HYDROBIO: Indices + Taxons
hydrobio_chroniques_job = define_asset_job(
    name="hydrobio_chroniques_bronze",
    description="Bronze: Hydrobiology indices + taxons (partitioned: full or year)",
    selection=AssetSelection.keys(
        AssetKey("hydrobio_indices_raw"),
        AssetKey("hydrobio_taxons_raw")
    ),
    partitions_def=MODE_PARTITIONS,
    tags={"dagster/concurrency_key": "hydrobio_chroniques_bronze"}
)

# ECOULEMENT: Observations only
ecoulement_chroniques_job = define_asset_job(
    name="ecoulement_chroniques_bronze",
    description="Bronze: Ecoulement observations (partitioned: full or year)",
    selection=AssetSelection.keys(
        AssetKey("ecoulement_observations_raw")
    ),
    partitions_def=MODE_PARTITIONS,
    tags={"dagster/concurrency_key": "ecoulement_chroniques_bronze"}
)

# PRELEVEMENTS: Chroniques only
prelevements_chroniques_job = define_asset_job(
    name="prelevements_chroniques_bronze",
    description="Bronze: Prelevements chroniques (partitioned: full or year)",
    selection=AssetSelection.keys(
        AssetKey("prelevements_chroniques_raw")
    ),
    partitions_def=MODE_PARTITIONS,
    tags={"dagster/concurrency_key": "prelevements_chroniques_bronze"}
)


# ====================================
# JOBS GLOBAUX - Bronze Layer
# ====================================

# Job pour TOUTES les stations/référentiels (FULL load - NO PARTITIONS)
all_stations_job = define_asset_job(
    name="all_stations_bronze",
    description="Bronze: ALL stations/referentiels (FULL load, no partition)",
    selection=AssetSelection.keys(
        # Stations
        AssetKey("piezometry_stations_raw"),
        AssetKey("quality_rivers_stations_raw"),
        AssetKey("quality_groundwater_stations_raw"),
        AssetKey("hydrometry_sites_raw"),
        AssetKey("hydrometry_stations_raw"),
        AssetKey("temperature_stations_raw"),
        AssetKey("hydrobio_stations_raw"),
        AssetKey("ecoulement_stations_raw"),
        AssetKey("ecoulement_campagnes_raw"),
        AssetKey("prelevements_ouvrages_raw"),
        AssetKey("prelevements_points_raw")
    ),
    tags={"dagster/concurrency_key": "all_stations_bronze"}  # NO partitions_def - this is FULL load
)

# Job pour TOUTES les chroniques/analyses/observations (MODE partitioned)
all_chroniques_job = define_asset_job(
    name="all_chroniques_bronze",
    description="Bronze: ALL chroniques/analyses/observations (partitioned: full or year)",
    selection=AssetSelection.keys(
        # Chroniques/Observations ONLY (no stations/referentiels)
        AssetKey("piezometry_chroniques_raw"),
        AssetKey("quality_rivers_analyses_raw"),
        AssetKey("quality_rivers_conditions_raw"),
        AssetKey("quality_rivers_operations_raw"),
        AssetKey("quality_groundwater_analyses_raw"),
        AssetKey("hydrometry_obs_elab_raw"),
        AssetKey("temperature_chroniques_raw"),
        AssetKey("hydrobio_indices_raw"),
        AssetKey("hydrobio_taxons_raw"),
        AssetKey("ecoulement_observations_raw"),
        AssetKey("prelevements_chroniques_raw")
        # NOTE: ecoulement_campagnes_raw, prelevements_ouvrages_raw, prelevements_points_raw
        # are in all_stations_job (they are referentiels, not chroniques)
    ),
    partitions_def=MODE_PARTITIONS,
    tags={"dagster/concurrency_key": "all_chroniques_bronze"}
)
