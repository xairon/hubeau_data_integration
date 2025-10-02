from dagster import define_asset_job, AssetSelection
from ..assets.bronze.dlt_assets import (
    hydrobio_taxons,
    hydrobio_indices,
    hydrometry_observations,
    piezometry_chroniques,
    quality_rivers_analyses,
    quality_groundwater_analyses,
    ecoulement_observations,
    prelevements_chroniques,
    temperature_chroniques,
    temperature_stations_reference,
)

# Define jobs for each API
hydrobio_job = define_asset_job(
    name="hubeau_hydrobio_job",
    selection=AssetSelection.assets(hydrobio_taxons, hydrobio_indices),
    description="Job for ingesting Hub'Eau Hydrobiology data.",
)

hydrometry_job = define_asset_job(
    name="hubeau_hydrometry_job",
    selection=AssetSelection.assets(hydrometry_observations),
    description="Job for ingesting Hub'Eau Hydrometry data.",
)

piezometry_job = define_asset_job(
    name="hubeau_piezometry_job",
    selection=AssetSelection.assets(piezometry_chroniques),
    description="Job for ingesting Hub'Eau Piezometry data.",
)

quality_job = define_asset_job(
    name="hubeau_quality_job",
    selection=AssetSelection.assets(quality_rivers_analyses, quality_groundwater_analyses),
    description="Job for ingesting Hub'Eau Water Quality data.",
)

ecoulement_job = define_asset_job(
    name="hubeau_ecoulement_job",
    selection=AssetSelection.assets(ecoulement_observations),
    description="Job for ingesting Hub'Eau Ecoulement data.",
)

prelevements_job = define_asset_job(
    name="hubeau_prelevements_job",
    selection=AssetSelection.assets(prelevements_chroniques),
    description="Job for ingesting Hub'Eau Prelevements data.",
)

temperature_job = define_asset_job(
    name="hubeau_temperature_job",
    selection=AssetSelection.assets(temperature_chroniques, temperature_stations_reference),
    description="Job for ingesting Hub'Eau Temperature data.",
)

# Jobs par type de partition pour éviter les conflits

# Job pour les assets avec partitions ANNUELLES (2020-2025)
sync_hubeau_yearly = define_asset_job(
    name="sync_hubeau_yearly",
    selection=AssetSelection.assets(
        hydrobio_taxons,
        hydrobio_indices,
        quality_rivers_analyses,
        quality_groundwater_analyses,
        ecoulement_observations,
        prelevements_chroniques,
        temperature_chroniques,
    ),
    description="Job pour les données historiques annuelles (qualité, température, écoulement, etc.).",
)

# Job pour les assets avec partitions JOURNALIÈRES
sync_hubeau_daily = define_asset_job(
    name="sync_hubeau_daily",
    selection=AssetSelection.assets(piezometry_chroniques),
    description="Job pour les données journalières (piézométrie).",
)

# Job pour les assets SANS partition (temps réel ou référentiels)
sync_hubeau_realtime = define_asset_job(
    name="sync_hubeau_realtime",
    selection=AssetSelection.assets(hydrometry_observations, temperature_stations_reference),
    description="Job pour les données temps réel sans partition (hydrométrie 30j, référentiels).",
)