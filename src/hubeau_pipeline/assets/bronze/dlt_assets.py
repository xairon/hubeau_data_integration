"""
Bronze Layer DLT Assets - Hub'Eau Data Pipeline

This module defines 22 Dagster assets that load raw Hub'Eau data into PostgreSQL
using DLT (Data Load Tool) with standard patterns:

PATTERN 1: STATIONS (8 assets) - FULL mode
- Uses hubeau_stations() resource with write_disposition="replace"
- Replaces all data on each run

PATTERN 2: CHRONIQUES (14 assets) - MODE partition + INCREMENTAL
- With partition "full": Uses hubeau_chroniques_year() to load ALL historical data
- With partition "2020"-"2025": Uses hubeau_chroniques_year() for specific year backfills
- Without partition: Uses hubeau_chroniques_incremental() for ongoing updates
- Bronze layer: Append-only, deduplication handled in Silver layer (dbt)
"""

# pyright: reportMissingImports=false

import os
import yaml
import dlt
from dagster import asset, StaticPartitionsDefinition
from typing import Dict, Any
from datetime import datetime

from hubeau_pipeline.sources.hubeau_csv_source import (
    hubeau_stations,
    hubeau_chroniques_year,
    hubeau_chroniques_incremental,
)
from hubeau_pipeline.utils.dlt_batching import (
    create_dlt_pipeline,
    run_dlt_resource,
)

# ============================================================================
# PARTITIONS DEFINITION
# ============================================================================

# MODE_PARTITIONS: YEAR-based backfills from 1960 to current year
# - Each year is a separate partition to avoid loading all data in memory
# - For historical backfill: run partitions from oldest (1960) to newest
# - NO PARTITION (incremental) = Load since last date - for ongoing updates (DLT managed)
#
# Historical context:
# - French piezometry data goes back to ~1960s
# - Each year is loaded independently to ensure data is written to PostgreSQL
#   immediately after processing (no memory accumulation)
# - Delete existing data for each year before reload (idempotence)

# Generate year partitions from 1967 to current year
# Historical context: French national piezometry network (ADES) started in 1967
# Some older data may exist but 1967 marks the systematic monitoring start
CURRENT_YEAR = datetime.now().year
OLDEST_YEAR = 1967  # ADES national piezometry network started in 1967

# Create list of year strings from oldest to current
# This generates: ["1967", "1968", ..., "2024", "2025"]
# Total: 59 partitions for complete historical backfill
YEAR_PARTITIONS = [str(year) for year in range(OLDEST_YEAR, CURRENT_YEAR + 1)]

MODE_PARTITIONS = StaticPartitionsDefinition(YEAR_PARTITIONS)


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

# ============================================================================
# HELPER FUNCTION: DLT Pipeline Creation
# ============================================================================

def _create_dlt_pipeline(pipeline_name: str, context=None) -> dlt.Pipeline:
    """Compatibilité ascendante vers `utils.dlt_batching.create_dlt_pipeline`."""

    return create_dlt_pipeline(
        pipeline_name,
        context=context,
        dataset_name=os.getenv("DLT_BRONZE_DATASET", "staging"),
        progress="log",
    )


def _run_resource_with_metrics(
    context,
    pipeline: dlt.Pipeline,
    resource,
    table_name: str,
    *,
    extra_metadata: Dict[str, Any] | None = None,
    **run_kwargs,
) -> Dict[str, Any]:
    """Exécute une ressource DLT et journalise les métriques standardisées."""

    metrics = run_dlt_resource(
        pipeline=pipeline,
        resource=resource,
        context=context,
        table_name=table_name,
        extra_metadata=extra_metadata,
        **run_kwargs,
    )

    context.log.info(
        "✅ Loaded %s rows to %s",
        f"{metrics.get('rows_loaded', 0):,}",
        table_name,
    )
    return metrics


# ============================================================================
# PATTERN 1: STATIONS ASSETS (FULL MODE)
# ============================================================================

@asset(
    compute_kind="dlt",
    group_name="temperature",
    io_manager_key="noop_io_manager"
)
def temperature_stations_raw(context):
    """
    Temperature stations - FULL load (replace all)
    
    Uses DLT advanced features:
    - Automatic schema detection and evolution
    - Type inference and validation
    - Column normalization
    - Error handling and retries
    """
    config_path = "configs/hubeau/temperature_stations.yml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    pipeline = _create_dlt_pipeline("hubeau_temperature_stations", context)

    metrics = _run_resource_with_metrics(
        context,
        pipeline,
        hubeau_stations(config, dagster_context=context),
        "temperature_stations_raw",
    )

    return metrics


@asset(
    compute_kind="dlt",
    group_name="piezometry",
    io_manager_key="noop_io_manager"
)
def piezometry_stations_raw(context):
    """
    Piezometry stations - FULL load (replace all)
    """
    config_path = "configs/hubeau/piezometry_stations.yml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    pipeline = _create_dlt_pipeline("hubeau_piezometry_stations", context)

    return _run_resource_with_metrics(
        context,
        pipeline,
        hubeau_stations(config, dagster_context=context),
        "piezometry_stations_raw",
    )


@asset(
    compute_kind="dlt",
    group_name="hydrometry",
    io_manager_key="noop_io_manager"
)
def hydrometry_sites_raw(context):
    """
    Hydrometry sites - FULL load (replace all)
    """
    config_path = "configs/hubeau/hydrometry_sites.yml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    pipeline = _create_dlt_pipeline("hubeau_hydrometry_sites", context)

    table_name = context.asset_key.path[-1]

    return _run_resource_with_metrics(
        context,
        pipeline,
        hubeau_stations(config, dagster_context=context),
        table_name,
    )


@asset(
    compute_kind="dlt",
    group_name="hydrometry",
    io_manager_key="noop_io_manager"
)
def hydrometry_stations_raw(context):
    """
    Hydrometry stations - FULL load (replace all)
    """
    config_path = "configs/hubeau/hydrometry_stations.yml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    pipeline = _create_dlt_pipeline("hubeau_hydrometry_stations", context)

    table_name = context.asset_key.path[-1]

    return _run_resource_with_metrics(
        context,
        pipeline,
        hubeau_stations(config, dagster_context=context),
        table_name,
    )


@asset(
    compute_kind="dlt",
    group_name="hydrobio",
    io_manager_key="noop_io_manager"
)
def hydrobio_stations_raw(context):
    """
    Hydrobiology stations - FULL load (replace all)
    """
    config_path = "configs/hubeau/hydrobio_stations.yml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    pipeline = _create_dlt_pipeline("hubeau_hydrobio_stations", context)

    table_name = context.asset_key.path[-1]

    return _run_resource_with_metrics(
        context,
        pipeline,
        hubeau_stations(config, dagster_context=context),
        table_name,
    )


@asset(
    compute_kind="dlt",
    group_name="quality_rivers",
    io_manager_key="noop_io_manager"
)
def quality_rivers_stations_raw(context):
    """
    River quality stations - FULL load (replace all)
    """
    config_path = "configs/hubeau/quality_rivers_stations.yml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    pipeline = _create_dlt_pipeline("hubeau_quality_rivers_stations", context)

    table_name = context.asset_key.path[-1]

    return _run_resource_with_metrics(
        context,
        pipeline,
        hubeau_stations(config, dagster_context=context),
        table_name,
    )


@asset(
    compute_kind="dlt",
    group_name="quality_groundwater",
    io_manager_key="noop_io_manager"
)
def quality_groundwater_stations_raw(context):
    """
    Groundwater quality stations - FULL load (replace all)
    """
    config_path = "configs/hubeau/quality_groundwater_stations.yml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    pipeline = _create_dlt_pipeline("hubeau_quality_groundwater_stations", context)

    table_name = context.asset_key.path[-1]

    return _run_resource_with_metrics(
        context,
        pipeline,
        hubeau_stations(config, dagster_context=context),
        table_name,
    )


@asset(
    compute_kind="dlt",
    group_name="ecoulement",
    io_manager_key="noop_io_manager"
)
def ecoulement_stations_raw(context):
    """
    Flow (ecoulement) stations - FULL load (replace all)
    """
    config_path = "configs/hubeau/ecoulement_stations.yml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    pipeline = _create_dlt_pipeline("hubeau_ecoulement_stations", context)

    table_name = context.asset_key.path[-1]

    return _run_resource_with_metrics(
        context,
        pipeline,
        hubeau_stations(config, dagster_context=context),
        table_name,
    )


# ============================================================================
# PATTERN 2: CHRONIQUES ASSETS (MODE PARTITION + INCREMENTAL)
# ============================================================================

@asset(
    compute_kind="dlt",
    group_name="temperature",
    partitions_def=MODE_PARTITIONS,
    io_manager_key="noop_io_manager"
)
def temperature_chroniques_raw(context):
    """
    Temperature chroniques - MODE partition + INCREMENTAL
    - With partition "full": Load ALL historical data
    - With partition "2020"-"2025": Load specific year (backfill/testing)
    - Without partition: Incremental from last date
    """
    config_path = "configs/hubeau/temperature_chroniques.yml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    pipeline = _create_dlt_pipeline("hubeau_temperature_chroniques", context)

    if context.has_partition_key:
        # ===== YEAR PARTITION MODE =====
        year = context.partition_key
        context.log.info(f"YEAR PARTITION: {year}")

        # Bronze layer: Append-only strategy (no DELETE)
        # Deduplication will be handled in Silver layer (dbt)

        # Load year data
        metrics = _run_resource_with_metrics(
            context,
            pipeline,
            hubeau_chroniques_year(config, year=year, dagster_context=context),
            "temperature_chroniques_raw",
        )
    else:
        # ===== INCREMENTAL MODE =====
        context.log.info("INCREMENTAL mode")

        # Use DLT incremental (tracks last date automatically)
        metrics = _run_resource_with_metrics(
            context,
            pipeline,
            hubeau_chroniques_incremental(
                config,
                last_date=dlt.sources.incremental("date_mesure_temp"),
                dagster_context=context
            ),
            "temperature_chroniques_raw",
        )

    return metrics


@asset(
    compute_kind="dlt",
    group_name="piezometry",
    partitions_def=MODE_PARTITIONS,
    io_manager_key="noop_io_manager"
)
def piezometry_chroniques_raw(context):
    """
    Piezometry chroniques - MODE partition + INCREMENTAL
    - With partition "full": Load ALL historical data
    - With partition "2020"-"2025": Load specific year (backfill/testing)
    - Without partition: Incremental from last date
    """
    config_path = "configs/hubeau/piezometry_chroniques.yml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    pipeline = _create_dlt_pipeline("hubeau_piezometry_chroniques", context)

    if context.has_partition_key:
        # ===== YEAR PARTITION MODE =====
        year = context.partition_key
        context.log.info(f"YEAR PARTITION: {year}")

        # Bronze layer: Append-only strategy (no DELETE)
        # Deduplication will be handled in Silver layer (dbt)

        # Load year data
        metrics = _run_resource_with_metrics(
            context,
            pipeline,
            hubeau_chroniques_year(config, year=year, dagster_context=context),
            "piezometry_chroniques_raw",
        )
    else:
        # ===== INCREMENTAL MODE =====
        context.log.info("INCREMENTAL mode")

        # Use DLT incremental (tracks last date automatically)
        metrics = _run_resource_with_metrics(
            context,
            pipeline,
            hubeau_chroniques_incremental(
                config,
                last_date=dlt.sources.incremental("date_mesure"),
                dagster_context=context
            ),
            "piezometry_chroniques_raw",
        )

    return metrics


@asset(
    compute_kind="dlt",
    group_name="hydrometry",
    partitions_def=MODE_PARTITIONS,
    io_manager_key="noop_io_manager"
)
def hydrometry_obs_elab_raw(context):
    """
    Hydrometry observations elaborated - MODE partition + INCREMENTAL
    - With partition "full": Load ALL historical data
    - With partition "2020"-"2025": Load specific year (backfill/testing)
    - Without partition: Incremental from last date
    """
    config_path = "configs/hubeau/hydrometry_obs_elab.yml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    pipeline = _create_dlt_pipeline("hubeau_hydrometry_obs_elab", context)

    if context.has_partition_key:
        # ===== YEAR PARTITION MODE =====
        year = context.partition_key
        context.log.info(f"YEAR PARTITION: {year}")

        # Bronze layer: Append-only strategy (no DELETE)
        # Deduplication will be handled in Silver layer (dbt)

        # Load year data
        metrics = _run_resource_with_metrics(
            context,
            pipeline,
            hubeau_chroniques_year(config, year=year, dagster_context=context),
            "hydrometry_obs_elab_raw",
        )
    else:
        # ===== INCREMENTAL MODE =====
        context.log.info("INCREMENTAL mode")

        # Use DLT incremental (tracks last date automatically)
        metrics = _run_resource_with_metrics(
            context,
            pipeline,
            hubeau_chroniques_incremental(
                config,
                last_date=dlt.sources.incremental("date_obs_elab"),
                dagster_context=context
            ),
            "hydrometry_obs_elab_raw",
        )

    return metrics


@asset(
    compute_kind="dlt",
    group_name="hydrobio",
    partitions_def=MODE_PARTITIONS,
    io_manager_key="noop_io_manager"
)
def hydrobio_indices_raw(context):
    """
    Hydrobiology indices - MODE partition + INCREMENTAL
    - With partition "full": Load ALL historical data
    - With partition "2020"-"2025": Load specific year (backfill/testing)
    - Without partition: Incremental from last date
    """
    config_path = "configs/hubeau/hydrobio_indices.yml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    pipeline = _create_dlt_pipeline("hubeau_hydrobio_indices", context)

    if context.has_partition_key:
        # ===== YEAR PARTITION MODE =====
        year = context.partition_key
        context.log.info(f"YEAR PARTITION: {year}")

        # Bronze layer: Append-only strategy (no DELETE)
        # Deduplication will be handled in Silver layer (dbt)

        # Load year data
        metrics = _run_resource_with_metrics(
            context,
            pipeline,
            hubeau_chroniques_year(config, year=year, dagster_context=context),
            "hydrobio_indices_raw",
        )
    else:
        # ===== INCREMENTAL MODE =====
        context.log.info("INCREMENTAL mode")

        # Use DLT incremental (tracks last date automatically)
        metrics = _run_resource_with_metrics(
            context,
            pipeline,
            hubeau_chroniques_incremental(
                config,
                last_date=dlt.sources.incremental("date_prelevement"),
                dagster_context=context
            ),
            "hydrobio_indices_raw",
        )

    return metrics


@asset(
    compute_kind="dlt",
    group_name="hydrobio",
    partitions_def=MODE_PARTITIONS,
    io_manager_key="noop_io_manager"
)
def hydrobio_taxons_raw(context):
    """
    Hydrobiology taxons - MODE partition + INCREMENTAL
    - With partition "full": Load ALL historical data
    - With partition "2020"-"2025": Load specific year (backfill/testing)
    - Without partition: Incremental from last date
    """
    config_path = "configs/hubeau/hydrobio_taxons.yml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    pipeline = _create_dlt_pipeline("hubeau_hydrobio_taxons", context)

    if context.has_partition_key:
        # ===== YEAR PARTITION MODE =====
        year = context.partition_key
        context.log.info(f"YEAR PARTITION: {year}")

        # Bronze layer: Append-only strategy (no DELETE)
        # Deduplication will be handled in Silver layer (dbt)

        # Load year data
        metrics = _run_resource_with_metrics(
            context,
            pipeline,
            hubeau_chroniques_year(config, year=year, dagster_context=context),
            "hydrobio_taxons_raw",
        )
    else:
        # ===== INCREMENTAL MODE =====
        context.log.info("INCREMENTAL mode")

        # Use DLT incremental (tracks last date automatically)
        metrics = _run_resource_with_metrics(
            context,
            pipeline,
            hubeau_chroniques_incremental(
                config,
                last_date=dlt.sources.incremental("date_prelevement"),
                dagster_context=context
            ),
            "hydrobio_taxons_raw",
        )

    return metrics


@asset(
    compute_kind="dlt",
    group_name="quality_rivers",
    partitions_def=MODE_PARTITIONS,
    io_manager_key="noop_io_manager"
)
def quality_rivers_analyses_raw(context):
    """
    River quality analyses - MODE partition + INCREMENTAL
    - With partition "full": Load ALL historical data
    - With partition "2020"-"2025": Load specific year (backfill/testing)
    - Without partition: Incremental from last date
    """
    config_path = "configs/hubeau/quality_rivers_analyses.yml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    pipeline = _create_dlt_pipeline("hubeau_quality_rivers_analyses", context)

    if context.has_partition_key:
        # ===== YEAR PARTITION MODE =====
        year = context.partition_key
        context.log.info(f"YEAR PARTITION: {year}")

        # Bronze layer: Append-only strategy (no DELETE)
        # Deduplication will be handled in Silver layer (dbt)

        # Load year data
        metrics = _run_resource_with_metrics(
            context,
            pipeline,
            hubeau_chroniques_year(config, year=year, dagster_context=context),
            "quality_rivers_analyses_raw",
        )
    else:
        # ===== INCREMENTAL MODE =====
        context.log.info("INCREMENTAL mode")

        # Use DLT incremental (tracks last date automatically)
        metrics = _run_resource_with_metrics(
            context,
            pipeline,
            hubeau_chroniques_incremental(
                config,
                last_date=dlt.sources.incremental("date_prelevement"),
                dagster_context=context
            ),
            "quality_rivers_analyses_raw",
        )

    return metrics


@asset(
    compute_kind="dlt",
    group_name="quality_rivers",
    partitions_def=MODE_PARTITIONS,
    io_manager_key="noop_io_manager"
)
def quality_rivers_conditions_raw(context):
    """
    River quality conditions - MODE partition + INCREMENTAL
    - With partition "full": Load ALL historical data
    - With partition "2020"-"2025": Load specific year (backfill/testing)
    - Without partition: Incremental from last date
    """
    config_path = "configs/hubeau/quality_rivers_conditions.yml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    pipeline = _create_dlt_pipeline("hubeau_quality_rivers_conditions", context)

    if context.has_partition_key:
        # ===== YEAR PARTITION MODE =====
        year = context.partition_key
        context.log.info(f"YEAR PARTITION: {year}")

        # Bronze layer: Append-only strategy (no DELETE)
        # Deduplication will be handled in Silver layer (dbt)

        # Load year data
        metrics = _run_resource_with_metrics(
            context,
            pipeline,
            hubeau_chroniques_year(config, year=year, dagster_context=context),
            "quality_rivers_conditions_raw",
        )
    else:
        # ===== INCREMENTAL MODE =====
        context.log.info("INCREMENTAL mode")

        # Use DLT incremental (tracks last date automatically)
        metrics = _run_resource_with_metrics(
            context,
            pipeline,
            hubeau_chroniques_incremental(
                config,
                last_date=dlt.sources.incremental("date_prelevement"),
                dagster_context=context
            ),
            "quality_rivers_conditions_raw",
        )

    return metrics


@asset(
    compute_kind="dlt",
    group_name="quality_rivers",
    partitions_def=MODE_PARTITIONS,
    io_manager_key="noop_io_manager"
)
def quality_rivers_operations_raw(context):
    """
    River quality operations - MODE partition + INCREMENTAL
    - With partition "full": Load ALL historical data
    - With partition "2020"-"2025": Load specific year (backfill/testing)
    - Without partition: Incremental from last date
    """
    config_path = "configs/hubeau/quality_rivers_operations.yml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    pipeline = _create_dlt_pipeline("hubeau_quality_rivers_operations", context)

    if context.has_partition_key:
        # ===== YEAR PARTITION MODE =====
        year = context.partition_key
        context.log.info(f"YEAR PARTITION: {year}")

        # Bronze layer: Append-only strategy (no DELETE)
        # Deduplication will be handled in Silver layer (dbt)

        # Load year data
        metrics = _run_resource_with_metrics(
            context,
            pipeline,
            hubeau_chroniques_year(config, year=year, dagster_context=context),
            "quality_rivers_operations_raw",
        )
    else:
        # ===== INCREMENTAL MODE =====
        context.log.info("INCREMENTAL mode")

        # Use DLT incremental (tracks last date automatically)
        metrics = _run_resource_with_metrics(
            context,
            pipeline,
            hubeau_chroniques_incremental(
                config,
                last_date=dlt.sources.incremental("date_prelevement"),
                dagster_context=context
            ),
            "quality_rivers_operations_raw",
        )

    return metrics


@asset(
    compute_kind="dlt",
    group_name="quality_groundwater",
    partitions_def=MODE_PARTITIONS,
    io_manager_key="noop_io_manager"
)
def quality_groundwater_analyses_raw(context):
    """
    Groundwater quality analyses - MODE partition + INCREMENTAL
    - With partition "full": Load ALL historical data
    - With partition "2020"-"2025": Load specific year (backfill/testing)
    - Without partition: Incremental from last date
    """
    config_path = "configs/hubeau/quality_groundwater_analyses.yml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    pipeline = _create_dlt_pipeline("hubeau_quality_groundwater_analyses", context)

    if context.has_partition_key:
        # ===== YEAR PARTITION MODE =====
        year = context.partition_key
        context.log.info(f"YEAR PARTITION: {year}")

        # Bronze layer: Append-only strategy (no DELETE)
        # Deduplication will be handled in Silver layer (dbt)

        # Load year data
        metrics = _run_resource_with_metrics(
            context,
            pipeline,
            hubeau_chroniques_year(config, year=year, dagster_context=context),
            "quality_groundwater_analyses_raw",
        )
    else:
        # ===== INCREMENTAL MODE =====
        context.log.info("INCREMENTAL mode")

        # Use DLT incremental (tracks last date automatically)
        metrics = _run_resource_with_metrics(
            context,
            pipeline,
            hubeau_chroniques_incremental(
                config,
                last_date=dlt.sources.incremental("date_prelevement"),
                dagster_context=context
            ),
            "quality_groundwater_analyses_raw",
        )

    return metrics


@asset(
    compute_kind="dlt",
    group_name="ecoulement",
    io_manager_key="noop_io_manager"
)
def ecoulement_campagnes_raw(context):
    """
    Flow (ecoulement) campaigns - FULL load (replace all)
    """
    config_path = "configs/hubeau/ecoulement_campagnes.yml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    pipeline = _create_dlt_pipeline("hubeau_ecoulement_campagnes", context)

    table_name = context.asset_key.path[-1]

    return _run_resource_with_metrics(
        context,
        pipeline,
        hubeau_stations(config, dagster_context=context),
        table_name,
    )


@asset(
    compute_kind="dlt",
    group_name="ecoulement",
    partitions_def=MODE_PARTITIONS,
    io_manager_key="noop_io_manager"
)
def ecoulement_observations_raw(context):
    """
    Flow observations - MODE partition + INCREMENTAL
    - With partition "full": Load ALL historical data
    - With partition "2020"-"2025": Load specific year (backfill/testing)
    - Without partition: Incremental from last date
    """
    config_path = "configs/hubeau/ecoulement_observations.yml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    pipeline = _create_dlt_pipeline("hubeau_ecoulement_observations", context)

    if context.has_partition_key:
        # ===== YEAR PARTITION MODE =====
        year = context.partition_key
        context.log.info(f"YEAR PARTITION: {year}")

        # Bronze layer: Append-only strategy (no DELETE)
        # Deduplication will be handled in Silver layer (dbt)

        # Load year data
        metrics = _run_resource_with_metrics(
            context,
            pipeline,
            hubeau_chroniques_year(config, year=year, dagster_context=context),
            "ecoulement_observations_raw",
        )
    else:
        # ===== INCREMENTAL MODE =====
        context.log.info("INCREMENTAL mode")

        # Use DLT incremental (tracks last date automatically)
        metrics = _run_resource_with_metrics(
            context,
            pipeline,
            hubeau_chroniques_incremental(
                config,
                last_date=dlt.sources.incremental("date_observation"),
                dagster_context=context
            ),
            "ecoulement_observations_raw",
        )

    return metrics


@asset(
    compute_kind="dlt",
    group_name="prelevements",
    partitions_def=MODE_PARTITIONS,
    io_manager_key="noop_io_manager"
)
def prelevements_chroniques_raw(context):
    """
    Water withdrawals chroniques - MODE partition + INCREMENTAL
    - With partition "full": Load ALL historical data
    - With partition "2020"-"2025": Load specific year (backfill/testing)
    - Without partition: Incremental from last date
    """
    config_path = "configs/hubeau/prelevements_chroniques.yml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    pipeline = _create_dlt_pipeline("hubeau_prelevements_chroniques", context)

    if context.has_partition_key:
        # ===== YEAR PARTITION MODE =====
        year = context.partition_key
        context.log.info(f"YEAR PARTITION: {year}")

        # Bronze layer: Append-only strategy (no DELETE)
        # Deduplication will be handled in Silver layer (dbt)

        # Load year data
        metrics = _run_resource_with_metrics(
            context,
            pipeline,
            hubeau_chroniques_year(config, year=year, dagster_context=context),
            "prelevements_chroniques_raw",
        )
    else:
        # ===== INCREMENTAL MODE =====
        context.log.info("INCREMENTAL mode")

        # Use DLT incremental (tracks last date automatically)
        metrics = _run_resource_with_metrics(
            context,
            pipeline,
            hubeau_chroniques_incremental(
                config,
                last_date=dlt.sources.incremental("annee"),
                dagster_context=context
            ),
            "prelevements_chroniques_raw",
        )

    return metrics


@asset(
    compute_kind="dlt",
    group_name="prelevements",
    io_manager_key="noop_io_manager"
)
def prelevements_ouvrages_raw(context):
    """
    Water withdrawal structures (ouvrages) - FULL load (replace all)
    """
    config_path = "configs/hubeau/prelevements_ouvrages.yml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    pipeline = _create_dlt_pipeline("hubeau_prelevements_ouvrages", context)

    table_name = context.asset_key.path[-1]

    return _run_resource_with_metrics(
        context,
        pipeline,
        hubeau_stations(config, dagster_context=context),
        table_name,
    )


@asset(
    compute_kind="dlt",
    group_name="prelevements",
    io_manager_key="noop_io_manager"
)
def prelevements_points_raw(context):
    """
    Water withdrawal points - FULL load (replace all)
    """
    config_path = "configs/hubeau/prelevements_points.yml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    pipeline = _create_dlt_pipeline("hubeau_prelevements_points", context)

    table_name = context.asset_key.path[-1]

    return _run_resource_with_metrics(
        context,
        pipeline,
        hubeau_stations(config, dagster_context=context),
        table_name,
    )
