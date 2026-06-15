from dagster import AssetSelection, In, Out, define_asset_job, job, op
from dagster_dbt import DbtCliResource, build_dbt_asset_selection

from ..assets.dbt_assets import hubeau_dbt_assets

# ==============================================================================
# FULL DBT PIPELINE (All models - for bootstrap/full refresh)
# ==============================================================================

dbt_silver_gold_pipeline_job = define_asset_job(
    name="dbt_silver_gold_pipeline",
    description=(
        "Run the FULL dbt transformation pipeline (Silver/Gold layers). "
        "Executes ALL dbt models: Staging (Silver) -> Intermediate (Gold) -> Marts (Gold). "
        "Use this for bootstrap or full refresh. For incremental runs, use domain-specific jobs."
    ),
    selection=build_dbt_asset_selection(
        [hubeau_dbt_assets],
        # No dbt_select = all models
    ).without_checks(),
    tags={"dagster/concurrency_key": "dbt_pipeline"},
    hooks=set(),
)

# ==============================================================================
# SHARED STAGING (Run FIRST - prerequisite for domain pipelines)
# ==============================================================================

# Shared models used by both piezo and hydro pipelines
# Must run BEFORE domain pipelines to avoid conflicts
SHARED_STAGING_MODELS = [
    "stg_era5_timeseries",        # ERA5 climate data (used by both domains)
    "int_era5_grid_points",       # ERA5 grid reference (used by both KNN mappings)
    "int_era5_for_all_stations",  # ERA5 filtered for all station grid points (piezo + hydro)
]

dbt_shared_staging_job = define_asset_job(
    name="dbt_shared_staging",
    description=(
        "Build shared staging models (ERA5 data). "
        "IMPORTANT: Run this BEFORE dbt_piezo_pipeline and dbt_hydro_pipeline. "
        "This job builds models used by both domains to avoid conflicts during parallel execution."
    ),
    selection=build_dbt_asset_selection(
        [hubeau_dbt_assets],
        dbt_select=" ".join(SHARED_STAGING_MODELS),
    ).without_checks(),
    tags={"dagster/concurrency_key": "dbt_pipeline"},
    hooks=set(),
)

# ==============================================================================
# PIEZOMETRY PIPELINE (Domain-specific - can run in parallel with hydro)
# ==============================================================================

# Piezo models: staging -> intermediate -> marts
# EXCLUDES shared models (stg_era5_timeseries, int_era5_grid_points) - run dbt_shared_staging_job first
PIEZO_MODELS = [
    # Staging (Silver) - piezo specific
    "stg_piezo_stations",
    "stg_piezo_chroniques",
    "stg_tme_entites",
    # Rejects (Silver) - audit des lignes ecartees
    "stg_piezo_chroniques_rejected",
    # Intermediate (Gold)
    "int_daily_measurements",
    "int_station_era5_mapping",
    # Marts (Gold)
    "hubeau_daily_chroniques",
    "fct_monthly_chroniques",
    "fct_yearly_stats",
    "dim_piezo_stations",
    "stations_piezo_carte",
]

# ==============================================================================
# HYDROMETRY PIPELINE (Domain-specific)
# ==============================================================================

# Hydro models: staging -> intermediate -> marts
# EXCLUDES shared models (stg_era5_timeseries, int_era5_grid_points) - run dbt_shared_staging_job first
HYDRO_MODELS = [
    # Staging (Silver) - hydro specific
    "stg_hydrometry_sites",
    "stg_hydrometry_stations",
    "stg_hydrometry_obs_elab",
    # Intermediate (Gold)
    "int_hydro_daily_measurements",
    "int_hydro_station_era5_mapping",
    # Marts (Gold)
    "hydro_daily_chroniques",
    "fct_monthly_hydro",
    "fct_yearly_hydro",
    "dim_hydro_stations",
    "stations_hydro_carte",
    # Rejects
    "stg_hydrometry_obs_elab_rejected",
    "stg_hydrometry_stations_rejected",
]

# ==============================================================================
# SHARED DIMENSIONS (used by dbt_daily_transform_job)
# ==============================================================================

SHARED_DIMENSION_MODELS = [
    "dim_geography",  # Depends on stg_piezo_stations + stg_hydrometry_sites
    "dim_date",       # Depends on hubeau_daily_chroniques + hydro_daily_chroniques
]

# ==============================================================================
# DAILY TRANSFORM (single job: full Silver→Gold for both domains + shared dims)
# ==============================================================================

# Builds EVERYTHING downstream of shared staging in ONE job. dbt resolves the
# ref() DAG internally, so ordering (staging → int → daily marts → monthly →
# yearly → station dims → carte → dim_geography/dim_date) is guaranteed without
# any cross-job/sensor coordination.
#
# WHY a single job: runs are serialized globally (QueuedRunCoordinator
# max_concurrent_runs=1), so the previous parallel piezo+hydro fan-out gave no
# speedup, and the rejoin step relied on RunStatusSensorContext.cursor — which
# does not exist, so it crashed on every tick. Collapsing to one job removes
# that failure mode entirely and ensures monthly/yearly/station dimensions
# (derniere_mesure!) and dim_date refresh on every nightly chain.
DAILY_TRANSFORM_MODELS = PIEZO_MODELS + HYDRO_MODELS + SHARED_DIMENSION_MODELS

dbt_daily_transform_job = define_asset_job(
    name="dbt_daily_transform",
    description=(
        "Daily Silver→Gold transform for BOTH domains + shared dimensions, in one job. "
        "PREREQUISITE: dbt_shared_staging_job (ERA5) must run first. "
        "Triggered by the shared_staging_to_domain sensor. Includes monthly/yearly "
        "aggregates, station dimensions and dim_date that the daily fast-path jobs omit."
    ),
    selection=build_dbt_asset_selection(
        [hubeau_dbt_assets],
        dbt_select=" ".join(DAILY_TRANSFORM_MODELS),
    ).without_checks(),
    tags={"dagster/concurrency_key": "dbt_pipeline"},
    hooks=set(),
)

# Nightly (sensor-driven apres dbt_daily_transform_job) : reconstruit le snapshot
# courant ET re-score tout l'historique mensuel contre la baseline de reference fixe.
# Les deux lisent gold.station_reference_stats (rebuilt chaque semaine, cf. ci-dessous).
station_current_index_job = define_asset_job(
    name="station_index_refresh",
    description=(
        "Rebuild gold.fct_monthly_index + gold.station_current_index (IPS/SSFI) apres le "
        "daily transform. Lit la baseline fixe gold.station_reference_stats."
    ),
    selection=AssetSelection.assets("fct_monthly_index", "station_current_index"),
    tags={"dagster/concurrency_key": "dbt_pipeline"},
)

# Hebdomadaire (schedule) : recalcule la baseline de reference pluriannuelle (grilles
# de quantiles). Lourde et lentement variable -> ne tourne PAS chaque nuit. En amont
# des deux assets d'indice ci-dessus.
station_reference_stats_job = define_asset_job(
    name="station_reference_stats_refresh",
    description="Recompute the fixed reference baseline gold.station_reference_stats (weekly).",
    selection=AssetSelection.assets("station_reference_stats"),
    tags={"dagster/concurrency_key": "dbt_pipeline"},
)


# ==============================================================================
# DBT TEST JOB - Data Quality Validation
# ==============================================================================

@op(
    required_resource_keys={"dbt"},
    out=Out(str),
    description="Run dbt test to validate data quality",
)
def run_dbt_tests(context):
    """
    Execute dbt test command to validate all data quality tests.
    Tests are defined in schema.yml files (not_null, unique, accepted_values, etc.)
    """
    dbt: DbtCliResource = context.resources.dbt

    context.log.info("🧪 Starting dbt test...")

    try:
        dbt.cli(["test"], raise_on_error=True, context=context).wait()
    except Exception as e:
        context.log.error(f"❌ dbt test failed: {e}")
        raise

    context.log.info("✅ dbt test completed successfully")

    return "dbt test completed"


@op(
    required_resource_keys={"dbt"},
    out=Out(str),
    description="Check source data freshness",
)
def run_dbt_source_freshness(context):
    """
    Execute dbt source freshness to check if source data is up to date.
    Freshness thresholds are defined in sources.yml.
    """
    dbt: DbtCliResource = context.resources.dbt

    context.log.info("📅 Checking source freshness...")

    try:
        dbt.cli(["source", "freshness"], raise_on_error=True, context=context).wait()
    except Exception as e:
        context.log.error(f"❌ Source freshness check failed: {e}")
        raise

    context.log.info("✅ Source freshness check completed")

    return "freshness check completed"


@op(
    ins={"freshness_result": In(str)},
    required_resource_keys={"dbt"},
    out=Out(str),
    description="Run dbt test after freshness check",
)
def run_dbt_tests_after_freshness(context, freshness_result):
    """Run dbt tests sequentially after freshness check completes."""
    return run_dbt_tests(context)


@job(
    description="Run dbt tests to validate data quality across all models",
    tags={"dagster/concurrency_key": "dbt_pipeline"},
)
def dbt_test_job():
    """Job to run dbt test command for data quality validation."""
    run_dbt_tests()


@job(
    description="Check source data freshness (Bronze layer)",
    tags={"dagster/concurrency_key": "dbt_pipeline"},
)
def dbt_freshness_job():
    """Job to check source data freshness."""
    run_dbt_source_freshness()


@job(
    description="Run both dbt tests and freshness checks (sequential: freshness first, then tests)",
    tags={"dagster/concurrency_key": "dbt_pipeline"},
)
def dbt_quality_job():
    """Combined job: freshness check THEN tests (sequential via data dependency)."""
    freshness_result = run_dbt_source_freshness()
    run_dbt_tests_after_freshness(freshness_result)


# ==============================================================================
# DBT DOCS JOB - Auto-generate Documentation
# ==============================================================================

@op(
    required_resource_keys={"dbt"},
    description="Generate dbt documentation (catalog.json + manifest.json)",
)
def run_dbt_docs_generate(context):
    """
    Execute dbt docs generate to create documentation artifacts.
    Generates:
    - catalog.json: Database catalog (columns, types, stats)
    - manifest.json: dbt project metadata (models, tests, lineage)

    These files are used by 'dbt docs serve' to render the documentation site.
    """
    dbt: DbtCliResource = context.resources.dbt

    context.log.info("📚 Generating dbt documentation...")

    # Run dbt docs generate command
    dbt.cli(["docs", "generate"], context=context).wait()

    context.log.info("✅ dbt documentation generated successfully")
    context.log.info("📂 Documentation artifacts saved to: target/catalog.json, target/manifest.json")
    context.log.info("💡 To view docs locally: cd src/dbt_hubeau && dbt docs serve")

    return "dbt docs generated"


@job(
    description="Generate dbt documentation (catalog + manifest)",
    tags={"dagster/concurrency_key": "dbt_pipeline"},
)
def dbt_docs_job():
    """Job to generate dbt documentation artifacts."""
    run_dbt_docs_generate()
