from dagster import AssetSelection, define_asset_job, job, op
from dagster_dbt import DbtCliResource, build_dbt_asset_selection

from ..assets.dbt_assets import hubeau_dbt_assets

# ==============================================================================
# DBT TRANSFORM (single job: ALL models, incremental)
# ==============================================================================
# Builds every dbt model (Silver staging + rejects → Gold intermediate → marts
# → shared dimensions). dbt resolves ordering via the ref() DAG (ERA5 staging
# runs before the domain models that depend on it), so there is NO cross-job
# coordination to get wrong.
#
# History: this replaces a former split (dbt_shared_staging_job → dbt_daily_transform_job
# → dbt_silver_gold_pipeline_job). The split dated from a parallel piezo+hydro
# design; runs are serialized globally (QueuedRunCoordinator max_concurrent_runs=1)
# so it gave no speedup, only fragile inter-job sequencing. One job = one source of
# truth, used both nightly (sensor-driven) and for manual full refresh.
#
# `.without_checks()`: dbt tests are NOT run here (they would gate the data refresh).
# They run right after, non-blocking, via dbt_quality_job (transform_to_quality sensor).
dbt_transform_job = define_asset_job(
    name="dbt_transform",
    description=(
        "Build ALL dbt models (incremental): Silver → Gold → marts → dimensions. "
        "Triggered nightly by the bronze→transform sensor; also usable for a manual "
        "full refresh. dbt handles intra-DAG ordering via ref()."
    ),
    selection=build_dbt_asset_selection([hubeau_dbt_assets]).without_checks(),
    tags={"dagster/concurrency_key": "dbt_pipeline"},
    hooks=set(),
)

# ==============================================================================
# INDICES (IPS / SSFI) — produced by Dagster/scipy assets, not dbt
# ==============================================================================

# Nightly (sensor-driven after dbt_transform_job): rebuild the current snapshot AND
# re-score the whole monthly history against the fixed reference baseline.
# Both read gold.station_reference_stats (rebuilt weekly, see below).
station_current_index_job = define_asset_job(
    name="station_index_refresh",
    description=(
        "Rebuild gold.fct_monthly_index + gold.station_current_index (IPS/SSFI) + "
        "gold.fct_era5_indices_grid (SPI/STI grille ERA5) after the transform. "
        "Reads the fixed baseline gold.station_reference_stats."
    ),
    selection=AssetSelection.assets(
        "fct_monthly_index", "station_current_index", "fct_era5_indices_grid"
    ),
    tags={"dagster/concurrency_key": "dbt_pipeline"},
)

# Weekly (schedule): recompute the multi-year reference baseline (quantile grids).
# Heavy and slow-moving → not every night. Upstream of the two index assets above.
station_reference_stats_job = define_asset_job(
    name="station_reference_stats_refresh",
    description="Recompute the fixed reference baseline gold.station_reference_stats (weekly).",
    selection=AssetSelection.assets("station_reference_stats"),
    tags={"dagster/concurrency_key": "dbt_pipeline"},
)


# ==============================================================================
# DBT QUALITY JOB — data-quality tests (schema.yml)
# ==============================================================================
# Runs `dbt test` only. NOT `dbt source freshness`: the piezo/hydro sources use a
# text `loaded_at_field` (date_mesure/date_obs_elab) so freshness raises a DB error,
# and ERA5 legitimately lags — coupling it as a hard gate blocked the tests. Source
# arrival/coverage is monitored separately by data_completeness_job (weekly).

@op(
    required_resource_keys={"dbt"},
    description="Run dbt test (schema.yml data-quality tests). Failing rows persisted to dbt_audit.",
)
def run_dbt_tests(context):
    context.log.info("🧪 Starting dbt test...")
    dbt: DbtCliResource = context.resources.dbt
    dbt.cli(["test"], raise_on_error=True, context=context).wait()
    context.log.info("✅ dbt test completed successfully")


@job(
    description="Data quality: dbt tests (schema.yml). Nightly after dbt_transform; fails loud on violations.",
    tags={"dagster/concurrency_key": "dbt_pipeline"},
)
def dbt_quality_job():
    run_dbt_tests()


# ==============================================================================
# DBT DOCS JOB — generate catalog.json + manifest.json
# ==============================================================================

@op(
    required_resource_keys={"dbt"},
    description="Generate dbt documentation (catalog.json + manifest.json)",
)
def run_dbt_docs_generate(context):
    context.log.info("📚 Generating dbt documentation...")
    dbt: DbtCliResource = context.resources.dbt
    dbt.cli(["docs", "generate"], context=context).wait()
    context.log.info("✅ dbt documentation generated (target/catalog.json, target/manifest.json)")


@job(
    description="Generate dbt documentation (catalog + manifest)",
    tags={"dagster/concurrency_key": "dbt_pipeline"},
)
def dbt_docs_job():
    run_dbt_docs_generate()
