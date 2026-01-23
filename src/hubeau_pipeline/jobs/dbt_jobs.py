from dagster import define_asset_job, AssetSelection
from ..assets.dbt_assets import hubeau_dbt_assets
from dagster_dbt import build_dbt_asset_selection
from ..hooks import log_failure_hook, slack_failure_hook, email_failure_hook

# Common hooks for all jobs
# FAILURE_HOOKS removed as per request

# Select all assets from the dbt AssetsDefinition
# Using build_dbt_asset_selection without dbt_select selects all dbt models
# This is the recommended way to select all dbt assets
dbt_silver_gold_pipeline_job = define_asset_job(
    name="dbt_silver_gold_pipeline",
    description=(
        "Run the full dbt transformation pipeline (Silver/Gold layers). "
        "Executes all dbt models: Staging (Silver) -> Intermediate (Gold) -> Marts (Gold)."
    ),
    # build_dbt_asset_selection without dbt_select selects all assets from the manifest
    selection=build_dbt_asset_selection(
        [hubeau_dbt_assets],
        # No dbt_select means: select all models from the manifest
    ),
    tags={"dagster/concurrency_key": "dbt_pipeline"},
    hooks=set(),
)

