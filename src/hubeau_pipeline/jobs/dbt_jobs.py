from dagster import define_asset_job, AssetSelection, AssetKey
from ..assets.dbt_assets import hubeau_dbt_assets, dbt_resource
from dagster_dbt import build_dbt_asset_selection

dbt_silver_gold_pipeline_job = define_asset_job(
    name="dbt_silver_gold_pipeline",
    description=(
        "Run the full dbt transformation pipeline (Silver/Gold layers). "
        "Executes all dbt models: Staging (Silver) -> Intermediate (Gold) -> Marts (Gold)."
    ),
    selection=build_dbt_asset_selection([hubeau_dbt_assets]),
    tags={"dagster/concurrency_key": "dbt_pipeline"}
)
