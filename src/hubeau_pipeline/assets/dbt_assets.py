import os
from pathlib import Path
from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets, DbtProject

# Path to the dbt project
DBT_PROJECT_DIR = Path(__file__).parent.parent.parent.joinpath("dbt_hubeau").resolve()

# Define the dbt project configuration
dbt_project = DbtProject(
    project_dir=DBT_PROJECT_DIR,
)

# Compile dbt manifest (required for Dagster to know the assets)
if not dbt_project.manifest_path.exists():
    dbt_project.prepare_if_dev()

# Create the Dagster resource for dbt
dbt_resource = DbtCliResource(project_dir=dbt_project)

@dbt_assets(manifest=dbt_project.manifest_path)
def hubeau_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    """
    dbt models for Hub'Eau pipeline.
    Includes:
    - Staging views (era5, piezo)
    - Intermediate (mapping, aggregation)
    - Marts (daily_chroniques)
    """
    yield from dbt.cli(["build"], context=context).stream()
