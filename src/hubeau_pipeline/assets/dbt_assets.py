import os
from pathlib import Path
from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets, DbtProject

# Path to the dbt project
DBT_PROJECT_DIR = Path(__file__).parent.parent.parent.joinpath("dbt_hubeau").resolve()

# Define the dbt project configuration
dbt_project = DbtProject(
    project_dir=DBT_PROJECT_DIR,
    packaged_project_dir=Path(__file__).parent.parent.parent.joinpath("dbt_hubeau_packaged").resolve(),
)

# Compile dbt manifest (required for Dagster to know the assets)
if os.getenv("DAGSTER_DBT_PARSE_PROJECT_ON_LOAD"):
    dbt_project.prepare_if_dev()
elif not dbt_project.manifest_path.exists():
    # In dev, if manifest missing, force parse
    import subprocess
    print(f"⚠️ dbt manifest not found at {dbt_project.manifest_path}. Running 'dbt parse'...")
    try:
        # Run dbt parse using the project dir
        subprocess.run(
            ["dbt", "parse", "--project-dir", str(DBT_PROJECT_DIR), "--profiles-dir", str(DBT_PROJECT_DIR)],
            check=True,
            capture_output=True
        )
        print("✅ dbt parse completed successfully.")
    except Exception as e:
        print(f"❌ Failed to run dbt parse: {e}")
        # Let it fail downstream if manifest is still missing

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
