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
        subprocess.run(
            ["dbt", "parse", "--project-dir", str(DBT_PROJECT_DIR), "--profiles-dir", str(DBT_PROJECT_DIR)],
            check=True,
            capture_output=True
        )
        print("✅ dbt parse completed successfully.")
    except Exception as e:
        print(f"❌ Failed to run dbt parse: {e}")

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
    import time
    start_time = time.time()
    
    context.log.info("🚀 Démarrage du build dbt...")
    context.log.info(f"📁 Projet dbt: {DBT_PROJECT_DIR}")
    
    # Use stream() to get real-time logs from dbt
    dbt_invocation = dbt.cli(["build"], context=context)
    
    # Stream events and log them
    for event in dbt_invocation.stream():
        # Each event is a dbt asset materialization
        yield event
    
    elapsed_time = time.time() - start_time
    context.log.info(f"✅ Build dbt terminé en {elapsed_time:.1f} secondes")
