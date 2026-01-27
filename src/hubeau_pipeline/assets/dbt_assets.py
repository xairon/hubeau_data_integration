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
# 
# BEST PRACTICES:
# - In PRODUCTION: Manifest is generated in Dockerfile at build time (see docker/worker/Dockerfile)
#   This ensures the manifest is always in sync with dbt models and doesn't require DB connection
# - In DEVELOPMENT: Manifest can be auto-generated if missing (fallback)
#
# The manifest should be versioned with the code and regenerated whenever dbt models change.
# Check if dbt_packages exists, if not run dbt deps (handles dev volume mount issue)
dbt_packages_dir = DBT_PROJECT_DIR.joinpath("dbt_packages")
packages_yml = DBT_PROJECT_DIR.joinpath("packages.yml")

# If packages.yml exists but dbt_packages is empty or missing, run dbt deps
if packages_yml.exists() and (not dbt_packages_dir.exists() or not any(dbt_packages_dir.iterdir())):
    import subprocess
    print(f"⚠️ dbt packages not found in {dbt_packages_dir}. Running 'dbt deps'...")
    try:
        subprocess.run(
            ["dbt", "deps", "--project-dir", str(DBT_PROJECT_DIR), "--profiles-dir", str(DBT_PROJECT_DIR)],
            check=True,
            capture_output=True
        )
        print("✅ dbt deps completed successfully.")
    except Exception as e:
        print(f"❌ Failed to run dbt deps: {e}")

if os.getenv("DAGSTER_DBT_PARSE_PROJECT_ON_LOAD"):
    # Development mode: auto-parse on load
    # Force explicit parse to ensure we catch errors and don't rely on opaque internal logic
    import subprocess
    print(f"🔄 DAGSTER_DBT_PARSE_PROJECT_ON_LOAD is set. Force running 'dbt parse'...")
    try:
        subprocess.run(
            ["dbt", "parse", "--project-dir", str(DBT_PROJECT_DIR), "--profiles-dir", str(DBT_PROJECT_DIR)],
            check=True,
            capture_output=True
        )
        print("✅ dbt parse completed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to run dbt parse: {e.stderr.decode('utf-8') if e.stderr else str(e)}")
        # In dev mode, we want to fail hard if parse fails
        raise e
    except Exception as e:
        print(f"❌ Failed to run dbt parse: {e}")
        raise e

elif not dbt_project.manifest_path.exists():
    # Fallback: Generate manifest if missing (useful for local dev or if Dockerfile step failed)
    import subprocess
    print(f"⚠️ dbt manifest not found at {dbt_project.manifest_path}. Running 'dbt parse'...")
    print("💡 In production, the manifest should be generated in the Dockerfile at build time.")
    try:
        subprocess.run(
            [
                "dbt", "parse",
                "--project-dir", str(DBT_PROJECT_DIR),
                "--profiles-dir", str(DBT_PROJECT_DIR)
                # Note: dbt parse doesn't require DB connection, it only parses files
            ],
            check=True,
            capture_output=True
        )
        print("✅ dbt parse completed successfully.")
    except Exception as e:
        print(f"❌ Failed to run dbt parse: {e}")
        print("⚠️  Dagster will still work, but dbt assets may not be available.")
else:
    # Check if manifest is stale using modification timestamps
    # This is more robust than counting files
    import glob
    
    try:
        manifest_mtime = dbt_project.manifest_path.stat().st_mtime
        models_dir = DBT_PROJECT_DIR / "models"
        
        is_stale = False
        # Check both SQL and YML files (tests are often in YML)
        for ext in ["**/*.sql", "**/*.yml", "**/*.yaml"]:
            for source_file in models_dir.glob(ext):
                if source_file.stat().st_mtime > manifest_mtime:
                    print(f"🔄 Detected change in {source_file.name}, manifest is stale.")
                    is_stale = True
                    break
            if is_stale:
                break
        
        if is_stale:
            import subprocess
            print("⚠️ Manifest stale. Running 'dbt parse'...")
            try:
                subprocess.run(
                    ["dbt", "parse", "--project-dir", str(DBT_PROJECT_DIR), "--profiles-dir", str(DBT_PROJECT_DIR)],
                    check=True,
                    capture_output=True
                )
                print("✅ dbt parse completed successfully (manifest refreshed).")
            except Exception as e:
                print(f"❌ Failed to reparse: {e}")
    except Exception as e:
        print(f"⚠️ Could not check manifest freshness: {e}")

# Create the Dagster resource for dbt
dbt_resource = DbtCliResource(project_dir=dbt_project, profiles_dir=DBT_PROJECT_DIR)

# Verify manifest exists before creating assets
manifest_path = dbt_project.manifest_path
if not manifest_path.exists():
    raise FileNotFoundError(
        f"dbt manifest not found at {manifest_path}. "
        "Please run 'dbt parse' or rebuild the Docker image. "
        "See docs/DBT_MANIFEST_MANAGEMENT.md for details."
    )

# Log manifest info for debugging
import json
try:
    with open(manifest_path, 'r') as f:
        manifest_data = json.load(f)
        model_count = len(manifest_data.get('nodes', {}))
        print(f"✅ dbt manifest loaded: {model_count} models found in {manifest_path}")
except Exception as e:
    print(f"⚠️  Warning: Could not read manifest at {manifest_path}: {e}")

@dbt_assets(manifest=manifest_path)
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
