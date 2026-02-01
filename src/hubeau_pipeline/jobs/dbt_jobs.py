from dagster import define_asset_job, AssetSelection, op, job, In, Nothing, graph
from ..assets.dbt_assets import hubeau_dbt_assets
from dagster_dbt import build_dbt_asset_selection, DbtCliResource

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


# ==============================================================================
# DBT TEST JOB - Data Quality Validation
# ==============================================================================

@op(
    required_resource_keys={"dbt"},
    description="Run dbt test to validate data quality",
)
def run_dbt_tests(context):
    """
    Execute dbt test command to validate all data quality tests.
    Tests are defined in schema.yml files (not_null, unique, accepted_values, etc.)
    """
    dbt: DbtCliResource = context.resources.dbt
    
    context.log.info("🧪 Starting dbt test...")
    
    # Run dbt test command
    test_result = dbt.cli(["test"], context=context).wait()
    
    context.log.info(f"✅ dbt test completed")
    
    return "dbt test completed"


@op(
    required_resource_keys={"dbt"},
    description="Check source data freshness",
)
def run_dbt_source_freshness(context):
    """
    Execute dbt source freshness to check if source data is up to date.
    Freshness thresholds are defined in sources.yml.
    """
    dbt: DbtCliResource = context.resources.dbt
    
    context.log.info("📅 Checking source freshness...")
    
    # Run dbt source freshness command
    freshness_result = dbt.cli(["source", "freshness"], context=context).wait()
    
    context.log.info("✅ Source freshness check completed")
    
    return "freshness check completed"


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
    description="Run both dbt tests and freshness checks",
    tags={"dagster/concurrency_key": "dbt_pipeline"},
)
def dbt_quality_job():
    """Combined job: freshness check then tests."""
    tests = run_dbt_tests()
    run_dbt_source_freshness()


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
    docs_result = dbt.cli(["docs", "generate"], context=context).wait()

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
