"""
Test file to verify DLT works with the simplest possible setup
"""

import os
import yaml
import dlt
from dagster import asset, AssetExecutionContext
from dagster_dlt import DagsterDltResource, dlt_assets

# Test 1: Ultra-simple asset without any wrapper
@asset(
    compute_kind="dlt",
    group_name="test",
    io_manager_key="noop_io_manager"
)
def test_piezometry_2004_direct(context: AssetExecutionContext):
    """
    Test loading 2004 data with DIRECT DLT usage (no wrappers)
    """
    context.log.info("Starting direct DLT test for 2004")

    # Load config
    config_path = "configs/hubeau/piezometry_chroniques.yml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    # Create pipeline directly
    pipeline = dlt.pipeline(
        pipeline_name="test_direct_2004",
        destination="postgres",
        dataset_name="staging",
        progress="log",
    )

    # Import source
    from hubeau_pipeline.sources.hubeau_csv_source import hubeau_chroniques_year

    # Run pipeline DIRECTLY without any wrapper
    # IMPORTANT: Create generator inline to ensure it's fresh
    result = pipeline.run(
        hubeau_chroniques_year(config, year="2004", dagster_context=context),
        table_name="test_piezometry_2004_direct",
        write_disposition="replace"
    )

    context.log.info(f"Direct test result: {result}")

    # Extract metrics manually
    rows_loaded = 0
    try:
        for package in getattr(result, "load_packages", []) or []:
            for job in getattr(package, "jobs", []) or []:
                metrics = getattr(job, "metrics", None) or {}
                items = metrics.get("items", 0)
                if isinstance(items, (int, float)):
                    rows_loaded += int(items)
    except:
        pass

    context.log.info(f"Loaded {rows_loaded} rows")
    return {"rows_loaded": rows_loaded}


# Test 2: Using the official @dlt_assets decorator
@dlt_assets(
    dlt_source=lambda: test_source_2004(),
    dlt_pipeline=dlt.pipeline(
        pipeline_name="test_official_2004",
        destination="postgres",
        dataset_name="staging",
        progress="log",
    ),
    name="test_piezometry_2004_official",
    group_name="test",
)
def test_piezometry_2004_official(
    context: AssetExecutionContext,
    dlt: DagsterDltResource
):
    """
    Test using the OFFICIAL dagster-dlt integration pattern
    """
    yield from dlt.run(context=context)


def test_source_2004():
    """Create test source for 2004 data"""
    config_path = "configs/hubeau/piezometry_chroniques.yml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    from hubeau_pipeline.sources.hubeau_csv_source import hubeau_chroniques_year
    return hubeau_chroniques_year(config, year="2004", dagster_context=None)