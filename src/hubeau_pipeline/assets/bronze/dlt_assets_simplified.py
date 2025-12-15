"""
Simplified DLT Assets using official dagster-dlt integration

This shows how to properly use @dlt_assets decorator instead of custom wrappers
"""

import os
import yaml
import dlt
from dagster import AssetExecutionContext, Definitions, StaticPartitionsDefinition
from dagster_dlt import DagsterDltResource, dlt_assets
from datetime import datetime

from hubeau_pipeline.sources.hubeau_csv_source import (
    hubeau_chroniques_year,
    hubeau_chroniques_incremental,
)

# Year partitions from 1967 to current year
CURRENT_YEAR = datetime.now().year
OLDEST_YEAR = 1967
YEAR_PARTITIONS = [str(year) for year in range(OLDEST_YEAR, CURRENT_YEAR + 1)]
MODE_PARTITIONS = StaticPartitionsDefinition(YEAR_PARTITIONS)


def create_piezometry_pipeline():
    """Create DLT pipeline for piezometry data"""
    return dlt.pipeline(
        pipeline_name="hubeau_piezometry_chroniques",
        destination="postgres",
        dataset_name=os.getenv("DLT_BRONZE_DATASET", "staging"),
        progress="log",
    )


@dlt_assets(
    dlt_source=lambda: hubeau_chroniques_year_wrapper(),
    dlt_pipeline=create_piezometry_pipeline(),
    name="piezometry_chroniques_raw_simplified",
    group_name="piezometry",
    partitions_def=MODE_PARTITIONS,
)
def piezometry_chroniques_raw_simplified(
    context: AssetExecutionContext,
    dlt_resource: DagsterDltResource
):
    """
    Simplified implementation using official dagster-dlt integration

    This is the CORRECT way to use DLT with Dagster according to official docs:
    1. Use @dlt_assets decorator (not @asset)
    2. Use DagsterDltResource (not custom wrappers)
    3. Yield from dlt_resource.run() (not return metrics)
    """
    # The dlt_resource handles everything automatically!
    yield from dlt_resource.run(context=context)


def hubeau_chroniques_year_wrapper():
    """
    Wrapper to create the appropriate DLT source based on context

    Note: In the official pattern, the source should be created here,
    but we need access to the context for partition key.
    This is a limitation we need to work around.
    """
    # This is a simplified example - in reality we need to handle:
    # 1. Reading config from YAML
    # 2. Getting partition key from context
    # 3. Creating the appropriate source (year or incremental)

    config_path = "configs/hubeau/piezometry_chroniques.yml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    # For demo purposes, return a simple source
    # In real implementation, this needs to be partition-aware
    return hubeau_chroniques_year(config, year="2024", dagster_context=None)


# Alternative approach: Create separate assets for each partition mode
@dlt_assets(
    dlt_source=lambda: create_year_source("2024"),
    dlt_pipeline=dlt.pipeline(
        pipeline_name="hubeau_piezometry_2024",
        destination="postgres",
        dataset_name="staging",
        progress="log",
    ),
    name="piezometry_2024_raw",
    group_name="piezometry",
)
def piezometry_2024_raw(context: AssetExecutionContext, dlt: DagsterDltResource):
    """
    Example for a specific year - this is the pattern that WORKS
    """
    yield from dlt.run(context=context)


def create_year_source(year: str):
    """Create a DLT source for a specific year"""
    config_path = "configs/hubeau/piezometry_chroniques.yml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    # Return the generator function directly
    return hubeau_chroniques_year(config, year=year, dagster_context=None)


# ============================================================================
# ANALYSIS OF THE PROBLEM
# ============================================================================

"""
WHY OUR CURRENT APPROACH FAILS:

1. CUSTOM WRAPPERS BREAK DLT:
   - Our `run_dlt_resource()` wrapper may not handle generators correctly
   - The multiprocess executor in Dagster might consume the generator
   - Custom metrics extraction might interfere with DLT's internal state

2. NOT USING OFFICIAL INTEGRATION:
   - @dlt_assets decorator handles all the complexity automatically
   - DagsterDltResource manages pipeline state correctly
   - The yield pattern ensures proper resource cleanup

3. PARTITION COMPLEXITY:
   - Our partition logic adds complexity that might break generator handling
   - The context switching between partition/incremental modes is error-prone
   - Each partition should probably be a separate pipeline instance

RECOMMENDED SOLUTION:

1. IMMEDIATE FIX (Quick):
   - Remove all debug prints that might consume the generator
   - Pass lambda functions instead of generators to defer evaluation
   - Ensure generator is created fresh just before pipeline.run()

2. PROPER FIX (Best):
   - Migrate to @dlt_assets decorator pattern
   - Use DagsterDltResource instead of custom wrappers
   - Simplify partition handling or create separate assets per year

3. DEBUGGING STEPS:
   - Add logging to see if generator is being consumed
   - Check if multiprocess executor is the issue (try in_process_executor)
   - Verify PostgreSQL permissions and connection from worker

The official pattern is MUCH simpler and more reliable!
"""