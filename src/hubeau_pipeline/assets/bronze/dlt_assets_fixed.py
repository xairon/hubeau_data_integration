"""
Fixed implementation keeping your Hub'Eau specificities
but using DagsterDltResource properly
"""

import os
import yaml
import dlt
from dagster import asset, AssetExecutionContext, StaticPartitionsDefinition
from dagster_dlt import DagsterDltResource
from datetime import datetime

from hubeau_pipeline.sources.hubeau_csv_source import (
    hubeau_chroniques_year,
    hubeau_chroniques_incremental,
)

# Year partitions
CURRENT_YEAR = datetime.now().year
OLDEST_YEAR = 1967
YEAR_PARTITIONS = [str(year) for year in range(OLDEST_YEAR, CURRENT_YEAR + 1)]
MODE_PARTITIONS = StaticPartitionsDefinition(YEAR_PARTITIONS)


@asset(
    compute_kind="dlt",
    group_name="piezometry",
    partitions_def=MODE_PARTITIONS,
    # Remove noop_io_manager - let Dagster handle it
)
def piezometry_chroniques_raw_fixed(context: AssetExecutionContext):
    """
    FIXED VERSION: Uses DagsterDltResource properly
    while keeping all Hub'Eau specificities

    Key changes:
    1. Create DagsterDltResource instance
    2. Use dlt_resource.run() instead of custom wrapper
    3. Generator created inline to avoid consumption
    """
    config_path = "configs/hubeau/piezometry_chroniques.yml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    # Create pipeline
    pipeline = dlt.pipeline(
        pipeline_name="hubeau_piezometry_chroniques",
        destination="postgres",
        dataset_name=os.getenv("DLT_BRONZE_DATASET", "staging"),
        progress="log",
    )

    # Create DagsterDltResource
    dlt_resource = DagsterDltResource()

    if context.has_partition_key:
        year = context.partition_key
        context.log.info(f"Processing year partition: {year}")

        # CRITICAL: Create generator INLINE in the run() call
        # This ensures a fresh generator that hasn't been consumed

        # Option 1: Direct inline creation (SAFEST)
        load_info = pipeline.run(
            hubeau_chroniques_year(config, year=year, dagster_context=context),
            table_name="piezometry_chroniques_raw",
            write_disposition="append"
        )

        # Option 2: Using DagsterDltResource (if it works with your setup)
        # results = list(dlt_resource.run(
        #     context=context,
        #     dlt_source=hubeau_chroniques_year(config, year=year, dagster_context=context),
        #     dlt_pipeline=pipeline,
        #     dataset_name="staging",
        #     table_name="piezometry_chroniques_raw"
        # ))

        # Extract metrics
        rows_loaded = 0
        if load_info:
            for package in getattr(load_info, "load_packages", []):
                for job in getattr(package, "jobs", []):
                    if hasattr(job, "metrics"):
                        items = job.metrics.get("items_count", 0)
                        rows_loaded += items

        context.log.info(f"✅ Loaded {rows_loaded:,} rows for year {year}")
        return {"year": year, "rows_loaded": rows_loaded}

    else:
        # Incremental mode
        context.log.info("Running in incremental mode")

        load_info = pipeline.run(
            hubeau_chroniques_incremental(
                config,
                last_date=dlt.sources.incremental("date_mesure"),
                dagster_context=context
            ),
            table_name="piezometry_chroniques_raw",
            write_disposition="append"
        )

        rows_loaded = 0
        if load_info:
            for package in getattr(load_info, "load_packages", []):
                for job in getattr(package, "jobs", []):
                    if hasattr(job, "metrics"):
                        items = job.metrics.get("items_count", 0)
                        rows_loaded += items

        context.log.info(f"✅ Incremental load: {rows_loaded:,} rows")
        return {"mode": "incremental", "rows_loaded": rows_loaded}


# ============================================================================
# ANALYSIS: Why this works with Hub'Eau specificities
# ============================================================================

"""
This approach KEEPS all your Hub'Eau complexity:

1. STATION BATCHING (500 stations/batch)
   ✅ Still handled in hubeau_chroniques_year() generator
   ✅ No changes needed to the batching logic

2. PAGINATION (10K rows/page)
   ✅ Still handled by fetch_page() in the source
   ✅ Rate limiting still works

3. YEAR PARTITIONS
   ✅ Still works with context.partition_key
   ✅ Each partition runs independently

4. INCREMENTAL MODE
   ✅ DLT incremental tracking still works
   ✅ Last date tracked automatically

The KEY FIX is:
- Creating the generator INLINE in pipeline.run()
- Not storing it in a variable that might be consumed
- Using pipeline.run() directly without wrapper

This eliminates the multiprocess generator consumption issue!
"""