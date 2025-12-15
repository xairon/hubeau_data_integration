"""
Simplified Dagster assets using direct PostgreSQL loader.
NO DLT - just simple Python + psycopg2.
"""
import yaml
from dagster import asset, AssetExecutionContext, StaticPartitionsDefinition

from hubeau_pipeline.sources.simple_fetcher import fetch_chroniques_year
from hubeau_pipeline.utils.direct_loader import (
    load_records_to_postgres,
    delete_partition
)

# Year partitions from 1967 to 2025
YEAR_PARTITIONS = StaticPartitionsDefinition(
    [str(year) for year in range(1967, 2026)]
)


@asset(
    compute_kind="python",
    group_name="piezometry_simple",
    partitions_def=YEAR_PARTITIONS,
    io_manager_key="noop_io_manager"
)
def piezometry_chroniques_simple(context: AssetExecutionContext):
    """
    Piezometry chroniques - SIMPLE VERSION (no DLT).
    
    Uses direct PostgreSQL loader for reliability.
    Each partition = one year of data.
    
    NOTE: Writes to a SEPARATE table (piezometry_chroniques_simple_raw)
    to avoid conflicts with the DLT version (piezometry_chroniques_raw).
    Use this as a fallback if DLT fails.
    """
    year = context.partition_key
    table_name = "piezometry_chroniques_simple_raw"  # Different table to avoid DLT conflict
    
    context.log.info(f"🚀 Starting load for year {year}")
    
    # Load config
    config_path = "configs/hubeau/piezometry_chroniques.yml"
    with open(config_path) as f:
        config = yaml.safe_load(f)
    
    # Delete existing partition data (for clean reload)
    deleted = delete_partition(table_name, year, context)
    if deleted > 0:
        context.log.info(f"♻️ Replaced {deleted:,} existing rows")
    
    # Fetch and load data
    data_generator = fetch_chroniques_year(config, year, context)
    
    result = load_records_to_postgres(
        data_generator=data_generator,
        table_name=table_name,
        partition_year=year,
        dagster_context=context,
        batch_commit_size=10  # Commit every 10 batches
    )
    
    context.log.info(f"✅ Loaded {result['rows_loaded']:,} rows to {result['table_name']}")
    
    return result
