#!/usr/bin/env python
"""
Test with ACTUAL hubeau_chroniques_year to see what's different
Use a very limited batch to be quick
"""
import os
import sys
sys.path.insert(0, '/app/src')
os.chdir('/app')

import yaml
import dlt

# Import the ACTUAL sources
from hubeau_pipeline.sources.hubeau_csv_source import hubeau_chroniques_year

print("=" * 60, flush=True)
print("TEST: Using ACTUAL hubeau_chroniques_year", flush=True)
print("=" * 60, flush=True)

# Load config
config_path = "configs/hubeau/piezometry_chroniques.yml"
with open(config_path) as f:
    config = yaml.safe_load(f)

# LIMIT stations to just 1 batch for quick test
config['extraction']['station_slicing']['batch_size'] = 1

print(f"Config loaded, batch_size limited to 1", flush=True)

# Create pipeline
pipeline = dlt.pipeline(
    pipeline_name="test_actual_hubeau",
    destination="postgres",
    dataset_name="staging",
    progress="log"
)

print(f"Pipeline: {pipeline.pipeline_name}", flush=True)

# Create resource
year = "2004"
print(f"\nCreating resource for year={year}...", flush=True)
resource = hubeau_chroniques_year(config, year=year, dagster_context=None)
print(f"Resource type: {type(resource)}", flush=True)
print(f"Resource name: {getattr(resource, 'name', 'N/A')}", flush=True)

# Run pipeline
print("\nRunning pipeline.run()...", flush=True)
load_info = pipeline.run(
    resource,
    table_name="test_actual_hubeau_table"
)

print(f"\nLoad info: {load_info}", flush=True)

# Extract metrics
rows_loaded = 0
try:
    for package in getattr(load_info, "load_packages", []) or []:
        for job in getattr(package, "jobs", []) or []:
            metrics = getattr(job, "metrics", None) or {}
            items = metrics.get("items", 0)
            if isinstance(items, (int, float)):
                rows_loaded += int(items)
except Exception as e:
    print(f"Error extracting metrics: {e}", flush=True)

print(f"\nrows_loaded from metrics: {rows_loaded}", flush=True)

# Check database
import psycopg2
conn = psycopg2.connect(
    host=os.getenv('PG_HOST', 'postgres'),
    port=os.getenv('PG_PORT', '5432'),
    database=os.getenv('PG_DB', 'postgres'),
    user=os.getenv('PG_USER', 'postgres'),
    password=os.getenv('PG_PASSWORD')
)
cur = conn.cursor()

try:
    cur.execute("SELECT COUNT(*) FROM staging.test_actual_hubeau_table")
    count = cur.fetchone()[0]
    print(f"\nRows in staging.test_actual_hubeau_table: {count}", flush=True)

    if count > 0:
        print("\n✅ SUCCESS - hubeau_chroniques_year works!")
    else:
        print("\n❌ FAILURE - 0 rows written!")
except Exception as e:
    print(f"\n❌ Error querying table: {e}")

conn.close()
