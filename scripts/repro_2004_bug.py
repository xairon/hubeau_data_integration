#!/usr/bin/env python
"""
Full reproduction of 2004 bug with limited scope
"""
import os
import sys
import yaml

sys.path.insert(0, '/app/src')
os.chdir('/app')

from hubeau_pipeline.sources.hubeau_csv_source import hubeau_chroniques_year
from hubeau_pipeline.utils.dlt_batching import create_dlt_pipeline, run_dlt_resource

# Load config  
with open('/app/configs/hubeau/piezometry_chroniques.yml') as f:
    config = yaml.safe_load(f)

# Limit to 2 stations per batch for quick test
test_config = config.copy()
test_config['extraction'] = config.get('extraction', {}).copy()
test_config['extraction']['station_slicing'] = config['extraction'].get('station_slicing', {}).copy()
test_config['extraction']['station_slicing']['batch_size'] = 2

print("=" * 60)
print("REPRODUCTION: Testing 2004 with full pipeline flow")
print("=" * 60)

# Create pipeline (similar to what Dagster does)
pipeline = create_dlt_pipeline("repro_piezo_2004", dataset_name="staging")
print(f"Pipeline: {pipeline.pipeline_name}")

# Create resource exactly as Dagster does
print("\nCreating resource...")
resource = hubeau_chroniques_year(test_config, year="2004")
print(f"Resource type: {type(resource)}")
print(f"Resource name: {getattr(resource, 'name', 'N/A')}")

# Run exactly as run_dlt_resource does
print("\n--- Running pipeline.run() ---")
table_name = "repro_piezo_chroniques_raw"

load_info = pipeline.run(resource, table_name=table_name)

print(f"\n--- Load Info ---")
print(f"Load info type: {type(load_info)}")

# Count from metrics
rows = 0
jobs = 0
for package in getattr(load_info, 'load_packages', []) or []:
    print(f"Package: {package}")
    for job in getattr(package, 'jobs', []) or []:
        jobs += 1
        metrics = getattr(job, 'metrics', None) or {}
        print(f"  Job metrics: {metrics}")
        items = metrics.get('items', 0)
        if isinstance(items, int):
            rows += items

print(f"\nRows from DLT metrics: {rows}")
print(f"Jobs count: {jobs}")

# Check actual DB
import psycopg2
conn = psycopg2.connect(
    host=os.getenv('PG_HOST', 'postgres'),
    port=os.getenv('PG_PORT', '5432'),
    database=os.getenv('PG_DB', 'postgres'),
    user=os.getenv('PG_USER', 'postgres'),
    password=os.getenv('PG_PASSWORD')
)
cur = conn.cursor()

# Check if table exists
cur.execute("""
    SELECT COUNT(*) FROM information_schema.tables 
    WHERE table_schema = 'staging' AND table_name = %s
""", (table_name,))
exists = cur.fetchone()[0]

if exists:
    cur.execute(f"SELECT COUNT(*) FROM staging.{table_name}")
    db_count = cur.fetchone()[0]
    print(f"\n✅ Table staging.{table_name} exists with {db_count} rows")
else:
    print(f"\n❌ Table staging.{table_name} does NOT exist!")

conn.close()
