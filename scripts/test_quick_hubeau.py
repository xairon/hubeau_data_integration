#!/usr/bin/env python
"""
Quick test with VERY limited data to ensure DLT completes all phases
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
print("QUICK TEST: hubeau_chroniques_year with very limited data", flush=True)
print("=" * 60, flush=True)

# Load config
config_path = "configs/hubeau/piezometry_chroniques.yml"
with open(config_path) as f:
    config = yaml.safe_load(f)

# VERY LIMITED: Only 1 batch of 3 stations
config['extraction']['station_slicing']['batch_size'] = 3

print(f"Config loaded, batch_size=3 (very limited)", flush=True)

# Create pipeline
pipeline = dlt.pipeline(
    pipeline_name="test_quick_hubeau",
    destination="postgres",
    dataset_name="staging",
    progress="log"
)

print(f"Pipeline: {pipeline.pipeline_name}", flush=True)

# Create resource
year = "2004"
print(f"\nCreating resource for year={year}...", flush=True)

# Run pipeline
print("\nRunning pipeline.run()...", flush=True)
load_info = pipeline.run(
    hubeau_chroniques_year(config, year=year, dagster_context=None),
    table_name="test_quick_hubeau_table"
)

print(f"\n{'='*60}", flush=True)
print(f"Load info: {load_info}", flush=True)
print(f"{'='*60}", flush=True)

# Extract metrics
rows_loaded = 0
try:
    for package in getattr(load_info, "load_packages", []) or []:
        print(f"Package: {package}", flush=True)
        for job in getattr(package, "jobs", []) or []:
            print(f"Job: {job}", flush=True)
            metrics = getattr(job, "metrics", None) or {}
            print(f"Metrics: {metrics}", flush=True)
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
    cur.execute("SELECT COUNT(*) FROM staging.test_quick_hubeau_table")
    count = cur.fetchone()[0]
    print(f"\n{'='*60}", flush=True)
    print(f"Rows in staging.test_quick_hubeau_table: {count}", flush=True)
    print(f"{'='*60}", flush=True)

    if count > 0:
        print("\n✅ SUCCESS - hubeau_chroniques_year CAN write data!")
    else:
        print("\n❌ FAILURE - 0 rows written. Check load_info above!")
except Exception as e:
    print(f"\n❌ Error querying table: {e}")

conn.close()
