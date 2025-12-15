#!/usr/bin/env python
"""Quick test of the DLT pipeline fix for 2004"""
import os
import sys
import yaml

sys.path.insert(0, '/app/src')
os.chdir('/app')

from hubeau_pipeline.sources.hubeau_csv_source import hubeau_chroniques_year
from hubeau_pipeline.utils.dlt_batching import create_dlt_pipeline

# Load config  
with open('/app/configs/hubeau/piezometry_chroniques.yml') as f:
    config = yaml.safe_load(f)

# Limit to very few stations for quick test
test_config = config.copy()
test_config['extraction'] = config.get('extraction', {}).copy()
test_config['extraction']['station_slicing'] = config['extraction'].get('station_slicing', {}).copy()
test_config['extraction']['station_slicing']['batch_size'] = 5  # Only 5 stations

print("=" * 60)
print("FIX TEST: piezometry_chroniques 2004 (5 stations)")
print("=" * 60)

# Create pipeline - should now use STABLE name
pipeline = create_dlt_pipeline("hubeau_piezometry_chroniques", dataset_name="staging")
print(f"Pipeline name: {pipeline.pipeline_name}")
print(f"Expected: hubeau_piezometry_chroniques (without run_id suffix)")

# Check if name is stable
if "_" in pipeline.pipeline_name.split("hubeau_piezometry_chroniques")[-1]:
    print("❌ FAIL: Pipeline name still has suffix!")
    sys.exit(1)
else:
    print("✅ Pipeline name is stable (no suffix)")

# Create resource and run
print("\nRunning pipeline.run()...")
resource = hubeau_chroniques_year(test_config, year="2004")
load_info = pipeline.run(resource, table_name="test_fix_2004")

print(f"\n--- Load Complete ---")

# Check rows in DB
import psycopg2
conn = psycopg2.connect(
    host=os.getenv('PG_HOST', 'postgres'),
    port=os.getenv('PG_PORT', '5432'),
    database=os.getenv('PG_DB', 'postgres'),
    user=os.getenv('PG_USER', 'postgres'),
    password=os.getenv('PG_PASSWORD')
)
cur = conn.cursor()
cur.execute("SELECT COUNT(*) FROM staging.test_fix_2004")
count = cur.fetchone()[0]
print(f"Rows in staging.test_fix_2004: {count}")

if count > 0:
    print(f"✅ SUCCESS! {count} rows written to database")
else:
    print("❌ FAIL: 0 rows in database")

conn.close()
