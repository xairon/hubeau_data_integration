#!/usr/bin/env python
"""
Debug: Test exact DLT resource flow for 2004
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

print("=" * 60)
print("DEBUG: Testing DLT resource for piezometry 2004 (limited)")
print("=" * 60)

# Create pipeline
pipeline = create_dlt_pipeline("debug_piezo_2004", dataset_name="staging")
print(f"Pipeline created: {pipeline.pipeline_name}")
print(f"Dataset: {pipeline.dataset_name}")

# Create resource with limit for testing
# Modify config to test with fewer stations
test_config = config.copy()
test_config['extraction'] = test_config.get('extraction', {}).copy()
test_config['extraction']['station_slicing'] = test_config['extraction'].get('station_slicing', {}).copy()
test_config['extraction']['station_slicing']['batch_size'] = 5  # Only 5 stations per batch

# Create resource
print("\nCreating resource (limited to 5 stations per batch)...")
resource = hubeau_chroniques_year(test_config, year="2004")

# Check what type it is
print(f"Resource type: {type(resource)}")
print(f"Resource name: {getattr(resource, 'name', 'N/A')}")
print(f"Resource write_disposition: {getattr(resource, 'write_disposition', 'N/A')}")

# Manually consume a few items to see what happens
print("\n--- Testing manual iteration ---")
count = 0
total_rows = 0
for batch in resource:
    count += 1
    batch_len = len(batch) if isinstance(batch, list) else 1
    total_rows += batch_len
    print(f"Batch {count}: {batch_len} rows")
    if count >= 3:  # Only test first 3 batches
        print("(stopping after 3 batches for test)")
        break

print(f"\nTotal from manual iteration: {total_rows} rows in {count} batches")
print("\n⚠️  PROBLEM: After manual iteration, the generator is EXHAUSTED!")
print("If DLT tries to iterate again, it will get 0 rows.")
