#!/usr/bin/env python
"""
Debug: Test DLT resource with ACTUAL pipeline.run() call
"""
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

# Modify config to test with MUCH fewer stations (1 station = quick test)
test_config = config.copy()
test_config['extraction'] = test_config.get('extraction', {}).copy()
test_config['extraction']['station_slicing'] = test_config['extraction'].get('station_slicing', {}).copy()
test_config['extraction']['station_slicing']['batch_size'] = 1  # Only 1 station per batch

print("=" * 60)
print("DEBUG: Testing pipeline.run() with 1 station batch")
print("=" * 60)

# Create pipeline
pipeline = create_dlt_pipeline("debug_piezo_1station", dataset_name="staging")
print(f"Pipeline: {pipeline.pipeline_name}")

# Create resource
print("\nCreating resource (1 station per batch, limited test)...")

# We need to limit the scope - get station codes and only use first 3
from hubeau_pipeline.sources.hubeau_csv_source import get_station_codes

base_url = config['resource']['base_url']
slicing = config['extraction']['station_slicing']
station_codes = get_station_codes(base_url, slicing['stations_endpoint'], slicing['station_param'], 0.3)
print(f"Got {len(station_codes)} stations, using first 3 for test")

# Override config with limited stations
limited_config = config.copy()
limited_config['extraction'] = limited_config.get('extraction', {}).copy()
limited_config['extraction']['station_slicing'] = {
    'enabled': False  # Disable station slicing, we'll pass codes manually
}

# Test with specific stations directly using API
import requests
print("\n--- Testing direct API call + DLT load ---")

# Create a simple resource that just returns data
import dlt

@dlt.resource(name="test_piezo", write_disposition="append")
def test_resource():
    """Simple test resource with known data"""
    test_stations = station_codes[:3]
    for station in test_stations:
        params = {
            'code_bss': station,
            'date_debut_mesure': '2004-01-01',
            'date_fin_mesure': '2004-12-31',
            'size': 100
        }
        r = requests.get(f"{base_url}/chroniques", params=params, timeout=60)
        if r.status_code == 200:
            data = r.json().get('data', [])
            if data:
                print(f"  Station {station}: {len(data)} records")
                yield data  # Yield list of dicts

print("\nRunning pipeline.run() with test resource...")
load_info = pipeline.run(test_resource(), table_name="test_piezo_2004")

print(f"\n--- Load Info ---")
print(f"Type: {type(load_info)}")
print(f"Has load_packages: {hasattr(load_info, 'load_packages')}")

# Check metrics
rows = 0
jobs = 0
for package in getattr(load_info, 'load_packages', []) or []:
    for job in getattr(package, 'jobs', []) or []:
        jobs += 1
        metrics = getattr(job, 'metrics', None) or {}
        items = metrics.get('items', 0)
        if isinstance(items, int):
            rows += items

print(f"Rows loaded (from metrics): {rows}")
print(f"Jobs count: {jobs}")

# Verify in database
import psycopg2
conn = psycopg2.connect(
    host=os.getenv('PG_HOST', 'postgres'),
    port=os.getenv('PG_PORT', '5432'),
    database=os.getenv('PG_DB', 'postgres'),
    user=os.getenv('PG_USER', 'postgres'),
    password=os.getenv('PG_PASSWORD')
)
cur = conn.cursor()
cur.execute("SELECT COUNT(*) FROM staging.test_piezo_2004")
db_count = cur.fetchone()[0]
print(f"\nActual rows in staging.test_piezo_2004: {db_count}")
conn.close()
