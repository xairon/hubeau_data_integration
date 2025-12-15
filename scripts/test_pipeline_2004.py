#!/usr/bin/env python
"""
Test direct du pipeline piezometry_chroniques pour 2004
Simule exactement ce que fait le pipeline Dagster
"""
import yaml
import sys
sys.path.insert(0, '/app/src')

from hubeau_pipeline.sources.hubeau_csv_source import (
    hubeau_chroniques_year,
    get_station_codes,
    HubeauAPIClient,
    get_total_pages,
)

# Load config
with open('/app/configs/hubeau/piezometry_chroniques.yml') as f:
    config = yaml.safe_load(f)

print("=" * 60)
print("TEST: piezometry_chroniques partition 2004")
print("=" * 60)

# Test configuration
print(f"\nConfig loaded:")
print(f"  base_url: {config['resource']['base_url']}")
print(f"  endpoint: {config['resource']['endpoint']}")
print(f"  station_slicing enabled: {config.get('extraction', {}).get('station_slicing', {}).get('enabled', False)}")

# Test station fetching
station_slicing = config.get('extraction', {}).get('station_slicing', {})
base_url = config['resource']['base_url']
rate_limit = config.get('performance', {}).get('rate_limit', 0.3)

print(f"\nFetching stations...")
station_codes = get_station_codes(
    base_url,
    station_slicing.get('stations_endpoint'),
    station_slicing.get('station_param'),
    rate_limit
)
print(f"Total stations: {len(station_codes)}")
print(f"First 5: {station_codes[:5]}")

# Test API call for first batch with 2004
print(f"\n--- Testing API call for 2004 with first 20 stations ---")
client = HubeauAPIClient(base_url, rate_limit=rate_limit)
endpoint = config['resource']['endpoint']

test_params = {
    'date_debut_mesure': '2004-01-01',
    'date_fin_mesure': '2004-12-31',
    'code_bss': ','.join(station_codes[:20])
}
print(f"Params: {test_params}")

try:
    total_pages, total_count = get_total_pages(client, endpoint, test_params)
    print(f"Result: {total_count} records across {total_pages} pages")
except Exception as e:
    print(f"Error: {e}")

# Test with known good station
print(f"\n--- Testing with known good station 07548X0009/F ---")
test_params2 = {
    'date_debut_mesure': '2004-01-01',
    'date_fin_mesure': '2004-12-31',
    'code_bss': '07548X0009/F'
}
try:
    total_pages, total_count = get_total_pages(client, endpoint, test_params2)
    print(f"Result: {total_count} records across {total_pages} pages")
except Exception as e:
    print(f"Error: {e}")
