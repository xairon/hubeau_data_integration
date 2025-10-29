#!/usr/bin/env python3
"""Full pipeline test with larger dataset"""

import sys
import os
import time

# Add src to path
sys.path.insert(0, '/app/src')

from hubeau_pipeline.sources.hubeau_csv_source import hubeau_csv_source
from hubeau_pipeline.destinations.postgres_optimized_v2 import postgres_bulk_destination_v2

def test_full_pipeline():
    """Test the complete pipeline with more data"""

    print("🚀 Testing full Hub'Eau pipeline with hydrometry_stations...")
    print("-" * 60)

    # Configuration like in the actual asset
    config = {
        'endpoint': 'v2/hydrometrie/referentiel/stations',
        'size': 20000,  # Large batch to test performance
        'resource_name': 'hydrometry_stations',
        'write_disposition': 'replace',
        'primary_key': ['code_station']
    }

    # Fetch data from API
    print(f"📡 Fetching up to {config['size']} records from Hub'Eau API...")
    start_fetch = time.time()

    all_data = []
    for batch in hubeau_csv_source(
        endpoint=config['endpoint'],
        size=config['size'],
        resource_name=config['resource_name']
    ):
        all_data.extend(batch)
        print(f"  📦 Batch received: {len(batch)} records")

    fetch_time = time.time() - start_fetch
    print(f"✅ Fetched {len(all_data)} records in {fetch_time:.2f}s")

    if not all_data:
        print("❌ No data fetched!")
        return

    # Check for coordLatLon in the data
    first_record_keys = list(all_data[0].keys())
    print(f"\n🔍 Total columns: {len(first_record_keys)}")

    if 'coordLatLon' in first_record_keys:
        print(f"⚠️ Found 'coordLatLon' (CamelCase) in raw data at position {first_record_keys.index('coordLatLon')}")
    elif 'coordlatlon' in first_record_keys:
        print(f"✅ Found 'coordlatlon' (lowercase) in raw data at position {first_record_keys.index('coordlatlon')}")

    # Load data using our destination
    print(f"\n💾 Loading {len(all_data)} records into PostgreSQL...")
    start_load = time.time()

    postgres_bulk_destination_v2.load_batch(
        table_name=config['resource_name'],
        data=all_data,
        write_disposition=config['write_disposition'],
        primary_keys=config['primary_key']
    )

    load_time = time.time() - start_load
    print(f"✅ Loaded in {load_time:.2f}s ({len(all_data)/load_time:.0f} records/sec)")

    # Verify in database
    import psycopg2
    conn_str = os.environ.get('POSTGRES_CONNECTION_STRING',
                              'postgresql://postgres:postgres@postgres:5432/postgres')

    print("\n🔎 Verifying in database...")
    try:
        conn = psycopg2.connect(conn_str.replace('postgres@postgres', 'postgres@localhost'))
    except:
        # Fallback for container environment
        conn = psycopg2.connect(host='postgres', database='postgres',
                               user='postgres', password='postgres')

    cursor = conn.cursor()

    # Check row count
    cursor.execute("SELECT COUNT(*) FROM hubeau.hydrometry_stations")
    count = cursor.fetchone()[0]
    print(f"  📊 Total rows in table: {count}")

    # Check column names
    cursor.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'hubeau'
        AND table_name = 'hydrometry_stations'
        AND column_name LIKE '%coord%'
        ORDER BY ordinal_position
    """)

    coord_cols = cursor.fetchall()
    print(f"\n  🔍 Columns with 'coord' in table:")
    all_lowercase = True
    for col in coord_cols:
        col_name = col[0]
        is_lowercase = col_name == col_name.lower()
        status = "✅" if is_lowercase else "❌"
        print(f"    {status} {col_name}")
        if not is_lowercase:
            all_lowercase = False

    cursor.close()
    conn.close()

    print("\n" + "=" * 60)
    if all_lowercase and count == len(all_data):
        print("✅ SUCCESS: Pipeline test passed!")
        print(f"  - All {count} records loaded successfully")
        print(f"  - All column names are lowercase")
        print(f"  - Total time: {fetch_time + load_time:.2f}s")
    else:
        print("❌ FAILURE: Issues detected")
        if count != len(all_data):
            print(f"  - Row count mismatch: {count} in DB vs {len(all_data)} fetched")
        if not all_lowercase:
            print(f"  - Some columns are not lowercase")

    return all_lowercase and count == len(all_data)

if __name__ == "__main__":
    success = test_full_pipeline()
    exit(0 if success else 1)