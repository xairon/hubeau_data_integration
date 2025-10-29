#!/usr/bin/env python3
"""Simple pipeline test focusing on the coordLatLon normalization"""

import sys
import time
import requests
import pandas as pd
from io import StringIO

# Add src to path
sys.path.insert(0, '/app/src')

from hubeau_pipeline.destinations.postgres_optimized_v2 import postgres_bulk_destination_v2

def test_simple_pipeline():
    """Simple test to verify coordLatLon normalization in full pipeline"""

    print("🚀 Testing coordLatLon normalization with real data...")
    print("-" * 60)

    # Fetch a larger batch from API
    size = 5000
    url = f"https://hubeau.eaufrance.fr/api/v2/hydrometrie/referentiel/stations.csv?size={size}"

    print(f"📡 Fetching {size} records from Hub'Eau API...")
    response = requests.get(url)
    response.raise_for_status()

    # Parse CSV
    df = pd.read_csv(StringIO(response.text), sep=';')
    print(f"✅ Got {len(df)} rows, {len(df.columns)} columns")

    # Convert to records
    data = df.to_dict('records')

    # Check for coordLatLon in raw data
    if 'coordLatLon' in df.columns:
        print(f"⚠️ Found 'coordLatLon' (CamelCase) in API response")

    # Load with our destination
    print(f"\n💾 Loading {len(data)} records into PostgreSQL...")
    start = time.time()

    postgres_bulk_destination_v2.load_batch(
        table_name='hydrometry_stations',
        data=data,
        write_disposition='replace',
        primary_keys=['code_station']
    )

    elapsed = time.time() - start
    print(f"✅ Loaded in {elapsed:.2f}s ({len(data)/elapsed:.0f} records/sec)")

    # Verify in database
    import psycopg2
    conn = psycopg2.connect(host='postgres', database='postgres',
                           user='postgres', password='postgres')
    cursor = conn.cursor()

    # Check count
    cursor.execute("SELECT COUNT(*) FROM hubeau.hydrometry_stations")
    count = cursor.fetchone()[0]

    # Check coordLatLon column
    cursor.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'hubeau'
        AND table_name = 'hydrometry_stations'
        AND column_name IN ('coordLatLon', 'coordlatlon')
    """)

    result = cursor.fetchone()

    print("\n" + "=" * 60)
    print(f"📊 Results:")
    print(f"  - Rows loaded: {count}")

    if result:
        col_name = result[0]
        if col_name == 'coordlatlon':
            print(f"  ✅ Column normalized: 'coordLatLon' → 'coordlatlon'")
            print("\n✅ SUCCESS: Pipeline working correctly!")
            return True
        else:
            print(f"  ❌ Column NOT normalized: '{col_name}'")
            print("\n❌ FAILURE: Normalization not working!")
            return False
    else:
        print(f"  ❌ No coord column found!")
        return False

    cursor.close()
    conn.close()

if __name__ == "__main__":
    success = test_simple_pipeline()
    exit(0 if success else 1)