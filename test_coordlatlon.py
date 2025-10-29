#!/usr/bin/env python3
"""Test script to debug coordLatLon column normalization"""

import sys
import os
import logging
import requests
import pandas as pd
from io import StringIO

# Setup logging to see our debug output
logging.basicConfig(level=logging.WARNING, format='%(message)s')
logger = logging.getLogger(__name__)

# Add src to path
sys.path.insert(0, '/app/src')

from hubeau_pipeline.destinations.postgres_optimized_v2 import postgres_bulk_destination_v2

def test_coordlatlon():
    """Test the coordLatLon normalization issue"""

    # Fetch data from Hub'Eau API
    print("📡 Fetching data from Hub'Eau API...")
    url = "https://hubeau.eaufrance.fr/api/v2/hydrometrie/referentiel/stations.csv?size=100"
    response = requests.get(url)
    response.raise_for_status()

    # Parse CSV
    df = pd.read_csv(StringIO(response.text), sep=';')

    print(f"📊 Got {len(df)} rows, {len(df.columns)} columns")
    print(f"🔍 Column names (last 10): {list(df.columns[-10:])}")

    # Check for coordLatLon
    if 'coordLatLon' in df.columns:
        idx = list(df.columns).index('coordLatLon')
        print(f"⚠️ Found 'coordLatLon' at position {idx}")

    # Convert to list of dicts (as DLT would do)
    data = df.to_dict('records')

    # Use the singleton instance
    dest = postgres_bulk_destination_v2

    # Load data
    print("\n🚀 Loading data with our destination...")
    dest.load_batch(
        table_name="hydrometry_stations",
        data=data,
        write_disposition="replace"
    )

    print("\n✅ Load completed! Check the logs above for debug output.")

    # Check what was created in the database
    import psycopg2
    conn = psycopg2.connect("postgresql://postgres:postgres@postgres:5432/postgres")
    cursor = conn.cursor()

    cursor.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'hubeau'
        AND table_name = 'hydrometry_stations'
        AND column_name LIKE '%coord%'
        ORDER BY ordinal_position
    """)

    coord_cols = cursor.fetchall()
    print("\n🔎 Columns with 'coord' in database:")
    for col in coord_cols:
        print(f"  - {col[0]}")
        if col[0] != col[0].lower():
            print(f"    ❌ NOT LOWERCASE!")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    test_coordlatlon()