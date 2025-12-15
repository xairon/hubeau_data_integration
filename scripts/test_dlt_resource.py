#!/usr/bin/env python
"""
Test that mimics EXACTLY what hubeau_chroniques_year does
to isolate why pipeline.run() returns 0 rows
"""
import os
import sys
sys.path.insert(0, '/app/src')
os.chdir('/app')

import dlt
from typing import Iterator, List, Dict, Any

# Mimic the @dlt.resource decorator exactly as in hubeau_csv_source.py
@dlt.resource(
    parallelized=False,
    write_disposition="append"
)
def test_resource_like_hubeau() -> Iterator[List[Dict]]:
    """
    Mimics hubeau_chroniques_year structure:
    - Uses @dlt.resource decorator with append
    - Yields List[Dict] (batches of records)
    """
    print("test_resource_like_hubeau: Starting...", flush=True)
    
    # Simulate 3 chunks of data (like station batches)
    for chunk_num in range(1, 4):
        # Create a batch of records (like page of API results)
        batch = [
            {"id": i, "chunk": chunk_num, "name": f"record_{chunk_num}_{i}", "value": i * 100}
            for i in range(1, 11)  # 10 records per batch
        ]
        print(f"  Yielding chunk {chunk_num}: {len(batch)} records", flush=True)
        yield batch
    
    print("test_resource_like_hubeau: Generator complete - yielded 30 records", flush=True)

# Create pipeline exactly like _create_dlt_pipeline does
print("=" * 60, flush=True)
print("TEST: Mimicking hubeau_chroniques_year", flush=True)
print("=" * 60, flush=True)

pipeline = dlt.pipeline(
    pipeline_name="test_hubeau_mimic",
    destination="postgres",
    dataset_name="staging",
    progress="log"
)

print(f"Pipeline: {pipeline.pipeline_name}", flush=True)

# Run exactly like the simplified asset does
print("\nRunning pipeline.run() with @dlt.resource...", flush=True)
load_info = pipeline.run(
    test_resource_like_hubeau(),
    table_name="test_hubeau_mimic_table"
    # NOTE: No write_disposition here since it's in @dlt.resource
)

print(f"\nLoad info: {load_info}", flush=True)

# Extract metrics like the asset does
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
cur.execute("SELECT COUNT(*) FROM staging.test_hubeau_mimic_table")
count = cur.fetchone()[0]
print(f"\nRows in staging.test_hubeau_mimic_table: {count}", flush=True)

if count > 0:
    cur.execute("SELECT * FROM staging.test_hubeau_mimic_table LIMIT 5")
    rows = cur.fetchall()
    print(f"Sample rows: {rows}", flush=True)
    print("\n✅ SUCCESS - @dlt.resource with List[Dict] works!")
else:
    print("\n❌ FAILURE - 0 rows written. Problem is in @dlt.resource or List[Dict] yield pattern!")

conn.close()
