#!/usr/bin/env python
"""
MINIMAL DLT test - isolate why pipeline.run() doesn't write data
"""
import os
import sys
sys.path.insert(0, '/app/src')
os.chdir('/app')

import dlt

# Create a SIMPLE generator that yields test data
def simple_test_data():
    """Simple generator that yields a few records"""
    for i in range(10):
        yield {"id": i, "name": f"test_{i}", "value": i * 100}
    print("Generator exhausted - yielded 10 records", flush=True)

# Create pipeline
print("Creating pipeline...", flush=True)
pipeline = dlt.pipeline(
    pipeline_name="test_minimal",
    destination="postgres",
    dataset_name="staging",
    progress="log"
)

print(f"Pipeline created: {pipeline.pipeline_name}", flush=True)
print(f"Pipeline destination: {pipeline.destination}", flush=True)
print(f"Pipeline dataset: {pipeline.dataset_name}", flush=True)

# Run pipeline with simple data
print("\nRunning pipeline.run()...", flush=True)
load_info = pipeline.run(
    simple_test_data(),
    table_name="test_minimal_table",
    write_disposition="replace"
)

print(f"\nLoad info type: {type(load_info)}", flush=True)
print(f"Load info: {load_info}", flush=True)

# Check if data was written
import psycopg2
conn = psycopg2.connect(
    host=os.getenv('PG_HOST', 'postgres'),
    port=os.getenv('PG_PORT', '5432'),
    database=os.getenv('PG_DB', 'postgres'),
    user=os.getenv('PG_USER', 'postgres'),
    password=os.getenv('PG_PASSWORD')
)
cur = conn.cursor()
cur.execute("SELECT COUNT(*) FROM staging.test_minimal_table")
count = cur.fetchone()[0]
print(f"\nRows in staging.test_minimal_table: {count}", flush=True)

if count > 0:
    cur.execute("SELECT * FROM staging.test_minimal_table LIMIT 3")
    rows = cur.fetchall()
    print(f"Sample rows: {rows}", flush=True)
    print("\n✅ SUCCESS - DLT can write to database!")
else:
    print("\n❌ FAILURE - DLT wrote 0 rows!")

conn.close()
