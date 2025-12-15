#!/usr/bin/env python
"""
Reproduce EXACT Dagster asset execution path for piezometry_chroniques_raw
"""
import os
import sys
import yaml

sys.path.insert(0, '/app/src')
os.chdir('/app')

# Import exactly as Dagster does
from hubeau_pipeline.sources.hubeau_csv_source import hubeau_chroniques_year
from hubeau_pipeline.utils.dlt_batching import create_dlt_pipeline, run_dlt_resource

print("=" * 60)
print("EXACT DAGSTER PATH REPRODUCTION: piezometry_chroniques_raw")
print("=" * 60)

# Step 1: Load config (same as asset)
config_path = "configs/hubeau/piezometry_chroniques.yml"
with open(config_path) as f:
    config = yaml.safe_load(f)
print(f"Config loaded from {config_path}")

# Limit stations for quick test
config['extraction']['station_slicing']['batch_size'] = 3

# Step 2: Create pipeline (same as asset - but without context)
# In Dagster, this is: _create_dlt_pipeline("hubeau_piezometry_chroniques", context)
pipeline = create_dlt_pipeline("hubeau_piezometry_chroniques", context=None)
print(f"Pipeline created: {pipeline.pipeline_name}")

# Step 3: Create resource (same as asset)
# In Dagster: hubeau_chroniques_year(config, year=year, dagster_context=context)
year = "2004"
print(f"\nCreating resource for year: {year}")
resource = hubeau_chroniques_year(config, year=year, dagster_context=None)
print(f"Resource type: {type(resource)}")
print(f"Resource name: {getattr(resource, 'name', 'N/A')}")

# Step 4: Call run_dlt_resource (same as _run_resource_with_metrics)
# This is the key call!
print("\n--- Calling run_dlt_resource (EXACTLY AS DAGSTER DOES) ---")
table_name = "piezometry_chroniques_raw"

metrics = run_dlt_resource(
    pipeline=pipeline,
    resource=resource,
    context=None,
    table_name=table_name,
)

print(f"\n--- Metrics returned ---")
print(f"metrics: {metrics}")

# Step 5: Check database
import psycopg2
conn = psycopg2.connect(
    host=os.getenv('PG_HOST', 'postgres'),
    port=os.getenv('PG_PORT', '5432'),
    database=os.getenv('PG_DB', 'postgres'),
    user=os.getenv('PG_USER', 'postgres'),
    password=os.getenv('PG_PASSWORD')
)
cur = conn.cursor()
cur.execute("""
    SELECT EXTRACT(YEAR FROM date_mesure::date) as year, COUNT(*) as rows 
    FROM staging.piezometry_chroniques_raw 
    GROUP BY year ORDER BY year
""")
result = cur.fetchall()
print(f"\n--- Data in piezometry_chroniques_raw ---")
for row in result:
    print(f"  Year {int(row[0])}: {row[1]:,} rows")
conn.close()
