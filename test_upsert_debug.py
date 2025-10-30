#!/usr/bin/env python3
"""Test script to reproduce and debug the UPSERT issue"""

import sys
sys.path.insert(0, '/app/src')

from hubeau_pipeline.destinations.postgres_optimized_v2 import PostgresBulkDestinationV2
from hubeau_pipeline.schema.hubeau_type_mappings import TABLE_PK_MAPPING

# Test data simulating Hub'Eau CSV response
test_data = [
    {
        "CODE_BSS": "03426X0199/PZ2",
        "DATE_MESURE": "2024-01-01",
        "URN_BSS": "urn:bss:03426X0199/PZ2",
        "NIVEAU_NAPPE_EAU": 10.5,
        "PROFONDEUR_NAPPE": 5.2,
        "TIMESTAMP_MESURE": 1704067200,
        "MODE_OBTENTION": "Manual",
        "STATUT": "Valid",
        "QUALIFICATION": "Good",
        "CODE_CONTINUITE": "C1",
        "NOM_CONTINUITE": "Continue",
        "CODE_PRODUCTEUR": "P1",
        "NOM_PRODUCTEUR": "Producer 1",
        "CODE_NATURE_MESURE": "N1",
        "NOM_NATURE_MESURE": "Nature 1"
    },
    {
        "CODE_BSS": "03426X0199/PZ2",
        "DATE_MESURE": "2024-01-02",
        "URN_BSS": "urn:bss:03426X0199/PZ2",
        "NIVEAU_NAPPE_EAU": 10.7,
        "PROFONDEUR_NAPPE": 5.4,
        "TIMESTAMP_MESURE": 1704153600,
        "MODE_OBTENTION": "Manual",
        "STATUT": "Valid",
        "QUALIFICATION": "Good",
        "CODE_CONTINUITE": "C1",
        "NOM_CONTINUITE": "Continue",
        "CODE_PRODUCTEUR": "P1",
        "NOM_PRODUCTEUR": "Producer 1",
        "CODE_NATURE_MESURE": "N1",
        "NOM_NATURE_MESURE": "Nature 1"
    }
]

print("=" * 80)
print("TESTING UPSERT WITH DEBUG LOGS")
print("=" * 80)

# Initialize destination
destination = PostgresBulkDestinationV2()

# Table name - use real table name so mapping works
table_name = "piezometry_chroniques"

# Primary keys from mapping
primary_keys = TABLE_PK_MAPPING.get("piezometry_chroniques", ["code_bss", "date_mesure"])

print(f"\nTable: {table_name}")
print(f"Primary keys from mapping: {primary_keys}")
print(f"Test data keys (original): {list(test_data[0].keys())[:5]}")

try:
    # Call load_batch with merge disposition
    print("\nCalling load_batch with write_disposition='merge'...")
    destination.load_batch(
        table_name=table_name,
        data=test_data,
        write_disposition="merge",
        primary_keys=primary_keys,
        column_mappings=None
    )
    print("\n✅ SUCCESS: load_batch completed without error!")

except Exception as e:
    print(f"\n❌ ERROR: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "=" * 80)
print("TEST COMPLETE")
print("=" * 80)