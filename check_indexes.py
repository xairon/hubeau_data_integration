import psycopg2
import os

try:
    conn = psycopg2.connect(
        host='localhost', 
        port=5433, 
        user='postgres', 
        password='REDACTED', 
        dbname='postgres'
    )
    cur = conn.cursor()
    
    print("Checking indexes for staging.era5_france_timeseries:")
    cur.execute("SELECT indexname, indexdef FROM pg_indexes WHERE tablename = 'era5_france_timeseries' AND schemaname = 'staging'")
    rows = cur.fetchall()
    if not rows:
        print("No indexes found!")
    for row in rows:
        print(f"- {row[0]}: {row[1]}")
        
    print("\nChecking indexes for staging.piezometry_chroniques_raw:")
    cur.execute("SELECT indexname, indexdef FROM pg_indexes WHERE tablename = 'piezometry_chroniques_raw' AND schemaname = 'staging'")
    rows = cur.fetchall()
    for row in rows:
        print(f"- {row[0]}: {row[1]}")

except Exception as e:
    print(f"Error: {e}")
finally:
    if 'conn' in locals() and conn:
        conn.close()
