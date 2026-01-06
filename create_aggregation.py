import psycopg2
import time

conn = psycopg2.connect(
    host='localhost',
    port=5433,
    user='postgres',
    password='REDACTED',
    dbname='postgres'
)
conn.autocommit = True
cur = conn.cursor()

print("=" * 60)
print("STEP 2: Creating daily_piezometry_era5 table")
print("=" * 60)

# First, let's check how many rows we expect (sample test)
print("\nEstimating final table size...")
cur.execute("""
    SELECT COUNT(*) 
    FROM staging.piezometry_chroniques_raw p
    WHERE EXISTS (
        SELECT 1 FROM staging.station_era5_mapping m 
        WHERE m.code_bss = p.code_bss
    )
""")
piezo_rows = cur.fetchone()[0]
print(f"Piezometry rows with valid mapping: {piezo_rows:,}")

# Drop if exists
print("\nDropping existing table (if any)...")
cur.execute("DROP TABLE IF EXISTS staging.daily_piezometry_era5")

# Create the aggregated table
# Note: This may take a while due to the large join
print("\nCreating aggregated table (this may take several minutes)...")
start_time = time.time()

cur.execute("""
    CREATE TABLE staging.daily_piezometry_era5 AS
    SELECT 
        p.code_bss,
        p.date_mesure::date AS date,
        AVG(p.niveau_nappe_eau::numeric) AS niveau_nappe_eau,
        AVG(p.profondeur_nappe::numeric) AS profondeur_nappe,
        AVG(e.temperature_2m) AS temperature_2m,
        AVG(e.total_precipitation) AS total_precipitation,
        AVG(e.potential_evaporation) AS potential_evaporation
    FROM staging.piezometry_chroniques_raw p
    JOIN staging.station_era5_mapping m ON p.code_bss = m.code_bss
    JOIN staging.era5_france_timeseries e 
        ON e.latitude = m.era5_latitude 
        AND e.longitude = m.era5_longitude
        AND e.time::date = p.date_mesure::date
    GROUP BY p.code_bss, p.date_mesure::date
""")

elapsed = time.time() - start_time
print(f"Table created in {elapsed:.1f} seconds")

# Add indexes
print("\nCreating indexes...")
cur.execute("CREATE INDEX idx_daily_piezo_code_bss ON staging.daily_piezometry_era5 (code_bss)")
cur.execute("CREATE INDEX idx_daily_piezo_date ON staging.daily_piezometry_era5 (date)")
print("Indexes created")

# Verify
cur.execute("SELECT COUNT(*) FROM staging.daily_piezometry_era5")
count = cur.fetchone()[0]
print(f"\nFinal table has {count:,} rows")

# Sample
cur.execute("""
    SELECT * FROM staging.daily_piezometry_era5 
    ORDER BY date DESC 
    LIMIT 5
""")
print("\nSample rows (most recent):")
for row in cur.fetchall():
    print(f"  {row[0]}: {row[1]} | nappe={row[2]:.2f}m | temp={row[4]:.1f}°C | precip={row[5]:.2f}mm")

# Stats
cur.execute("""
    SELECT 
        MIN(date) as min_date,
        MAX(date) as max_date,
        COUNT(DISTINCT code_bss) as station_count,
        COUNT(*) as total_rows
    FROM staging.daily_piezometry_era5
""")
stats = cur.fetchone()
print(f"\nStatistics:")
print(f"  Date range: {stats[0]} to {stats[1]}")
print(f"  Stations: {stats[2]:,}")
print(f"  Total rows: {stats[3]:,}")

conn.close()
print("\nStep 2 complete! Aggregated table ready.")
