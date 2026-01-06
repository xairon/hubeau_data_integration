import psycopg2

conn = psycopg2.connect(
    host='localhost',
    port=5433,
    user='postgres',
    password='REDACTED',
    dbname='postgres'
)
cur = conn.cursor()

print("=" * 70)
print("VERIFICATION: daily_piezometry_era5 table")
print("=" * 70)

# Table structure
cur.execute("""
    SELECT column_name, data_type
    FROM information_schema.columns 
    WHERE table_schema = 'staging' AND table_name = 'daily_piezometry_era5'
    ORDER BY ordinal_position
""")
print("\nColumns:")
for row in cur.fetchall():
    print(f"  {row[0]:30} {row[1]}")

# Statistics
cur.execute("""
    SELECT 
        COUNT(*) as total_rows,
        COUNT(DISTINCT code_bss) as stations,
        MIN(date) as min_date,
        MAX(date) as max_date,
        AVG(niveau_nappe_eau) as avg_niveau,
        AVG(temperature_2m) as avg_temp
    FROM staging.daily_piezometry_era5
""")
stats = cur.fetchone()
print(f"\nStatistics:")
print(f"  Total rows: {stats[0]:,}")
print(f"  Distinct stations: {stats[1]:,}")
print(f"  Date range: {stats[2]} to {stats[3]}")
print(f"  Average water level: {stats[4]:.2f} m")
print(f"  Average temperature: {stats[5]:.2f} °C")

# Sample query: get a specific station with all its data
cur.execute("""
    SELECT code_bss FROM staging.daily_piezometry_era5 
    GROUP BY code_bss 
    ORDER BY COUNT(*) DESC 
    LIMIT 1
""")
best_station = cur.fetchone()[0]
print(f"\nSample: Station with most data = {best_station}")

cur.execute(f"""
    SELECT date, niveau_nappe_eau, temperature_2m, total_precipitation
    FROM staging.daily_piezometry_era5
    WHERE code_bss = '{best_station}'
    ORDER BY date DESC
    LIMIT 10
""")
print("\nRecent data for this station:")
print(f"  {'Date':<12} {'Niveau (m)':<12} {'Temp (°C)':<12} {'Precip (mm)':<12}")
print("  " + "-" * 48)
for row in cur.fetchall():
    print(f"  {str(row[0]):<12} {row[1]:>10.2f}  {row[2]:>10.1f}  {row[3]:>10.4f}")

conn.close()
print("\nVerification complete!")
