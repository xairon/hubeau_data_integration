import psycopg2

conn = psycopg2.connect(
    host='localhost',
    port=5433,
    user='postgres',
    password='REDACTED',
    dbname='postgres'
)
cur = conn.cursor()

with open('grid_analysis.txt', 'w', encoding='utf-8') as f:
    # ERA5 grid analysis
    f.write("=" * 70 + "\n")
    f.write("ERA5 GRID ANALYSIS\n")
    f.write("=" * 70 + "\n")
    
    # Distinct lat/lon values
    cur.execute("SELECT COUNT(DISTINCT latitude) FROM staging.era5_france_timeseries")
    lat_count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(DISTINCT longitude) FROM staging.era5_france_timeseries")
    lon_count = cur.fetchone()[0]
    f.write(f"\nDistinct latitude values: {lat_count}\n")
    f.write(f"Distinct longitude values: {lon_count}\n")
    f.write(f"Total grid points: {lat_count * lon_count}\n")
    
    # Lat/lon bounds
    cur.execute("""
        SELECT MIN(latitude), MAX(latitude), MIN(longitude), MAX(longitude)
        FROM staging.era5_france_timeseries
    """)
    bounds = cur.fetchone()
    f.write(f"\nLatitude range: {bounds[0]} to {bounds[1]}\n")
    f.write(f"Longitude range: {bounds[2]} to {bounds[3]}\n")
    
    # Sample grid points to understand resolution
    cur.execute("""
        SELECT DISTINCT latitude FROM staging.era5_france_timeseries 
        ORDER BY latitude LIMIT 10
    """)
    f.write("\nSample latitude values (first 10):\n")
    lats = [r[0] for r in cur.fetchall()]
    for lat in lats:
        f.write(f"  {lat}\n")
    if len(lats) > 1:
        f.write(f"\nGrid resolution (lat step): {float(lats[1]) - float(lats[0])} degrees\n")
    
    cur.execute("""
        SELECT DISTINCT longitude FROM staging.era5_france_timeseries 
        ORDER BY longitude LIMIT 10
    """)
    f.write("\nSample longitude values (first 10):\n")
    lons = [r[0] for r in cur.fetchall()]
    for lon in lons:
        f.write(f"  {lon}\n")
    if len(lons) > 1:
        f.write(f"\nGrid resolution (lon step): {float(lons[1]) - float(lons[0])} degrees\n")
    
    # Time range
    cur.execute("""
        SELECT MIN(time), MAX(time) FROM staging.era5_france_timeseries
    """)
    time_range = cur.fetchone()
    f.write(f"\nTime range: {time_range[0]} to {time_range[1]}\n")
    
    # Piezometry stations spatial coverage
    f.write("\n" + "=" * 70 + "\n")
    f.write("PIEZOMETRY STATIONS SPATIAL COVERAGE\n")
    f.write("=" * 70 + "\n")
    
    cur.execute("""
        SELECT MIN(x), MAX(x), MIN(y), MAX(y)
        FROM staging.piezometry_stations_raw
    """)
    piezo_bounds = cur.fetchone()
    f.write(f"\nLongitude (x) range: {piezo_bounds[0]} to {piezo_bounds[1]}\n")
    f.write(f"Latitude (y) range: {piezo_bounds[2]} to {piezo_bounds[3]}\n")
    
    # Check overlap with ERA5
    f.write("\n" + "=" * 70 + "\n")
    f.write("SPATIAL OVERLAP ANALYSIS\n")
    f.write("=" * 70 + "\n")
    f.write(f"\nERA5 covers: lat [{bounds[0]}, {bounds[1]}], lon [{bounds[2]}, {bounds[3]}]\n")
    f.write(f"Piezo covers: lat [{piezo_bounds[2]}, {piezo_bounds[3]}], lon [{piezo_bounds[0]}, {piezo_bounds[1]}]\n")
    
    # How many piezo stations fall within ERA5 coverage?
    cur.execute(f"""
        SELECT COUNT(*) FROM staging.piezometry_stations_raw
        WHERE y >= {bounds[0]} AND y <= {bounds[1]}
        AND x >= {bounds[2]} AND x <= {bounds[3]}
    """)
    overlap_count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM staging.piezometry_stations_raw")
    total_piezo = cur.fetchone()[0]
    f.write(f"\nPiezo stations within ERA5 grid: {overlap_count} / {total_piezo} ({100*overlap_count/total_piezo:.1f}%)\n")

conn.close()
print("Grid analysis saved to grid_analysis.txt")
