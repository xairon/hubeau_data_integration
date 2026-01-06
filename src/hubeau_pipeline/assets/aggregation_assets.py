"""
Aggregation Assets - Silver Layer

Creates aggregated tables combining ERA5 weather data with piezometry measurements.
Spatial join using ERA5 0.1° grid alignment.
"""

import os
import psycopg2
from dagster import asset, AssetExecutionContext


def _get_db_connection():
    """Get PostgreSQL connection from environment."""
    return psycopg2.connect(
        host=os.getenv('PG_HOST', 'postgres'),
        port=os.getenv('PG_PORT', '5432'),
        database=os.getenv('PG_DB', 'postgres'),
        user=os.getenv('PG_USER', 'postgres'),
        password=os.getenv('PG_PASSWORD')
    )


@asset(
    compute_kind="sql",
    group_name="aggregation",
    io_manager_key="noop_io_manager",
    deps=["era5_france_timeseries", "piezometry_stations_raw", "piezometry_chroniques_raw"]
)
def station_era5_mapping(context: AssetExecutionContext):
    """
    Station to ERA5 Grid Mapping

    Maps each piezometry station (x, y coordinates) to the nearest ERA5 grid point
    by rounding to 0.1° resolution.

    Coverage: Only France métropolitaine stations (41°N-51.5°N, -5.5°E-10°E)
    """
    conn = _get_db_connection()
    conn.autocommit = True

    try:
        with conn.cursor() as cur:
            # Drop and recreate
            context.log.info("🗑️ Dropping existing mapping table...")
            cur.execute("DROP TABLE IF EXISTS staging.station_era5_mapping")

            context.log.info("📍 Creating station → ERA5 mapping table...")
            cur.execute("""
                CREATE TABLE staging.station_era5_mapping AS
                SELECT 
                    code_bss,
                    ROUND(y::numeric * 10) / 10 AS era5_latitude,
                    ROUND(x::numeric * 10) / 10 AS era5_longitude,
                    y AS station_latitude,
                    x AS station_longitude
                FROM staging.piezometry_stations_raw
                WHERE y >= 41.0 AND y <= 51.5 
                  AND x >= -5.5 AND x <= 10.0
            """)

            # Add primary key and index
            cur.execute("ALTER TABLE staging.station_era5_mapping ADD PRIMARY KEY (code_bss)")
            cur.execute("""
                CREATE INDEX idx_mapping_era5_coords 
                ON staging.station_era5_mapping (era5_latitude, era5_longitude)
            """)

            # Get stats
            cur.execute("SELECT COUNT(*) FROM staging.station_era5_mapping")
            count = cur.fetchone()[0]

            context.log.info(f"✅ Mapping table created with {count:,} stations")

            return {"status": "success", "stations_mapped": count}

    finally:
        conn.close()


@asset(
    compute_kind="sql",
    group_name="aggregation",
    io_manager_key="noop_io_manager",
    deps=["station_era5_mapping"]
)
def daily_piezometry_era5(context: AssetExecutionContext):
    """
    Daily Piezometry + ERA5 Aggregated Table

    Combines piezometry water level measurements with ERA5 weather data
    at daily granularity. Each row contains:
    - code_bss: Station ID
    - date: Measurement date
    - niveau_nappe_eau: Water level (m)
    - profondeur_nappe: Water depth (m)
    - temperature_2m: ERA5 temperature (°C)
    - total_precipitation: ERA5 precipitation (mm)
    - potential_evaporation: ERA5 evaporation (mm)

    Spatial join uses 0.1° grid alignment (nearest ERA5 grid point).
    Expected size: ~20M rows
    Runtime: ~15 minutes
    """
    conn = _get_db_connection()
    conn.autocommit = True

    try:
        with conn.cursor() as cur:
            # Drop and recreate
            context.log.info("🗑️ Dropping existing aggregated table...")
            cur.execute("DROP TABLE IF EXISTS staging.daily_piezometry_era5")

            context.log.info("📊 Creating aggregated table (this may take ~15 minutes)...")

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

            context.log.info("📌 Creating indexes...")
            cur.execute("""
                CREATE INDEX idx_daily_piezo_code_bss 
                ON staging.daily_piezometry_era5 (code_bss)
            """)
            cur.execute("""
                CREATE INDEX idx_daily_piezo_date 
                ON staging.daily_piezometry_era5 (date)
            """)
            cur.execute("""
                CREATE INDEX idx_daily_piezo_code_date 
                ON staging.daily_piezometry_era5 (code_bss, date)
            """)

            # Get stats
            cur.execute("""
                SELECT 
                    COUNT(*) as total_rows,
                    COUNT(DISTINCT code_bss) as stations,
                    MIN(date) as min_date,
                    MAX(date) as max_date
                FROM staging.daily_piezometry_era5
            """)
            stats = cur.fetchone()

            context.log.info(f"✅ Aggregated table created!")
            context.log.info(f"   Rows: {stats[0]:,}")
            context.log.info(f"   Stations: {stats[1]:,}")
            context.log.info(f"   Period: {stats[2]} → {stats[3]}")

            return {
                "status": "success",
                "rows": stats[0],
                "stations": stats[1],
                "min_date": str(stats[2]),
                "max_date": str(stats[3])
            }

    finally:
        conn.close()
