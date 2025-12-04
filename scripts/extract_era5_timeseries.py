#!/usr/bin/env python3
"""
ERA5 NetCDF to Time Series Extraction
======================================

Extracts ERA5 NetCDF data stored in PostgreSQL bytea columns and creates
a normalized time series table for SQL analytics.

This script:
1. Reads NetCDF files from staging.era5_france_meteo_raw (bytea column)
2. Extracts time series data (time, lat, lon, temperature, precipitation, evaporation)
3. Inserts into staging.era5_france_timeseries (normalized table)

Usage:
    python scripts/extract_era5_timeseries.py [--batch-size 1000] [--file-id era5_france_2020_2021]

Architecture:
    Source: staging.era5_france_meteo_raw (38 files × 80 MB bytea = 3 GB)
    Target: staging.era5_france_timeseries (~10 million rows × 50 bytes = 500 MB)

    Each NetCDF file contains:
    - ~730 timesteps (2 years × 365 days)
    - ~10,000 grid points (0.25° grid over France)
    - 3 variables (temperature, precipitation, evaporation)
    = ~730 × 10,000 = 7.3M rows per file
    = ~38 files × 7.3M = ~277M total rows

Author: Hub'Eau Pipeline Team
Date: 2025-01-04
"""

import os
import sys
import io
import argparse
import logging
from datetime import datetime
from typing import Optional

import psycopg2
import pandas as pd
import xarray as xr
from psycopg2.extras import execute_values

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ERA5Extractor:
    """Extracts ERA5 NetCDF data from PostgreSQL bytea to time series table."""

    def __init__(self, conn_string: str):
        """Initialize extractor with database connection."""
        self.conn_string = conn_string
        self.conn = None

    def connect(self):
        """Connect to PostgreSQL."""
        try:
            self.conn = psycopg2.connect(self.conn_string)
            logger.info("✅ Connected to PostgreSQL")
        except Exception as e:
            logger.error(f"❌ Failed to connect to PostgreSQL: {e}")
            raise

    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            logger.info("Connection closed")

    def create_target_table(self):
        """Create target time series table if not exists."""
        logger.info("📊 Creating target table...")

        with self.conn.cursor() as cur:
            cur.execute("""
                -- Drop table if exists (for fresh start)
                -- DROP TABLE IF EXISTS staging.era5_france_timeseries CASCADE;

                -- Create time series table
                CREATE TABLE IF NOT EXISTS staging.era5_france_timeseries (
                    id BIGSERIAL PRIMARY KEY,
                    time TIMESTAMP NOT NULL,
                    latitude NUMERIC(6,3) NOT NULL,
                    longitude NUMERIC(6,3) NOT NULL,
                    temperature_2m NUMERIC(6,2),      -- Celsius (converted from Kelvin)
                    total_precipitation NUMERIC(8,4),  -- mm (converted from m)
                    potential_evaporation NUMERIC(8,4), -- mm (converted from m)
                    source_file_id TEXT NOT NULL,      -- Reference to original NetCDF file
                    created_at TIMESTAMP DEFAULT NOW()
                );

                -- Create indexes for common queries
                CREATE INDEX IF NOT EXISTS idx_era5_time
                    ON staging.era5_france_timeseries (time);

                CREATE INDEX IF NOT EXISTS idx_era5_location
                    ON staging.era5_france_timeseries (latitude, longitude);

                CREATE INDEX IF NOT EXISTS idx_era5_time_location
                    ON staging.era5_france_timeseries (time, latitude, longitude);

                CREATE INDEX IF NOT EXISTS idx_era5_source_file
                    ON staging.era5_france_timeseries (source_file_id);

                -- Grant permissions to readonly user
                GRANT SELECT ON staging.era5_france_timeseries TO readonly;
            """)
            self.conn.commit()
            logger.info("✅ Target table created with indexes")

    def get_files_to_process(self, file_id: Optional[str] = None):
        """Get list of NetCDF files to process."""
        with self.conn.cursor() as cur:
            if file_id:
                # Process specific file
                cur.execute("""
                    SELECT file_id, file_size_mb, start_year, end_year
                    FROM staging.era5_france_meteo_raw
                    WHERE file_id = %s
                """, (file_id,))
            else:
                # Process all files
                cur.execute("""
                    SELECT file_id, file_size_mb, start_year, end_year
                    FROM staging.era5_france_meteo_raw
                    ORDER BY start_year
                """)

            files = cur.fetchall()
            logger.info(f"📁 Found {len(files)} files to process")
            return files

    def check_if_processed(self, file_id: str) -> bool:
        """Check if file has already been processed."""
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM staging.era5_france_timeseries
                WHERE source_file_id = %s
            """, (file_id,))
            count = cur.fetchone()[0]
            return count > 0

    def extract_netcdf(self, file_id: str) -> xr.Dataset:
        """Extract NetCDF data from PostgreSQL bytea column."""
        logger.info(f"📥 Fetching NetCDF data for {file_id}...")

        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT netcdf_data
                FROM staging.era5_france_meteo_raw
                WHERE file_id = %s
            """, (file_id,))

            row = cur.fetchone()
            if not row:
                raise ValueError(f"File {file_id} not found in database")

            netcdf_bytes = row[0]
            logger.info(f"✅ Fetched {len(netcdf_bytes) / (1024*1024):.2f} MB")

        # Load NetCDF from bytes
        logger.info(f"📊 Loading NetCDF into xarray...")
        ds = xr.open_dataset(io.BytesIO(netcdf_bytes), engine='h5netcdf')
        logger.info(f"✅ Loaded NetCDF with {len(ds.time)} timesteps, {len(ds.latitude)} latitudes, {len(ds.longitude)} longitudes")

        return ds

    def netcdf_to_dataframe(self, ds: xr.Dataset, file_id: str) -> pd.DataFrame:
        """Convert NetCDF dataset to pandas DataFrame."""
        logger.info("🔄 Converting NetCDF to DataFrame...")

        # Convert to DataFrame (this will create multi-index with time, latitude, longitude)
        df = ds.to_dataframe().reset_index()

        # Rename columns to match our schema
        # ERA5-Land variables: t2m (temperature), tp (total precipitation), pev (potential evaporation)
        df = df.rename(columns={
            't2m': 'temperature_2m',
            'tp': 'total_precipitation',
            'pev': 'potential_evaporation'
        })

        # Convert units
        # Temperature: Kelvin to Celsius
        if 'temperature_2m' in df.columns:
            df['temperature_2m'] = df['temperature_2m'] - 273.15

        # Precipitation: meters to millimeters
        if 'total_precipitation' in df.columns:
            df['total_precipitation'] = df['total_precipitation'] * 1000

        # Evaporation: meters to millimeters
        if 'potential_evaporation' in df.columns:
            df['potential_evaporation'] = df['potential_evaporation'] * 1000

        # Add source file reference
        df['source_file_id'] = file_id

        # Select only needed columns
        columns = [
            'time', 'latitude', 'longitude',
            'temperature_2m', 'total_precipitation', 'potential_evaporation',
            'source_file_id'
        ]
        df = df[columns]

        # Drop rows with all NaN values (ocean points)
        df = df.dropna(subset=['temperature_2m', 'total_precipitation', 'potential_evaporation'], how='all')

        logger.info(f"✅ Created DataFrame with {len(df):,} rows")
        return df

    def insert_dataframe(self, df: pd.DataFrame, batch_size: int = 10000):
        """Insert DataFrame into PostgreSQL in batches."""
        logger.info(f"💾 Inserting {len(df):,} rows in batches of {batch_size:,}...")

        total_rows = len(df)
        inserted = 0

        with self.conn.cursor() as cur:
            for i in range(0, total_rows, batch_size):
                batch = df.iloc[i:i+batch_size]

                # Prepare data for execute_values (list of tuples)
                data = [
                    (
                        row.time,
                        float(row.latitude),
                        float(row.longitude),
                        float(row.temperature_2m) if pd.notna(row.temperature_2m) else None,
                        float(row.total_precipitation) if pd.notna(row.total_precipitation) else None,
                        float(row.potential_evaporation) if pd.notna(row.potential_evaporation) else None,
                        row.source_file_id
                    )
                    for row in batch.itertuples(index=False)
                ]

                # Bulk insert using execute_values (much faster than executemany)
                execute_values(
                    cur,
                    """
                    INSERT INTO staging.era5_france_timeseries
                        (time, latitude, longitude, temperature_2m, total_precipitation,
                         potential_evaporation, source_file_id)
                    VALUES %s
                    """,
                    data,
                    page_size=batch_size
                )

                inserted += len(batch)
                progress = (inserted / total_rows) * 100
                logger.info(f"  Progress: {inserted:,} / {total_rows:,} ({progress:.1f}%)")

        self.conn.commit()
        logger.info(f"✅ Inserted {inserted:,} rows")

    def process_file(self, file_id: str, batch_size: int = 10000, force: bool = False):
        """Process a single NetCDF file."""
        logger.info(f"")
        logger.info(f"{'='*60}")
        logger.info(f"Processing: {file_id}")
        logger.info(f"{'='*60}")

        # Check if already processed
        if not force and self.check_if_processed(file_id):
            logger.info(f"⚠️  File {file_id} already processed, skipping (use --force to reprocess)")
            return

        start_time = datetime.now()

        try:
            # Extract NetCDF from bytea
            ds = self.extract_netcdf(file_id)

            # Convert to DataFrame
            df = self.netcdf_to_dataframe(ds, file_id)

            # Insert into PostgreSQL
            self.insert_dataframe(df, batch_size=batch_size)

            elapsed = (datetime.now() - start_time).total_seconds()
            logger.info(f"✅ Completed in {elapsed:.1f} seconds")

        except Exception as e:
            logger.error(f"❌ Failed to process {file_id}: {e}")
            raise

    def process_all_files(self, batch_size: int = 10000, force: bool = False, file_id: Optional[str] = None):
        """Process all NetCDF files."""
        files = self.get_files_to_process(file_id=file_id)

        if not files:
            logger.warning("⚠️  No files to process")
            return

        total_files = len(files)
        processed = 0
        skipped = 0
        failed = 0

        logger.info(f"")
        logger.info(f"{'='*60}")
        logger.info(f"Starting extraction of {total_files} files")
        logger.info(f"{'='*60}")

        for idx, (file_id, file_size_mb, start_year, end_year) in enumerate(files, 1):
            logger.info(f"")
            logger.info(f"[{idx}/{total_files}] {file_id} ({start_year}-{end_year}, {file_size_mb:.2f} MB)")

            try:
                if not force and self.check_if_processed(file_id):
                    logger.info(f"⚠️  Already processed, skipping")
                    skipped += 1
                    continue

                self.process_file(file_id, batch_size=batch_size, force=force)
                processed += 1

            except Exception as e:
                logger.error(f"❌ Failed: {e}")
                failed += 1
                continue

        logger.info(f"")
        logger.info(f"{'='*60}")
        logger.info(f"Extraction Complete")
        logger.info(f"{'='*60}")
        logger.info(f"  Processed: {processed}/{total_files}")
        logger.info(f"  Skipped:   {skipped}/{total_files}")
        logger.info(f"  Failed:    {failed}/{total_files}")
        logger.info(f"{'='*60}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Extract ERA5 NetCDF data from PostgreSQL bytea to time series table'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=10000,
        help='Number of rows to insert per batch (default: 10000)'
    )
    parser.add_argument(
        '--file-id',
        type=str,
        help='Process specific file only (e.g., era5_france_2020_2021)'
    )
    parser.add_argument(
        '--force',
        action='store_true',
        help='Reprocess files even if already extracted'
    )
    parser.add_argument(
        '--db-host',
        type=str,
        default=os.getenv('PG_HOST', 'localhost'),
        help='PostgreSQL host (default: localhost)'
    )
    parser.add_argument(
        '--db-port',
        type=int,
        default=int(os.getenv('PG_PORT', '5432')),
        help='PostgreSQL port (default: 5432)'
    )
    parser.add_argument(
        '--db-name',
        type=str,
        default=os.getenv('PG_DB', 'postgres'),
        help='PostgreSQL database (default: postgres)'
    )
    parser.add_argument(
        '--db-user',
        type=str,
        default=os.getenv('PG_USER', 'postgres'),
        help='PostgreSQL user (default: postgres)'
    )
    parser.add_argument(
        '--db-password',
        type=str,
        default=os.getenv('PG_PASSWORD'),
        help='PostgreSQL password (default: from env PG_PASSWORD)'
    )

    args = parser.parse_args()

    # Build connection string
    conn_string = (
        f"host={args.db_host} "
        f"port={args.db_port} "
        f"dbname={args.db_name} "
        f"user={args.db_user} "
        f"password={args.db_password}"
    )

    # Initialize extractor
    extractor = ERA5Extractor(conn_string)

    try:
        # Connect to database
        extractor.connect()

        # Create target table
        extractor.create_target_table()

        # Process files
        extractor.process_all_files(
            batch_size=args.batch_size,
            force=args.force,
            file_id=args.file_id
        )

    except Exception as e:
        logger.error(f"❌ Extraction failed: {e}")
        sys.exit(1)

    finally:
        # Close connection
        extractor.close()

    logger.info("")
    logger.info("🎉 Extraction complete!")


if __name__ == '__main__':
    main()
