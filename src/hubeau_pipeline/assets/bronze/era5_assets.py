"""
ERA5 Bronze Layer Assets

Stockage NetCDF4 bruts en PostgreSQL
- 1 timestep/jour (00:00 UTC)
- Chunks de 2 ans (limite API)
- ~43 fichiers pour 1940-2025
"""

import yaml
import dlt
import gc
import io
import os
import zipfile
from typing import Dict, Any
from datetime import datetime
from dagster import asset, AssetExecutionContext, StaticPartitionsDefinition
from hubeau_pipeline.sources.era5_source import era5_france_meteo
from dlt.destinations import postgres
import psycopg2
import xarray as xr
import pandas as pd
from psycopg2.extras import execute_values


# ERA5 Partitions: chunks de 2 ans (1950-1951, 1952-1953, ..., 2024-2025)
# Permet de re-télécharger un chunk spécifique si manquant
ERA5_START_YEAR = 1950
ERA5_CURRENT_YEAR = datetime.now().year
ERA5_CHUNK_PARTITIONS = StaticPartitionsDefinition(
    [str(year) for year in range(ERA5_START_YEAR, ERA5_CURRENT_YEAR + 1, 2)]
)


def _create_pipeline(pipeline_name: str, context=None) -> dlt.Pipeline:
    """Create DLT pipeline with PostgreSQL destination."""
    destination = postgres(
        credentials={
            "database": os.environ.get("PG_DB", "postgres"),
            "username": os.environ.get("PG_USER", "postgres"),
            "password": os.environ.get("PG_PASSWORD"),
            "host": os.environ.get("PG_HOST", "postgres"),
            "port": int(os.environ.get("PG_PORT", "5432")),
        }
    )
    return dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=destination,
        dataset_name=os.environ.get("DLT_BRONZE_DATASET", "staging"),
        progress="log",
    )


@asset(
    compute_kind="era5",
    group_name="era5_meteo",
    io_manager_key="noop_io_manager",
    partitions_def=ERA5_CHUNK_PARTITIONS
)
def era5_france_meteo_raw(context: AssetExecutionContext):
    """
    ERA5 France Météo - NetCDF4 daily data (00:00 UTC) [PARTITIONNÉ PAR CHUNK]

    Variables:
    - 2m_temperature (K)
    - total_precipitation (m)
    - potential_evaporation (m)

    Coverage: France métropolitaine (0.25° grid)
    Period: 1950-present (ERA5-Land)
    Frequency: Daily at 00:00 UTC (previous day data)
    Storage: PostgreSQL bytea (~38 files × 50-100 MB)

    Note: Downloads in 2-year chunks (API limitation)
    Partitionnement: Clique sur une année (ex: 2006) pour re-télécharger uniquement ce chunk

    ⚠️ Each chunk is stored IMMEDIATELY in PostgreSQL after download (true incremental loading)
    """
    config_path = "configs/era5/era5_france_meteo.yml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    # Si une partition est demandée, télécharger uniquement ce chunk
    if context.has_partition_key:
        chunk_start_year = int(context.partition_key)
        context.log.info(f"🎯 Partition demandée: chunk {chunk_start_year}-{chunk_start_year+1}")
        
        # Override config pour ne télécharger que ce chunk
        config['extraction']['chunking']['only_chunk_start_years'] = [chunk_start_year]
        config['extraction']['chunking']['skip_existing_in_db'] = False  # Re-télécharger même si existe
    else:
        context.log.info("🚀 Téléchargement complet (toutes les années)")

    total_files = 0
    total_size_mb = 0.0

    # Iterate through ERA5 generator and save EACH chunk individually
    for chunk_index, record in enumerate(era5_france_meteo(config, dagster_context=context), start=1):

        # ⚠️ MEMORY FIX: Recreate pipeline for each chunk to avoid memory accumulation
        context.log.info(f"🔧 [{chunk_index}] Creating fresh DLT pipeline to avoid memory leaks...")
        pipeline = _create_pipeline("era5_france_meteo", context=context)

        # Create a single-record resource for this chunk
        @dlt.resource(
            name="era5_netcdf_files",
            write_disposition="append",
            primary_key="file_id"
        )
        def single_chunk():
            """Single chunk resource for immediate storage"""
            yield record

        # Store THIS chunk immediately in PostgreSQL
        context.log.info(
            f"💾 [{chunk_index}] Storing chunk {record['file_id']} in PostgreSQL NOW "
            f"({record['file_size_mb']:.2f} MB)..."
        )

        load_info = pipeline.run(single_chunk, table_name="era5_france_meteo_raw")

        total_files += 1
        total_size_mb += record.get('file_size_mb', 0)

        context.log.info(
            f"✅ [{chunk_index}] Chunk {record['file_id']} stored successfully in PostgreSQL! "
            f"Total: {total_files} files, {total_size_mb:.2f} MB"
        )

        # ⚠️ MEMORY FIX: Force garbage collection after each chunk
        del pipeline
        del load_info
        gc.collect()
        context.log.info(f"🧹 [{chunk_index}] Memory cleaned (garbage collection completed)")

    context.log.info(
        f"🎉 ERA5 download complete! Stored {total_files} NetCDF4 files in PostgreSQL ({total_size_mb:.2f} MB)"
    )

    return {
        "rows_loaded": total_files,
        "total_size_mb": total_size_mb,
        "status": "success",
        "table_name": "era5_france_meteo_raw"
    }


@asset(
    compute_kind="era5",
    group_name="era5_meteo",
    io_manager_key="noop_io_manager",
    deps=["era5_france_meteo_raw"],
    partitions_def=ERA5_CHUNK_PARTITIONS
)
def era5_france_timeseries(context: AssetExecutionContext):
    """
    ERA5 France Time Series - Normalized table from NetCDF bytea storage [PARTITIONNÉ]

    Extracts NetCDF data from staging.era5_france_meteo_raw (bytea columns)
    and creates a queryable time series table with normalized structure.

    Source: staging.era5_france_meteo_raw (38 files × 80 MB = 3 GB bytea)
    Target: staging.era5_france_timeseries (~277M rows × 50 bytes = ~14 GB)

    This asset:
    - Unpacks ZIP-compressed NetCDF files from bytea storage
    - Extracts time series data (time, lat, lon, temp, precip, evap)
    - Converts units (Kelvin → Celsius, meters → millimeters)
    - Creates indexed table for efficient SQL queries
    - Skips files already processed (incremental)

    Partitionnement: Clique sur une année (ex: 2006) pour extraire uniquement ce chunk

    Expected runtime: ~30-60 minutes for full dataset, ~1-2 min per chunk
    """
    import os

    # Get database connection from environment
    conn = psycopg2.connect(
        host=os.getenv('PG_HOST', 'postgres'),
        port=os.getenv('PG_PORT', '5432'),
        database=os.getenv('PG_DB', 'postgres'),
        user=os.getenv('PG_USER', 'postgres'),
        password=os.getenv('PG_PASSWORD')
    )

    try:
        # Create target table
        context.log.info("📊 Creating target table...")
        _create_timeseries_table(conn)
        context.log.info("✅ Target table created with indexes")

        # Si une partition est demandée, extraire uniquement ce chunk
        target_file_id = None
        if context.has_partition_key:
            chunk_start_year = int(context.partition_key)
            chunk_end_year = chunk_start_year + 1
            target_file_id = f"era5_france_{chunk_start_year}_{chunk_end_year}"
            context.log.info(f"🎯 Partition demandée: extraction de {target_file_id}")

        # Get files to process
        files = _get_files_to_process(conn, context, target_file_id=target_file_id)
        context.log.info(f"📁 Found {len(files)} files to process")

        if len(files) == 0:
            context.log.info("✅ All files already processed!")
            return {"status": "success", "rows_inserted": 0, "files_processed": 0}

        total_rows = 0
        files_processed = 0

        # Process each file
        for idx, (file_id, file_size_mb, start_year, end_year) in enumerate(files, 1):
            context.log.info(f"\n[{idx}/{len(files)}] Processing {file_id} ({start_year}-{end_year}, {file_size_mb:.2f} MB)")

            try:
                # Si mode partition, supprimer les anciennes données pour éviter les doublons
                if context.has_partition_key:
                    _delete_existing_timeseries(conn, file_id, context)

                # Extract NetCDF
                ds = _extract_netcdf(conn, file_id, context)

                # Convert to DataFrame
                df = _netcdf_to_dataframe(ds, file_id, context)

                # Insert into database
                rows = _insert_dataframe(conn, df, context)

                total_rows += rows
                files_processed += 1

                context.log.info(f"✅ [{idx}/{len(files)}] {file_id} complete! {rows:,} rows inserted")

                # Force garbage collection
                del ds, df
                gc.collect()

            except Exception as e:
                context.log.error(f"❌ Failed to process {file_id}: {e}")
                continue

        context.log.info(f"\n🎉 Extraction complete! {files_processed} files processed, {total_rows:,} rows inserted")

        return {
            "status": "success",
            "rows_inserted": total_rows,
            "files_processed": files_processed,
            "total_files": len(files)
        }

    finally:
        conn.close()


def _create_timeseries_table(conn):
    """Create target time series table if not exists."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS staging.era5_france_timeseries (
                id BIGSERIAL PRIMARY KEY,
                time TIMESTAMP NOT NULL,
                latitude NUMERIC(6,3) NOT NULL,
                longitude NUMERIC(6,3) NOT NULL,
                temperature_2m NUMERIC(6,2),
                total_precipitation NUMERIC(8,4),
                potential_evaporation NUMERIC(8,4),
                source_file_id TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT NOW()
            );

            CREATE INDEX IF NOT EXISTS idx_era5_time
                ON staging.era5_france_timeseries (time);

            CREATE INDEX IF NOT EXISTS idx_era5_location
                ON staging.era5_france_timeseries (latitude, longitude);

            CREATE INDEX IF NOT EXISTS idx_era5_time_location
                ON staging.era5_france_timeseries (time, latitude, longitude);

            CREATE INDEX IF NOT EXISTS idx_era5_source_file
                ON staging.era5_france_timeseries (source_file_id);

            GRANT SELECT ON staging.era5_france_timeseries TO readonly;
        """)
        conn.commit()


def _delete_existing_timeseries(conn, file_id: str, context):
    """Delete existing timeseries data for a file_id before re-extraction.
    
    Utilisé en mode partition pour éviter les doublons lors de la re-matérialisation.
    """
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM staging.era5_france_timeseries
            WHERE source_file_id = %s
        """, (file_id,))
        deleted = cur.rowcount
        conn.commit()
        
        if deleted > 0:
            context.log.info(f"🗑️  Supprimé {deleted:,} lignes existantes pour {file_id} (évite les doublons)")
        else:
            context.log.info(f"✅ Aucune donnée existante pour {file_id}")


def _get_files_to_process(conn, context, target_file_id=None):
    """Get list of files that haven't been processed yet.
    
    Args:
        conn: Database connection
        context: Dagster context
        target_file_id: If specified, only process this specific file (for partition runs)
    """
    with conn.cursor() as cur:
        if target_file_id:
            # Mode partition: extraire un chunk spécifique même s'il existe déjà
            cur.execute("""
                SELECT r.file_id, r.file_size_mb, r.start_year, r.end_year
                FROM staging.era5_france_meteo_raw r
                WHERE r.file_id = %s
                ORDER BY r.start_year
            """, (target_file_id,))
        else:
            # Mode normal: uniquement les fichiers non encore extraits
            cur.execute("""
                SELECT r.file_id, r.file_size_mb, r.start_year, r.end_year
                FROM staging.era5_france_meteo_raw r
                WHERE NOT EXISTS (
                    SELECT 1 FROM staging.era5_france_timeseries t
                    WHERE t.source_file_id = r.file_id
                )
                ORDER BY r.start_year
            """)
        return cur.fetchall()


def _extract_netcdf(conn, file_id: str, context) -> xr.Dataset:
    """Extract NetCDF data from PostgreSQL bytea column."""
    context.log.info(f"📥 Fetching NetCDF data for {file_id}...")

    with conn.cursor() as cur:
        cur.execute("""
            SELECT netcdf_data
            FROM staging.era5_france_meteo_raw
            WHERE file_id = %s
        """, (file_id,))

        row = cur.fetchone()
        if not row:
            raise ValueError(f"File {file_id} not found")

        netcdf_bytes = row[0]
        context.log.info(f"✅ Fetched {len(netcdf_bytes) / (1024*1024):.2f} MB")

    # Convert memoryview to bytes if needed
    if isinstance(netcdf_bytes, memoryview):
        netcdf_bytes = bytes(netcdf_bytes)

    # Check for ZIP signature and extract if needed
    if netcdf_bytes[:4] == b'PK\x03\x04':
        context.log.info(f"🔓 File is ZIP compressed, extracting...")
        with zipfile.ZipFile(io.BytesIO(netcdf_bytes)) as zf:
            nc_filename = zf.namelist()[0]
            context.log.info(f"📦 Extracting {nc_filename} from ZIP...")
            with zf.open(nc_filename) as nc_file:
                netcdf_bytes = nc_file.read()
                context.log.info(f"✅ Extracted {len(netcdf_bytes) / (1024*1024):.2f} MB")

    # Load NetCDF
    ds = xr.open_dataset(io.BytesIO(netcdf_bytes), engine='h5netcdf')
    time_dim = 'valid_time' if 'valid_time' in ds.dims else 'time'
    context.log.info(f"✅ Loaded NetCDF: {len(ds[time_dim])} timesteps, {len(ds.latitude)} lats, {len(ds.longitude)} lons")

    return ds


def _netcdf_to_dataframe(ds: xr.Dataset, file_id: str, context) -> pd.DataFrame:
    """Convert NetCDF dataset to pandas DataFrame."""
    context.log.info("🔄 Converting NetCDF to DataFrame...")

    df = ds.to_dataframe().reset_index()

    # Rename columns
    df = df.rename(columns={
        't2m': 'temperature_2m',
        'tp': 'total_precipitation',
        'pev': 'potential_evaporation',
        'valid_time': 'time'
    })

    # Convert units
    if 'temperature_2m' in df.columns:
        df['temperature_2m'] = df['temperature_2m'] - 273.15  # K → °C

    if 'total_precipitation' in df.columns:
        df['total_precipitation'] = df['total_precipitation'] * 1000  # m → mm

    if 'potential_evaporation' in df.columns:
        df['potential_evaporation'] = df['potential_evaporation'] * 1000  # m → mm

    # Add source file reference
    df['source_file_id'] = file_id

    # Select needed columns
    columns = ['time', 'latitude', 'longitude', 'temperature_2m', 'total_precipitation', 'potential_evaporation', 'source_file_id']
    df = df[columns]

    # Drop NaN rows
    df = df.dropna(subset=['temperature_2m', 'total_precipitation', 'potential_evaporation'], how='all')

    context.log.info(f"✅ Created DataFrame with {len(df):,} rows")
    return df


def _insert_dataframe(conn, df: pd.DataFrame, context, batch_size: int = 10000) -> int:
    """Insert DataFrame into PostgreSQL in batches."""
    context.log.info(f"💾 Inserting {len(df):,} rows in batches of {batch_size:,}...")

    total_rows = len(df)
    rows_inserted = 0

    with conn.cursor() as cur:
        for i in range(0, total_rows, batch_size):
            batch = df.iloc[i:i+batch_size]

            values = [
                (
                    row.time,
                    row.latitude,
                    row.longitude,
                    row.temperature_2m,
                    row.total_precipitation,
                    row.potential_evaporation,
                    row.source_file_id
                )
                for row in batch.itertuples(index=False)
            ]

            execute_values(
                cur,
                """
                INSERT INTO staging.era5_france_timeseries
                (time, latitude, longitude, temperature_2m, total_precipitation, potential_evaporation, source_file_id)
                VALUES %s
                """,
                values
            )

            # Commit after each batch to avoid transaction bloat
            conn.commit()

            rows_inserted += len(batch)
            if rows_inserted % 100000 == 0 or rows_inserted == total_rows:
                context.log.info(f"  💾 {rows_inserted:,}/{total_rows:,} rows inserted ({rows_inserted/total_rows*100:.1f}%)")

    context.log.info(f"✅ All {rows_inserted:,} rows committed to database")

    return rows_inserted
