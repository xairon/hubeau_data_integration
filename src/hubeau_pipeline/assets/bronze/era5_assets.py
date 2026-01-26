"""
ERA5 Bronze Layer Assets (Direct-to-Timeseries)

Pipeline unifié pour l'ingestion des données ERA5 :
1. Téléchargement NetCDF (temporaire)
2. Insertion DIRECTE dans `bronze.era5_france_timeseries`
3. Suppression immédiate du fichier temporaire

Plus de stockage intermédiaire "RAW" en base de données pour économiser de l'espace.
"""

import yaml
import gc
import io
import os
import logging
import zipfile
import tempfile
import cdsapi
import psycopg2
import xarray as xr
import pandas as pd
from typing import Dict, Any, List, Tuple
from datetime import datetime, timedelta
from psycopg2.extras import execute_values
from dagster import asset, AssetExecutionContext, StaticPartitionsDefinition, Output, MetadataValue

# ============================================================================
# PARTITIONS - ERA5 (chunks de 2 ans)
# ============================================================================

CURRENT_YEAR = datetime.now().year
ERA5_START_YEAR = 1950  # ERA5-Land commence en 1950
ERA5_YEARS_PER_CHUNK = 2

# Créer les partitions par chunks de 2 ans
# Ex: ["1950_1951", "1952_1953", ..., "2024_2025"]
ERA5_PARTITIONS = []
year = ERA5_START_YEAR
while year <= CURRENT_YEAR:
    chunk_end = min(year + ERA5_YEARS_PER_CHUNK - 1, CURRENT_YEAR)
    partition_key = f"{year}_{chunk_end}"
    ERA5_PARTITIONS.append(partition_key)
    year += ERA5_YEARS_PER_CHUNK

ERA5_PARTITIONS_DEF = StaticPartitionsDefinition(ERA5_PARTITIONS)


# ============================================================================
# SHARED HELPERS
# ============================================================================

def _create_timeseries_table(conn):
    """Create target time series table if not exists."""
    with conn.cursor() as cur:
        # 1. Ensure schema exists
        cur.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
        
        # 2. Create table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS bronze.era5_france_timeseries (
                id BIGSERIAL,
                time TIMESTAMP NOT NULL,
                latitude NUMERIC(6,3) NOT NULL,
                longitude NUMERIC(6,3) NOT NULL,
                temperature_2m NUMERIC(6,2),
                total_precipitation NUMERIC(8,4),
                potential_evaporation NUMERIC(8,4),
                source_file_id TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (time, id)
            );
        """)
        
        # 3. Create indexes (one by one for safety)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_era5_time ON bronze.era5_france_timeseries (time);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_era5_location ON bronze.era5_france_timeseries (latitude, longitude);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_era5_source_file ON bronze.era5_france_timeseries (source_file_id);")
        
    # Commit table creation IMMEDIATELY to save it even if TimescaleDB setup fails
    conn.commit()
    
    # Separate transaction for optional TimescaleDB setup
    with conn.cursor() as cur:
        
        # TimescaleDB Optimization (if extension enabled)
        try:
            cur.execute("SELECT 1 FROM pg_extension WHERE extname = 'timescaledb'")
            if cur.fetchone():
                context = logging.getLogger("dagster") # Fallback logger
                # Note: chunk_time_interval set to 1 year for ERA5 (adjust as needed)
                cur.execute("""
                    SELECT create_hypertable(
                        'bronze.era5_france_timeseries', 
                        'time', 
                        chunk_time_interval => INTERVAL '1 year', 
                        if_not_exists => TRUE,
                        migrate_data => TRUE
                    );
                """)
                # Enable compression (policy to be added separately or defaults)
                cur.execute("""
                    ALTER TABLE bronze.era5_france_timeseries SET (
                        timescaledb.compress,
                        timescaledb.compress_segmentby = 'source_file_id'
                    );
                """)
                # Add automatic compression policy (compress chunks older than 30 days)
                cur.execute("""
                    SELECT add_compression_policy(
                        'bronze.era5_france_timeseries', 
                        INTERVAL '30 days',
                        if_not_exists => TRUE
                    );
                """)
        except Exception as e:
            # Non-blocking: standard Postgres fallback
            pass

        conn.commit()


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
                INSERT INTO bronze.era5_france_timeseries
                (time, latitude, longitude, temperature_2m, total_precipitation, potential_evaporation, source_file_id)
                VALUES %s
                """,
                values
            )
            conn.commit()
            rows_inserted += len(batch)
            if rows_inserted % 100000 == 0 or rows_inserted == total_rows:
                context.log.info(f"  💾 {rows_inserted:,}/{total_rows:,} rows inserted ({rows_inserted/total_rows*100:.1f}%)")

    context.log.info(f"✅ All {rows_inserted:,} rows committed to database")
    return rows_inserted


def process_era5_range_to_timeseries(
    context: AssetExecutionContext,
    start_date: datetime,
    end_date: datetime,
    file_id: str
) -> int:
    """
    Télécharge, traite et insère les données ERA5 directement dans la table timeseries.
    Retourne le nombre de lignes insérées.
    """
    tmp_path = None
    actual_nc_path = None
    conn = None
    
    try:
        # 1. Connexion DB Check
        conn = psycopg2.connect(
            host=os.getenv('PG_HOST', 'postgres'),
            port=os.getenv('PG_PORT', '5432'),
            database=os.getenv('PG_DB', 'postgres'),
            user=os.getenv('PG_USER', 'postgres'),
            password=os.getenv('PG_PASSWORD'),
            sslmode=os.getenv('PG_SSLMODE', 'prefer')
        )
        
        # Ensure table exists
        _create_timeseries_table(conn)
        
        # Check idempotency - use DATE RANGE not file_id (more robust)
        with conn.cursor() as cur:
            # First check if table exists (in case _create_timeseries_table failed silently)
            cur.execute("""
                SELECT EXISTS (
                   SELECT 1 FROM information_schema.tables 
                   WHERE table_schema = 'bronze' AND table_name = 'era5_france_timeseries'
                )
            """)
            table_exists = cur.fetchone()[0]
            
            if table_exists:
                # Check if we have data covering this date range
                cur.execute("""
                    SELECT COUNT(*) FROM bronze.era5_france_timeseries 
                    WHERE time >= %s AND time <= %s
                """, (start_date, end_date))
                existing_count = cur.fetchone()[0]
                
                if existing_count > 0:
                    context.log.info(f"✅ Données pour période {start_date.date()} → {end_date.date()} déjà présentes ({existing_count:,} rows), skipping.")
                    return 0

        # 2. Download from CDS
        context.log.info(f"🌐 Downloading ERA5 data for {file_id}...")
        
        # Load config
        config_path = "configs/era5/era5_france_meteo.yml"
        with open(config_path) as f:
            config = yaml.safe_load(f)

        cds_api_key = config['credentials'].get('cds_api_key') or os.getenv('COPERNICUS_API_KEY')
        
        if not cds_api_key:
            raise ValueError(
                "❌ CDS API Key Missing! Please set the 'COPERNICUS_API_KEY' environment variable "
                "or configure 'cds_api_key' in configs/era5/era5_france_meteo.yml."
            )

        client = cdsapi.Client(url=config['credentials']['cds_api_url'], key=cds_api_key, verify=False)
        
        # Build request - FIXED: proper enumeration of years and months
        # Generate all years in range
        years = list(range(start_date.year, end_date.year + 1))
        
        # Generate all months in range (for multi-year spans, we need all 12 months)
        if start_date.year == end_date.year:
            months = list(range(start_date.month, end_date.month + 1))
        else:
            # Multi-year: request all months (CDS will filter by actual data)
            months = list(range(1, 13))
        
        days_list = list(range(1, 32))
        
        request_params = {
            'product_type': 'reanalysis',
            'data_format': 'netcdf',
            'variable': config['resource']['variables'],
            'year': [str(y) for y in years],
            'month': [f'{m:02d}' for m in months],
            'day': [f'{d:02d}' for d in days_list],
            'time': config['extraction']['temporal_config']['time'],
            'area': config['resource']['area'],
        }
        
        with tempfile.NamedTemporaryFile(suffix='.nc', delete=False) as tmp:
            tmp_path = tmp.name
        
        # Retry logic for CDS API (transient failures)
        max_retries = 3
        retry_delay = 30  # seconds
        for attempt in range(max_retries):
            try:
                client.retrieve(config['resource']['dataset'], request_params, tmp_path)
                context.log.info(f"✅ Download complete: {tmp_path}")
                break
            except Exception as e:
                if attempt < max_retries - 1:
                    context.log.warning(f"⚠️ CDS API failed (attempt {attempt + 1}/{max_retries}): {e}. Retrying in {retry_delay}s...")
                    import time
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    context.log.error(f"❌ CDS API failed after {max_retries} attempts: {e}")
                    raise

        # 3. Process NetCDF (Handle ZIP or Direct NetCDF)
        context.log.info("📊 Processing file signature...")
        
        # Check signature
        with open(tmp_path, 'rb') as f:
            header = f.read(4)
        
        actual_nc_path = tmp_path
        
        # If ZIP file (PK..)
        if header == b'PK\x03\x04':
            context.log.info("📦 Format détecté: ZIP. Extraction en cours...")
            with zipfile.ZipFile(tmp_path, 'r') as zf:
                # Find first .nc file
                nc_files = [n for n in zf.namelist() if n.endswith('.nc')]
                if not nc_files:
                    raise ValueError("❌ Aucun fichier .nc trouvé dans le ZIP CDS.")
                
                target_file = nc_files[0]
                context.log.info(f"  📂 Extraction de {target_file}...")
                
                # Extract to same dir
                zf.extract(target_file, os.path.dirname(tmp_path))
                actual_nc_path = os.path.join(os.path.dirname(tmp_path), target_file)
        else:
            context.log.info("📄 Format détecté: NetCDF direct.")

        context.log.info(f"📊 Opening NetCDF: {actual_nc_path}")
        ds = xr.open_dataset(actual_nc_path, engine='h5netcdf')
        
        # Filter exact dates
        time_dim = 'valid_time' if 'valid_time' in ds.dims else 'time'
        ds = ds.sel({time_dim: slice(start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))})
        
        # Rename & Convert
        df = ds.to_dataframe().reset_index()
        df = df.rename(columns={
            't2m': 'temperature_2m',
            'tp': 'total_precipitation',
            'pev': 'potential_evaporation',
            'valid_time': 'time'
        })
        
        if 'temperature_2m' in df.columns:
            df['temperature_2m'] = df['temperature_2m'] - 273.15
        if 'total_precipitation' in df.columns:
            df['total_precipitation'] = df['total_precipitation'] * 1000
        if 'potential_evaporation' in df.columns:
            df['potential_evaporation'] = df['potential_evaporation'] * 1000
            
        df['source_file_id'] = file_id
        
        columns = ['time', 'latitude', 'longitude', 'temperature_2m', 'total_precipitation', 'potential_evaporation', 'source_file_id']
        df = df[columns].dropna(subset=['temperature_2m', 'total_precipitation', 'potential_evaporation'], how='all')
        
        context.log.info(f"📊 DataFrame ready: {len(df):,} rows")

        # 4. Insert
        rows_inserted = _insert_dataframe(conn, df, context)
        return rows_inserted

    finally:
        if conn:
            conn.close()
        if tmp_path and os.path.exists(tmp_path):
            os.remove(tmp_path)
        # Clean up extracted .nc file if different from tmp_path
        if actual_nc_path and actual_nc_path != tmp_path and os.path.exists(actual_nc_path):
            os.remove(actual_nc_path)
        gc.collect()


# ============================================================================
# ASSETS
# ============================================================================

@asset(
    compute_kind="era5",
    group_name="era5_historical",
    io_manager_key="noop_io_manager",
    partitions_def=ERA5_PARTITIONS_DEF
)
def era5_france_timeseries_historical(context: AssetExecutionContext):
    """
    Historique ERA5 (1950-Present) - Direct to Timeseries.
    
    Partitionné par chunks de 2 ans.
    Télécharge et insère directement dans `era5_france_timeseries`.
    Pas de stockage intermédiaire "RAW".
    """
    partition_key = context.partition_key
    start_year, end_year = map(int, partition_key.split('_'))
    
    start_date = datetime(start_year, 1, 1)
    end_date = datetime(end_year, 12, 31)
    file_id = f"era5_hist_{start_year}_{end_year}"
    
    context.log.info(f"📅 Traitement historique: {start_year}-{end_year}")
    
    rows = process_era5_range_to_timeseries(context, start_date, end_date, file_id)
    
    return Output(
        {"rows_inserted": rows, "partition": partition_key},
        metadata={"rows": MetadataValue.int(rows)}
    )


@asset(
    compute_kind="era5",
    group_name="era5_weekly",
    io_manager_key="noop_io_manager",
    deps=["era5_france_timeseries_historical"]  # Dépendance explicite pour dagster
)
def era5_weekly_update(context: AssetExecutionContext):
    """
    Mise à jour Hebdomadaire ERA5 - Smart Update.
    
    Logique:
    1. Vérifie la dernière donnée présente en base (MAX(time)).
    2. Si données récentes présentes, ne télécharge que le delta (+ tampon sécurité).
    3. Si vide, télécharge les 14 derniers jours par défaut.
    
    Cela économise les appels API et le réseau en ne demandant que ce qu'on n'a pas encore.
    """
    days_back_default = 14
    
    # 1. Déterminer date de fin (Today - 5 jours délai ERA5)
    end_date = datetime.now() - timedelta(days=5)
    
    # 2. Chercher MAX(time) en base
    last_date_in_db = None
    conn = psycopg2.connect(
        host=os.getenv('PG_HOST', 'postgres'),
        port=os.getenv('PG_PORT', '5432'),
        database=os.getenv('PG_DB', 'postgres'),
        user=os.getenv('PG_USER', 'postgres'),
        password=os.getenv('PG_PASSWORD'),
        sslmode=os.getenv('PG_SSLMODE', 'prefer')
    )
    try:
        with conn.cursor() as cur:
            # Vérifier si table existe
            cur.execute("""
                SELECT EXISTS (
                   SELECT 1 FROM information_schema.tables 
                   WHERE table_schema = 'bronze' AND table_name = 'era5_france_timeseries'
                )
            """)
            if cur.fetchone()[0]:
                cur.execute("SELECT MAX(time) FROM bronze.era5_france_timeseries")
                res = cur.fetchone()
                if res and res[0]:
                    last_date_in_db = res[0]
                    context.log.info(f"📅 Dernière donnée en base: {last_date_in_db}")
    finally:
        conn.close()

    # 3. Calculer start_date optimisée avec SAFETY CAP
    max_lookback_days = 60  # Sécurité pour éviter timeout CDS si gros trou
    safety_start_date = end_date - timedelta(days=max_lookback_days)

    if last_date_in_db:
        # On tente de reprendre là où on s'est arrêté
        proposed_start_date = last_date_in_db - timedelta(days=2)
        
        if proposed_start_date < safety_start_date:
            context.log.warning(
                f"⚠️ GAP DÉTÉCTÉ: La base date de {last_date_in_db}, ce qui est trop vieux pour le job weekly (> {max_lookback_days} jours). "
                f"Le job ne chargera que les {max_lookback_days} derniers jours. "
                "Veuillez lancer un BACKFILL MANUEL (job historique) pour combler le trou."
            )
            start_date = safety_start_date
        else:
            start_date = proposed_start_date
            context.log.info(f"⚡ Smart Update activé: Reprise à {start_date.date()}")
    else:
        # Fallback table vide : on charge juste le récent pour initier
        start_date = safety_start_date
        context.log.info(f"⚠️ Table vide: Initialisation avec les {max_lookback_days} derniers jours.")

    # Si la période est incohérente (déjà à jour), on arrête
    if start_date >= end_date:
        context.log.info("✅ Données déjà à jour (Start >= End). Rien à faire.")
        return Output(
            {"rows_inserted": 0, "status": "up_to_date"},
            metadata={"status": MetadataValue.text("up_to_date")}
        )

    file_id = f"era5_weekly_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"
    
    context.log.info(f"📅 Traitement weekly: {start_date.date()} -> {end_date.date()}")
    
    rows = process_era5_range_to_timeseries(context, start_date, end_date, file_id)
    
    return Output(
        {"rows_inserted": rows, "mode": "weekly_smart"},
        metadata={"rows": MetadataValue.int(rows)}
    )


