-- ============================================================================
-- PostgreSQL Init Script - Enable Extensions
-- Runs automatically on first container startup
-- ============================================================================

-- Enable TimescaleDB (time-series optimization)
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- TimescaleDB: autoriser la décompression illimitée pour les DML sur hypertables compressées
-- (évite "tuple decompression limit exceeded" sur stg_piezo_chroniques et autres modèles incrémentaux)
ALTER DATABASE postgres SET timescaledb.max_tuples_decompressed_per_dml_transaction = 0;

-- Enable PostGIS (geospatial support)
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;

-- pg_stat_statements: déjà préchargé par timescaledb-ha via shared_preload_libraries
-- NE PAS faire ALTER SYSTEM SET shared_preload_libraries ici
-- (l'image timescaledb-ha gère ce paramètre via son entrypoint)
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- ============================================================================
-- Performance Tuning - Data Warehouse Workload
-- NOTE: ALTER SYSTEM écrit dans postgresql.auto.conf (priorité max)
-- Les paramètres "postmaster" nécessitent un restart: docker compose restart postgres
-- Les paramètres "user/sighup" s'appliquent au reload: SELECT pg_reload_conf();
-- ============================================================================

-- === MÉMOIRE === (host: 502GB RAM partagé — baseline faible, burst max)
-- shared_buffers: seule RAM permanente → modéré (4GB)
-- Le reste du cache = OS page cache Linux, libéré automatiquement sous pression mémoire
ALTER SYSTEM SET shared_buffers = '4GB';
-- effective_cache_size: hint planner (zéro allocation réelle) → agressif
ALTER SYSTEM SET effective_cache_size = '128GB';
-- work_mem: alloué/libéré par opération de tri/join → agressif
ALTER SYSTEM SET work_mem = '512MB';
-- maintenance_work_mem: alloué pendant VACUUM/INDEX, libéré après
ALTER SYSTEM SET maintenance_work_mem = '2GB';
-- hash_mem_multiplier: hash joins 2× work_mem (1GB), libéré après
ALTER SYSTEM SET hash_mem_multiplier = 2.0;
-- huge_pages off: mémoire pinnée non récupérable → mauvais pour host partagé
ALTER SYSTEM SET huge_pages = 'off';
-- temp_buffers: sessions temporaires, libéré à la déconnexion
ALTER SYSTEM SET temp_buffers = '256MB';

-- === I/O SSD ===
ALTER SYSTEM SET random_page_cost = 1.1;
ALTER SYSTEM SET effective_io_concurrency = 200;
ALTER SYSTEM SET maintenance_io_concurrency = 200;

-- === PARALLÉLISME === (host: 96 CPUs — workers éphémères, spawned par requête)
ALTER SYSTEM SET max_worker_processes = 64;
ALTER SYSTEM SET max_parallel_workers = 48;
ALTER SYSTEM SET max_parallel_workers_per_gather = 8;
ALTER SYSTEM SET max_parallel_maintenance_workers = 8;

-- === WAL & CHECKPOINTS ===
-- Optimisé pour les écritures lourdes DLT (bulk ingestion)
ALTER SYSTEM SET max_wal_size = '16GB';
ALTER SYSTEM SET min_wal_size = '4GB';
ALTER SYSTEM SET wal_buffers = '64MB';
ALTER SYSTEM SET wal_compression = 'on';
ALTER SYSTEM SET checkpoint_timeout = '15min';

-- === QUERY PLANNER ===
ALTER SYSTEM SET default_statistics_target = 500;
ALTER SYSTEM SET jit = 'off';

-- pgvector extension (for ML embeddings — HNSW similarity search)
CREATE EXTENSION IF NOT EXISTS vector;

-- Create schemas if not exist
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS silver_rejects;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS ops;
CREATE SCHEMA IF NOT EXISTS ml;

-- Create Superset database (idempotent way)
SELECT 'CREATE DATABASE superset'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'superset')\gexec

-- Log enabled extensions
DO $$
BEGIN
    RAISE NOTICE 'Extensions enabled: timescaledb, postgis';
    RAISE NOTICE 'Schemas created: bronze, silver, silver_rejects, gold, ops';
END $$;

-- ============================================================================
-- TimescaleDB Hypertable Conversion for Hub'Eau (runs after tables exist)
-- These are converted by dlt on first load, this is a safety net
-- ============================================================================

-- ============================================================================
-- ML Schema: UMAP 2D/3D columns for latent space visualization
-- Added to both piezo and hydro station_embeddings tables
-- ============================================================================

-- These columns are added via ALTER TABLE for live databases.
-- The CREATE TABLE templates in persistence.py already include these columns.
-- The DO blocks below are idempotent (safe to re-run).

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'ml' AND table_name = 'piezo_station_embeddings' AND column_name = 'umap_2d_x'
    ) THEN
        ALTER TABLE ml.piezo_station_embeddings ADD COLUMN umap_2d_x FLOAT;
        ALTER TABLE ml.piezo_station_embeddings ADD COLUMN umap_2d_y FLOAT;
        ALTER TABLE ml.piezo_station_embeddings ADD COLUMN umap_3d_x FLOAT;
        ALTER TABLE ml.piezo_station_embeddings ADD COLUMN umap_3d_y FLOAT;
        ALTER TABLE ml.piezo_station_embeddings ADD COLUMN umap_3d_z FLOAT;
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'ml' AND table_name = 'hydro_station_embeddings' AND column_name = 'umap_2d_x'
    ) THEN
        ALTER TABLE ml.hydro_station_embeddings ADD COLUMN umap_2d_x FLOAT;
        ALTER TABLE ml.hydro_station_embeddings ADD COLUMN umap_2d_y FLOAT;
        ALTER TABLE ml.hydro_station_embeddings ADD COLUMN umap_3d_x FLOAT;
        ALTER TABLE ml.hydro_station_embeddings ADD COLUMN umap_3d_y FLOAT;
        ALTER TABLE ml.hydro_station_embeddings ADD COLUMN umap_3d_z FLOAT;
    END IF;
END $$;

-- Function to convert existing tables to hypertables (safe, idempotent)
CREATE OR REPLACE FUNCTION convert_to_hypertable_if_exists(
    table_name TEXT,
    time_column TEXT,
    chunk_interval INTERVAL DEFAULT INTERVAL '1 year'
) RETURNS VOID AS $$
BEGIN
    -- Check if table exists and is not already a hypertable
    IF EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = 'bronze' AND table_name = $1
    ) AND NOT EXISTS (
        SELECT 1 FROM timescaledb_information.hypertables 
        WHERE hypertable_schema = 'bronze' AND hypertable_name = $1
    ) THEN
        EXECUTE format(
            'SELECT create_hypertable(''bronze.%I'', %L, chunk_time_interval => %L, migrate_data => TRUE)',
            $1, $2, chunk_interval
        );
        RAISE NOTICE 'Converted bronze.% to hypertable', $1;
    END IF;
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Could not convert %: %', $1, SQLERRM;
END;
$$ LANGUAGE plpgsql;
