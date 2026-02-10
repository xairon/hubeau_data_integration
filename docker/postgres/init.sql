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

-- Enable pg_stat_statements (query performance monitoring)
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- ============================================================================
-- Performance Tuning - Data Warehouse Workload
-- NOTE: ALTER SYSTEM écrit dans postgresql.auto.conf (priorité max)
-- Les paramètres "postmaster" nécessitent un restart: docker compose restart postgres
-- Les paramètres "user/sighup" s'appliquent au reload: SELECT pg_reload_conf();
-- ============================================================================

-- Shared preload libraries (context: postmaster → restart requis)
ALTER SYSTEM SET shared_preload_libraries = 'timescaledb,pg_stat_statements';
ALTER SYSTEM SET pg_stat_statements.track = 'all';

-- === MÉMOIRE ===
-- work_mem: mémoire par opération (tri, hash join, GROUP BY, CTE)
-- CRITIQUE: le défaut timescaledb-ha est ~328KB, causant des spills disque massifs
-- 128MB permet aux requêtes dbt complexes de travailler en RAM
-- Formule: safe tant que max_connections × work_mem × 3 < RAM container
-- 100 × 128MB × 3 = 37.5GB > 12GB, mais en pratique <10 requêtes actives simultanées
ALTER SYSTEM SET work_mem = '128MB';

-- === I/O SSD ===
-- random_page_cost: coût relatif lecture aléatoire vs séquentielle (défaut: 4.0 = HDD)
-- 1.1 pour SSD: le planner choisira plus souvent les index scans
ALTER SYSTEM SET random_page_cost = 1.1;
-- effective_io_concurrency: opérations I/O parallèles (défaut: 1 = HDD)
ALTER SYSTEM SET effective_io_concurrency = 200;
ALTER SYSTEM SET maintenance_io_concurrency = 200;

-- === PARALLÉLISME ===
-- Exploiter les multiples cores pour les requêtes analytiques dbt
ALTER SYSTEM SET max_worker_processes = 24;
ALTER SYSTEM SET max_parallel_workers = 16;
ALTER SYSTEM SET max_parallel_workers_per_gather = 4;
ALTER SYSTEM SET max_parallel_maintenance_workers = 4;

-- === WAL & CHECKPOINTS ===
-- Optimisé pour les écritures lourdes DLT (bulk ingestion)
-- max_wal_size élevé = moins de checkpoints pendant les chargements
ALTER SYSTEM SET max_wal_size = '8GB';
ALTER SYSTEM SET min_wal_size = '2GB';
ALTER SYSTEM SET wal_buffers = '64MB';
ALTER SYSTEM SET wal_compression = 'on';
ALTER SYSTEM SET checkpoint_timeout = '15min';

-- === QUERY PLANNER ===
-- Plus d'échantillons ANALYZE = meilleurs plans sur tables volumineuses
ALTER SYSTEM SET default_statistics_target = 500;
-- JIT: overhead inutile sur les requêtes dbt de durée moyenne
ALTER SYSTEM SET jit = 'off';

-- Create schemas if not exist
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS silver_rejects;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS ops;

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
