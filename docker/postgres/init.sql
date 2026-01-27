-- ============================================================================
-- PostgreSQL Init Script - Enable Extensions
-- Runs automatically on first container startup
-- ============================================================================

-- Enable TimescaleDB (time-series optimization)
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- Enable PostGIS (geospatial support)
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;

-- Create schemas if not exist
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Create Superset database (idempotent way)
SELECT 'CREATE DATABASE superset'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'superset')\gexec

-- Log enabled extensions
DO $$
BEGIN
    RAISE NOTICE 'Extensions enabled: timescaledb, postgis';
    RAISE NOTICE 'Schemas created: bronze, silver, gold';
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
