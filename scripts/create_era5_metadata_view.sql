-- ============================================================================
-- ERA5 Metadata View - For Adminer Consultation
-- ============================================================================
-- This view excludes the netcdf_data (bytea) column to allow browsing
-- ERA5 data in Adminer without memory issues.
--
-- Usage:
--   psql -U postgres -d postgres -f scripts/create_era5_metadata_view.sql
--
-- Or via Docker:
--   docker exec -i brgm-postgres psql -U postgres -d postgres < scripts/create_era5_metadata_view.sql
-- ============================================================================

-- Drop view if exists
DROP VIEW IF EXISTS staging.era5_france_meteo_raw_metadata CASCADE;

-- Create metadata view (without bytea column)
CREATE VIEW staging.era5_france_meteo_raw_metadata AS
SELECT
    file_id,
    variables,
    start_year,
    end_year,
    area,
    file_size_mb,
    download_timestamp,
    file_metadata,
    _dlt_load_id,
    _dlt_id
FROM staging.era5_france_meteo_raw;

-- Grant read access to readonly user
GRANT SELECT ON staging.era5_france_meteo_raw_metadata TO readonly;

-- Summary
DO $$
DECLARE
    total_files INT;
    total_size_mb NUMERIC;
    oldest_year INT;
    newest_year INT;
BEGIN
    SELECT
        COUNT(*),
        SUM(file_size_mb),
        MIN(start_year),
        MAX(end_year)
    INTO total_files, total_size_mb, oldest_year, newest_year
    FROM staging.era5_france_meteo_raw_metadata;

    RAISE NOTICE '================================================';
    RAISE NOTICE '✅ ERA5 Metadata View Created';
    RAISE NOTICE '================================================';
    RAISE NOTICE 'View name: staging.era5_france_meteo_raw_metadata';
    RAISE NOTICE 'Total files: %', total_files;
    RAISE NOTICE 'Total size: % MB', ROUND(total_size_mb, 2);
    RAISE NOTICE 'Period: % - %', oldest_year, newest_year;
    RAISE NOTICE '================================================';
    RAISE NOTICE 'Usage in Adminer:';
    RAISE NOTICE '  ❌ DO NOT open: era5_france_meteo_raw (contains bytea)';
    RAISE NOTICE '  ✅ USE this view: era5_france_meteo_raw_metadata';
    RAISE NOTICE '================================================';
END $$;
