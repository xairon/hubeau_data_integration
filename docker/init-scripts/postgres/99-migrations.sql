-- ============================================
-- MIGRATIONS FOR EXISTING DATABASES
-- ============================================
-- This file contains ALTER statements to update existing databases
-- It's safe to run multiple times (idempotent)

-- Migration 1: Increase urn_bss column size for piezometry_stations
DO $$
BEGIN
    -- Check if column exists and needs resizing
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'hubeau'
        AND table_name = 'piezometry_stations'
        AND column_name = 'urn_bss'
        AND character_maximum_length < 100
    ) THEN
        ALTER TABLE hubeau.piezometry_stations
        ALTER COLUMN urn_bss TYPE VARCHAR(100);
        RAISE NOTICE 'Updated urn_bss column size to VARCHAR(100) in piezometry_stations';
    ELSE
        RAISE NOTICE 'urn_bss column already has sufficient size or does not exist';
    END IF;
END $$;

-- Migration 2: Handle any other column size issues that might arise
-- Add more migrations here as needed