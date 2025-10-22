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

-- Migration 2: Supprimer la FK sur hydrometry_stations.code_site
-- Raison: L'API Hub'Eau retourne des références incohérentes
DO $$
BEGIN
    -- Vérifier si la contrainte FK existe
    IF EXISTS (
        SELECT 1
        FROM information_schema.table_constraints
        WHERE constraint_schema = 'hubeau'
        AND table_name = 'hydrometry_stations'
        AND constraint_name = 'hydrometry_stations_code_site_fkey'
        AND constraint_type = 'FOREIGN KEY'
    ) THEN
        ALTER TABLE hubeau.hydrometry_stations
        DROP CONSTRAINT hydrometry_stations_code_site_fkey;
        RAISE NOTICE 'Supprimé la contrainte FK hydrometry_stations_code_site_fkey';
    ELSE
        RAISE NOTICE 'Contrainte FK hydrometry_stations_code_site_fkey déjà absente';
    END IF;
END $$;

-- Migration 3: Créer un index sur code_site pour les performances JOIN
-- Comme on a supprimé la FK, l'index n'est plus créé automatiquement
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_indexes
        WHERE schemaname = 'hubeau'
        AND tablename = 'hydrometry_stations'
        AND indexname = 'idx_hydrometry_stations_code_site'
    ) THEN
        CREATE INDEX idx_hydrometry_stations_code_site
        ON hubeau.hydrometry_stations(code_site);
        RAISE NOTICE 'Créé index idx_hydrometry_stations_code_site';
    ELSE
        RAISE NOTICE 'Index idx_hydrometry_stations_code_site existe déjà';
    END IF;
END $$;

-- Migration 4: Handle any other issues that might arise
-- Add more migrations here as needed