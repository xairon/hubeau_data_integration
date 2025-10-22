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

-- Migration 4: Renommer colonnes hydrobio_stations pour matcher l'API
-- Raison: Aligner les noms de colonnes avec l'API Hub'Eau au lieu d'utiliser des mappings
DO $$
BEGIN
    -- Renommer code_station → code_station_hydrobio
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'hubeau' AND table_name = 'hydrobio_stations'
        AND column_name = 'code_station'
    ) THEN
        -- Drop FK constraints first
        IF EXISTS (
            SELECT 1 FROM information_schema.table_constraints
            WHERE constraint_schema = 'hubeau' AND table_name = 'hydrobio_indices'
            AND constraint_name LIKE '%code_station%' AND constraint_type = 'FOREIGN KEY'
        ) THEN
            ALTER TABLE hubeau.hydrobio_indices DROP CONSTRAINT IF EXISTS hydrobio_indices_code_station_fkey;
        END IF;
        IF EXISTS (
            SELECT 1 FROM information_schema.table_constraints
            WHERE constraint_schema = 'hubeau' AND table_name = 'hydrobio_taxons'
            AND constraint_name LIKE '%code_station%' AND constraint_type = 'FOREIGN KEY'
        ) THEN
            ALTER TABLE hubeau.hydrobio_taxons DROP CONSTRAINT IF EXISTS hydrobio_taxons_code_station_fkey;
        END IF;

        -- Rename columns
        ALTER TABLE hubeau.hydrobio_stations RENAME COLUMN code_station TO code_station_hydrobio;
        ALTER TABLE hubeau.hydrobio_stations RENAME COLUMN libelle_station TO libelle_station_hydrobio;

        -- Add uri_station_hydrobio if it doesn't exist
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'hubeau' AND table_name = 'hydrobio_stations'
            AND column_name = 'uri_station_hydrobio'
        ) THEN
            ALTER TABLE hubeau.hydrobio_stations ADD COLUMN uri_station_hydrobio TEXT;
        END IF;

        RAISE NOTICE 'Renommé colonnes dans hydrobio_stations';
    END IF;
END $$;

-- Migration 5: Renommer colonnes hydrobio_indices pour matcher l'API
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'hubeau' AND table_name = 'hydrobio_indices'
        AND column_name = 'code_station'
    ) THEN
        ALTER TABLE hubeau.hydrobio_indices RENAME COLUMN code_station TO code_station_hydrobio;

        -- Add code_support if it doesn't exist
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'hubeau' AND table_name = 'hydrobio_indices'
            AND column_name = 'code_support'
        ) THEN
            ALTER TABLE hubeau.hydrobio_indices ADD COLUMN code_support VARCHAR(20);
        END IF;

        -- Re-create FK
        ALTER TABLE hubeau.hydrobio_indices
        ADD CONSTRAINT hydrobio_indices_code_station_hydrobio_fkey
        FOREIGN KEY (code_station_hydrobio) REFERENCES hubeau.hydrobio_stations(code_station_hydrobio);

        RAISE NOTICE 'Renommé colonnes dans hydrobio_indices';
    END IF;
END $$;

-- Migration 6: Renommer colonnes hydrobio_taxons pour matcher l'API
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'hubeau' AND table_name = 'hydrobio_taxons'
        AND column_name = 'code_station'
    ) THEN
        ALTER TABLE hubeau.hydrobio_taxons RENAME COLUMN code_station TO code_station_hydrobio;

        -- Add code_appel_taxon and code_support if they don't exist
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'hubeau' AND table_name = 'hydrobio_taxons'
            AND column_name = 'code_appel_taxon'
        ) THEN
            ALTER TABLE hubeau.hydrobio_taxons ADD COLUMN code_appel_taxon VARCHAR(20);
        END IF;

        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'hubeau' AND table_name = 'hydrobio_taxons'
            AND column_name = 'code_support'
        ) THEN
            ALTER TABLE hubeau.hydrobio_taxons ADD COLUMN code_support VARCHAR(20);
        END IF;

        -- Re-create FK
        ALTER TABLE hubeau.hydrobio_taxons
        ADD CONSTRAINT hydrobio_taxons_code_station_hydrobio_fkey
        FOREIGN KEY (code_station_hydrobio) REFERENCES hubeau.hydrobio_stations(code_station_hydrobio);

        RAISE NOTICE 'Renommé colonnes dans hydrobio_taxons';
    END IF;
END $$;

-- Migration 7: Renommer colonne ecoulement_observations.date_obs → date_observation
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'hubeau' AND table_name = 'ecoulement_observations'
        AND column_name = 'date_obs'
    ) THEN
        ALTER TABLE hubeau.ecoulement_observations RENAME COLUMN date_obs TO date_observation;
        RAISE NOTICE 'Renommé date_obs → date_observation dans ecoulement_observations';
    END IF;
END $$;

-- Migration 8: Renommer colonnes prelevements_points pour matcher l'API
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'hubeau' AND table_name = 'prelevements_points'
        AND column_name = 'code_point'
    ) THEN
        ALTER TABLE hubeau.prelevements_points RENAME COLUMN code_point TO code_point_prelevement;
        ALTER TABLE hubeau.prelevements_points RENAME COLUMN libelle_point TO nom_point_prelevement;
        RAISE NOTICE 'Renommé colonnes dans prelevements_points';
    END IF;
END $$;

-- Migration 9: Handle any other issues that might arise
-- Add more migrations here as needed