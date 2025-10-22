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

-- ============================================
-- MIGRATIONS 10+: Alignement complet des noms de colonnes avec API Hub'Eau
-- Date: 2025-10-22
-- Raison: Simplifier le code en utilisant directement les noms de l'API
-- ============================================

-- Migration 10: hydrometry_observations - Renommer colonnes pour matcher API
DO $$
BEGIN
    -- Renommer date_obs → date_obs_elab
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'hubeau' AND table_name = 'hydrometry_observations'
        AND column_name = 'date_obs'
    ) THEN
        ALTER TABLE hubeau.hydrometry_observations RENAME COLUMN date_obs TO date_obs_elab;
        ALTER TABLE hubeau.hydrometry_observations RENAME COLUMN grandeur_hydro TO grandeur_hydro_elab;
        ALTER TABLE hubeau.hydrometry_observations RENAME COLUMN resultat TO resultat_obs_elab;

        -- Recréer index
        DROP INDEX IF EXISTS hubeau.idx_hydrometry_observations_date;
        CREATE INDEX idx_hydrometry_observations_date ON hubeau.hydrometry_observations(date_obs_elab);

        RAISE NOTICE 'Renommé colonnes dans hydrometry_observations';
    END IF;
END $$;

-- Migration 11: piezometry_chroniques - Ajouter timestamp_mesure et migrer données
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'hubeau' AND table_name = 'piezometry_chroniques'
        AND column_name = 'timestamp_mesure'
    ) THEN
        -- Renommer date_mesure → date_mesure_old temporairement
        ALTER TABLE hubeau.piezometry_chroniques RENAME COLUMN date_mesure TO date_mesure_old;

        -- Ajouter les nouvelles colonnes
        ALTER TABLE hubeau.piezometry_chroniques ADD COLUMN date_mesure DATE;
        ALTER TABLE hubeau.piezometry_chroniques ADD COLUMN timestamp_mesure TIMESTAMP;

        -- Migrer les données
        UPDATE hubeau.piezometry_chroniques
        SET timestamp_mesure = date_mesure_old,
            date_mesure = date_mesure_old::DATE;

        -- Rendre timestamp_mesure NOT NULL
        ALTER TABLE hubeau.piezometry_chroniques ALTER COLUMN timestamp_mesure SET NOT NULL;

        -- Supprimer l'ancienne colonne
        ALTER TABLE hubeau.piezometry_chroniques DROP COLUMN date_mesure_old;

        -- Recréer index
        DROP INDEX IF EXISTS hubeau.idx_piezometry_chroniques_date;
        CREATE INDEX idx_piezometry_chroniques_date ON hubeau.piezometry_chroniques(timestamp_mesure);

        RAISE NOTICE 'Ajouté timestamp_mesure dans piezometry_chroniques';
    END IF;
END $$;

-- Migration 12: temperature_chroniques - Renommer colonnes
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'hubeau' AND table_name = 'temperature_chroniques'
        AND column_name = 'date_mesure'
    ) THEN
        ALTER TABLE hubeau.temperature_chroniques RENAME COLUMN date_mesure TO date_mesure_temp;
        ALTER TABLE hubeau.temperature_chroniques RENAME COLUMN temperature TO resultat;

        -- Ajouter heure_mesure_temp si manquante
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'hubeau' AND table_name = 'temperature_chroniques'
            AND column_name = 'heure_mesure_temp'
        ) THEN
            ALTER TABLE hubeau.temperature_chroniques ADD COLUMN heure_mesure_temp TIME;
        END IF;

        -- Renommer colonnes qualification
        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'hubeau' AND table_name = 'temperature_chroniques'
            AND column_name = 'qualification'
        ) THEN
            ALTER TABLE hubeau.temperature_chroniques RENAME COLUMN qualification TO code_qualification;
        END IF;

        -- Ajouter libelle_qualification
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'hubeau' AND table_name = 'temperature_chroniques'
            AND column_name = 'libelle_qualification'
        ) THEN
            ALTER TABLE hubeau.temperature_chroniques ADD COLUMN libelle_qualification VARCHAR(50);
        END IF;

        -- Recréer index
        DROP INDEX IF EXISTS hubeau.idx_temperature_chroniques_date;
        CREATE INDEX idx_temperature_chroniques_date ON hubeau.temperature_chroniques(date_mesure_temp);

        RAISE NOTICE 'Renommé colonnes dans temperature_chroniques';
    END IF;
END $$;

-- Migration 13: quality_rivers_analyses - Ajouter colonnes et contrainte UNIQUE
DO $$
BEGIN
    -- Ajouter code_support
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'hubeau' AND table_name = 'quality_rivers_analyses'
        AND column_name = 'code_support'
    ) THEN
        ALTER TABLE hubeau.quality_rivers_analyses ADD COLUMN code_support VARCHAR(10);
    END IF;

    -- Ajouter code_fraction
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'hubeau' AND table_name = 'quality_rivers_analyses'
        AND column_name = 'code_fraction'
    ) THEN
        ALTER TABLE hubeau.quality_rivers_analyses ADD COLUMN code_fraction VARCHAR(10);
    END IF;

    -- Ajouter contrainte UNIQUE si manquante
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE constraint_schema = 'hubeau' AND table_name = 'quality_rivers_analyses'
        AND constraint_name LIKE '%unique%'
    ) THEN
        ALTER TABLE hubeau.quality_rivers_analyses
        ADD CONSTRAINT quality_rivers_analyses_unique_key
        UNIQUE(code_station, date_prelevement, code_parametre, code_support, code_fraction);

        RAISE NOTICE 'Ajouté contrainte UNIQUE sur quality_rivers_analyses';
    END IF;
END $$;

-- Migration 14: quality_groundwater_analyses - Ajouter colonnes et contrainte UNIQUE
DO $$
BEGIN
    -- Ajouter code_support
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'hubeau' AND table_name = 'quality_groundwater_analyses'
        AND column_name = 'code_support'
    ) THEN
        ALTER TABLE hubeau.quality_groundwater_analyses ADD COLUMN code_support VARCHAR(10);
    END IF;

    -- Ajouter code_fraction
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'hubeau' AND table_name = 'quality_groundwater_analyses'
        AND column_name = 'code_fraction'
    ) THEN
        ALTER TABLE hubeau.quality_groundwater_analyses ADD COLUMN code_fraction VARCHAR(10);
    END IF;

    -- Ajouter contrainte UNIQUE si manquante
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE constraint_schema = 'hubeau' AND table_name = 'quality_groundwater_analyses'
        AND constraint_name LIKE '%unique%'
    ) THEN
        ALTER TABLE hubeau.quality_groundwater_analyses
        ADD CONSTRAINT quality_groundwater_analyses_unique_key
        UNIQUE(code_bss, date_prelevement, code_parametre, code_support, code_fraction);

        RAISE NOTICE 'Ajouté contrainte UNIQUE sur quality_groundwater_analyses';
    END IF;
END $$;

-- Migration 15: quality_rivers_operations - Renommer libelle_methode → nom_methode
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'hubeau' AND table_name = 'quality_rivers_operations'
        AND column_name = 'libelle_methode'
    ) THEN
        ALTER TABLE hubeau.quality_rivers_operations RENAME COLUMN libelle_methode TO nom_methode;
        RAISE NOTICE 'Renommé libelle_methode → nom_methode dans quality_rivers_operations';
    END IF;
END $$;

-- Migration 16: prelevements_chroniques - Renommer volume_preleve → volume
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'hubeau' AND table_name = 'prelevements_chroniques'
        AND column_name = 'volume_preleve'
    ) THEN
        ALTER TABLE hubeau.prelevements_chroniques RENAME COLUMN volume_preleve TO volume;
        RAISE NOTICE 'Renommé volume_preleve → volume dans prelevements_chroniques';
    END IF;
END $$;

-- Migration 17: Handle any other issues that might arise
-- Add more migrations here as needed