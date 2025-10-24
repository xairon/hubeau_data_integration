-- ============================================================================
-- Script de vérification de l'initialisation de la base Hub'Eau
-- ============================================================================
-- NOTE: Les tables Hub'Eau sont créées AUTOMATIQUEMENT par DLT au premier run
-- Ce script vérifie seulement que le schéma et les extensions sont prêts
-- ============================================================================

-- Vérifier que le schéma existe
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'hubeau') THEN
        RAISE EXCEPTION '❌ Le schéma hubeau n''existe pas !';
    END IF;
    RAISE NOTICE '✅ Schéma hubeau: OK';
END $$;

-- ⚠️ NE PLUS VÉRIFIER L'EXISTENCE DES TABLES
-- Les tables seront créées automatiquement par DLT lors du premier run
-- Pandas infère les types depuis les CSV
-- DLT crée les tables avec les types corrects
DO $$
DECLARE
    table_count INT;
BEGIN
    SELECT COUNT(*) INTO table_count
    FROM information_schema.tables
    WHERE table_schema = 'hubeau';

    IF table_count = 0 THEN
        RAISE NOTICE '📋 Schéma hubeau prêt - DLT créera les tables au premier run';
    ELSE
        RAISE NOTICE '📊 Tables existantes dans hubeau: %', table_count;
    END IF;
END $$;

-- Vérifier que PostGIS est installé
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'postgis') THEN
        RAISE EXCEPTION '❌ L''extension PostGIS n''est pas installée !';
    END IF;
    RAISE NOTICE '✅ Extension PostGIS: installée';
END $$;

-- ⚠️ NE PLUS VÉRIFIER LES INDEX GÉOSPATIAUX
-- Les index seront créés automatiquement par DLT avec les tables
DO $$
DECLARE
    index_count INT;
BEGIN
    SELECT COUNT(*) INTO index_count
    FROM pg_indexes
    WHERE schemaname = 'hubeau';

    IF index_count = 0 THEN
        RAISE NOTICE '📋 Aucun index - DLT créera les index au premier run';
    ELSE
        RAISE NOTICE '📊 Index existants dans hubeau: %', index_count;
    END IF;
END $$;

-- Message de confirmation final
DO $$
DECLARE
    table_count INT;
BEGIN
    SELECT COUNT(*) INTO table_count
    FROM information_schema.tables
    WHERE table_schema = 'hubeau';

    RAISE NOTICE '';
    RAISE NOTICE '═══════════════════════════════════════════════════════';
    RAISE NOTICE '✅ Initialisation Hub''Eau RÉUSSIE !';
    RAISE NOTICE '═══════════════════════════════════════════════════════';
    RAISE NOTICE '📊 Schéma: hubeau (prêt)';
    RAISE NOTICE '🗺️  Extension PostGIS: installée';

    IF table_count = 0 THEN
        RAISE NOTICE '📋 Tables: 0 (DLT les créera automatiquement au premier run)';
        RAISE NOTICE '';
        RAISE NOTICE '💡 Pour créer les tables:';
        RAISE NOTICE '   1. Accédez à Dagster UI';
        RAISE NOTICE '   2. Lancez un asset CSV (ex: temperature_stations_csv)';
        RAISE NOTICE '   3. DLT analysera le CSV et créera la table automatiquement';
    ELSE
        RAISE NOTICE '🗄️  Tables existantes: %', table_count;
    END IF;

    RAISE NOTICE '═══════════════════════════════════════════════════════';
    RAISE NOTICE '';
END $$;
