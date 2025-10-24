-- ============================================================================
-- Hub'Eau PostgreSQL - Minimal Initialization
-- ============================================================================
-- DLT créera les tables automatiquement au premier run
-- Pandas infère les types depuis les CSV
-- Plus de définition manuelle de schéma = zéro maintenance !
-- ============================================================================

-- Créer le schéma Hub'Eau
CREATE SCHEMA IF NOT EXISTS hubeau;

-- Activer PostGIS pour les géométries
CREATE EXTENSION IF NOT EXISTS postgis;

-- Permissions
GRANT ALL PRIVILEGES ON SCHEMA hubeau TO postgres;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA hubeau TO postgres;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA hubeau TO postgres;

-- Permissions futures (tables créées par DLT)
ALTER DEFAULT PRIVILEGES IN SCHEMA hubeau
GRANT ALL PRIVILEGES ON TABLES TO postgres;

ALTER DEFAULT PRIVILEGES IN SCHEMA hubeau
GRANT ALL PRIVILEGES ON SEQUENCES TO postgres;

-- Log confirmation
DO $$
BEGIN
    RAISE NOTICE '✅ Schéma Hub''Eau initialisé - DLT créera les tables automatiquement';
END $$;
