-- ============================================================================
-- Hub'Eau PostgreSQL - Minimal Initialization
-- ============================================================================
-- DLT créera les tables automatiquement au premier run
-- Pandas infère les types depuis les CSV
-- Plus de définition manuelle de schéma = zéro maintenance !
-- ============================================================================

-- Créer le schéma Hub'Eau
CREATE SCHEMA IF NOT EXISTS hubeau;

-- Créer le schéma staging (DLT default dataset)
CREATE SCHEMA IF NOT EXISTS staging;

-- Activer PostGIS pour les géométries
CREATE EXTENSION IF NOT EXISTS postgis;

-- ============================================================================
-- PERMISSIONS MAIN USER (postgres or custom user from env)
-- ============================================================================

-- Permissions sur schéma hubeau
GRANT ALL PRIVILEGES ON SCHEMA hubeau TO postgres;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA hubeau TO postgres;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA hubeau TO postgres;

-- Permissions futures (tables créées par DLT)
ALTER DEFAULT PRIVILEGES IN SCHEMA hubeau
GRANT ALL PRIVILEGES ON TABLES TO postgres;

ALTER DEFAULT PRIVILEGES IN SCHEMA hubeau
GRANT ALL PRIVILEGES ON SEQUENCES TO postgres;

-- Permissions sur schéma staging (DLT)
GRANT ALL PRIVILEGES ON SCHEMA staging TO postgres;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA staging TO postgres;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA staging TO postgres;

ALTER DEFAULT PRIVILEGES IN SCHEMA staging
GRANT ALL PRIVILEGES ON TABLES TO postgres;

ALTER DEFAULT PRIVILEGES IN SCHEMA staging
GRANT ALL PRIVILEGES ON SEQUENCES TO postgres;

-- ============================================================================
-- PERMISSIONS READ-ONLY USER (if exists)
-- ============================================================================

DO $$
DECLARE
    readonly_user TEXT := current_setting('hubeau.readonly_user', true);
BEGIN
    IF readonly_user IS NOT NULL AND readonly_user != '' THEN
        -- Check if user exists
        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = readonly_user) THEN
            -- Grant read-only permissions on hubeau schema
            EXECUTE format('GRANT USAGE ON SCHEMA hubeau TO %I', readonly_user);
            EXECUTE format('GRANT SELECT ON ALL TABLES IN SCHEMA hubeau TO %I', readonly_user);
            EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA hubeau GRANT SELECT ON TABLES TO %I', readonly_user);

            -- Grant read-only permissions on staging schema (DLT)
            EXECUTE format('GRANT USAGE ON SCHEMA staging TO %I', readonly_user);
            EXECUTE format('GRANT SELECT ON ALL TABLES IN SCHEMA staging TO %I', readonly_user);
            EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA staging GRANT SELECT ON TABLES TO %I', readonly_user);

            RAISE NOTICE '✅ Read-only permissions granted on schemas (hubeau, staging) for user %', readonly_user;
        END IF;
    END IF;
END $$;

-- ============================================================================
-- LOG CONFIRMATION
-- ============================================================================

DO $$
BEGIN
    RAISE NOTICE '================================================';
    RAISE NOTICE '✅ Hub''Eau schemas initialized (hubeau, staging)';
    RAISE NOTICE '✅ PostGIS extension enabled';
    RAISE NOTICE '✅ DLT will create tables automatically on first run';
    RAISE NOTICE '================================================';
END $$;
