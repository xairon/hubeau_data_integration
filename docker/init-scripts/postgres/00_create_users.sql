-- ============================================================================
-- Hub'Eau PostgreSQL - User Creation Script
-- ============================================================================
-- Automatically creates 2 users at PostgreSQL container initialization:
-- 1. Main user (read/write access) - configurable via PG_USER/PG_PASSWORD
-- 2. Read-only user (read access only) - configurable via PG_READONLY_USER/PG_READONLY_PASSWORD
--
-- Note: This script runs ONLY on first container startup (empty volume)
-- To recreate users on existing DB, run manually or delete postgres_data volume
-- ============================================================================

-- ============================================================================
-- MAIN USER (already created by Docker)
-- ============================================================================
-- Docker automatically creates the main user specified in POSTGRES_USER env var
-- We just need to ensure it has the right permissions (done in 01_init_minimal.sql)

-- ============================================================================
-- READ-ONLY USER CREATION
-- ============================================================================
-- This user can only SELECT data (perfect for Adminer, BI tools, etc.)

DO $$
DECLARE
    readonly_user TEXT := current_setting('hubeau.readonly_user', true);
    readonly_password TEXT := current_setting('hubeau.readonly_password', true);
BEGIN
    -- Check if env vars are set
    IF readonly_user IS NULL OR readonly_user = '' THEN
        RAISE NOTICE '⚠️  PG_READONLY_USER not set, skipping read-only user creation';
        RETURN;
    END IF;

    IF readonly_password IS NULL OR readonly_password = '' THEN
        RAISE NOTICE '⚠️  PG_READONLY_PASSWORD not set, skipping read-only user creation';
        RETURN;
    END IF;

    -- Create read-only user if not exists
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = readonly_user) THEN
        EXECUTE format('CREATE USER %I WITH PASSWORD %L', readonly_user, readonly_password);
        RAISE NOTICE '✅ Read-only user % created successfully', readonly_user;
    ELSE
        RAISE NOTICE '⚠️  User % already exists, skipping creation', readonly_user;
    END IF;

    -- Grant read-only permissions on public schema
    EXECUTE format('GRANT CONNECT ON DATABASE %I TO %I', current_database(), readonly_user);
    EXECUTE format('GRANT USAGE ON SCHEMA public TO %I', readonly_user);
    EXECUTE format('GRANT SELECT ON ALL TABLES IN SCHEMA public TO %I', readonly_user);
    EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO %I', readonly_user);

    RAISE NOTICE '✅ Read-only permissions granted on schema public for user %', readonly_user;

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE '❌ Error creating read-only user: %', SQLERRM;
END $$;

-- ============================================================================
-- SUMMARY
-- ============================================================================
DO $$
BEGIN
    RAISE NOTICE '================================================';
    RAISE NOTICE 'PostgreSQL User Setup Complete';
    RAISE NOTICE '================================================';
    RAISE NOTICE 'Main user: % (read/write access)', current_setting('POSTGRES_USER', true);
    RAISE NOTICE 'Read-only user: % (read-only access)', current_setting('hubeau.readonly_user', true);
    RAISE NOTICE '================================================';
    RAISE NOTICE 'Next steps:';
    RAISE NOTICE '  1. Schema "hubeau" will be created in 01_init_minimal.sql';
    RAISE NOTICE '  2. Read-only user will get permissions in 01_init_minimal.sql';
    RAISE NOTICE '  3. DLT will create tables automatically on first run';
    RAISE NOTICE '================================================';
END $$;
