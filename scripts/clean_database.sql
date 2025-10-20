-- Script SQL pour nettoyer complètement la base de données Hub'Eau
-- ATTENTION: Ce script supprime TOUTES les données !

-- 1. Supprimer le schéma staging s'il existe
DROP SCHEMA IF EXISTS hubeau_staging CASCADE;
DROP SCHEMA IF EXISTS staging CASCADE;

-- 2. Supprimer toutes les tables parasites DLT
DO $$ 
DECLARE
    r RECORD;
BEGIN
    -- Supprimer les tables avec __geometry__coordinates
    FOR r IN (
        SELECT schemaname, tablename 
        FROM pg_tables 
        WHERE tablename LIKE '%__geometry__coordinates%'
        AND schemaname = 'hubeau'
    ) LOOP
        EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.schemaname) || '.' || quote_ident(r.tablename) || ' CASCADE';
        RAISE NOTICE 'Supprimé: %.%', r.schemaname, r.tablename;
    END LOOP;

    -- Supprimer les tables avec __codes_ et __libelles_
    FOR r IN (
        SELECT schemaname, tablename 
        FROM pg_tables 
        WHERE (tablename LIKE '%__codes_%' OR tablename LIKE '%__libelles_%')
        AND schemaname = 'hubeau'
    ) LOOP
        EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.schemaname) || '.' || quote_ident(r.tablename) || ' CASCADE';
        RAISE NOTICE 'Supprimé: %.%', r.schemaname, r.tablename;
    END LOOP;

    -- Supprimer les tables DLT système parasites
    FOR r IN (
        SELECT schemaname, tablename 
        FROM pg_tables 
        WHERE tablename LIKE '_dlt_%'
        AND schemaname = 'hubeau'
    ) LOOP
        EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.schemaname) || '.' || quote_ident(r.tablename) || ' CASCADE';
        RAISE NOTICE 'Supprimé: %.%', r.schemaname, r.tablename;
    END LOOP;
END $$;

-- 3. Supprimer toutes les données des tables principales (garder la structure)
TRUNCATE TABLE IF EXISTS hubeau.piezometry_chroniques CASCADE;
TRUNCATE TABLE IF EXISTS hubeau.piezometry_stations CASCADE;
TRUNCATE TABLE IF EXISTS hubeau.hydrometry_observations CASCADE;
TRUNCATE TABLE IF EXISTS hubeau.hydrometry_sites CASCADE;
TRUNCATE TABLE IF EXISTS hubeau.hydrometry_stations CASCADE;
TRUNCATE TABLE IF EXISTS hubeau.quality_rivers_analyses CASCADE;
TRUNCATE TABLE IF EXISTS hubeau.quality_rivers_operations CASCADE;
TRUNCATE TABLE IF EXISTS hubeau.quality_rivers_conditions CASCADE;
TRUNCATE TABLE IF EXISTS hubeau.quality_rivers_stations CASCADE;
TRUNCATE TABLE IF EXISTS hubeau.quality_groundwater_analyses CASCADE;
TRUNCATE TABLE IF EXISTS hubeau.quality_groundwater_stations CASCADE;
TRUNCATE TABLE IF EXISTS hubeau.ecoulement_observations CASCADE;
TRUNCATE TABLE IF EXISTS hubeau.ecoulement_campagnes CASCADE;
TRUNCATE TABLE IF EXISTS hubeau.ecoulement_stations CASCADE;
TRUNCATE TABLE IF EXISTS hubeau.hydrobio_taxons CASCADE;
TRUNCATE TABLE IF EXISTS hubeau.hydrobio_indices CASCADE;
TRUNCATE TABLE IF EXISTS hubeau.hydrobio_stations CASCADE;
TRUNCATE TABLE IF EXISTS hubeau.prelevements_chroniques CASCADE;
TRUNCATE TABLE IF EXISTS hubeau.prelevements_ouvrages CASCADE;
TRUNCATE TABLE IF EXISTS hubeau.prelevements_points CASCADE;
TRUNCATE TABLE IF EXISTS hubeau.temperature_chroniques CASCADE;
TRUNCATE TABLE IF EXISTS hubeau.temperature_stations CASCADE;

-- 4. Vérifier ce qui reste
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size,
    n_live_tup AS row_count
FROM pg_stat_user_tables
WHERE schemaname = 'hubeau'
ORDER BY tablename;

-- 5. Message de confirmation
SELECT 'Nettoyage terminé! Toutes les données parasites ont été supprimées.' AS message;
