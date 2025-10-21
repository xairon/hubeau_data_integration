-- Script pour supprimer toutes les tables DLT créées automatiquement
-- et garder seulement notre schéma PostgreSQL propre

-- Supprimer toutes les tables avec des noms DLT (contiennent des underscores multiples)
DO $$
DECLARE
    table_name TEXT;
BEGIN
    -- Supprimer les tables DLT (qui ont des noms comme table__field__subfield)
    FOR table_name IN 
        SELECT tablename 
        FROM pg_tables 
        WHERE schemaname = 'hubeau' 
        AND tablename LIKE '%__%'
    LOOP
        EXECUTE 'DROP TABLE IF EXISTS hubeau.' || quote_ident(table_name) || ' CASCADE';
        RAISE NOTICE 'Table supprimée: %', table_name;
    END LOOP;
    
    -- Supprimer les tables de métadonnées DLT
    DROP TABLE IF EXISTS hubeau._dlt_loads CASCADE;
    DROP TABLE IF EXISTS hubeau._dlt_pipeline_state CASCADE;
    DROP TABLE IF EXISTS hubeau._dlt_version CASCADE;
    
    RAISE NOTICE 'Nettoyage DLT terminé - Tables DLT supprimées';
END $$;

-- Vérifier les tables restantes (devraient être nos tables PostgreSQL propres)
SELECT 
    schemaname,
    tablename,
    tableowner
FROM pg_tables 
WHERE schemaname = 'hubeau'
ORDER BY tablename;
