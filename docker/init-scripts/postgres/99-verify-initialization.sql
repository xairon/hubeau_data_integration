-- Script de vérification de l'initialisation de la base Hub'Eau
-- Exécuté après tous les autres scripts pour vérifier que tout est en place

-- Vérifier que le schéma existe
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'hubeau') THEN
        RAISE EXCEPTION 'Le schéma hubeau n''existe pas !';
    END IF;
END $$;

-- Vérifier que les tables principales existent
DO $$
DECLARE
    missing_tables TEXT[] := ARRAY[]::TEXT[];
    table_name TEXT;
    required_tables TEXT[] := ARRAY[
        'hydrometry_sites',
        'hydrometry_stations', 
        'hydrometry_observations',
        'piezometry_stations',
        'piezometry_chroniques',
        'quality_rivers_stations',
        'quality_rivers_analyses',
        'quality_groundwater_stations',
        'quality_groundwater_analyses',
        'temperature_stations',
        'temperature_chroniques',
        'ecoulement_stations',
        'ecoulement_observations',
        'hydrobio_stations',
        'hydrobio_indices',
        'prelevements_ouvrages',
        'prelevements_chroniques'
    ];
BEGIN
    FOREACH table_name IN ARRAY required_tables
    LOOP
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.tables 
            WHERE table_schema = 'hubeau' AND table_name = table_name
        ) THEN
            missing_tables := array_append(missing_tables, table_name);
        END IF;
    END LOOP;
    
    IF array_length(missing_tables, 1) > 0 THEN
        RAISE EXCEPTION 'Tables manquantes: %', array_to_string(missing_tables, ', ');
    END IF;
END $$;

-- Vérifier que PostGIS est installé
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'postgis') THEN
        RAISE EXCEPTION 'L''extension PostGIS n''est pas installée !';
    END IF;
END $$;

-- Vérifier que les index géospatiaux existent
DO $$
DECLARE
    missing_indexes TEXT[] := ARRAY[]::TEXT[];
    index_name TEXT;
    required_indexes TEXT[] := ARRAY[
        'idx_piezometry_stations_geom',
        'idx_hydrometry_sites_geom',
        'idx_hydrometry_stations_geom',
        'idx_quality_rivers_stations_geom',
        'idx_quality_groundwater_stations_geom'
    ];
BEGIN
    FOREACH index_name IN ARRAY required_indexes
    LOOP
        IF NOT EXISTS (
            SELECT 1 FROM pg_indexes 
            WHERE schemaname = 'hubeau' AND indexname = index_name
        ) THEN
            missing_indexes := array_append(missing_indexes, index_name);
        END IF;
    END LOOP;
    
    IF array_length(missing_indexes, 1) > 0 THEN
        RAISE WARNING 'Index géospatiaux manquants: %', array_to_string(missing_indexes, ', ');
    END IF;
END $$;

-- Créer la table de log si elle n'existe pas
CREATE TABLE IF NOT EXISTS hubeau.initialization_log (
    id SERIAL PRIMARY KEY,
    message TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Log de succès
INSERT INTO hubeau.initialization_log (message, created_at)
VALUES ('Base de données Hub''Eau initialisée avec succès', CURRENT_TIMESTAMP)
ON CONFLICT DO NOTHING;

-- Message de confirmation
DO $$
BEGIN
    RAISE NOTICE '✅ Initialisation Hub''Eau terminée avec succès !';
    RAISE NOTICE '📊 Schéma: hubeau';
    RAISE NOTICE '🗄️ Tables créées: %', (
        SELECT COUNT(*) FROM information_schema.tables 
        WHERE table_schema = 'hubeau'
    );
    RAISE NOTICE '🗺️ Extension PostGIS: installée';
END $$;
