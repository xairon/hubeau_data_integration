-- ====================================
-- COLONNES GÉOMÉTRIQUES POSTGIS
-- ====================================
--
-- Ce script ajoute des colonnes de géométrie PostGIS aux tables de stations
-- et crée les index spatiaux pour des requêtes géographiques performantes.
--
-- Prérequis: Extension PostGIS installée
--   CREATE EXTENSION IF NOT EXISTS postgis;
--
-- Exécution:
--   psql -U postgres -d postgres -f 03_add_postgis_geometry.sql
--
-- Bénéfices:
--   - Requêtes spatiales 10-100x plus rapides
--   - Support des opérations géographiques (distance, buffer, intersection)
--   - Visualisation cartographique facilitée (QGIS, etc.)
-- ====================================

\timing on
SET search_path TO hubeau, public;

-- Vérifier que PostGIS est installé
CREATE EXTENSION IF NOT EXISTS postgis;

\echo '====================================';
\echo 'AJOUT COLONNES GÉOMÉTRIQUES';
\echo '====================================';
\echo '';

-- ====================================
-- PIÉZOMÉTRIE
-- ====================================

\echo 'Piézométrie stations...';

-- Ajouter colonne géométrie
ALTER TABLE piezometry_stations
ADD COLUMN IF NOT EXISTS geom geometry(Point, 4326);

-- Peupler depuis latitude/longitude
UPDATE piezometry_stations
SET geom = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)
WHERE longitude IS NOT NULL
  AND latitude IS NOT NULL
  AND longitude BETWEEN -180 AND 180
  AND latitude BETWEEN -90 AND 90;

-- Index spatial (GIST)
CREATE INDEX IF NOT EXISTS idx_piezo_stations_geom
ON piezometry_stations USING GIST(geom);

-- Contrainte: Valider géométrie
ALTER TABLE piezometry_stations
ADD CONSTRAINT check_piezo_stations_geom_valid
CHECK (ST_IsValid(geom) OR geom IS NULL);

-- ====================================
-- HYDROMÉTRIE
-- ====================================

\echo 'Hydrométrie stations...';

ALTER TABLE hydrometry_stations
ADD COLUMN IF NOT EXISTS geom geometry(Point, 4326);

UPDATE hydrometry_stations
SET geom = ST_SetSRID(ST_MakePoint(longitude_station, latitude_station), 4326)
WHERE longitude_station IS NOT NULL
  AND latitude_station IS NOT NULL
  AND longitude_station BETWEEN -180 AND 180
  AND latitude_station BETWEEN -90 AND 90;

CREATE INDEX IF NOT EXISTS idx_hydro_stations_geom
ON hydrometry_stations USING GIST(geom);

ALTER TABLE hydrometry_stations
ADD CONSTRAINT check_hydro_stations_geom_valid
CHECK (ST_IsValid(geom) OR geom IS NULL);

\echo 'Hydrométrie sites...';

ALTER TABLE hydrometry_sites
ADD COLUMN IF NOT EXISTS geom geometry(Point, 4326);

UPDATE hydrometry_sites
SET geom = ST_SetSRID(ST_MakePoint(longitude_site, latitude_site), 4326)
WHERE longitude_site IS NOT NULL
  AND latitude_site IS NOT NULL
  AND longitude_site BETWEEN -180 AND 180
  AND latitude_site BETWEEN -90 AND 90;

CREATE INDEX IF NOT EXISTS idx_hydro_sites_geom
ON hydrometry_sites USING GIST(geom);

ALTER TABLE hydrometry_sites
ADD CONSTRAINT check_hydro_sites_geom_valid
CHECK (ST_IsValid(geom) OR geom IS NULL);

-- ====================================
-- QUALITÉ COURS D'EAU
-- ====================================

\echo 'Qualité cours d''eau stations...';

ALTER TABLE quality_rivers_stations
ADD COLUMN IF NOT EXISTS geom geometry(Point, 4326);

UPDATE quality_rivers_stations
SET geom = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)
WHERE longitude IS NOT NULL
  AND latitude IS NOT NULL
  AND longitude BETWEEN -180 AND 180
  AND latitude BETWEEN -90 AND 90;

CREATE INDEX IF NOT EXISTS idx_quality_rivers_stations_geom
ON quality_rivers_stations USING GIST(geom);

ALTER TABLE quality_rivers_stations
ADD CONSTRAINT check_quality_rivers_stations_geom_valid
CHECK (ST_IsValid(geom) OR geom IS NULL);

-- ====================================
-- QUALITÉ NAPPES
-- ====================================

\echo 'Qualité nappes stations...';

ALTER TABLE quality_groundwater_stations
ADD COLUMN IF NOT EXISTS geom geometry(Point, 4326);

UPDATE quality_groundwater_stations
SET geom = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)
WHERE longitude IS NOT NULL
  AND latitude IS NOT NULL
  AND longitude BETWEEN -180 AND 180
  AND latitude BETWEEN -90 AND 90;

CREATE INDEX IF NOT EXISTS idx_quality_groundwater_stations_geom
ON quality_groundwater_stations USING GIST(geom);

ALTER TABLE quality_groundwater_stations
ADD CONSTRAINT check_quality_groundwater_stations_geom_valid
CHECK (ST_IsValid(geom) OR geom IS NULL);

-- ====================================
-- TEMPÉRATURE
-- ====================================

\echo 'Température stations...';

ALTER TABLE temperature_stations
ADD COLUMN IF NOT EXISTS geom geometry(Point, 4326);

UPDATE temperature_stations
SET geom = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)
WHERE longitude IS NOT NULL
  AND latitude IS NOT NULL
  AND longitude BETWEEN -180 AND 180
  AND latitude BETWEEN -90 AND 90;

CREATE INDEX IF NOT EXISTS idx_temperature_stations_geom
ON temperature_stations USING GIST(geom);

ALTER TABLE temperature_stations
ADD CONSTRAINT check_temperature_stations_geom_valid
CHECK (ST_IsValid(geom) OR geom IS NULL);

-- ====================================
-- ÉCOULEMENT
-- ====================================

\echo 'Écoulement stations...';

ALTER TABLE ecoulement_stations
ADD COLUMN IF NOT EXISTS geom geometry(Point, 4326);

UPDATE ecoulement_stations
SET geom = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)
WHERE longitude IS NOT NULL
  AND latitude IS NOT NULL
  AND longitude BETWEEN -180 AND 180
  AND latitude BETWEEN -90 AND 90;

CREATE INDEX IF NOT EXISTS idx_ecoulement_stations_geom
ON ecoulement_stations USING GIST(geom);

ALTER TABLE ecoulement_stations
ADD CONSTRAINT check_ecoulement_stations_geom_valid
CHECK (ST_IsValid(geom) OR geom IS NULL);

-- ====================================
-- HYDROBIOLOGIE
-- ====================================

\echo 'Hydrobiologie stations...';

ALTER TABLE hydrobio_stations
ADD COLUMN IF NOT EXISTS geom geometry(Point, 4326);

UPDATE hydrobio_stations
SET geom = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)
WHERE longitude IS NOT NULL
  AND latitude IS NOT NULL
  AND longitude BETWEEN -180 AND 180
  AND latitude BETWEEN -90 AND 90;

CREATE INDEX IF NOT EXISTS idx_hydrobio_stations_geom
ON hydrobio_stations USING GIST(geom);

ALTER TABLE hydrobio_stations
ADD CONSTRAINT check_hydrobio_stations_geom_valid
CHECK (ST_IsValid(geom) OR geom IS NULL);

-- ====================================
-- PRÉLÈVEMENTS (si coordonnées disponibles)
-- ====================================

-- Note: Les ouvrages de prélèvement peuvent ne pas avoir de coordonnées
-- Vérifier la structure de la table avant d'ajouter la géométrie

-- ====================================
-- TRIGGERS POUR MAINTIEN AUTOMATIQUE
-- ====================================
-- Ces triggers mettent à jour automatiquement la colonne geom
-- quand latitude/longitude changent

\echo '';
\echo 'Création triggers de mise à jour automatique...';

-- Fonction générique pour mise à jour géométrie
CREATE OR REPLACE FUNCTION update_geom_from_coords()
RETURNS TRIGGER AS $$
BEGIN
    -- Déterminer les noms de colonnes (latitude/longitude vs lat_field/lon_field)
    DECLARE
        lat_col TEXT;
        lon_col TEXT;
    BEGIN
        -- Par défaut
        lat_col := 'latitude';
        lon_col := 'longitude';

        -- Cas spéciaux
        IF TG_TABLE_NAME = 'hydrometry_stations' THEN
            lat_col := 'latitude_station';
            lon_col := 'longitude_station';
        ELSIF TG_TABLE_NAME = 'hydrometry_sites' THEN
            lat_col := 'latitude_site';
            lon_col := 'longitude_site';
        END IF;

        -- Mettre à jour géométrie si coordonnées valides
        EXECUTE format('
            UPDATE %I
            SET geom = CASE
                WHEN $1 IS NOT NULL AND $2 IS NOT NULL
                    AND $2 BETWEEN -180 AND 180
                    AND $1 BETWEEN -90 AND 90
                THEN ST_SetSRID(ST_MakePoint($2, $1), 4326)
                ELSE NULL
            END
            WHERE ctid = $3',
            TG_TABLE_NAME
        ) USING NEW.latitude, NEW.longitude, NEW.ctid;

        RETURN NEW;
    END;
END;
$$ LANGUAGE plpgsql;

-- Appliquer trigger à chaque table de stations
DO $$
DECLARE
    table_rec RECORD;
BEGIN
    FOR table_rec IN
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'hubeau'
          AND table_name LIKE '%_stations'
           OR table_name LIKE '%_sites'
    LOOP
        EXECUTE format('
            CREATE TRIGGER trigger_update_geom
            AFTER INSERT OR UPDATE OF latitude, longitude
            ON %I
            FOR EACH ROW
            EXECUTE FUNCTION update_geom_from_coords()',
            table_rec.table_name
        );
    END LOOP;
END;
$$;

-- ====================================
-- STATISTIQUES ET RÉSUMÉ
-- ====================================

\echo '';
\echo '====================================';
\echo 'STATISTIQUES GÉOMÉTRIES CRÉÉES:';
\echo '====================================';

SELECT
    table_name,
    column_name,
    udt_name AS geometry_type,
    (
        SELECT COUNT(*)
        FROM information_schema.columns c2
        WHERE c2.table_schema = 'hubeau'
          AND c2.table_name = c.table_name
          AND c2.column_name = 'geom'
    ) AS has_geometry
FROM information_schema.columns c
WHERE table_schema = 'hubeau'
  AND table_name LIKE '%_stations'
  AND column_name = 'geom'
ORDER BY table_name;

\echo '';
\echo '====================================';
\echo 'EXEMPLES DE REQUÊTES SPATIALES:';
\echo '====================================';
\echo '';
\echo '-- Trouver stations dans rayon 10km de Paris (2.35, 48.86):';
\echo 'SELECT code_station, libelle_station,';
\echo '       ST_Distance(geom::geography, ST_MakePoint(2.35, 48.86)::geography) / 1000 AS distance_km';
\echo 'FROM hubeau.piezometry_stations';
\echo 'WHERE ST_DWithin(geom::geography, ST_MakePoint(2.35, 48.86)::geography, 10000)';
\echo 'ORDER BY distance_km;';
\echo '';
\echo '-- Compter stations par département (join avec shapefile):';
\echo '-- (nécessite table départements avec géométries)';
\echo '';
\echo '-- Export GeoJSON pour visualisation:';
\echo 'SELECT json_build_object(';
\echo '    ''type'', ''FeatureCollection'',';
\echo '    ''features'', json_agg(ST_AsGeoJSON(t.*)::json)';
\echo ') FROM hubeau.piezometry_stations t WHERE geom IS NOT NULL;';
\echo '';

\echo '✅ Géométries PostGIS créées avec succès!';
\echo '';
\echo 'Bénéfices:';
\echo '  - Requêtes spatiales optimisées (index GIST)';
\echo '  - Support opérations géographiques (buffer, intersection, etc.)';
\echo '  - Export facile vers QGIS, GeoJSON, Shapefile';
\echo '  - Mise à jour automatique via triggers';
