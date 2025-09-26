-- PostGIS Hub'Eau Pipeline - Initialisation
-- Base: water_geo

\c water_geo;
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;

-- BDLISA - Masses d'eau souterraine
CREATE TABLE IF NOT EXISTS bdlisa_masses_eau_souterraine (
    id VARCHAR(50) PRIMARY KEY,
    nom VARCHAR(200) NOT NULL,
    type_masse_eau VARCHAR(100),
    bassin_district VARCHAR(100),
    nature VARCHAR(100),
    code_bdlisa VARCHAR(50),
    geom GEOMETRY(MULTIPOLYGON, 4326),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Index spatial
CREATE INDEX IF NOT EXISTS idx_masses_eau_geom ON bdlisa_masses_eau_souterraine USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_masses_eau_type ON bdlisa_masses_eau_souterraine (type_masse_eau);
CREATE INDEX IF NOT EXISTS idx_masses_eau_bassin ON bdlisa_masses_eau_souterraine (bassin_district);

-- BDLISA - Formations géologiques
CREATE TABLE IF NOT EXISTS bdlisa_formations_geologiques (
    id VARCHAR(50) PRIMARY KEY,
    formation_name VARCHAR(200) NOT NULL,
    age_geologique VARCHAR(100),
    lithologie VARCHAR(200),
    permeabilite VARCHAR(100),
    epaisseur_moyenne INTEGER,
    geom GEOMETRY(MULTIPOLYGON, 4326),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_formations_geom ON bdlisa_formations_geologiques USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_formations_age ON bdlisa_formations_geologiques (age_geologique);
CREATE INDEX IF NOT EXISTS idx_formations_litho ON bdlisa_formations_geologiques (lithologie);

-- BDLISA - Limites administratives
CREATE TABLE IF NOT EXISTS bdlisa_limites_administratives (
    id VARCHAR(50) PRIMARY KEY,
    nom VARCHAR(200) NOT NULL,
    type_limite VARCHAR(100),
    niveau_admin INTEGER,
    code_insee VARCHAR(10),
    population INTEGER,
    geom GEOMETRY(MULTIPOLYGON, 4326),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_limites_geom ON bdlisa_limites_administratives USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_limites_type ON bdlisa_limites_administratives (type_limite);
CREATE INDEX IF NOT EXISTS idx_limites_insee ON bdlisa_limites_administratives (code_insee);

-- Vues utilitaires
CREATE OR REPLACE VIEW masses_eau_summary AS
SELECT 
    type_masse_eau,
    bassin_district,
    COUNT(*) as count,
    ST_Area(ST_Union(geom)::geography) / 1000000 as area_km2
FROM bdlisa_masses_eau_souterraine
GROUP BY type_masse_eau, bassin_district;

CREATE OR REPLACE VIEW formations_by_age AS
SELECT 
    age_geologique,
    COUNT(*) as count,
    array_agg(DISTINCT lithologie) as lithologies
FROM bdlisa_formations_geologiques
GROUP BY age_geologique
ORDER BY count DESC;

-- Fonctions spatiales utiles
CREATE OR REPLACE FUNCTION get_masses_eau_at_point(lat DOUBLE PRECISION, lon DOUBLE PRECISION)
RETURNS TABLE(id VARCHAR, nom VARCHAR, type_masse_eau VARCHAR) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        me.id,
        me.nom,
        me.type_masse_eau
    FROM bdlisa_masses_eau_souterraine me
    WHERE ST_Contains(me.geom, ST_SetSRID(ST_MakePoint(lon, lat), 4326));
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_formations_in_radius(lat DOUBLE PRECISION, lon DOUBLE PRECISION, radius_km DOUBLE PRECISION)
RETURNS TABLE(id VARCHAR, formation_name VARCHAR, distance_km DOUBLE PRECISION) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        f.id,
        f.formation_name,
        ST_Distance(f.geom::geography, ST_SetSRID(ST_MakePoint(lon, lat), 4326)::geography) / 1000 as distance_km
    FROM bdlisa_formations_geologiques f
    WHERE ST_DWithin(f.geom::geography, ST_SetSRID(ST_MakePoint(lon, lat), 4326)::geography, radius_km * 1000)
    ORDER BY distance_km;
END;
$$ LANGUAGE plpgsql;

SELECT 'PostGIS initialisé avec succès' as status;
