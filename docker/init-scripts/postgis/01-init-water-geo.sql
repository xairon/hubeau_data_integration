-- PostGIS Hub'Eau Pipeline - Initialisation automatique
-- Exécuté automatiquement au premier démarrage du conteneur
-- Base: water_geo

-- Création de la base de données géographique
CREATE DATABASE water_geo;

-- Se connecter à la nouvelle base
\c water_geo;

-- Activation des extensions PostGIS
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;

-- ==========================================
-- BDLISA - MASSES D'EAU SOUTERRAINE
-- ==========================================

CREATE TABLE bdlisa_masses_eau_souterraine (
    id VARCHAR(50) PRIMARY KEY,
    nom VARCHAR(200) NOT NULL,
    type_masse_eau VARCHAR(100),
    bassin_district VARCHAR(100),
    nature VARCHAR(100),
    code_bdlisa VARCHAR(50),
    superficie_km2 DOUBLE PRECISION,
    volume_exploitable_mm3 DOUBLE PRECISION,
    geom GEOMETRY(MULTIPOLYGON, 4326),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Index spatial principal
CREATE INDEX idx_masses_eau_geom ON bdlisa_masses_eau_souterraine USING GIST(geom);

-- Index attributaires
CREATE INDEX idx_masses_eau_type ON bdlisa_masses_eau_souterraine (type_masse_eau);
CREATE INDEX idx_masses_eau_bassin ON bdlisa_masses_eau_souterraine (bassin_district);
CREATE INDEX idx_masses_eau_code ON bdlisa_masses_eau_souterraine (code_bdlisa);

-- ==========================================
-- BDLISA - FORMATIONS GÉOLOGIQUES
-- ==========================================

CREATE TABLE bdlisa_formations_geologiques (
    id VARCHAR(50) PRIMARY KEY,
    formation_name VARCHAR(200) NOT NULL,
    age_geologique VARCHAR(100),
    lithologie VARCHAR(200),
    permeabilite VARCHAR(100),
    porosite_pct DOUBLE PRECISION,
    epaisseur_moyenne_m INTEGER,
    transmissivite_m2s DOUBLE PRECISION,
    geom GEOMETRY(MULTIPOLYGON, 4326),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Index spatial
CREATE INDEX idx_formations_geom ON bdlisa_formations_geologiques USING GIST(geom);

-- Index attributaires
CREATE INDEX idx_formations_age ON bdlisa_formations_geologiques (age_geologique);
CREATE INDEX idx_formations_litho ON bdlisa_formations_geologiques (lithologie);
CREATE INDEX idx_formations_perm ON bdlisa_formations_geologiques (permeabilite);

-- ==========================================
-- BDLISA - LIMITES ADMINISTRATIVES
-- ==========================================

CREATE TABLE bdlisa_limites_administratives (
    id VARCHAR(50) PRIMARY KEY,
    nom VARCHAR(200) NOT NULL,
    type_limite VARCHAR(100), -- commune, département, région, bassin
    niveau_admin INTEGER,
    code_insee VARCHAR(10),
    population INTEGER,
    superficie_km2 DOUBLE PRECISION,
    geom GEOMETRY(MULTIPOLYGON, 4326),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Index spatial
CREATE INDEX idx_limites_geom ON bdlisa_limites_administratives USING GIST(geom);

-- Index attributaires
CREATE INDEX idx_limites_type ON bdlisa_limites_administratives (type_limite);
CREATE INDEX idx_limites_insee ON bdlisa_limites_administratives (code_insee);
CREATE INDEX idx_limites_niveau ON bdlisa_limites_administratives (niveau_admin);

-- ==========================================
-- STATIONS HUB'EAU (GÉORÉFÉRENCEMENT)
-- ==========================================

-- Table de liaison pour géoréférencer les stations
CREATE TABLE hubeau_stations_geo (
    station_id VARCHAR(50) PRIMARY KEY,
    station_name VARCHAR(200),
    source_type VARCHAR(50), -- piezo, hydro, quality_surface, quality_groundwater, temperature
    geom GEOMETRY(POINT, 4326),
    masse_eau_id VARCHAR(50),
    formation_geo_id VARCHAR(50),
    commune_id VARCHAR(50),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Index spatial
CREATE INDEX idx_stations_geo_geom ON hubeau_stations_geo USING GIST(geom);

-- Index attributaires
CREATE INDEX idx_stations_geo_type ON hubeau_stations_geo (source_type);
CREATE INDEX idx_stations_geo_masse_eau ON hubeau_stations_geo (masse_eau_id);

-- Contraintes de clés étrangères
ALTER TABLE hubeau_stations_geo 
ADD CONSTRAINT fk_stations_masse_eau 
FOREIGN KEY (masse_eau_id) REFERENCES bdlisa_masses_eau_souterraine(id);

ALTER TABLE hubeau_stations_geo 
ADD CONSTRAINT fk_stations_formation 
FOREIGN KEY (formation_geo_id) REFERENCES bdlisa_formations_geologiques(id);

ALTER TABLE hubeau_stations_geo 
ADD CONSTRAINT fk_stations_commune 
FOREIGN KEY (commune_id) REFERENCES bdlisa_limites_administratives(id);

-- ==========================================
-- DONNÉES EXEMPLE POUR TESTS
-- ==========================================

-- Masse d'eau exemple
INSERT INTO bdlisa_masses_eau_souterraine (
    id, nom, type_masse_eau, bassin_district, nature, code_bdlisa, 
    superficie_km2, volume_exploitable_mm3, geom
) VALUES (
    'ME_EXAMPLE_001',
    'Alluvions de la Loire moyenne',
    'Alluviale',
    'Loire-Bretagne',
    'Libre',
    'FRHG001',
    250.5,
    15.2,
    ST_GeomFromText('POLYGON((2.0 46.0, 2.5 46.0, 2.5 46.3, 2.0 46.3, 2.0 46.0))', 4326)
);

-- Formation géologique exemple  
INSERT INTO bdlisa_formations_geologiques (
    id, formation_name, age_geologique, lithologie, permeabilite, 
    porosite_pct, epaisseur_moyenne_m, geom
) VALUES (
    'FORM_EXAMPLE_001',
    'Sables et graviers alluviaux',
    'Quaternaire',
    'Sables grossiers et graviers',
    'Très perméable',
    25.5,
    8,
    ST_GeomFromText('POLYGON((2.0 46.0, 2.5 46.0, 2.5 46.3, 2.0 46.3, 2.0 46.0))', 4326)
);

-- Limite administrative exemple
INSERT INTO bdlisa_limites_administratives (
    id, nom, type_limite, niveau_admin, code_insee, 
    population, superficie_km2, geom
) VALUES (
    'COMM_EXAMPLE_001',
    'Commune Test',
    'commune',
    1,
    '45001',
    2500,
    25.8,
    ST_GeomFromText('POLYGON((2.1 46.1, 2.4 46.1, 2.4 46.2, 2.1 46.2, 2.1 46.1))', 4326)
);

-- Station géoréférencée exemple
INSERT INTO hubeau_stations_geo (
    station_id, station_name, source_type, geom, 
    masse_eau_id, formation_geo_id, commune_id
) VALUES (
    'BSS00000001',
    'Station Piézo Test Géo',
    'piezo',
    ST_GeomFromText('POINT(2.3 46.15)', 4326),
    'ME_EXAMPLE_001',
    'FORM_EXAMPLE_001', 
    'COMM_EXAMPLE_001'
);

SELECT 'PostGIS: Schéma géographique créé avec données exemple' as status;
