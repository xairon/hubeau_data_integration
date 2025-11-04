-- Table: quality_rivers_stations_raw (Bronze Layer)
-- Source: Hub'Eau API - RAW data (no PK/FK, duplicates allowed)
-- Purpose: Ingest all data as-is from API, transformations happen in Silver layer (dbt)

CREATE SCHEMA IF NOT EXISTS hubeau;

CREATE TABLE IF NOT EXISTS hubeau.quality_rivers_stations_raw (
    altitude_point_caracteristique DOUBLE PRECISION,
    code_bassin TEXT,
    code_commune TEXT,
    code_cours_eau TEXT,
    code_departement TEXT,
    code_eu_bassin TEXT,
    code_eu_masse_deau TEXT,
    code_eu_sous_bassin TEXT,
    code_masse_deau TEXT,
    code_projection TEXT,
    code_region TEXT,
    code_station BIGINT,
    commentaire TEXT,
    coordonnee_x DOUBLE PRECISION,
    coordonnee_y DOUBLE PRECISION,
    date_arret TEXT,
    date_creation TIMESTAMP,
    date_maj_information TIMESTAMP,
    durete DOUBLE PRECISION,
    finalite TEXT,
    latitude DOUBLE PRECISION,
    libelle_commune TEXT,
    libelle_departement TEXT,
    libelle_projection TEXT,
    libelle_region TEXT,
    libelle_station TEXT,
    localisation_precise TEXT,
    longitude DOUBLE PRECISION,
    nature TEXT,
    nom_bassin TEXT,
    nom_cours_eau TEXT,
    nom_masse_deau TEXT,
    nom_sous_bassin TEXT,
    point_kilometrique DOUBLE PRECISION,
    premier_mois_annee_etiage DOUBLE PRECISION,
    superficie_bassin_versant_reel DOUBLE PRECISION,
    superficie_bassin_versant_topo DOUBLE PRECISION,
    type_entite_hydro BIGINT,
    uri_bassin TEXT,
    uri_cours_eau TEXT,
    uri_masse_deau TEXT,
    uri_sous_bassin TEXT,
    uri_station TEXT,
    -- Audit columns for tracking
    _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for station-based queries
CREATE INDEX IF NOT EXISTS idx_quality_rivers_stations_raw_code_station
ON hubeau.quality_rivers_stations_raw(code_station);

-- Index for date updates
CREATE INDEX IF NOT EXISTS idx_quality_rivers_stations_raw_date_maj_information
ON hubeau.quality_rivers_stations_raw(date_maj_information);

COMMENT ON TABLE hubeau.quality_rivers_stations_raw IS
'Bronze layer: Raw data from Hub''Eau API - duplicates allowed, no constraints, transformations in dbt';
