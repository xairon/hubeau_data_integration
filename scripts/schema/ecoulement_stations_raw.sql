-- Table: ecoulement_stations_raw (Bronze Layer)
-- Source: Hub'Eau API - RAW data (no PK/FK, duplicates allowed)
-- Purpose: Ingest all data as-is from API, transformations happen in Silver layer (dbt)

CREATE SCHEMA IF NOT EXISTS hubeau;

CREATE TABLE IF NOT EXISTS hubeau.ecoulement_stations_raw (
    code_bassin TEXT,
    code_commune TEXT,
    code_cours_eau TEXT,
    code_departement TEXT,
    code_epsg_station TEXT,
    code_projection_station TEXT,
    code_region TEXT,
    code_station BIGINT,
    coordonnee_x_station DOUBLE PRECISION,
    coordonnee_y_station DOUBLE PRECISION,
    date_maj_station TIMESTAMP,
    etat_station TEXT,
    latitude DOUBLE PRECISION,
    libelle_bassin TEXT,
    libelle_commune TEXT,
    libelle_cours_eau TEXT,
    libelle_departement TEXT,
    libelle_projection_station TEXT,
    libelle_region TEXT,
    libelle_station TEXT,
    longitude DOUBLE PRECISION,
    uri_cours_eau TEXT,
    uri_station TEXT,
    -- Audit columns for tracking
    _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for station-based queries
CREATE INDEX IF NOT EXISTS idx_ecoulement_stations_raw_code_station
ON hubeau.ecoulement_stations_raw(code_station);

-- Index for date updates
CREATE INDEX IF NOT EXISTS idx_ecoulement_stations_raw_date_maj_station
ON hubeau.ecoulement_stations_raw(date_maj_station);

COMMENT ON TABLE hubeau.ecoulement_stations_raw IS
'Bronze layer: Raw data from Hub''Eau API - duplicates allowed, no constraints, transformations in dbt';
