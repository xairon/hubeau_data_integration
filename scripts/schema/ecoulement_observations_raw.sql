-- Table: ecoulement_observations_raw (Bronze Layer)
-- Source: Hub'Eau API - RAW data (no PK/FK, duplicates allowed)
-- Purpose: Ingest all data as-is from API, transformations happen in Silver layer (dbt)

CREATE SCHEMA IF NOT EXISTS hubeau;

CREATE TABLE IF NOT EXISTS hubeau.ecoulement_observations_raw (
    code_bassin TEXT,
    code_campagne TEXT,
    code_commune TEXT,
    code_cours_eau TEXT,
    code_departement TEXT,
    code_ecoulement TEXT,
    code_projection_station TEXT,
    code_region TEXT,
    code_reseau TEXT,
    code_station BIGINT,
    coordonnee_x_station DOUBLE PRECISION,
    coordonnee_y_station DOUBLE PRECISION,
    date_observation TIMESTAMP,
    latitude DOUBLE PRECISION,
    libelle_bassin TEXT,
    libelle_commune TEXT,
    libelle_cours_eau TEXT,
    libelle_departement TEXT,
    libelle_ecoulement TEXT,
    libelle_projection_station TEXT,
    libelle_region TEXT,
    libelle_reseau TEXT,
    libelle_station TEXT,
    longitude DOUBLE PRECISION,
    uri_cours_eau TEXT,
    uri_reseau TEXT,
    uri_station TEXT,
    -- Audit columns for tracking
    _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for incremental loading (MAX(date_observation))
CREATE INDEX IF NOT EXISTS idx_ecoulement_observations_raw_date_observation
ON hubeau.ecoulement_observations_raw(date_observation);

-- Index for station-based queries
CREATE INDEX IF NOT EXISTS idx_ecoulement_observations_raw_code_station
ON hubeau.ecoulement_observations_raw(code_station);

COMMENT ON TABLE hubeau.ecoulement_observations_raw IS
'Bronze layer: Raw data from Hub''Eau API - duplicates allowed, no constraints, transformations in dbt';
