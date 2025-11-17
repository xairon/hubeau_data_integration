-- Table: temperature_stations_raw (Bronze Layer)
-- Source: Hub'Eau API - RAW data (no PK/FK, duplicates allowed)
-- Purpose: Ingest all data as-is from API, transformations happen in Silver layer (dbt)

CREATE SCHEMA IF NOT EXISTS hubeau;

CREATE TABLE IF NOT EXISTS hubeau.temperature_stations_raw (
    altitude DOUBLE PRECISION,
    code_bassin TEXT,
    code_commune TEXT,
    code_cours_eau TEXT,
    code_departement TEXT,
    code_masse_eau TEXT,
    code_region TEXT,
    code_sous_bassin TEXT,
    code_station BIGINT,
    code_troncon_hydro TEXT,
    code_type_projection TEXT,
    coordonnee_x DOUBLE PRECISION,
    coordonnee_y DOUBLE PRECISION,
    date_maj_infos TIMESTAMP,
    latitude DOUBLE PRECISION,
    libelle_bassin TEXT,
    libelle_commune TEXT,
    libelle_cours_eau TEXT,
    libelle_departement TEXT,
    libelle_masse_eau TEXT,
    libelle_region TEXT,
    libelle_sous_bassin TEXT,
    libelle_station TEXT,
    localisation TEXT,
    longitude DOUBLE PRECISION,
    pk DOUBLE PRECISION,
    uri_bassin TEXT,
    uri_cours_eau TEXT,
    uri_masse_eau TEXT,
    uri_station TEXT,
    -- Audit columns for tracking
    _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for station-based queries
CREATE INDEX IF NOT EXISTS idx_temperature_stations_raw_code_station
ON hubeau.temperature_stations_raw(code_station);

-- Index for date updates
CREATE INDEX IF NOT EXISTS idx_temperature_stations_raw_date_maj_infos
ON hubeau.temperature_stations_raw(date_maj_infos);

COMMENT ON TABLE hubeau.temperature_stations_raw IS
'Bronze layer: Raw data from Hub''Eau API - duplicates allowed, no constraints, transformations in dbt';
