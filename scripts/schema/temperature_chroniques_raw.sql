-- Table: temperature_chroniques_raw (Bronze Layer)
-- Source: Hub'Eau API - RAW data (no PK/FK, duplicates allowed)
-- Purpose: Ingest all data as-is from API, transformations happen in Silver layer (dbt)

CREATE SCHEMA IF NOT EXISTS hubeau;

CREATE TABLE IF NOT EXISTS hubeau.temperature_chroniques_raw (
    code_commune TEXT,
    code_cours_eau TEXT,
    code_parametre BIGINT,
    code_qualification TEXT,
    code_station BIGINT,
    code_unite TEXT,
    date_mesure_temp TIMESTAMP,
    heure_mesure_temp TEXT,
    latitude DOUBLE PRECISION,
    libelle_commune TEXT,
    libelle_cours_eau TEXT,
    libelle_parametre TEXT,
    libelle_qualification TEXT,
    libelle_station TEXT,
    longitude DOUBLE PRECISION,
    resultat DOUBLE PRECISION,
    symbole_unite TEXT,
    uri_cours_eau TEXT,
    uri_station TEXT,
    -- Audit columns for tracking
    _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for incremental loading (MAX(date_mesure_temp))
CREATE INDEX IF NOT EXISTS idx_temperature_chroniques_raw_date_mesure_temp
ON hubeau.temperature_chroniques_raw(date_mesure_temp);

-- Index for station-based queries
CREATE INDEX IF NOT EXISTS idx_temperature_chroniques_raw_code_station
ON hubeau.temperature_chroniques_raw(code_station);

COMMENT ON TABLE hubeau.temperature_chroniques_raw IS
'Bronze layer: Raw data from Hub''Eau API - duplicates allowed, no constraints, transformations in dbt';
