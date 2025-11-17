-- Table: hydrobio_stations_raw (Bronze Layer)
-- Source: Hub'Eau API - RAW data (no PK/FK, duplicates allowed)
-- Purpose: Ingest all data as-is from API, transformations happen in Silver layer (dbt)

CREATE SCHEMA IF NOT EXISTS hubeau;

CREATE TABLE IF NOT EXISTS hubeau.hydrobio_stations_raw (
    code_bassin TEXT,
    code_commune TEXT,
    code_cours_eau TEXT,
    code_departement TEXT,
    code_masse_eau TEXT,
    code_projection TEXT,
    code_region TEXT,
    code_sous_bassin TEXT,
    code_station_hydrobio TEXT,
    codes_appel_taxons TEXT,
    codes_indices TEXT,
    codes_reseaux TEXT,
    codes_supports TEXT,
    coordonnee_x DOUBLE PRECISION,
    coordonnee_y DOUBLE PRECISION,
    latitude DOUBLE PRECISION,
    libelle_bassin TEXT,
    libelle_commune TEXT,
    libelle_cours_eau TEXT,
    libelle_departement TEXT,
    libelle_masse_eau TEXT,
    libelle_region TEXT,
    libelle_sous_bassin TEXT,
    libelle_station_hydrobio TEXT,
    libelles_appel_taxons TEXT,
    libelles_indices TEXT,
    libelles_reseaux TEXT,
    libelles_supports TEXT,
    longitude DOUBLE PRECISION,
    uri_cours_eau TEXT,
    uri_masse_eau TEXT,
    uri_station_hydrobio TEXT,
    -- Audit columns for tracking
    _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for station-based queries
CREATE INDEX IF NOT EXISTS idx_hydrobio_stations_raw_code_station_hydrobio
ON hubeau.hydrobio_stations_raw(code_station_hydrobio);

COMMENT ON TABLE hubeau.hydrobio_stations_raw IS
'Bronze layer: Raw data from Hub''Eau API - duplicates allowed, no constraints, transformations in dbt';
