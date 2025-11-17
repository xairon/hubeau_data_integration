-- Table: hydrobio_indices_raw (Bronze Layer)
-- Source: Hub'Eau API - RAW data (no PK/FK, duplicates allowed)
-- Purpose: Ingest all data as-is from API, transformations happen in Silver layer (dbt)

CREATE SCHEMA IF NOT EXISTS hubeau;

CREATE TABLE IF NOT EXISTS hubeau.hydrobio_indices_raw (
    code_banque_reference TEXT,
    code_bassin TEXT,
    code_commune TEXT,
    code_cours_eau TEXT,
    code_departement TEXT,
    code_indice TEXT,
    code_masse_eau TEXT,
    code_methode TEXT,
    code_operation_prelevement TEXT,
    code_prelevement TEXT,
    code_projection TEXT,
    code_qualification TEXT,
    code_region TEXT,
    code_sous_bassin TEXT,
    code_station_hydrobio TEXT,
    code_support TEXT,
    coordonnee_x DOUBLE PRECISION,
    coordonnee_y DOUBLE PRECISION,
    date_prelevement TIMESTAMP,
    latitude DOUBLE PRECISION,
    libelle_accreditation TEXT,
    libelle_bassin TEXT,
    libelle_commune TEXT,
    libelle_cours_eau TEXT,
    libelle_departement TEXT,
    libelle_indice TEXT,
    libelle_masse_eau TEXT,
    libelle_methode TEXT,
    libelle_qualification TEXT,
    libelle_region TEXT,
    libelle_sous_bassin TEXT,
    libelle_station_hydrobio TEXT,
    libelle_support TEXT,
    longitude DOUBLE PRECISION,
    resultat_indice DOUBLE PRECISION,
    unite_indice TEXT,
    uri_cours_eau TEXT,
    uri_masse_eau TEXT,
    uri_station_hydrobio TEXT,
    -- Audit columns for tracking
    _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for incremental loading (MAX(date_prelevement))
CREATE INDEX IF NOT EXISTS idx_hydrobio_indices_raw_date_prelevement
ON hubeau.hydrobio_indices_raw(date_prelevement);

-- Index for station-based queries
CREATE INDEX IF NOT EXISTS idx_hydrobio_indices_raw_code_station_hydrobio
ON hubeau.hydrobio_indices_raw(code_station_hydrobio);

COMMENT ON TABLE hubeau.hydrobio_indices_raw IS
'Bronze layer: Raw data from Hub''Eau API - duplicates allowed, no constraints, transformations in dbt';
