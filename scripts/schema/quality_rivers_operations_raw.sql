-- Table: quality_rivers_operations_raw (Bronze Layer)
-- Source: Hub'Eau API - RAW data (no PK/FK, duplicates allowed)
-- Purpose: Ingest all data as-is from API, transformations happen in Silver layer (dbt)

CREATE SCHEMA IF NOT EXISTS hubeau;

CREATE TABLE IF NOT EXISTS hubeau.quality_rivers_operations_raw (
    agrement TEXT,
    code_accreditation TEXT,
    code_banque_reference TEXT,
    code_difficulte TEXT,
    code_finalite TEXT,
    code_methode TEXT,
    code_operation TEXT,
    code_point_eau_surface TEXT,
    code_prelevement TEXT,
    code_preleveur TEXT,
    code_producteur TEXT,
    code_projection TEXT,
    code_reseau TEXT,
    code_station BIGINT,
    code_support TEXT,
    code_zone_verticale_prospectee TEXT,
    commentaires TEXT,
    date_fin TEXT,
    date_prelevement TIMESTAMP,
    heure_fin DOUBLE PRECISION,
    heure_prelevement TIMESTAMP,
    latitude DOUBLE PRECISION,
    libelle_finalite TEXT,
    libelle_projection TEXT,
    libelle_station TEXT,
    libelle_support TEXT,
    longitude DOUBLE PRECISION,
    mnemo_accreditation TEXT,
    mnemo_difficulte TEXT,
    mnemo_zone_verticale_prospectee TEXT,
    nom_methode TEXT,
    nom_preleveur TEXT,
    nom_producteur TEXT,
    nom_reseau TEXT,
    profondeur DOUBLE PRECISION,
    uri_methode TEXT,
    uri_preleveur TEXT,
    uri_producteur TEXT,
    uri_reseau TEXT,
    uri_station TEXT,
    uri_support TEXT,
    x_prelevement DOUBLE PRECISION,
    y_prelevement DOUBLE PRECISION,
    -- Audit columns for tracking
    _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for incremental loading (MAX(date_prelevement))
CREATE INDEX IF NOT EXISTS idx_quality_rivers_operations_raw_date_prelevement
ON hubeau.quality_rivers_operations_raw(date_prelevement);

-- Index for station-based queries
CREATE INDEX IF NOT EXISTS idx_quality_rivers_operations_raw_code_station
ON hubeau.quality_rivers_operations_raw(code_station);

COMMENT ON TABLE hubeau.quality_rivers_operations_raw IS
'Bronze layer: Raw data from Hub''Eau API - duplicates allowed, no constraints, transformations in dbt';
