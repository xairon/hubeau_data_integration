-- Table: ecoulement_campagnes_raw (Bronze Layer)
-- Source: Hub'Eau API - RAW data (no PK/FK, duplicates allowed)
-- Purpose: Ingest all data as-is from API, transformations happen in Silver layer (dbt)

CREATE SCHEMA IF NOT EXISTS hubeau;

CREATE TABLE IF NOT EXISTS hubeau.ecoulement_campagnes_raw (
    code_campagne TEXT,
    code_departement TEXT,
    code_reseau TEXT,
    code_type_campagne TEXT,
    date_campagne TIMESTAMP,
    libelle_departement TEXT,
    libelle_reseau TEXT,
    libelle_type_campagne TEXT,
    nombre_modalite_ecoulement BIGINT,
    uri_reseau TEXT,
    -- Audit columns for tracking
    _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for incremental loading (MAX(date_campagne))
CREATE INDEX IF NOT EXISTS idx_ecoulement_campagnes_raw_date_campagne
ON hubeau.ecoulement_campagnes_raw(date_campagne);

-- Index for campaign-based queries
CREATE INDEX IF NOT EXISTS idx_ecoulement_campagnes_raw_code_campagne
ON hubeau.ecoulement_campagnes_raw(code_campagne);

COMMENT ON TABLE hubeau.ecoulement_campagnes_raw IS
'Bronze layer: Raw data from Hub''Eau API - duplicates allowed, no constraints, transformations in dbt';
