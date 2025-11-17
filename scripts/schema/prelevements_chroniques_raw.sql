-- Table: prelevements_chroniques_raw (Bronze Layer)
-- Source: Hub'Eau API - RAW data (no PK/FK, duplicates allowed)
-- Purpose: Ingest all data as-is from API, transformations happen in Silver layer (dbt)

CREATE SCHEMA IF NOT EXISTS hubeau;

CREATE TABLE IF NOT EXISTS hubeau.prelevements_chroniques_raw (
    annee BIGINT,
    code_commune_insee TEXT,
    code_departement TEXT,
    code_mode_obtention_volume TEXT,
    code_ouvrage TEXT,
    code_qualification_volume TEXT,
    code_statut_instruction TEXT,
    code_statut_volume TEXT,
    code_usage TEXT,
    latitude DOUBLE PRECISION,
    libelle_departement TEXT,
    libelle_mode_obtention_volume TEXT,
    libelle_qualification_volume TEXT,
    libelle_statut_instruction TEXT,
    libelle_statut_volume TEXT,
    libelle_usage TEXT,
    longitude DOUBLE PRECISION,
    nom_commune TEXT,
    nom_ouvrage TEXT,
    prelevement_ecrasant BOOLEAN,
    producteur_donnee TEXT,
    volume DOUBLE PRECISION,
    -- Audit columns for tracking
    _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for year-based queries
CREATE INDEX IF NOT EXISTS idx_prelevements_chroniques_raw_annee
ON hubeau.prelevements_chroniques_raw(annee);

-- Index for ouvrage-based queries
CREATE INDEX IF NOT EXISTS idx_prelevements_chroniques_raw_code_ouvrage
ON hubeau.prelevements_chroniques_raw(code_ouvrage);

COMMENT ON TABLE hubeau.prelevements_chroniques_raw IS
'Bronze layer: Raw data from Hub''Eau API - duplicates allowed, no constraints, transformations in dbt';
