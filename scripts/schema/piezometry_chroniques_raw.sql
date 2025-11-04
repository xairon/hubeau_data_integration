-- Table: piezometry_chroniques_raw (Bronze Layer)
-- Source: Hub'Eau API - RAW data (no PK/FK, duplicates allowed)
-- Purpose: Ingest all data as-is from API, transformations happen in Silver layer (dbt)

CREATE SCHEMA IF NOT EXISTS hubeau;

CREATE TABLE IF NOT EXISTS hubeau.piezometry_chroniques_raw (
    code_bss TEXT,
    urn_bss TEXT,
    date_mesure TIMESTAMP,
    timestamp_mesure BIGINT,
    niveau_nappe_eau DOUBLE PRECISION,
    mode_obtention TEXT,
    statut TEXT,
    qualification TEXT,
    code_continuite TEXT,
    nom_continuite TEXT,
    code_producteur TEXT,
    nom_producteur TEXT,
    code_nature_mesure TEXT,
    nom_nature_mesure TEXT,
    profondeur_nappe DOUBLE PRECISION,
    -- Audit columns for tracking
    _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for incremental loading (MAX(date_mesure))
CREATE INDEX IF NOT EXISTS idx_piezometry_chroniques_raw_date_mesure
ON hubeau.piezometry_chroniques_raw(date_mesure);

-- Index for code_bss queries
CREATE INDEX IF NOT EXISTS idx_piezometry_chroniques_raw_code_bss
ON hubeau.piezometry_chroniques_raw(code_bss);

COMMENT ON TABLE hubeau.piezometry_chroniques_raw IS
'Bronze layer: Raw data from Hub''Eau API - duplicates allowed, no constraints, transformations in dbt';
