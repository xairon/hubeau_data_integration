-- Table: hydrometry_obs_elab_raw (Bronze Layer)
-- Source: Hub'Eau API - RAW data (no PK/FK, duplicates allowed)
-- Purpose: Ingest all data as-is from API, transformations happen in Silver layer (dbt)

CREATE SCHEMA IF NOT EXISTS hubeau;

CREATE TABLE IF NOT EXISTS hubeau.hydrometry_obs_elab_raw (
    code_methode TEXT,
    code_qualification TEXT,
    code_site TEXT,
    code_station BIGINT,
    code_statut TEXT,
    date_obs_elab TIMESTAMP,
    date_prod TIMESTAMP,
    grandeur_hydro_elab TEXT,
    latitude DOUBLE PRECISION,
    libelle_methode TEXT,
    libelle_qualification TEXT,
    libelle_statut TEXT,
    longitude DOUBLE PRECISION,
    resultat_obs_elab DOUBLE PRECISION,
    -- Audit columns for tracking
    _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for incremental loading (MAX(date_obs_elab))
CREATE INDEX IF NOT EXISTS idx_hydrometry_obs_elab_raw_date_obs_elab
ON hubeau.hydrometry_obs_elab_raw(date_obs_elab);

-- Index for site-based queries
CREATE INDEX IF NOT EXISTS idx_hydrometry_obs_elab_raw_code_site
ON hubeau.hydrometry_obs_elab_raw(code_site);

COMMENT ON TABLE hubeau.hydrometry_obs_elab_raw IS
'Bronze layer: Raw data from Hub''Eau API - duplicates allowed, no constraints, transformations in dbt';
