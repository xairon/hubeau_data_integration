-- Table: quality_rivers_conditions_raw (Bronze Layer)
-- Source: Hub'Eau API - RAW data (no PK/FK, duplicates allowed)
-- Purpose: Ingest all data as-is from API, transformations happen in Silver layer (dbt)

CREATE SCHEMA IF NOT EXISTS hubeau;

CREATE TABLE IF NOT EXISTS hubeau.quality_rivers_conditions_raw (
    code_banque_reference TEXT,
    code_eu_masse_deau TEXT,
    code_groupe_parametre TEXT,
    code_masse_deau TEXT,
    code_methode TEXT,
    code_operation_cep TEXT,
    code_parametre BIGINT,
    code_point_eau_surface TEXT,
    code_prelevement TEXT,
    code_preleveur TEXT,
    code_producteur TEXT,
    code_qualification TEXT,
    code_remarque TEXT,
    code_station BIGINT,
    code_statut TEXT,
    code_unite TEXT,
    commentaire TEXT,
    date_maj TIMESTAMP,
    date_mesure TIMESTAMP,
    date_prelevement TIMESTAMP,
    heure_mesure TEXT,
    latitude DOUBLE PRECISION,
    libelle_groupe_parametre TEXT,
    libelle_parametre TEXT,
    libelle_qualification TEXT,
    libelle_resultat TEXT,
    libelle_station TEXT,
    longitude DOUBLE PRECISION,
    mnemo_remarque TEXT,
    mnemo_statut TEXT,
    nom_masse_deau TEXT,
    nom_methode TEXT,
    nom_preleveur TEXT,
    nom_producteur TEXT,
    resultat TEXT,
    symbole_unite TEXT,
    uri_groupe_parametre TEXT,
    uri_methode TEXT,
    uri_parametre TEXT,
    uri_preleveur TEXT,
    uri_producteur TEXT,
    uri_station TEXT,
    uri_unite TEXT,
    -- Audit columns for tracking
    _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for incremental loading (MAX(date_mesure))
CREATE INDEX IF NOT EXISTS idx_quality_rivers_conditions_raw_date_mesure
ON hubeau.quality_rivers_conditions_raw(date_mesure);

-- Index for station-based queries
CREATE INDEX IF NOT EXISTS idx_quality_rivers_conditions_raw_code_station
ON hubeau.quality_rivers_conditions_raw(code_station);

COMMENT ON TABLE hubeau.quality_rivers_conditions_raw IS
'Bronze layer: Raw data from Hub''Eau API - duplicates allowed, no constraints, transformations in dbt';
