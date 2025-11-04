-- Table: quality_rivers_analyses_raw (Bronze Layer)
-- Source: Hub'Eau API - RAW data (no PK/FK, duplicates allowed)
-- Purpose: Ingest all data as-is from API, transformations happen in Silver layer (dbt)

CREATE SCHEMA IF NOT EXISTS hubeau;

CREATE TABLE IF NOT EXISTS hubeau.quality_rivers_analyses_raw (
    agrement TEXT,
    code_accreditation TEXT,
    code_analyse TEXT,
    code_banque_reference TEXT,
    code_difficulte_analyse TEXT,
    code_fraction TEXT,
    code_groupe_parametre TEXT,
    code_insitu TEXT,
    code_laboratoire TEXT,
    code_methode_analyse TEXT,
    code_methode_extraction TEXT,
    code_methode_fractionnement TEXT,
    code_operation TEXT,
    code_parametre BIGINT,
    code_point_eau_surface TEXT,
    code_prelevement TEXT,
    code_preleveur TEXT,
    code_producteur_analyse TEXT,
    code_qualification TEXT,
    code_remarque TEXT,
    code_reseau TEXT,
    code_station BIGINT,
    code_statut TEXT,
    code_support TEXT,
    code_unite TEXT,
    commentaires_analyse TEXT,
    commentaires_resultat_analyse TEXT,
    date_maj_analyse TIMESTAMP,
    date_prelevement TIMESTAMP,
    heure_analyse TEXT,
    heure_prelevement TEXT,
    incertitude_analytique DOUBLE PRECISION,
    latitude DOUBLE PRECISION,
    libelle_fraction TEXT,
    libelle_groupe_parametre TEXT,
    libelle_insitu TEXT,
    libelle_parametre TEXT,
    libelle_qualification TEXT,
    libelle_station TEXT,
    libelle_support TEXT,
    limite_detection DOUBLE PRECISION,
    limite_quantification DOUBLE PRECISION,
    limite_saturation DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    mnemo_accreditation TEXT,
    mnemo_difficulte_analyse TEXT,
    mnemo_remarque TEXT,
    mnemo_statut TEXT,
    nom_laboratoire TEXT,
    nom_methode_analyse TEXT,
    nom_methode_extraction TEXT,
    nom_methode_fractionnement TEXT,
    nom_preleveur TEXT,
    nom_producteur_analyse TEXT,
    nom_reseau TEXT,
    rendement_extraction DOUBLE PRECISION,
    resultat DOUBLE PRECISION,
    symbole_unite TEXT,
    uri_fraction TEXT,
    uri_groupe_parametre TEXT,
    uri_laboratoire TEXT,
    uri_methode_analyse TEXT,
    uri_methode_extraction TEXT,
    uri_methode_fractionnement TEXT,
    uri_parametre TEXT,
    uri_preleveur TEXT,
    uri_producteur_prelevement TEXT,
    uri_reseau TEXT,
    uri_station TEXT,
    uri_support TEXT,
    uri_unite TEXT,
    -- Audit columns for tracking
    _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for incremental loading (MAX(date_prelevement))
CREATE INDEX IF NOT EXISTS idx_quality_rivers_analyses_raw_date_prelevement
ON hubeau.quality_rivers_analyses_raw(date_prelevement);

-- Index for station-based queries
CREATE INDEX IF NOT EXISTS idx_quality_rivers_analyses_raw_code_station
ON hubeau.quality_rivers_analyses_raw(code_station);

COMMENT ON TABLE hubeau.quality_rivers_analyses_raw IS
'Bronze layer: Raw data from Hub''Eau API - duplicates allowed, no constraints, transformations in dbt';
