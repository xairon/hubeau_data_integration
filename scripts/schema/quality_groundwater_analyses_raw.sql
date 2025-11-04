-- Table: quality_groundwater_analyses_raw (Bronze Layer)
-- Source: Hub'Eau API - RAW data (no PK/FK, duplicates allowed)
-- Purpose: Ingest all data as-is from API, transformations happen in Silver layer (dbt)

CREATE SCHEMA IF NOT EXISTS hubeau;

CREATE TABLE IF NOT EXISTS hubeau.quality_groundwater_analyses_raw (
    altitude DOUBLE PRECISION,
    bss_id TEXT,
    code_bassin_dce TEXT,
    code_bss TEXT,
    code_circonscription_administrative_bassin TEXT,
    code_fraction TEXT,
    code_insee_actuel TEXT,
    code_lieu_analyse TEXT,
    code_methode TEXT,
    code_param BIGINT,
    code_producteur TEXT,
    code_qualification TEXT,
    code_region TEXT,
    code_remarque_analyse TEXT,
    code_statut_analyse TEXT,
    code_type_point_eau TEXT,
    code_type_qualito TEXT,
    code_unite TEXT,
    codes_entite_hg_bdlisa TEXT,
    codes_groupe_parametre TEXT,
    codes_masse_eau_edl TEXT,
    codes_masse_eau_rap TEXT,
    codes_reseau TEXT,
    date_debut_prelevement TIMESTAMP,
    incertitude_analytique DOUBLE PRECISION,
    latitude DOUBLE PRECISION,
    limite_detection DOUBLE PRECISION,
    limite_quantification DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    nom_bassin_dce TEXT,
    nom_circonscription_administrative_bassin TEXT,
    nom_commune_actuel TEXT,
    nom_departement TEXT,
    nom_fraction TEXT,
    nom_lieu_analyse TEXT,
    nom_methode TEXT,
    nom_param TEXT,
    nom_producteur TEXT,
    nom_qualification TEXT,
    nom_region TEXT,
    nom_remarque_analyse TEXT,
    nom_statut_analyse TEXT,
    nom_type_point_eau TEXT,
    nom_type_qualito TEXT,
    nom_unite TEXT,
    noms_entite_hg_bdlisa TEXT,
    noms_groupe_parametre TEXT,
    noms_masse_eau_edl TEXT,
    noms_masse_eau_rap TEXT,
    noms_reseau TEXT,
    num_departement TEXT,
    precision_coordonnees DOUBLE PRECISION,
    resultat DOUBLE PRECISION,
    seuil_saturation DOUBLE PRECISION,
    symbole_unite TEXT,
    uri_fraction TEXT,
    uri_lieu_analyse TEXT,
    uri_methode TEXT,
    uri_param TEXT,
    uri_producteur TEXT,
    uri_qualification TEXT,
    uri_remarque_analyse TEXT,
    uri_statut_analyse TEXT,
    uri_type_qualito TEXT,
    uri_unite TEXT,
    uris_groupe_parametre TEXT,
    uris_reseau TEXT,
    urn_bassin_dce TEXT,
    urn_bss TEXT,
    urns_bdlisa TEXT,
    urns_masse_eau_edl TEXT,
    urns_masse_eau_rap TEXT,
    -- Audit columns for tracking
    _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for incremental loading (MAX(date_debut_prelevement))
CREATE INDEX IF NOT EXISTS idx_quality_groundwater_analyses_raw_date_debut_prelevement
ON hubeau.quality_groundwater_analyses_raw(date_debut_prelevement);

-- Index for code_bss queries
CREATE INDEX IF NOT EXISTS idx_quality_groundwater_analyses_raw_code_bss
ON hubeau.quality_groundwater_analyses_raw(code_bss);

COMMENT ON TABLE hubeau.quality_groundwater_analyses_raw IS
'Bronze layer: Raw data from Hub''Eau API - duplicates allowed, no constraints, transformations in dbt';
